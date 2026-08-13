use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::config::{Config, Endpoint, Protocol};
use crate::error::{Error, Result};
use crate::expiration::Expiration;
use crate::protocol::{StoreCommand, binary, text};
use crate::value::{CasValue, Value};

/// A single pooled connection to the memcached endpoint.
///
/// Owns a reusable read buffer and knows which wire protocol to speak. The
/// protocol codecs in [`crate::protocol`] drive it through the buffered
/// primitives ([`Connection::send`], [`Connection::read_line`],
/// [`Connection::read_exact`]).
pub(crate) struct Connection {
    stream: TcpStream,
    protocol: Protocol,
    read_buf: BytesMut,
    /// Opaque counter for binary-protocol requests, incremented per request so
    /// a response can be matched to its request. Starts at 1 so the "no
    /// correlation" value 0 stays distinguishable.
    next_opaque: u32,
    /// Scratch buffer for assembling request frames, reused across operations
    /// on this connection so the hot path does not allocate a fresh `Vec`
    /// per request. Empty whenever the connection is idle.
    pub(crate) write_buf: Vec<u8>,
}

impl Connection {
    /// Establish a new connection according to `config`, dialing `endpoint`
    /// (the currently discovered mesh endpoint, which may differ from the one
    /// in `config` after a rediscovery).
    pub(crate) async fn connect(config: &Config, endpoint: &Endpoint) -> Result<Self> {
        let connect = TcpStream::connect((endpoint.host.as_str(), endpoint.port));
        let stream = timeout(config.connect_timeout, connect)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::Connect)?;
        if config.tcp_nodelay {
            let _ = stream.set_nodelay(true);
        }
        if config.tcp_keepalive {
            apply_keepalive(&stream, config);
        }
        Ok(Connection {
            stream,
            protocol: config.protocol,
            read_buf: BytesMut::with_capacity(4096),
            next_opaque: 1,
            write_buf: Vec::with_capacity(1024),
        })
    }

    /// Allocate the opaque token for the next binary-protocol request.
    pub(crate) fn next_opaque(&mut self) -> u32 {
        let opaque = self.next_opaque;
        // Wrap around without ever handing out 0.
        self.next_opaque = self.next_opaque.wrapping_add(1).max(1);
        opaque
    }

    // --- buffered IO primitives (used by protocol codecs) ---

    /// Write the full request and flush it to the socket.
    pub(crate) async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.stream.write_all(data).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn fill(&mut self) -> Result<usize> {
        self.read_buf.reserve(1024);
        let read = self.stream.read_buf(&mut self.read_buf).await?;
        Ok(read)
    }

    async fn ensure(&mut self, n: usize) -> Result<()> {
        while self.read_buf.len() < n {
            if self.fill().await? == 0 {
                return Err(Error::Protocol(
                    "connection closed with an incomplete frame".into(),
                ));
            }
        }
        Ok(())
    }

    /// Read exactly `n` bytes.
    pub(crate) async fn read_exact(&mut self, n: usize) -> Result<Bytes> {
        self.ensure(n).await?;
        Ok(self.read_buf.split_to(n).freeze())
    }

    /// Read exactly `n` bytes into a **dedicated** buffer (returned as the
    /// value's own storage). Unlike [`Connection::read_exact`], which splits
    /// from the shared read buffer, this gives large bodies their own
    /// right-sized allocation: the connection's read buffer stays small for
    /// headers, and a live `Value` never pins a large shared buffer.
    pub(crate) async fn read_owned(&mut self, n: usize) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(n);
        // Move any bytes already buffered (usually none beyond the header).
        let take = self.read_buf.len().min(n);
        buf.extend_from_slice(&self.read_buf.split_to(take));
        while buf.len() < n {
            if self.stream.read_buf(&mut buf).await? == 0 {
                return Err(Error::Protocol(
                    "connection closed with an incomplete frame".into(),
                ));
            }
        }
        Ok(buf.freeze())
    }

    /// Read a single CRLF-terminated line, returning it *without* the CRLF.
    pub(crate) async fn read_line(&mut self) -> Result<Bytes> {
        let mut searched = 0;
        loop {
            if let Some(offset) = find_crlf(&self.read_buf[searched..]) {
                let end = searched + offset;
                let mut line = self.read_buf.split_to(end + 2);
                line.truncate(end);
                return Ok(line.freeze());
            }
            searched = self.read_buf.len().saturating_sub(1);
            if self.fill().await? == 0 {
                return Err(Error::Protocol(
                    "connection closed before end of line".into(),
                ));
            }
        }
    }

    // --- protocol dispatch ---

    pub(crate) async fn get(&mut self, key: &str) -> Result<Option<Value>> {
        match self.protocol {
            Protocol::Text => text::get(self, key).await,
            Protocol::Binary => binary::get(self, key).await,
        }
    }

    pub(crate) async fn get_cas(&mut self, key: &str) -> Result<Option<CasValue>> {
        match self.protocol {
            Protocol::Text => text::get_cas(self, key).await,
            Protocol::Binary => binary::get_cas(self, key).await,
        }
    }

    pub(crate) async fn get_multi(&mut self, keys: &[&str]) -> Result<HashMap<String, Value>> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        match self.protocol {
            Protocol::Text => text::get_multi(self, keys).await,
            Protocol::Binary => binary::get_multi(self, keys).await,
        }
    }

    pub(crate) async fn store(
        &mut self,
        command: StoreCommand,
        key: &str,
        value: &Value,
        expire: Expiration,
    ) -> Result<bool> {
        match self.protocol {
            Protocol::Text => text::store(self, command, key, value, expire).await,
            Protocol::Binary => binary::store(self, command, key, value, expire, None).await,
        }
    }

    pub(crate) async fn cas(
        &mut self,
        key: &str,
        value: &Value,
        expire: Expiration,
        cas: u64,
    ) -> Result<bool> {
        match self.protocol {
            Protocol::Text => text::cas(self, key, value, expire, cas).await,
            Protocol::Binary => {
                binary::store(self, StoreCommand::Set, key, value, expire, Some(cas)).await
            }
        }
    }

    pub(crate) async fn delete(&mut self, key: &str) -> Result<bool> {
        match self.protocol {
            Protocol::Text => text::delete(self, key).await,
            Protocol::Binary => binary::delete(self, key).await,
        }
    }

    pub(crate) async fn incr_decr(
        &mut self,
        incr: bool,
        key: &str,
        delta: u64,
    ) -> Result<Option<u64>> {
        match self.protocol {
            Protocol::Text => text::incr_decr(self, incr, key, delta).await,
            Protocol::Binary => binary::incr_decr(self, incr, key, delta).await,
        }
    }

    pub(crate) async fn touch(&mut self, key: &str, expire: Expiration) -> Result<bool> {
        match self.protocol {
            Protocol::Text => text::touch(self, key, expire).await,
            Protocol::Binary => binary::touch(self, key, expire).await,
        }
    }

    pub(crate) async fn flush_all(&mut self) -> Result<()> {
        match self.protocol {
            Protocol::Text => text::flush_all(self).await,
            Protocol::Binary => binary::flush_all(self).await,
        }
    }

    pub(crate) async fn version(&mut self) -> Result<String> {
        match self.protocol {
            Protocol::Text => text::version(self).await,
            Protocol::Binary => binary::version(self).await,
        }
    }
}

/// Configure TCP keepalive on a connected stream via socket2, which (unlike
/// tokio's API) can set the idle interval. Kept short because the mesh is
/// local: a half-open connection is reaped after roughly
/// `keepalive_interval` plus a few probe rounds instead of only surfacing
/// when a request times out on it.
#[cfg(unix)]
fn apply_keepalive(stream: &TcpStream, config: &Config) {
    use socket2::{SockRef, TcpKeepalive};
    let keepalive = TcpKeepalive::new()
        .with_time(config.keepalive_interval)
        .with_interval(config.keepalive_interval);
    let socket = SockRef::from(stream);
    if let Err(err) = socket.set_tcp_keepalive(&keepalive) {
        tracing::warn!(error = %err, "mc mesh: failed to set TCP keepalive");
    }
}

#[cfg(not(unix))]
fn apply_keepalive(stream: &TcpStream, _: &Config) {
    if let Err(err) = stream.set_keepalive(true) {
        tracing::warn!(error = %err, "mc mesh: failed to set TCP keepalive");
    }
}

fn find_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|window| window == b"\r\n")
}
