use std::collections::HashMap;

use bytes::Bytes;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::expiration::Expiration;
use crate::protocol::StoreCommand;
use crate::value::{CasValue, Value};

const REQUEST_MAGIC: u8 = 0x80;
const RESPONSE_MAGIC: u8 = 0x81;
const HEADER_LEN: usize = 24;

/// Bodies at least this large are read into a dedicated allocation instead
/// of the connection's shared read buffer (4 KiB), so a live `Value` never
/// pins the connection buffer and big reads are single-shot.
const OWNED_BODY_THRESHOLD: usize = 4096;

/// Binary protocol opcodes.
pub(crate) mod opcode {
    pub const GET: u8 = 0x00;
    pub const SET: u8 = 0x01;
    pub const ADD: u8 = 0x02;
    pub const REPLACE: u8 = 0x03;
    pub const DELETE: u8 = 0x04;
    pub const INCREMENT: u8 = 0x05;
    pub const DECREMENT: u8 = 0x06;
    pub const FLUSH: u8 = 0x08;
    pub const NOOP: u8 = 0x0A;
    pub const VERSION: u8 = 0x0B;
    pub const GETKQ: u8 = 0x0D;
    pub const APPEND: u8 = 0x0E;
    pub const PREPEND: u8 = 0x0F;
    pub const TOUCH: u8 = 0x1C;
}

mod status {
    pub const OK: u16 = 0x0000;
    pub const KEY_NOT_FOUND: u16 = 0x0001;
    pub const KEY_EXISTS: u16 = 0x0002;
    pub const VALUE_TOO_LARGE: u16 = 0x0003;
    pub const INVALID_ARGUMENTS: u16 = 0x0004;
    pub const NOT_STORED: u16 = 0x0005;
    pub const NON_NUMERIC: u16 = 0x0006;
    pub const UNKNOWN_COMMAND: u16 = 0x0081;
    pub const OUT_OF_MEMORY: u16 = 0x0082;
}

/// A parsed binary response frame.
struct Response {
    opcode: u8,
    status: u16,
    opaque: u32,
    cas: u64,
    extras: Bytes,
    value: Bytes,
}

impl Response {
    /// The flags stored in the first 4 bytes of the extras (get responses).
    fn flags(&self) -> u32 {
        if self.extras.len() >= 4 {
            u32::from_be_bytes(self.extras[..4].try_into().unwrap())
        } else {
            0
        }
    }

    /// Turn a non-OK status into an [`Error`], using the value body as message.
    fn status_error(&self) -> Error {
        let message = String::from_utf8_lossy(&self.value).into_owned();
        match self.status {
            status::UNKNOWN_COMMAND => Error::Client(format!("unknown command: {message}")),
            status::INVALID_ARGUMENTS => Error::Client(format!("invalid arguments: {message}")),
            status::NON_NUMERIC => Error::Client(format!("non-numeric value: {message}")),
            status::VALUE_TOO_LARGE => Error::Server(format!("value too large: {message}")),
            status::OUT_OF_MEMORY => Error::Server(format!("out of memory: {message}")),
            other => Error::Server(format!("status 0x{other:04x}: {message}")),
        }
    }

    /// Ensure this response answers the request identified by `opaque`.
    ///
    /// A mismatch means the connection is out of sync — typically a leftover
    /// frame from a request that timed out earlier — and must be dropped:
    /// every subsequent response would otherwise be shifted by one frame.
    fn check_opaque(&self, opaque: u32) -> Result<()> {
        if self.opaque == opaque {
            Ok(())
        } else {
            Err(Error::Desynced(format!(
                "response opaque {} does not match request opaque {opaque}",
                self.opaque
            )))
        }
    }
}

/// Assemble a request frame into `buf` (appended; caller clears first).
#[allow(clippy::too_many_arguments)]
fn build_request_into(
    buf: &mut Vec<u8>,
    opcode: u8,
    key: &[u8],
    extras: &[u8],
    value: &[u8],
    cas: u64,
    opaque: u32,
) {
    let body_len = extras.len() + key.len() + value.len();
    buf.reserve(HEADER_LEN + body_len);
    buf.push(REQUEST_MAGIC);
    buf.push(opcode);
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.push(extras.len() as u8);
    buf.push(0); // data type
    buf.extend_from_slice(&[0, 0]); // reserved / vbucket
    buf.extend_from_slice(&(body_len as u32).to_be_bytes());
    buf.extend_from_slice(&opaque.to_be_bytes());
    buf.extend_from_slice(&cas.to_be_bytes());
    buf.extend_from_slice(extras);
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
}

/// Assemble a request frame into the connection's reusable scratch buffer
/// and send it. The scratch buffer is returned to the connection afterwards,
/// so the next request on this connection does not allocate.
#[allow(clippy::too_many_arguments)]
async fn send_request(
    conn: &mut Connection,
    opcode: u8,
    key: &[u8],
    extras: &[u8],
    value: &[u8],
    cas: u64,
    opaque: u32,
) -> Result<()> {
    let mut buf = std::mem::take(&mut conn.write_buf);
    buf.clear();
    build_request_into(&mut buf, opcode, key, extras, value, cas, opaque);
    let result = conn.send(&buf).await;
    conn.write_buf = buf;
    result
}

/// Read and parse a single response frame.
async fn read_response(conn: &mut Connection) -> Result<Response> {
    let header = conn.read_exact(HEADER_LEN).await?;
    if header[0] != RESPONSE_MAGIC {
        return Err(Error::Protocol(format!(
            "invalid binary response magic 0x{:02x}",
            header[0]
        )));
    }
    let opcode = header[1];
    let key_len = u16::from_be_bytes([header[2], header[3]]) as usize;
    let extras_len = header[4] as usize;
    let status = u16::from_be_bytes([header[6], header[7]]);
    let body_len = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
    let opaque = u32::from_be_bytes(header[12..16].try_into().unwrap());
    let cas = u64::from_be_bytes(header[16..24].try_into().unwrap());

    if extras_len + key_len > body_len {
        return Err(Error::Protocol(
            "binary response extras/key exceed body length".into(),
        ));
    }
    // Large bodies get their own right-sized allocation (see read_owned);
    // small frames stay on the shared read buffer.
    let body = if body_len >= OWNED_BODY_THRESHOLD {
        conn.read_owned(body_len).await?
    } else {
        conn.read_exact(body_len).await?
    };
    let extras = body.slice(0..extras_len);
    // The key is parsed out of the body but not needed: GETKQ responses are
    // correlated to their requests by opaque token.
    let value = body.slice(extras_len + key_len..body_len);
    Ok(Response {
        opcode,
        status,
        opaque,
        cas,
        extras,
        value,
    })
}

fn storage_extras(flags: u32, expire: Expiration) -> [u8; 8] {
    let mut extras = [0u8; 8];
    extras[0..4].copy_from_slice(&flags.to_be_bytes());
    extras[4..8].copy_from_slice(&expire.to_wire().to_be_bytes());
    extras
}

pub(crate) async fn store(
    conn: &mut Connection,
    command: StoreCommand,
    key: &str,
    value: &Value,
    expire: Expiration,
    cas: Option<u64>,
) -> Result<bool> {
    let extras_buf;
    let extras: &[u8] = if command.has_storage_extras() {
        extras_buf = storage_extras(value.flags(), expire);
        &extras_buf
    } else {
        &[]
    };
    let opaque = conn.next_opaque();
    send_request(
        conn,
        command.binary_opcode(),
        key.as_bytes(),
        extras,
        value.as_bytes(),
        cas.unwrap_or(0),
        opaque,
    )
    .await?;

    let response = read_response(conn).await?;
    response.check_opaque(opaque)?;
    match response.status {
        status::OK => Ok(true),
        status::KEY_EXISTS | status::NOT_STORED | status::KEY_NOT_FOUND => Ok(false),
        _ => Err(response.status_error()),
    }
}

pub(crate) async fn get(conn: &mut Connection, key: &str) -> Result<Option<Value>> {
    let opaque = conn.next_opaque();
    send_request(conn, opcode::GET, key.as_bytes(), &[], &[], 0, opaque).await?;

    let response = read_response(conn).await?;
    response.check_opaque(opaque)?;
    match response.status {
        status::OK => Ok(Some(Value::new(response.value.clone(), response.flags()))),
        status::KEY_NOT_FOUND => Ok(None),
        _ => Err(response.status_error()),
    }
}

pub(crate) async fn get_cas(conn: &mut Connection, key: &str) -> Result<Option<CasValue>> {
    let opaque = conn.next_opaque();
    send_request(conn, opcode::GET, key.as_bytes(), &[], &[], 0, opaque).await?;

    let response = read_response(conn).await?;
    response.check_opaque(opaque)?;
    match response.status {
        status::OK => {
            let value = Value::new(response.value.clone(), response.flags());
            Ok(Some(CasValue::new(value, response.cas)))
        }
        status::KEY_NOT_FOUND => Ok(None),
        _ => Err(response.status_error()),
    }
}

pub(crate) async fn get_multi(
    conn: &mut Connection,
    keys: &[&str],
) -> Result<HashMap<String, Value>> {
    // Pipeline: one quiet GETKQ per key, then a NOOP fence that terminates the
    // response stream. Misses produce no reply; the NOOP is the end marker.
    // Hit responses echo their request's opaque so each value can be matched
    // to its key; any unexpected frame means the connection is out of sync.
    let base_opaque = conn.next_opaque();
    let mut request = std::mem::take(&mut conn.write_buf);
    request.clear();
    for (index, key) in keys.iter().enumerate() {
        build_request_into(
            &mut request,
            opcode::GETKQ,
            key.as_bytes(),
            &[],
            &[],
            0,
            base_opaque + index as u32,
        );
    }
    let noop_opaque = base_opaque + keys.len() as u32;
    build_request_into(&mut request, opcode::NOOP, &[], &[], &[], 0, noop_opaque);
    let send_result = conn.send(&request).await;
    conn.write_buf = request;
    send_result?;

    let mut map = HashMap::with_capacity(keys.len());
    loop {
        let response = read_response(conn).await?;
        if response.opcode == opcode::NOOP && response.opaque == noop_opaque {
            return Ok(map);
        }
        let index = response.opaque.wrapping_sub(base_opaque) as usize;
        if response.opcode != opcode::GETKQ || index >= keys.len() {
            return Err(Error::Desynced(format!(
                "unexpected get_multi response frame (opcode 0x{:02x}, opaque {})",
                response.opcode, response.opaque
            )));
        }
        match response.status {
            status::OK => {
                map.insert(
                    keys[index].to_string(),
                    Value::new(response.value.clone(), response.flags()),
                );
            }
            status::KEY_NOT_FOUND => {}
            _ => return Err(response.status_error()),
        }
    }
}

pub(crate) async fn delete(conn: &mut Connection, key: &str) -> Result<bool> {
    let opaque = conn.next_opaque();
    send_request(conn, opcode::DELETE, key.as_bytes(), &[], &[], 0, opaque).await?;

    let response = read_response(conn).await?;
    response.check_opaque(opaque)?;
    match response.status {
        status::OK => Ok(true),
        status::KEY_NOT_FOUND => Ok(false),
        _ => Err(response.status_error()),
    }
}

pub(crate) async fn incr_decr(
    conn: &mut Connection,
    incr: bool,
    key: &str,
    delta: u64,
) -> Result<Option<u64>> {
    // extras: delta(8) + initial(8) + expiry(4). Expiry 0xffffffff means the
    // key is *not* created when missing, matching the text protocol's NOT_FOUND.
    let mut extras = [0u8; 20];
    extras[0..8].copy_from_slice(&delta.to_be_bytes());
    extras[16..20].copy_from_slice(&0xffff_ffffu32.to_be_bytes());
    let op = if incr {
        opcode::INCREMENT
    } else {
        opcode::DECREMENT
    };
    let opaque = conn.next_opaque();
    send_request(conn, op, key.as_bytes(), &extras, &[], 0, opaque).await?;

    let response = read_response(conn).await?;
    response.check_opaque(opaque)?;
    match response.status {
        status::OK => {
            if response.value.len() != 8 {
                return Err(Error::Protocol(
                    "incr/decr response value is not 8 bytes".into(),
                ));
            }
            Ok(Some(u64::from_be_bytes(
                response.value[..8].try_into().unwrap(),
            )))
        }
        status::KEY_NOT_FOUND => Ok(None),
        _ => Err(response.status_error()),
    }
}

pub(crate) async fn touch(conn: &mut Connection, key: &str, expire: Expiration) -> Result<bool> {
    let extras = expire.to_wire().to_be_bytes();
    let opaque = conn.next_opaque();
    send_request(conn, opcode::TOUCH, key.as_bytes(), &extras, &[], 0, opaque).await?;

    let response = read_response(conn).await?;
    response.check_opaque(opaque)?;
    match response.status {
        status::OK => Ok(true),
        status::KEY_NOT_FOUND => Ok(false),
        _ => Err(response.status_error()),
    }
}

pub(crate) async fn flush_all(conn: &mut Connection) -> Result<()> {
    let opaque = conn.next_opaque();
    send_request(conn, opcode::FLUSH, &[], &[], &[], 0, opaque).await?;

    let response = read_response(conn).await?;
    response.check_opaque(opaque)?;
    if response.status == status::OK {
        Ok(())
    } else {
        Err(response.status_error())
    }
}

pub(crate) async fn version(conn: &mut Connection) -> Result<String> {
    let opaque = conn.next_opaque();
    send_request(conn, opcode::VERSION, &[], &[], &[], 0, opaque).await?;

    let response = read_response(conn).await?;
    response.check_opaque(opaque)?;
    if response.status == status::OK {
        Ok(String::from_utf8_lossy(&response.value).into_owned())
    } else {
        Err(response.status_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_request_header_layout() {
        let mut request = Vec::new();
        build_request_into(&mut request, opcode::SET, b"key", &[0u8; 8], b"val", 7, 42);
        assert_eq!(request[0], REQUEST_MAGIC);
        assert_eq!(request[1], opcode::SET);
        assert_eq!(u16::from_be_bytes([request[2], request[3]]), 3); // key len
        assert_eq!(request[4], 8); // extras len
        // total body = 8 extras + 3 key + 3 value = 14
        assert_eq!(
            u32::from_be_bytes([request[8], request[9], request[10], request[11]]),
            14
        );
        assert_eq!(
            u32::from_be_bytes([request[12], request[13], request[14], request[15]]),
            42
        );
        assert_eq!(u64::from_be_bytes(request[16..24].try_into().unwrap()), 7);
    }

    #[test]
    fn storage_extras_encode_flags_and_expiry() {
        let extras = storage_extras(32, Expiration::Seconds(60));
        assert_eq!(u32::from_be_bytes(extras[0..4].try_into().unwrap()), 32);
        assert_eq!(u32::from_be_bytes(extras[4..8].try_into().unwrap()), 60);
    }
}
