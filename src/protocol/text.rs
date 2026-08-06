use std::collections::HashMap;
use std::io::Write;

use bytes::Bytes;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::expiration::Expiration;
use crate::protocol::StoreCommand;
use crate::value::{CasValue, Value};

/// One parsed `VALUE` entry: key, value, and (for `gets`) the CAS token.
struct Entry {
    key: String,
    value: Value,
    cas: u64,
}

pub(crate) async fn store(
    conn: &mut Connection,
    command: StoreCommand,
    key: &str,
    value: &Value,
    expire: Expiration,
    noreply: bool,
) -> Result<bool> {
    let mut buf = Vec::with_capacity(value.as_bytes().len() + 48);
    write!(
        buf,
        "{} {} {} {} {}",
        command.text_verb(),
        key,
        value.flags(),
        expire.to_wire(),
        value.as_bytes().len()
    )
    .expect("write to Vec is infallible");
    if noreply {
        buf.extend_from_slice(b" noreply");
    }
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(value.as_bytes());
    buf.extend_from_slice(b"\r\n");
    conn.send(&buf).await?;

    if noreply {
        return Ok(true);
    }
    parse_store_reply(conn).await
}

pub(crate) async fn cas(
    conn: &mut Connection,
    key: &str,
    value: &Value,
    expire: Expiration,
    cas: u64,
    noreply: bool,
) -> Result<bool> {
    let mut buf = Vec::with_capacity(value.as_bytes().len() + 64);
    write!(
        buf,
        "cas {} {} {} {} {}",
        key,
        value.flags(),
        expire.to_wire(),
        value.as_bytes().len(),
        cas
    )
    .expect("write to Vec is infallible");
    if noreply {
        buf.extend_from_slice(b" noreply");
    }
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(value.as_bytes());
    buf.extend_from_slice(b"\r\n");
    conn.send(&buf).await?;

    if noreply {
        return Ok(true);
    }
    parse_store_reply(conn).await
}

async fn parse_store_reply(conn: &mut Connection) -> Result<bool> {
    let line = conn.read_line().await?;
    let line = as_str(&line)?;
    match line {
        "STORED" => Ok(true),
        "NOT_STORED" | "EXISTS" | "NOT_FOUND" => Ok(false),
        other => Err(classify_error(other)
            .unwrap_or_else(|| Error::Protocol(format!("unexpected store reply: {other}")))),
    }
}

pub(crate) async fn get(conn: &mut Connection, key: &str) -> Result<Option<Value>> {
    conn.send(format!("get {key}\r\n").as_bytes()).await?;
    let mut entries = read_values(conn).await?;
    Ok(entries.pop().map(|entry| entry.value))
}

pub(crate) async fn get_cas(conn: &mut Connection, key: &str) -> Result<Option<CasValue>> {
    conn.send(format!("gets {key}\r\n").as_bytes()).await?;
    let mut entries = read_values(conn).await?;
    Ok(entries
        .pop()
        .map(|entry| CasValue::new(entry.value, entry.cas)))
}

pub(crate) async fn get_multi(
    conn: &mut Connection,
    keys: &[&str],
) -> Result<HashMap<String, Value>> {
    let mut request = String::from("get");
    for key in keys {
        request.push(' ');
        request.push_str(key);
    }
    request.push_str("\r\n");
    conn.send(request.as_bytes()).await?;

    let entries = read_values(conn).await?;
    let mut map = HashMap::with_capacity(entries.len());
    for entry in entries {
        map.insert(entry.key, entry.value);
    }
    Ok(map)
}

/// Read a `VALUE ... END` block (single or multi get, with or without CAS).
async fn read_values(conn: &mut Connection) -> Result<Vec<Entry>> {
    let mut entries = Vec::new();
    loop {
        let line = conn.read_line().await?;
        let line = as_str(&line)?;
        if line == "END" {
            return Ok(entries);
        }
        if let Some(rest) = line.strip_prefix("VALUE ") {
            let tokens: Vec<&str> = rest.split_ascii_whitespace().collect();
            if tokens.len() < 3 {
                return Err(Error::Protocol(format!("malformed VALUE line: {line}")));
            }
            let key = tokens[0].to_string();
            let flags = tokens[1]
                .parse::<u32>()
                .map_err(|_| Error::Protocol(format!("invalid VALUE flags: {line}")))?;
            let len = tokens[2]
                .parse::<usize>()
                .map_err(|_| Error::Protocol(format!("invalid VALUE length: {line}")))?;
            let cas = match tokens.get(3) {
                Some(token) => token
                    .parse::<u64>()
                    .map_err(|_| Error::Protocol(format!("invalid VALUE cas: {line}")))?,
                None => 0,
            };
            let data = conn.read_exact(len).await?;
            let trailer = conn.read_exact(2).await?;
            if trailer.as_ref() != b"\r\n" {
                return Err(Error::Protocol("VALUE body not terminated by CRLF".into()));
            }
            entries.push(Entry {
                key,
                value: Value::new(data, flags),
                cas,
            });
        } else if let Some(err) = classify_error(line) {
            return Err(err);
        } else {
            return Err(Error::Protocol(format!("unexpected get reply: {line}")));
        }
    }
}

pub(crate) async fn delete(conn: &mut Connection, key: &str, noreply: bool) -> Result<bool> {
    let mut request = format!("delete {key}");
    if noreply {
        request.push_str(" noreply");
    }
    request.push_str("\r\n");
    conn.send(request.as_bytes()).await?;

    if noreply {
        return Ok(true);
    }
    let line = conn.read_line().await?;
    let line = as_str(&line)?;
    match line {
        "DELETED" => Ok(true),
        "NOT_FOUND" => Ok(false),
        other => Err(classify_error(other)
            .unwrap_or_else(|| Error::Protocol(format!("unexpected delete reply: {other}")))),
    }
}

pub(crate) async fn incr_decr(
    conn: &mut Connection,
    incr: bool,
    key: &str,
    delta: u64,
    noreply: bool,
) -> Result<Option<u64>> {
    let verb = if incr { "incr" } else { "decr" };
    let mut request = format!("{verb} {key} {delta}");
    if noreply {
        request.push_str(" noreply");
    }
    request.push_str("\r\n");
    conn.send(request.as_bytes()).await?;

    if noreply {
        return Ok(None);
    }
    let line = conn.read_line().await?;
    let line = as_str(&line)?;
    if line == "NOT_FOUND" {
        return Ok(None);
    }
    if let Some(err) = classify_error(line) {
        return Err(err);
    }
    line.parse::<u64>()
        .map(Some)
        .map_err(|_| Error::Protocol(format!("unexpected incr/decr reply: {line}")))
}

pub(crate) async fn touch(conn: &mut Connection, key: &str, expire: Expiration) -> Result<bool> {
    conn.send(format!("touch {key} {}\r\n", expire.to_wire()).as_bytes())
        .await?;
    let line = conn.read_line().await?;
    let line = as_str(&line)?;
    match line {
        "TOUCHED" => Ok(true),
        "NOT_FOUND" => Ok(false),
        other => Err(classify_error(other)
            .unwrap_or_else(|| Error::Protocol(format!("unexpected touch reply: {other}")))),
    }
}

pub(crate) async fn flush_all(conn: &mut Connection) -> Result<()> {
    conn.send(b"flush_all\r\n").await?;
    let line = conn.read_line().await?;
    let line = as_str(&line)?;
    if line == "OK" {
        Ok(())
    } else {
        Err(classify_error(line)
            .unwrap_or_else(|| Error::Protocol(format!("unexpected flush_all reply: {line}"))))
    }
}

pub(crate) async fn version(conn: &mut Connection) -> Result<String> {
    conn.send(b"version\r\n").await?;
    let line = conn.read_line().await?;
    let line = as_str(&line)?;
    match line.strip_prefix("VERSION ") {
        Some(version) => Ok(version.to_string()),
        None => Err(classify_error(line)
            .unwrap_or_else(|| Error::Protocol(format!("unexpected version reply: {line}")))),
    }
}

fn as_str(bytes: &Bytes) -> Result<&str> {
    std::str::from_utf8(bytes).map_err(|_| Error::Protocol("response line is not UTF-8".into()))
}

/// Map a memcached error line to an [`Error`]; returns `None` for non-errors.
fn classify_error(line: &str) -> Option<Error> {
    if line == "ERROR" {
        return Some(Error::Client("unknown command".into()));
    }
    if let Some(message) = line.strip_prefix("CLIENT_ERROR ") {
        return Some(Error::Client(message.to_string()));
    }
    if let Some(message) = line.strip_prefix("SERVER_ERROR ") {
        return Some(Error::Server(message.to_string()));
    }
    None
}
