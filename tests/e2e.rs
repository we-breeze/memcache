//! End-to-end tests that drive the real client against an in-process fake
//! memcached server. The fake server implements enough of each wire protocol
//! (text and binary) to validate encoding and decoding round-trips without a
//! real memcached instance.

use std::collections::HashMap;
use std::sync::Arc;

use memcache::{CasValue, Client, Config, Protocol};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

#[derive(Clone)]
struct Item {
    data: Vec<u8>,
    flags: u32,
    cas: u64,
}

type Store = Arc<Mutex<HashMap<String, Item>>>;

/// Spawn a fake server speaking `protocol`; returns the bound port.
async fn spawn(protocol: Protocol) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let store: Store = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            let store = store.clone();
            tokio::spawn(async move {
                let _ = match protocol {
                    Protocol::Text => handle_text(sock, store).await,
                    Protocol::Binary => handle_binary(sock, store).await,
                };
            });
        }
    });
    port
}

async fn handle_text(sock: tokio::net::TcpStream, store: Store) -> std::io::Result<()> {
    let (read_half, mut writer) = sock.into_split();
    let mut reader = BufReader::new(read_half);
    let mut cas_counter: u64 = 1;
    loop {
        let mut line = Vec::new();
        let read = tokio::io::AsyncBufReadExt::read_until(&mut reader, b'\n', &mut line).await?;
        if read == 0 {
            return Ok(());
        }
        let trimmed = &line[..line.len().saturating_sub(2)];
        let text = String::from_utf8_lossy(trimmed).into_owned();
        let tokens: Vec<&str> = text.split_whitespace().collect();
        if tokens.is_empty() {
            continue;
        }
        match tokens[0] {
            cmd @ ("set" | "add" | "replace") => {
                let key = tokens[1].to_string();
                let flags: u32 = tokens[2].parse().unwrap();
                let bytes: usize = tokens[4].parse().unwrap();
                let noreply = tokens.last() == Some(&"noreply");
                let mut data = vec![0u8; bytes + 2];
                reader.read_exact(&mut data).await?;
                data.truncate(bytes);
                let mut map = store.lock().await;
                let exists = map.contains_key(&key);
                let ok = match cmd {
                    "add" => !exists,
                    "replace" => exists,
                    _ => true,
                };
                let reply = if ok {
                    cas_counter += 1;
                    map.insert(
                        key,
                        Item {
                            data,
                            flags,
                            cas: cas_counter,
                        },
                    );
                    "STORED\r\n"
                } else {
                    "NOT_STORED\r\n"
                };
                if !noreply {
                    writer.write_all(reply.as_bytes()).await?;
                }
            }
            "cas" => {
                let key = tokens[1].to_string();
                let flags: u32 = tokens[2].parse().unwrap();
                let bytes: usize = tokens[4].parse().unwrap();
                let cas: u64 = tokens[5].parse().unwrap();
                let mut data = vec![0u8; bytes + 2];
                reader.read_exact(&mut data).await?;
                data.truncate(bytes);
                let mut map = store.lock().await;
                let reply = match map.get(&key) {
                    None => "NOT_FOUND\r\n",
                    Some(item) if item.cas != cas => "EXISTS\r\n",
                    Some(_) => {
                        cas_counter += 1;
                        map.insert(
                            key,
                            Item {
                                data,
                                flags,
                                cas: cas_counter,
                            },
                        );
                        "STORED\r\n"
                    }
                };
                writer.write_all(reply.as_bytes()).await?;
            }
            cmd @ ("get" | "gets") => {
                let map = store.lock().await;
                let mut out = Vec::new();
                for key in &tokens[1..] {
                    if let Some(item) = map.get(*key) {
                        if cmd == "gets" {
                            out.extend_from_slice(
                                format!(
                                    "VALUE {} {} {} {}\r\n",
                                    key,
                                    item.flags,
                                    item.data.len(),
                                    item.cas
                                )
                                .as_bytes(),
                            );
                        } else {
                            out.extend_from_slice(
                                format!("VALUE {} {} {}\r\n", key, item.flags, item.data.len())
                                    .as_bytes(),
                            );
                        }
                        out.extend_from_slice(&item.data);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                out.extend_from_slice(b"END\r\n");
                writer.write_all(&out).await?;
            }
            "delete" => {
                let key = tokens[1];
                let noreply = tokens.last() == Some(&"noreply");
                let removed = store.lock().await.remove(key).is_some();
                if !noreply {
                    writer
                        .write_all(if removed {
                            b"DELETED\r\n"
                        } else {
                            b"NOT_FOUND\r\n"
                        })
                        .await?;
                }
            }
            cmd @ ("incr" | "decr") => {
                let key = tokens[1];
                let delta: u64 = tokens[2].parse().unwrap();
                let mut map = store.lock().await;
                let reply = match map.get_mut(key) {
                    None => "NOT_FOUND\r\n".to_string(),
                    Some(item) => {
                        let current: u64 = String::from_utf8_lossy(&item.data)
                            .trim()
                            .parse()
                            .unwrap_or(0);
                        let next = if cmd == "incr" {
                            current + delta
                        } else {
                            current.saturating_sub(delta)
                        };
                        item.data = next.to_string().into_bytes();
                        format!("{next}\r\n")
                    }
                };
                writer.write_all(reply.as_bytes()).await?;
            }
            "touch" => {
                let key = tokens[1];
                let found = store.lock().await.contains_key(key);
                writer
                    .write_all(if found {
                        b"TOUCHED\r\n"
                    } else {
                        b"NOT_FOUND\r\n"
                    })
                    .await?;
            }
            "flush_all" => {
                store.lock().await.clear();
                writer.write_all(b"OK\r\n").await?;
            }
            "version" => {
                writer.write_all(b"VERSION 1.6.0-fake\r\n").await?;
            }
            _ => {
                writer.write_all(b"ERROR\r\n").await?;
            }
        }
        writer.flush().await?;
    }
}

const REQ: u8 = 0x80;
const RESP: u8 = 0x81;

fn frame(
    opcode: u8,
    status: u16,
    extras: &[u8],
    key: &[u8],
    value: &[u8],
    opaque: u32,
    cas: u64,
) -> Vec<u8> {
    let body = extras.len() + key.len() + value.len();
    let mut buf = Vec::with_capacity(24 + body);
    buf.push(RESP);
    buf.push(opcode);
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.push(extras.len() as u8);
    buf.push(0);
    buf.extend_from_slice(&status.to_be_bytes());
    buf.extend_from_slice(&(body as u32).to_be_bytes());
    buf.extend_from_slice(&opaque.to_be_bytes());
    buf.extend_from_slice(&cas.to_be_bytes());
    buf.extend_from_slice(extras);
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

async fn handle_binary(mut sock: tokio::net::TcpStream, store: Store) -> std::io::Result<()> {
    let mut cas_counter: u64 = 1;
    loop {
        let mut header = [0u8; 24];
        if sock.read_exact(&mut header).await.is_err() {
            return Ok(());
        }
        assert_eq!(header[0], REQ, "bad request magic");
        let opcode = header[1];
        let key_len = u16::from_be_bytes([header[2], header[3]]) as usize;
        let extras_len = header[4] as usize;
        let body_len = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
        let opaque = u32::from_be_bytes(header[12..16].try_into().unwrap());
        let cas = u64::from_be_bytes(header[16..24].try_into().unwrap());
        let mut body = vec![0u8; body_len];
        sock.read_exact(&mut body).await?;
        let extras = &body[..extras_len];
        let key = String::from_utf8_lossy(&body[extras_len..extras_len + key_len]).into_owned();
        let value = &body[extras_len + key_len..];

        let response = match opcode {
            0x01..=0x03 => {
                // set / add / replace
                let flags = u32::from_be_bytes(extras[0..4].try_into().unwrap());
                let mut map = store.lock().await;
                let exists = map.contains_key(&key);
                let allowed = match opcode {
                    0x02 => !exists,
                    0x03 => exists,
                    _ => true,
                };
                let cas_ok = cas == 0 || map.get(&key).map(|item| item.cas) == Some(cas);
                if allowed && cas_ok {
                    cas_counter += 1;
                    map.insert(
                        key,
                        Item {
                            data: value.to_vec(),
                            flags,
                            cas: cas_counter,
                        },
                    );
                    frame(opcode, 0x0000, &[], &[], &[], opaque, cas_counter)
                } else if !cas_ok {
                    frame(opcode, 0x0002, &[], &[], b"exists", opaque, 0)
                } else {
                    frame(opcode, 0x0005, &[], &[], b"not stored", opaque, 0)
                }
            }
            0x00 => {
                // get
                let map = store.lock().await;
                match map.get(&key) {
                    Some(item) => frame(
                        opcode,
                        0x0000,
                        &item.flags.to_be_bytes(),
                        &[],
                        &item.data,
                        opaque,
                        item.cas,
                    ),
                    None => frame(opcode, 0x0001, &[], &[], b"not found", opaque, 0),
                }
            }
            0x0D => {
                // getkq (quiet multi-get): reply only on hit, with key echoed
                let map = store.lock().await;
                match map.get(&key) {
                    Some(item) => frame(
                        opcode,
                        0x0000,
                        &item.flags.to_be_bytes(),
                        key.as_bytes(),
                        &item.data,
                        opaque,
                        item.cas,
                    ),
                    None => Vec::new(),
                }
            }
            0x0A => frame(0x0A, 0x0000, &[], &[], &[], opaque, 0), // noop fence
            0x04 => {
                let removed = store.lock().await.remove(&key).is_some();
                if removed {
                    frame(opcode, 0x0000, &[], &[], &[], opaque, 0)
                } else {
                    frame(opcode, 0x0001, &[], &[], b"not found", opaque, 0)
                }
            }
            0x05 | 0x06 => {
                // incr / decr: delta is first 8 extra bytes
                let delta = u64::from_be_bytes(extras[0..8].try_into().unwrap());
                let mut map = store.lock().await;
                match map.get_mut(&key) {
                    None => frame(opcode, 0x0001, &[], &[], b"not found", opaque, 0),
                    Some(item) => {
                        let current: u64 = String::from_utf8_lossy(&item.data)
                            .trim()
                            .parse()
                            .unwrap_or(0);
                        let next = if opcode == 0x05 {
                            current + delta
                        } else {
                            current.saturating_sub(delta)
                        };
                        item.data = next.to_string().into_bytes();
                        frame(
                            opcode,
                            0x0000,
                            &[],
                            &[],
                            &next.to_be_bytes(),
                            opaque,
                            item.cas,
                        )
                    }
                }
            }
            0x1C => {
                let found = store.lock().await.contains_key(&key);
                frame(
                    opcode,
                    if found { 0x0000 } else { 0x0001 },
                    &[],
                    &[],
                    &[],
                    opaque,
                    0,
                )
            }
            0x08 => {
                store.lock().await.clear();
                frame(opcode, 0x0000, &[], &[], &[], opaque, 0)
            }
            0x0B => frame(opcode, 0x0000, &[], &[], b"1.6.0-fake", opaque, 0),
            _ => frame(opcode, 0x0081, &[], &[], b"unknown", opaque, 0),
        };
        if !response.is_empty() {
            sock.write_all(&response).await?;
            sock.flush().await?;
        }
    }
}

fn client(port: u16, protocol: Protocol) -> Client {
    Client::new(
        Config::tcp("127.0.0.1", port)
            .with_protocol(protocol)
            .with_max_connections(2),
    )
    .unwrap()
}

async fn run_crud_suite(protocol: Protocol) {
    let port = spawn(protocol).await;
    let client = client(port, protocol);

    // miss then set then hit
    assert!(client.get("alpha").await.unwrap().is_none());
    assert!(client.set("alpha", "hello", 60u32).await.unwrap());
    let value = client.get("alpha").await.unwrap().unwrap();
    assert_eq!(value.as_string().unwrap(), "hello");

    // add is rejected when the key exists, replace requires existence
    assert!(!client.add("alpha", "again", 60u32).await.unwrap());
    assert!(client.replace("alpha", "world", 60u32).await.unwrap());
    assert!(!client.replace("missing", "x", 60u32).await.unwrap());
    assert_eq!(
        client
            .get("alpha")
            .await
            .unwrap()
            .unwrap()
            .as_string()
            .unwrap(),
        "world"
    );

    // multi-get returns only the present keys
    client.set("beta", "2", 60u32).await.unwrap();
    let multi = client.get_multi(&["alpha", "beta", "ghost"]).await.unwrap();
    assert_eq!(multi.len(), 2);
    assert_eq!(multi["alpha"].as_string().unwrap(), "world");
    assert_eq!(multi["beta"].as_string().unwrap(), "2");

    // cas round-trip
    let cas = client.get_cas("alpha").await.unwrap().unwrap();
    let updated = CasValue::new("cas-updated".to_memcache_owned(), cas.cas);
    assert!(client.cas("alpha", &updated, 60u32).await.unwrap());
    let stale = CasValue::new("stale".to_memcache_owned(), cas.cas);
    assert!(!client.cas("alpha", &stale, 60u32).await.unwrap());

    // counters
    client.set("counter", "10", 60u32).await.unwrap();
    assert_eq!(client.incr("counter", 5).await.unwrap(), Some(15));
    assert_eq!(client.decr("counter", 3).await.unwrap(), Some(12));
    assert_eq!(client.incr("ghost", 1).await.unwrap(), None);

    // delete
    assert!(client.delete("beta").await.unwrap());
    assert!(!client.delete("beta").await.unwrap());
    assert!(client.get("beta").await.unwrap().is_none());

    // misc
    assert!(client.version().await.unwrap().contains("fake"));
    client.flush_all().await.unwrap();
    assert!(client.get("alpha").await.unwrap().is_none());
}

// Small helper so the test can build owned values for CasValue.
trait ToOwnedValue {
    fn to_memcache_owned(self) -> memcache::Value;
}
impl ToOwnedValue for &str {
    fn to_memcache_owned(self) -> memcache::Value {
        memcache::Value::new(self.as_bytes().to_vec(), 0)
    }
}

#[tokio::test]
async fn text_protocol_crud() {
    run_crud_suite(Protocol::Text).await;
}

#[tokio::test]
async fn binary_protocol_crud() {
    run_crud_suite(Protocol::Binary).await;
}

/// Spawn a misbehaving binary server: for each connection it answers the
/// first request normally, then injects one extra unsolicited GET response
/// frame before answering every subsequent request. This simulates a late
/// response frame left over from a timed-out operation. Returns the port.
async fn spawn_desyncing_binary_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut answered = 0u32;
                loop {
                    let mut header = [0u8; 24];
                    if sock.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let key_len = u16::from_be_bytes([header[2], header[3]]) as usize;
                    let extras_len = header[4] as usize;
                    let body_len =
                        u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
                    let opaque = u32::from_be_bytes(header[12..16].try_into().unwrap());
                    let mut body = vec![0u8; body_len];
                    if sock.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    let _ = (key_len, extras_len);
                    answered += 1;
                    if answered > 1 {
                        // Inject a leftover frame with a bogus opaque.
                        let junk = frame(0x00, 0x0000, &[0; 4], &[], b"stale", 0xdead, 0);
                        if sock.write_all(&junk).await.is_err() {
                            return;
                        }
                    }
                    // Answer every request with a well-formed GET miss.
                    let response = frame(0x00, 0x0001, &[], &[], b"not found", opaque, 0);
                    if sock.write_all(&response).await.is_err() {
                        return;
                    }
                    let _ = sock.flush().await;
                }
            });
        }
    });
    port
}

#[tokio::test]
async fn binary_desynced_frame_drops_connection() {
    let port = spawn_desyncing_binary_server().await;
    let client = Client::new(
        Config::tcp("127.0.0.1", port)
            .with_protocol(Protocol::Binary)
            .with_max_connections(1),
    )
    .unwrap();

    // First request: answered normally, connection goes back to the pool.
    assert!(client.get("k").await.unwrap().is_none());
    // Second request on the pooled connection hits the injected stale frame:
    // the opaque mismatch must surface as a desync error...
    let err = client.get("k").await.unwrap_err();
    assert!(
        matches!(err, memcache::Error::Desynced(_)),
        "expected desync error, got {err:?}"
    );
    // ...and the poisoned connection must have been dropped, so the next
    // request reconnects and succeeds (fresh connection: answered normally).
    assert!(client.get("k").await.unwrap().is_none());
}

#[tokio::test]
async fn binary_timeout_drops_connection() {
    use std::time::Duration;

    // Server that accepts connections but never answers the first request;
    // answers subsequent requests (on new connections) normally.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut conn_count = 0u32;
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            conn_count += 1;
            let conn_no = conn_count;
            tokio::spawn(async move {
                loop {
                    let mut header = [0u8; 24];
                    if sock.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let body_len =
                        u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
                    let opaque = u32::from_be_bytes(header[12..16].try_into().unwrap());
                    let mut body = vec![0u8; body_len];
                    if sock.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    if conn_no == 1 {
                        // Never answer on the first connection: force a timeout.
                        continue;
                    }
                    let response = frame(0x00, 0x0001, &[], &[], b"not found", opaque, 0);
                    if sock.write_all(&response).await.is_err() {
                        return;
                    }
                    let _ = sock.flush().await;
                }
            });
        }
    });

    let client = Client::new(
        Config::tcp("127.0.0.1", port)
            .with_protocol(Protocol::Binary)
            .with_max_connections(1)
            .with_op_timeout(Duration::from_millis(100)),
    )
    .unwrap();

    // First request times out; the connection must be dropped rather than
    // returned to the pool with a (never-arriving) pending response.
    let err = client.get("k").await.unwrap_err();
    assert!(
        matches!(err, memcache::Error::Timeout),
        "expected timeout, got {err:?}"
    );
    // The next request must use a *new* connection (the second one the server
    // answers) and succeed.
    assert!(client.get("k").await.unwrap().is_none());
}

/// A `MakeWriter` that appends all log output into a shared buffer.
#[derive(Clone)]
struct BufWriter(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for BufWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
    type Writer = BufWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn logs_error_in_mesh_format_on_request_failure() {
    use std::time::Duration;

    let buf = Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(BufWriter(buf.clone()))
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    // Nothing is listening on port 1, so the request fails during connect.
    let client = Client::new(
        Config::tcp("127.0.0.1", 1)
            .with_namespace("nstest")
            .with_max_connections(1)
            .with_connect_timeout(Duration::from_millis(100))
            .with_op_timeout(Duration::from_millis(100)),
    )
    .unwrap();

    let result = client.get("kx").await;
    assert!(result.is_err());

    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        logged.contains("mc mesh get error ,namespace:nstest ,key: kx"),
        "unexpected log output: {logged}"
    );
}
