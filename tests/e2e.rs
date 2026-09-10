//! End-to-end coverage for the unified CacheService facade.

use std::{collections::HashMap, sync::Arc, time::Duration};

use brz_memcache::{CacheService, Memcache};
use bytes::Bytes;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

type Store = Arc<Mutex<HashMap<String, (Vec<u8>, u32)>>>;

async fn spawn_text_server() -> (u16, Store) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let store = Store::default();
    let shared = store.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let store = shared.clone();
            tokio::spawn(async move {
                serve_text(stream, store).await;
            });
        }
    });
    (port, store)
}

async fn serve_text(stream: TcpStream, store: Store) {
    let mut stream = BufReader::new(stream);
    loop {
        let mut line = String::new();
        match stream.read_line(&mut line).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        let fields: Vec<_> = line.trim_end().split(' ').collect();
        match fields.as_slice() {
            ["get", key] | ["gets", key] => {
                let item = store.lock().await.get(*key).cloned();
                let output = if let Some((value, flags)) = item {
                    format!(
                        "VALUE {key} {flags} {}\r\n{}\r\nEND\r\n",
                        value.len(),
                        String::from_utf8_lossy(&value)
                    )
                } else {
                    "END\r\n".to_string()
                };
                if stream.get_mut().write_all(output.as_bytes()).await.is_err() {
                    return;
                }
            }
            ["set", key, flags, _, length] => {
                let Ok(length) = length.parse::<usize>() else {
                    return;
                };
                let Ok(flags) = flags.parse::<u32>() else {
                    return;
                };
                let mut value = vec![0; length + 2];
                if stream.read_exact(&mut value).await.is_err() {
                    return;
                }
                value.truncate(length);
                store
                    .lock()
                    .await
                    .insert((*key).to_string(), (value, flags));
                if stream.get_mut().write_all(b"STORED\r\n").await.is_err() {
                    return;
                }
            }
            _ => return,
        }
    }
}

async fn set_when_connected(cache: &CacheService, key: &str, value: Bytes) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match cache.set(key, value.clone()).await {
            Ok(result) => return result,
            Err(brz_memcache::Error::Unavailable) if tokio::time::Instant::now() < deadline => {
                tokio::task::yield_now().await;
            }
            result => return result.unwrap(),
        }
    }
}

#[tokio::test]
async fn single_is_one_fixed_cache_service_node() {
    let (port, _) = spawn_text_server().await;
    let cache = CacheService::single(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    assert!(set_when_connected(&cache, "single", Bytes::from_static(b"value")).await);
    assert_eq!(
        cache.get("single").await.unwrap().unwrap().data,
        Bytes::from_static(b"value")
    );
}

#[tokio::test]
async fn construction_does_not_wait_for_connection() {
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = unused.local_addr().unwrap();
    drop(unused);

    let cache = CacheService::single(endpoint.to_string()).await.unwrap();
    assert!(matches!(
        cache.get("not-connected").await,
        Err(brz_memcache::Error::Unavailable)
    ));
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn metrics_group_physical_accesses_by_port_and_split_direction() {
    let (port, _) = spawn_text_server().await;
    let cache = CacheService::single(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    assert!(set_when_connected(&cache, "setup", Bytes::from_static(b"value")).await);
    assert!(cache.get("setup").await.unwrap().is_some());
    let before = metric_totals(port);

    assert!(
        cache
            .set("measured", Bytes::from_static(b"value"))
            .await
            .unwrap()
    );
    assert!(cache.get("measured").await.unwrap().is_some());
    let after = metric_totals(port);

    assert_eq!(after.0 - before.0, 2, "MC counts reads and writes");
    assert_eq!(after.1 - before.1, 1, "MCDETAIL_up counts writes");
    assert_eq!(after.2 - before.2, 1, "MCDETAIL_down counts reads");
}

#[cfg(feature = "metrics")]
fn metric_totals(port: u16) -> (u64, u64, u64) {
    let port = port.to_string();
    let up = format!("{port}_up");
    let down = format!("{port}_down");
    let mut totals = (0, 0, 0);
    brz_metrics::visit(|name, metric_type, snapshot| match metric_type {
        "MC" if name == port => totals.0 = snapshot.total,
        "MCDETAIL" if name == up => totals.1 = snapshot.total,
        "MCDETAIL" if name == down => totals.2 = snapshot.total,
        _ => {}
    });
    totals
}
