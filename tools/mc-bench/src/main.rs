//! mc-bench — a load-test harness for the memcache SDK.
//!
//! Runs a fixed number of operations across `--concurrency` async workers
//! and reports throughput plus latency percentiles (p50/p95/p99). It drives
//! the SDK's access modes:
//!
//! - **sidecar mode** (default, `--namespace`): through
//!   [`SidecarClient`](memcache::sidecar::SidecarClient), measuring the full
//!   pool/discovery/maintenance stack against the breeze mesh.
//! - **direct mode** (`--direct host:port`): through
//!   [`DirectClient`](memcache::direct::DirectClient), the SDK's
//!   direct-backend stack straight to a raw memcached.
//! - **shards mode** (`--shards h:p,h:p,...`): through
//!   [`Shards`](memcache::direct::Shards), the client-side shard router.
//!
//! Usage:
//!   mc-bench --namespace my_ns --concurrency 64 --ops 100000 get
//!   mc-bench --direct 127.0.0.1:11211 --concurrency 64 --ops 100000 set
//!   mc-bench --shards 127.0.0.1:11211,127.0.0.1:11212 --ops 100000 get
//!
//! # Exit code
//!
//! Non-zero if the error rate exceeds `--max-error-rate`.

mod driver;
mod fault;
mod stats;

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use driver::{BenchClient, Workload, WorkloadKind};
use fault::FaultInjector;
use memcache::Protocol;
use memcache::direct::{DirectClient, ServerConfig, Shards};
use memcache::sidecar::{MeshConfig, SidecarClient};
use stats::{MemoryWindow, OpBudget, Summary, WorkerStats};

// Install mimalloc (with per-request heap accounting under the `memory-stats`
// feature) as the process-wide allocator. The SDK itself stays
// allocator-agnostic; only this binary pins one.
brz_mem::install_global_allocator!();

/// Arguments for the memcache load-test harness.
#[derive(Parser, Debug)]
#[command(name = "mc-bench", version, about = "Load-test the memcache SDK")]
struct Args {
    /// Mesh resource namespace to connect through.
    #[arg(long, env = "BREEZE_MC_NS")]
    namespace: Option<String>,

    /// Direct-backend mode: connect to a raw `host:port` memcached through
    /// the SDK's `direct::DirectClient` (no mesh).
    #[arg(long)]
    direct: Option<String>,

    /// Shards mode: comma-separated direct backends
    /// (`host:port,host:port,...`), driven through the SDK's
    /// `direct::Shards` client-side router.
    #[arg(long, value_delimiter = ',')]
    shards: Option<Vec<String>>,

    /// Service mode: master endpoints (`host:port,...`), driven through the
    /// SDK's `direct::HaClient` master/slave topology client. Combine with
    /// --slave-l1 / --slaves.
    #[arg(long, value_delimiter = ',')]
    masters: Option<Vec<String>>,

    /// Service-from-YAML mode: path to a cache-service YAML document (the
    /// Vintage statics-config `all` format; see
    /// tests/fixtures/cache_service_local.yaml). The topology for --ns is
    /// parsed and built exactly like the production path:
    /// CacheServiceConfig → HaConfig::from_namespace → HaClient.
    #[arg(long)]
    service_yaml: Option<String>,

    /// Namespace inside --service-yaml to build the client for.
    #[arg(long, default_value = "test.local")]
    ns: String,

    /// Service mode: L1 slave endpoints (read-first tier).
    #[arg(long, value_delimiter = ',')]
    slave_l1: Option<Vec<String>>,

    /// Service mode: L2 slave endpoints (read-next tier).
    #[arg(long, value_delimiter = ',')]
    slaves: Option<Vec<String>>,

    /// Service mode: also write the slave tier (double-write).
    #[arg(long)]
    write_slave: bool,

    /// With --service-yaml: drive the multi-tier `MemCacheTemplate`
    /// (memcache::service, the Java MemCacheTemplate port) instead of the
    /// HaClient. Requires the `template` feature.
    #[arg(long)]
    template: bool,

    /// Hash algorithm for --shards (breeze mesh names, e.g. crc32).
    #[arg(long, default_value = "crc32")]
    hash: String,

    /// Distribution for --shards (e.g. modula, range-256, ketama).
    #[arg(long, default_value = "modula")]
    distribution: String,

    /// With fault injection in --shards mode: only proxy (delay) this shard
    /// index. Defaults to shard 0.
    #[arg(long)]
    fault_shard: Option<usize>,

    /// Deployment group segment of the mesh sock-file name.
    #[arg(long, default_value = "default")]
    group: String,

    /// Directory the mesh publishes sock files into.
    #[arg(long, default_value = "/data1/breeze/socks")]
    socket_dir: String,

    /// Wire protocol (binary or text).
    #[arg(long, default_value = "binary")]
    protocol: String,

    /// Minimum live pooled connections kept warm (0 = fully lazy start).
    #[arg(long, default_value_t = 2)]
    min_conns: usize,

    /// Maximum live pooled connections.
    #[arg(long, default_value_t = 128)]
    max_conns: usize,

    /// Per-command operation timeout, in milliseconds.
    #[arg(long, default_value_t = 400)]
    op_timeout_ms: u64,

    /// Concurrent async workers issuing operations.
    #[arg(short = 'c', long, default_value_t = 32)]
    concurrency: usize,

    /// Total number of logical operations to issue (0 = run for --duration).
    #[arg(short = 'n', long, default_value_t = 0)]
    ops: u64,

    /// Run for this many seconds when --ops is 0.
    #[arg(short = 'd', long, default_value_t = 30)]
    duration: u64,

    /// Number of distinct keys in the pre-generated pool.
    #[arg(long, default_value_t = 10000)]
    keys: usize,

    /// Fixed length of each key, in bytes.
    #[arg(long, default_value_t = 32)]
    key_len: usize,

    /// Maximum value size, in bytes; the SET workload cycles through
    /// `1..=max`. Ignored by the GET workload.
    #[arg(long, default_value_t = 1024)]
    val_size: usize,

    /// Fraction of keys whose value is "big" (--big-value-size), spread
    /// deterministically across the key pool. Default 0 (no big values).
    #[arg(long, default_value_t = 0.0)]
    big_value_rate: f64,

    /// Size of the "big" values, in bytes (e.g. 1024 for 1k, 10240 for 10k).
    #[arg(long, default_value_t = 1024)]
    big_value_size: usize,

    /// Warm-up operations per worker before measurement begins.
    #[arg(long, default_value_t = 50)]
    warmup: usize,

    /// Fail the run (non-zero exit) if the error rate exceeds this fraction.
    #[arg(long, default_value_t = 0.01)]
    max_error_rate: f64,

    /// Fraction of operations delayed by --slow-ms (simulated slow requests).
    #[arg(long, default_value_t = 0.0)]
    slow_rate: f64,

    /// Delay injected for slow operations, in milliseconds.
    #[arg(long, default_value_t = 200)]
    slow_ms: u64,

    /// Fraction of operations delayed past the client timeout (simulated
    /// timeouts; the SDK's op_timeout path kicks in).
    #[arg(long, default_value_t = 0.0)]
    timeout_rate: f64,

    /// Delay injected for timed-out operations, in milliseconds. Should
    /// exceed --op-timeout-ms.
    #[arg(long, default_value_t = 5000)]
    timeout_ms: u64,

    /// Fraction of frames after which the proxy kills the connection
    /// (simulated resets: LB/proxy/mesh restart).
    #[arg(long, default_value_t = 0.0)]
    reset_rate: f64,

    /// Full-blackhole outage window length, in milliseconds (0 = off).
    /// While open, every frame is held until the window closes — models a
    /// backend outage or deploy window.
    #[arg(long, default_value_t = 0)]
    outage_ms: u64,

    /// Interval between outage windows, in milliseconds.
    #[arg(long, default_value_t = 30000)]
    outage_interval_ms: u64,

    /// Fraction of operations preceded by a client-side CPU stall
    /// (simulated SDK-process CPU overload / GC pause).
    #[arg(long, default_value_t = 0.0)]
    cpu_stall_rate: f64,

    /// Length of the client-side CPU stall, in milliseconds.
    #[arg(long, default_value_t = 20)]
    cpu_stall_ms: u64,

    /// Workload to run.
    #[arg(value_enum, default_value_t = WorkloadKindArg::Get)]
    workload: WorkloadKindArg,

    /// Verify replies: seed values as `key || padding` and check every reply
    /// matches, detecting request/response mixups (mismatches count as
    /// errors).
    #[arg(long)]
    verify: bool,
}

/// CLI-facing mirror of [`WorkloadKind`] (clap needs its own ValueEnum here).
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum WorkloadKindArg {
    /// One GET per op — pure single-round-trip read (ping/pong).
    Get,
    /// One multi-GET (4 keys) per op.
    GetMulti,
    /// One SET per op (overwrite of pre-seeded keys).
    Set,
    /// One INCR per op on pre-seeded decimal counters.
    Incr,
}

impl From<WorkloadKindArg> for WorkloadKind {
    fn from(a: WorkloadKindArg) -> Self {
        match a {
            WorkloadKindArg::Get => WorkloadKind::Get,
            WorkloadKindArg::GetMulti => WorkloadKind::GetMulti,
            WorkloadKindArg::Set => WorkloadKind::Set,
            WorkloadKindArg::Incr => WorkloadKind::Incr,
        }
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "memcache=warn,mc_bench=info".into()),
        )
        .init();

    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let code = rt.block_on(run(args));
    std::process::exit(code);
}

fn protocol(args: &Args) -> Protocol {
    match args.protocol.as_str() {
        "text" => Protocol::Text,
        _ => Protocol::Binary,
    }
}

async fn run(mut args: Args) -> i32 {
    // Optional fault injection: insert a delaying TCP proxy between the
    // bench and the target (mesh endpoint or raw memcached) so slow/timeout
    // faults exercise the SDK's timeout/reconnect machinery.
    let injector = FaultInjector::new(
        args.slow_rate,
        args.slow_ms,
        args.timeout_rate,
        args.timeout_ms,
        args.reset_rate,
        args.outage_ms,
        args.outage_interval_ms,
    )
    .map(Arc::new);
    if let Some(injector) = &injector
        && let Err(e) = inject_faults(&mut args, injector.clone()).await
    {
        eprintln!("error: fault injection setup failed: {e}");
        return 2;
    }

    let workload: WorkloadKind = args.workload.into();

    let client = match build_client(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: failed to connect: {e}");
            return 2;
        }
    };

    // Pre-generate the key/value pool once: workers index into it with a
    // plain integer, so the hot path has no `format!()` allocations — the
    // per-op allocation count then reflects the SDK alone.
    let pool = Arc::new(driver::Pool::new(
        args.keys,
        args.key_len,
        args.val_size,
        args.big_value_rate,
        args.big_value_size,
    ));
    eprintln!(
        "mc-bench: workload={workload:?} keys={} key_len={} val_size={} concurrency={} min_conns={} max_conns={}",
        args.keys, args.key_len, args.val_size, args.concurrency, args.min_conns, args.max_conns
    );

    // Seed the keyspace: for GET, the keys must exist first; for SET we
    // pre-fill so the workload is pure overwrite; for INCR the values must
    // be decimal counters.
    eprintln!("seeding {} keys...", args.keys);
    if let Err(e) = seed_keys(&client, &pool, workload).await {
        eprintln!("error: failed to seed keys: {e}");
        return 2;
    }

    if args.verify {
        eprintln!("verifying seeded data...");
        if let Err(e) = verify_seeds(&client, &pool).await {
            eprintln!("error: seed verification failed: {e}");
            return 2;
        }
    }

    let runner = Arc::from(workload.runner(pool, args.verify));
    eprintln!("warming up ({} ops/worker)...", args.warmup);
    warmup(&client, &runner, args.concurrency, args.warmup).await;

    let mode = if args.ops > 0 {
        RunMode::Count(args.ops)
    } else {
        RunMode::Timed(Duration::from_secs(args.duration))
    };

    eprintln!("running...");
    let mem_before = brz_mem::heap();
    let (summary, elapsed) = measured_run(
        &client,
        &runner,
        args.concurrency,
        mode,
        args.cpu_stall_rate,
        args.cpu_stall_ms,
    )
    .await;
    if let Some(injector) = &injector {
        let (slow, timeout, reset, outage) = injector.counts();
        eprintln!(
            "fault injection: slow={slow} timeout={timeout} reset={reset} outage={outage} injected"
        );
    }
    finish(&summary, elapsed, mem_before, &args)
}

/// Report the run and compute the exit code from the error rate.
fn finish(
    summary: &Summary,
    elapsed: Duration,
    mem_before: Option<brz_mem::HeapStats>,
    args: &Args,
) -> i32 {
    let mem_window = MemoryWindow::between(mem_before, brz_mem::heap(), summary.total());
    report(summary, elapsed, &mem_window);
    let error_rate = if summary.total() == 0 {
        0.0
    } else {
        summary.errors as f64 / summary.total() as f64
    };
    if error_rate > args.max_error_rate {
        eprintln!(
            "error rate {:.2}% exceeds threshold {:.2}% — failing",
            error_rate * 100.0,
            args.max_error_rate * 100.0
        );
        return 1;
    }
    0
}

/// Whether a run is bounded by a total op count or a wall-clock duration.
enum RunMode {
    Count(u64),
    Timed(Duration),
}

/// Insert the fault-injecting proxy between the bench and the configured
/// target, rewriting `args` to point at the proxy. Sidecar mode re-publishes
/// a bench-owned sock file (proxy port) in a fresh socket dir.
async fn inject_faults(args: &mut Args, injector: Arc<FaultInjector>) -> Result<(), String> {
    if let Some(shards) = args.shards.clone() {
        // Only the selected shard goes through the fault proxy; the others
        // connect directly — modeling "one slow shard among many backends".
        let fault_shard = args.fault_shard.unwrap_or(0);
        if fault_shard >= shards.len() {
            return Err(format!(
                "--fault-shard {fault_shard} out of range ({} shards)",
                shards.len()
            ));
        }
        let mut rewritten = shards;
        let target = resolve(&rewritten[fault_shard]).await?;
        let proxy = fault::start_proxy(target, injector).await?;
        eprintln!("fault proxy: {proxy} -> {target} (shard {fault_shard} only)");
        rewritten[fault_shard] = proxy.to_string();
        args.shards = Some(rewritten);
        return Ok(());
    }
    if let Some(addr) = args.direct.clone() {
        let target = resolve(&addr).await?;
        let proxy = fault::start_proxy(target, injector).await?;
        eprintln!("fault proxy: {proxy} -> {target}");
        args.direct = Some(proxy.to_string());
        return Ok(());
    }
    if let Some(ns) = args.namespace.clone() {
        let endpoint = memcache::sidecar::discovery::discover(
            std::path::Path::new(&args.socket_dir),
            &args.group,
            &ns,
        )
        .map_err(|e| e.to_string())?;
        let memcache::Endpoint { host, port } = endpoint;
        let target = resolve(&format!("{host}:{port}")).await?;
        let proxy = fault::start_proxy(target, injector).await?;
        let dir = std::env::temp_dir().join(format!("mc-bench-fault-{}", std::process::id()));
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let sock = dir.join(format!(
            "config.example.com+3+config+v1+{}+all:{}@mc:{}@cs",
            args.group,
            ns,
            proxy.port()
        ));
        std::fs::File::create(&sock).map_err(|e| e.to_string())?;
        eprintln!("fault proxy: {proxy} -> {target} (sock {})", sock.display());
        args.socket_dir = dir.to_string_lossy().into_owned();
        return Ok(());
    }
    Err("fault injection requires one of --namespace/--direct/--shards".to_string())
}

async fn resolve(host_port: &str) -> Result<std::net::SocketAddr, String> {
    tokio::net::lookup_host(host_port)
        .await
        .map_err(|e| format!("resolve {host_port}: {e}"))?
        .next()
        .ok_or_else(|| format!("resolve {host_port}: no addresses"))
}

/// Build the client the harness will drive.
fn build_client(args: &Args) -> Result<BenchClient, String> {
    if let Some(yaml_path) = &args.service_yaml {
        let yaml = std::fs::read_to_string(yaml_path)
            .map_err(|e| format!("read --service-yaml {yaml_path}: {e}"))?;
        let config = memcache::cacheservice::CacheServiceConfig::from_yaml_str(&yaml)
            .map_err(|e| format!("parse --service-yaml {yaml_path}: {e}"))?;
        if args.template {
            return build_template(&config, &args.ns);
        }
        let ns = config
            .namespace(&args.ns)
            .ok_or_else(|| format!("namespace '{}' not in {yaml_path}", args.ns))?;
        let mut ha = memcache::direct::HaConfig::from_namespace(ns)
            .map_err(|e| e.to_string())?
            .with_write_slave(args.write_slave);
        // Apply the CLI pool/timeout/protocol template to every tier.
        let first = ha.masters.first().cloned();
        if let Some(first) = first {
            ha = ha.with_server(server_config(args, &first)?);
        }
        eprintln!(
            "service-yaml: ns={} masters={:?} slave_l1={:?} slaves={:?} write_slave={}",
            args.ns, ha.masters, ha.slave_l1, ha.slaves, args.write_slave
        );
        let client = memcache::direct::HaClient::connect(ha).map_err(|e| e.to_string())?;
        return Ok(BenchClient::Ha(Arc::new(client)));
    }
    if let Some(masters) = &args.masters {
        let server = server_config(args, &masters[0])?;
        let mut ha = memcache::direct::HaConfig::new(masters.clone())
            .with_sharding(&args.hash, &args.distribution)
            .with_write_slave(args.write_slave)
            .with_server(server);
        if let Some(l1) = &args.slave_l1 {
            ha = ha.with_slave_l1(l1.clone());
        }
        if let Some(slaves) = &args.slaves {
            ha = ha.with_slaves(slaves.clone());
        }
        let client = memcache::direct::HaClient::connect(ha).map_err(|e| e.to_string())?;
        return Ok(BenchClient::Ha(Arc::new(client)));
    }
    if let Some(shards) = &args.shards {
        let mut clients = Vec::with_capacity(shards.len());
        for addr in shards {
            let cfg = server_config(args, addr)?;
            clients.push(DirectClient::connect(cfg).map_err(|e| format!("shard {addr}: {e}"))?);
        }
        let router = Shards::new(&args.hash, &args.distribution, shards.clone(), clients);
        return Ok(BenchClient::Shards(Arc::new(router)));
    }
    if let Some(addr) = &args.direct {
        let cfg = server_config(args, addr)?;
        let client = DirectClient::connect(cfg).map_err(|e| e.to_string())?;
        return Ok(BenchClient::Unified(client.into()));
    }

    let ns = args.namespace.clone().ok_or_else(|| {
        "one of --namespace, --direct, --shards, or --masters is required".to_string()
    })?;
    let cfg = MeshConfig::new(ns)
        .with_group(&args.group)
        .with_socket_dir(&args.socket_dir)
        .with_protocol(protocol(args))
        .with_min_connections(args.min_conns)
        .with_max_connections(args.max_conns)
        .with_op_timeout(Duration::from_millis(args.op_timeout_ms));
    let client = SidecarClient::from_config(cfg).map_err(|e| e.to_string())?;
    Ok(BenchClient::Unified(client.into()))
}

fn server_config(args: &Args, addr: &str) -> Result<ServerConfig, String> {
    Ok(ServerConfig::new(addr)
        .map_err(|e| e.to_string())?
        .with_protocol(protocol(args))
        .with_min_connections(args.min_conns)
        .with_max_connections(args.max_conns)
        .with_op_timeout(Duration::from_millis(args.op_timeout_ms)))
}

/// Build a `MemCacheTemplate` from the parsed YAML (feature `template`).
#[cfg(feature = "template")]
fn build_template(
    config: &memcache::cacheservice::CacheServiceConfig,
    ns: &str,
) -> Result<BenchClient, String> {
    let ns = config
        .namespace(ns)
        .ok_or_else(|| format!("namespace '{ns}' not in the YAML"))?;
    let template = memcache::service::MemCacheTemplate::from_namespace_conf(
        ns,
        memcache::service::PoolOptions::default(),
    )
    .map_err(|e| e.to_string())?;
    Ok(BenchClient::Template(Arc::new(template)))
}

/// Stub when the `template` feature is off.
#[cfg(not(feature = "template"))]
fn build_template(
    _config: &memcache::cacheservice::CacheServiceConfig,
    _ns: &str,
) -> Result<BenchClient, String> {
    Err(
        "--template requires the `template` feature (cargo run -p mc-bench --features template)"
            .into(),
    )
}

/// Issue throwaway operations to prime the pool; not measured.
async fn warmup(
    client: &BenchClient,
    runner: &Arc<dyn Workload>,
    concurrency: usize,
    per_worker: usize,
) {
    let mut handles = Vec::with_capacity(concurrency);
    for w in 0..concurrency {
        let client = client.clone();
        let runner = runner.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..per_worker {
                let _ = runner.run(&client, (w as u64) * 1000 + i as u64).await;
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

/// Pre-populate every key in the pool. Seeding is not the measured phase,
/// so it runs concurrently to keep it fast for large key counts.
async fn seed_keys(
    client: &BenchClient,
    pool: &Arc<driver::Pool>,
    workload: WorkloadKind,
) -> Result<(), String> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let next = Arc::new(AtomicUsize::new(0));
    let total = pool.keys().len();
    let workers = 8.min(total.max(1));
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let client = client.clone();
        let pool = pool.clone();
        let next = next.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= total {
                    break;
                }
                let key = pool.key(i);
                // INCR counters must be decimal; other workloads use the
                // self-describing `key || padding` value (see
                // `driver::seeded_value`).
                let value = match workload {
                    WorkloadKind::Incr => b"0".to_vec(),
                    _ => driver::seeded_value(key.as_bytes(), pool.value(i)),
                };
                // Tolerate transient faults (fault injection may hang a
                // connection).
                let mut ok = false;
                for attempt in 0..5 {
                    match client.set(key, &value).await {
                        Ok(true) => {
                            ok = true;
                            break;
                        }
                        _ if attempt == 4 => {
                            eprintln!("seed SET failed at key index {i}");
                        }
                        _ => {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
                if !ok {
                    return false;
                }
            }
            true
        }));
    }
    for h in handles {
        match h.await {
            Ok(true) => {}
            Ok(false) => return Err("a seed SET failed".into()),
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

/// Full pre-bench data check: GET every key and verify the value carries the
/// expected `key` prefix (see [`driver::seeded_value`]). Any mismatch aborts
/// the run.
async fn verify_seeds(client: &BenchClient, pool: &Arc<driver::Pool>) -> Result<(), String> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let next = Arc::new(AtomicUsize::new(0));
    let mismatches = Arc::new(AtomicUsize::new(0));
    let total = pool.keys().len();
    let workers = 8.min(total.max(1));
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let client = client.clone();
        let pool = pool.clone();
        let next = next.clone();
        let mismatches = mismatches.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= total {
                    break;
                }
                let key = pool.key(i);
                match client.get(key).await {
                    Ok(Some(value)) if driver::expected_value_matches(key.as_bytes(), &value) => {}
                    other => {
                        let prior = mismatches.fetch_add(1, Ordering::Relaxed);
                        if prior == 0 {
                            eprintln!("seed mismatch at key index {i}: {other:?}");
                        }
                    }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let bad = mismatches.load(Ordering::Relaxed);
    if bad > 0 {
        return Err(format!("{bad}/{total} keys failed verification"));
    }
    eprintln!("seed data verified: {total} keys");
    Ok(())
}

/// Run the measured phase. Workers claim from a shared op budget (count
/// mode) or race a deadline (timed mode); each records latency into a local
/// histogram, merged at the end.
async fn measured_run(
    client: &BenchClient,
    runner: &Arc<dyn Workload>,
    concurrency: usize,
    mode: RunMode,
    cpu_stall_rate: f64,
    cpu_stall_ms: u64,
) -> (Summary, Duration) {
    let budget = Arc::new(OpBudget::new(match &mode {
        RunMode::Count(n) => *n,
        RunMode::Timed(_) => u64::MAX,
    }));
    let deadline = match &mode {
        RunMode::Timed(d) => Some(Instant::now() + *d),
        RunMode::Count(_) => None,
    };
    // Whether workers stop when the op budget is exhausted (count mode).
    let bounded = matches!(mode, RunMode::Count(_));
    let start = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for w in 0..concurrency {
        let client = client.clone();
        let budget = budget.clone();
        let runner = runner.clone();
        handles.push(tokio::spawn(async move {
            let mut local = WorkerStats::new();
            let mut op = (w as u64) * 1_000_000;
            let mut rng = (w as u64 + 1).wrapping_mul(0x9E3779B97F4A7C15);
            loop {
                if let Some(dl) = deadline
                    && Instant::now() >= dl
                {
                    break;
                }
                if bounded && !budget.try_claim() {
                    break;
                }
                // Simulated SDK-side CPU overload: burn CPU on this worker
                // thread before issuing the op (GC pause / scheduling delay).
                if cpu_stall_rate > 0.0 {
                    rng ^= rng >> 33;
                    rng = rng.wrapping_mul(0xff51afd7ed558ccd);
                    rng ^= rng >> 33;
                    if (rng >> 40) as f64 / (1u64 << 24) as f64 <= cpu_stall_rate {
                        let deadline = Instant::now() + Duration::from_millis(cpu_stall_ms);
                        while Instant::now() < deadline {
                            std::hint::spin_loop();
                        }
                    }
                }
                let t = Instant::now();
                let ok = runner.run(&client, op).await;
                let elapsed = t.elapsed();
                if ok {
                    local.record(elapsed);
                } else {
                    local.record_error();
                    // Fast-fail loops complete without ever yielding; yield
                    // so timers/maintenance can run, like a real app that
                    // does other work between calls.
                    tokio::task::yield_now().await;
                }
                op += 1;
            }
            local
        }));
    }

    let mut summary = Summary::new();
    for h in handles {
        if let Ok(local) = h.await {
            local.add_to(&mut summary);
        }
    }
    (summary, start.elapsed())
}

/// Print the result table.
fn report(summary: &Summary, elapsed: Duration, memory: &MemoryWindow) {
    let total = summary.total();
    let secs = elapsed.as_secs_f64().max(1e-9);
    let rps = total as f64 / secs;
    let error_pct = if total == 0 {
        0.0
    } else {
        summary.errors as f64 / (total + summary.errors) as f64 * 100.0
    };

    println!();
    println!("======== mc-bench ========");
    println!("elapsed:        {:.3} s", secs);
    println!("ops:            {}", total);
    println!("errors:         {} ({:.2}%)", summary.errors, error_pct);
    println!("throughput:     {:.0} ops/s", rps);
    if total > 0 {
        println!(
            "latency (us):   min={}  mean={:.1}  p50={}  p95={}  p99={}  max={}",
            summary.min_us(),
            summary.mean_us(),
            summary.percentile_us(50.0),
            summary.percentile_us(95.0),
            summary.percentile_us(99.0),
            summary.max_us(),
        );
    }
    memory.print();
    println!("==========================");
}
