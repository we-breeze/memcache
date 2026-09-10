# Repository Map

This crate provides the `brz_memcache` library (`brz-memcache` package).

- `src/api.rs`: CacheService, fixed/live constructors, configuration source contract.
- `src/cacheservice.rs`: native configuration types and YAML parsing.
- `src/service/live.rs`: semantic comparison and atomic topology replacement.
- `src/cache_topology.rs`: replica/shard routing and writeback behavior.
- `src/session_protocol.rs`: text/binary protocol sessions using brz-net.
- `src/sharding/`: existing hash and distribution algorithms.
- `tests/`: local protocol-server and live-configuration tests.

Vintage polling, legacy configuration conversion, and mesh registry discovery
belong to the sibling local discovery crate. It depends on this library, never
the reverse. Live configuration is available with default features; `service`
is an empty compatibility feature. `metrics` enables brz-metrics reporting.

## Required checks

Before every commit, all of these must pass:

```sh
cargo fmt --all
cargo test --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Non-Negotiable Rules

- **Before every commit: run `cargo fmt --all` and the test suite; commit
  only when everything passes.** Push after the tests pass.
- Never force-push to `master` (the remote rejects it anyway).
- Keep async code on `tokio`.
- The binary protocol correlates every response to its request by opaque
  token; any timeout, I/O error, or opaque mismatch must drop the
  connection (`Error::Desynced` / `Object::take`), never return it to the
  pool with a poisoned read buffer.
- Infrastructure adapters and mesh discovery belong in the local discovery crate,
  not in this client. Do not add per-client configuration polling.
- Connection lifecycle belongs to brz-net; do not add per-client pool
  maintenance tasks.
- Read-path degradations fall back to the next tier
  (warn, not error); a confirmed miss and a tier failure must stay
  distinguishable.
- Setbacks (slave→master, miss→L1) are best-effort and run off the read
  path; keep them asynchronous and log-only on failure.
- Do not commit `target/`, pid/log files under `/tmp`, local swap files, or
  recorded traffic fixtures other than the curated ones in
  `tests/fixtures/`.
- Product source files should not exceed 1000 lines after a change; split
  the relevant responsibility first when a touched file is over the limit.

## Git Hygiene

Keep commits focused and avoid including unrelated local edits. Before pushing,
check `git status --short` and `git diff --check`. Discovery remains local and
must not be published as part of this client migration.
