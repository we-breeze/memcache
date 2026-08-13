# Repository Map

## Overview

This is the high-performance, high-availability async **memcached client SDK**
for the example breeze platform (Rust / tokio). It implements both the text and
binary wire protocols and offers two explicitly separated access modes:

- **Mesh mode** (`src/sidecar/`): talk to the local breeze mesh agent,
  discovered from the sock registry files it publishes (`@mc:<port>@cs`);
  the mesh owns sharding and backend failover. (The *byMesh* path.)
- **Direct mode** (`src/direct/`): connect to memcached backends directly,
  with client-side shard routing (`direct::Shards`) using the same
  hash/distribution algorithms as the mesh (`direct::sharding`), plus the
  master/slave/L1 topology client `direct::HaClient`. (The *byTcp* path.)
On top of these, `src/service/` (feature `service`) ports the Java
commons-memcache template stack: `Cacheable`, `MemcacheServiceTemplate`
(primary/backup routing with a circuit breaker), `MemCacheTemplate`
(multi-tier backup with cascading reads and set-backs), `ShardedCache`, and
the Vintage config watcher. `src/client.rs` provides the mode-agnostic
`Client` enum. `src/cacheservice.rs` parses Vintage statics-config YAML.

The crate is also the workspace root; the load-test harness lives in
`tools/mc-bench` (with `bench.sh` / `bench_local.sh` driver scripts).

## Read First

- Backend engineering standards for AI coding work (required for any
  product-code change; sibling checkout of traffic-e2e):
  `../../traffic-e2e/plugins/ai-software-engineering/skills/backend-engineering/SKILL.md`
  — read it fully before changing product code, and follow its
  `references/code-organization.md` boundary and size rules during the work.

## Essential Commands

**Before every commit, formatting and tests must pass — no exceptions:**

```bash
cargo fmt --all
cargo test --all-features            # superset: unit + e2e (in-process fake server)
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Feature combinations worth knowing:

```bash
cargo test                            # default features
cargo test --features service         # + service template tests (incl. local-memcached IT)
```

Docker-dependent integration tests are `#[ignore]`-gated; run them
explicitly when touching that area:

```bash
cargo test --features service --test yaml_integration -- --ignored   # needs Docker
```

Load testing (see `tools/mc-bench/bench_local.sh` header for the full
fault-injection matrix):

```bash
cargo run -p mc-bench --release -- --direct 127.0.0.1:11211 -c 64 -n 1000000 get --verify
tools/mc-bench/bench_local.sh --ops 1000000 get                       # direct
MODE=service-yaml tools/mc-bench/bench_local.sh --ops 1000000 get     # master/slave/L1 from YAML
```

Benchmark data must be self-describing: seed values are `key || padding`
and runs use `--verify` so request/response mixups fail the run.

## Non-Negotiable Rules

- **Use the `backend-engineering` skill as the binding constraint for all
  product-code work**: before changing product code, read
  `../../traffic-e2e/plugins/ai-software-engineering/skills/backend-engineering/SKILL.md`
  and apply its standards (working method, resource budgets, resilience,
  verification) together with its
  `references/code-organization.md` rules. Treat the initial module boundary
  as a hypothesis and review the implemented responsibilities again before
  completion. Keep feature-local structural convergence in scope and
  unrelated legacy cleanup out of scope. If the sibling traffic-e2e checkout
  is unavailable, state that and apply this file's own rules as the floor.
- **Before every commit: run `cargo fmt --all` and the test suite; commit
  only when everything passes.** Push after the tests pass.
- Never force-push to `master` (the remote rejects it anyway).
- Keep async code on `tokio`.
- The binary protocol correlates every response to its request by opaque
  token; any timeout, I/O error, or opaque mismatch must drop the
  connection (`Error::Desynced` / `Object::take`), never return it to the
  pool with a poisoned read buffer.
- Connections to a changed mesh endpoint are drained via the shared
  per-directory scanner (`sidecar::scanner`); do not add per-client scan
  loops — one process may host ~1000 namespaces.
- Pool floor maintenance is global (`src/maintenance.rs`); do not add
  per-client background tasks for that purpose.
- Read-path degradations in `MemCacheTemplate` fall back to the next tier
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

Work directly on `master` only for this SDK's current incubation phase;
keep commits focused and avoid committing unrelated local edits. Before
pushing, confirm:

```bash
git status --short
git diff --check
```
