# Scion

English | [简体中文](README.zh-CN.md)

A server-first source control system: history, merges and rebases are computed on the server, and clients stay online and fetch data on demand. The protocol is language-neutral; the project ships a Rust server, `scion`, and a Rust client, `scn`.

![Scion standalone architecture](docs/design/images/arch-01-standalone.en.png)

<sub>Diagram sources: [`arch-01-standalone.en.svg`](docs/design/images/arch-01-standalone.en.svg) (English) · [`arch-01-standalone.svg`](docs/design/images/arch-01-standalone.svg) (Chinese). The diagram includes planned components (L2 cache, deriver, git-gateway, …); see [Status](#status) for what is implemented today.</sub>

## Documentation

**Guides**
- [Importing from git](docs/guide/git-import.md) (Chinese): import an existing git repository with its full history
- [Object storage](docs/guide/object-storage.md) (Chinese): SeaweedFS, MinIO and other S3-compatible services; provider detection and startup checks
- [Clusters and landing](docs/guide/cluster.md) (Chinese): land semantics (rebase, exactly once), multi-node mode with `--advertise-url`, failover and overload protection
- [Caching](docs/guide/cache.md) (Chinese): the regional cache `scion cache` (sharding, hedging, warm-up) and the local daemon `scn daemon`
- [Authentication and access control](docs/guide/auth.md) (Chinese): tokens, ACL policy, OIDC login, external issuers, OPA-compatible authorization, starhub-server integration
- [Redaction and data checks](docs/guide/redaction.md) (Chinese): removing leaked content (redact, purge), `scion admin fsck`, `scion admin lease ls`

**Design and specifications** (Chinese)
- Design docs: [server](docs/design/rust-server-architecture.md) · [client](docs/design/rust-client-architecture.md) · [starhub / CSGLite integration](docs/design/starhub-integration.md)
- Object format: [`SPEC.md`](SPEC.md), with golden test vectors in [`testdata/vectors/`](testdata/vectors/)
- Protocol: [`proto/scion/v1/`](proto/scion/v1/)

## Status

**M5 (production hardening) is in progress.** Done so far:
- **Authentication and access control:**
  - tokens: self-issued ES256 access tokens, PATs, and refresh tokens;
  - external identities, standards only: JWKS or HS256 issuers, RFC 7662 introspection and an OPA-compatible authorization webhook. starhub-server is supported through configuration alone;
  - ACL rules by subject, repo, action, ref and path;
  - `scn login` with the OIDC device flow, credentials stored in the keychain.
- **Redaction:**
  - redact leaked file contents: object ids and history stay the same, reads return `REDACTED`;
  - the regional cache and the local daemon stop serving them within 10 seconds;
  - land never merges redacted content into new commits;
  - purge deletes the bytes after a grace period, once every repository that holds the object has redacted it.
- **Operations:** `scion admin fsck` checks refs, commits, trees and files against the object index and object store; `scion admin lease ls` lists leases.

Still to come in M5: observability, YugabyteDB, Helm and chaos testing.

**M4 (cache layer) is complete**:
- **Regional cache `scion cache` (L2):**
  - storage: foyer memory plus NVMe;
  - rendezvous sharding across nodes, so the region fetches each object from the server once;
  - single-flight coalescing, plus hedged reads and circuit breakers toward the servers;
  - warm-up from WatchRefs that only walks changed trees.
- **Local daemon `scn daemon` (L1):**
  - a verified on-disk object cache;
  - all other RPCs forwarded transparently;
  - prefetch of refs the working copy follows;
  - started on demand by `scn`.
- **WatchRefs:** streams ref changes to subscribers (snapshot first, then each advance), resumable after disconnects.
- **Server read path:**
  - in-process object cache;
  - coalesced, concurrency-limited backend reads;
  - `--read-endpoint` advertises caches to clients.

The cache serves 99.5% of reads without the server and 1.53M objects/s per node; see [Performance](#performance).

Earlier milestones:
- M3 added server-side rebase, exactly-once lands, batched landing (885 lands/s on one ref), clusters and overload protection.
- M2 added history queries (p99 under 20 ms on 100k commits).
- M1 added the write path, Lite mode, the `scn` working copy and git import.

The remaining M5 work: OpenTelemetry and metrics, the YugabyteDB tier, Helm and release builds.

## Performance

Benchmarks run on one machine (Apple M5, 10 cores) with objects in memory; client, cache and server share the machine. Each row links the benchmark so you can rerun it (`cargo test --release -p <crate> --test bench -- --ignored --nocapture`).

| Area | Workload | Result | Target |
|---|---|---|---|
| Cache hit rate ([`server/cache/tests/bench.rs`](server/cache/tests/bench.rs)) | 200 clients check out the same 20,000 objects (1–4 KiB) from a cold cache | 99.5% of reads served without L3; each object fetched from L3 exactly once | ≥ 99% |
| Cache throughput (same) | 64 clients, warm cache, batches of 64 objects over gRPC | 1.53M objects/s per node | ≥ 200k/s |
| Cache throughput (same) | 64 clients, warm cache, one object per request | 164k requests/s per node | — |
| Checkout ([`client/daemon/tests/bench.rs`](client/daemon/tests/bench.rs)) | `scn clone` of 100,000 files in 1,000 directories | Through a warm daemon 5.3 s; creating the same files without any network takes 5.4 s on this disk, so the daemon adds no measurable overhead | < 5 s, disk permitting |
| Landing ([`server/land/tests/bench.rs`](server/land/tests/bench.rs)) | 2,000 lands on one ref from 200 concurrent clients | PostgreSQL 885/s (p99 379 ms); SQLite 1,921/s (p99 173 ms) | ≥ 200/s, p99 < 500 ms |
| History queries ([`server/query/tests/bench.rs`](server/query/tests/bench.rs)) | 100,000 commits: log, diff, ancestry, merge-base, blame | p99 < 20 ms on SQLite and PostgreSQL (cold blame 13.6 ms, `log --path` 15.6 ms) | p99 < 100 ms |

## Quick start

```sh
# Start a server in Lite mode (SQLite + a local directory, persisted) with a repository named "mono"
cargo run -p scion -- server --lite --data-dir ./.scion-data --repo mono

# Other backends: --meta mem|sqlite://PATH|postgres://…  --blobs mem|DIR|s3://bucket/prefix
# S3 settings: SCION_S3_ENDPOINT, SCION_S3_ACCESS_KEY_ID, SCION_S3_SECRET_ACCESS_KEY, SCION_S3_REGION,
# SCION_S3_PROVIDER=auto|minio|seaweedfs|aws|generic (SeaweedFS and MinIO are tested in CI; see docs/guide/object-storage.md)

# In another terminal: install scn and check the connection
cargo install --path client/cli
scn doctor

# Everyday workflow
scn clone mono && cd mono          # check out (ref main by default; empty repositories work too)
echo 'hello' > README.md
scn status                          # A README.md
scn commit -m "add readme" --author "Ada <ada@example.com>"
scn land                            # land on main; if main moved, the server rebases (three-way merge)
scn sync                            # pull others' changes; rebases a pending commit at file level
scn cat main:README.md              # read files without a working copy (add --repo outside one)
scn ls main:                        # list a directory; every command supports --json
scn log --path src -n 10            # history, newest first, optionally limited to a path
scn diff                            # uncommitted local changes (unified diff)
scn diff main                       # what the tip of main changed; `scn diff A B` compares two revisions
scn blame src/lib.rs                # the commit that last changed each line

# scn talks to the server through a local daemon (scn daemon), started on demand; SCION_NO_DAEMON=1 opts out
scn daemon status

# A regional cache in front of one or more servers; advertise it with scion server --read-endpoint
# (see docs/guide/cache.md)
cargo run -p scion -- cache --upstream http://127.0.0.1:7080 --listen 127.0.0.1:7090 --warm mono:main

# Import an existing git repository with its history, authors and timestamps
# (see docs/guide/git-import.md)
scn import git ~/src/my-repo --repo mono

# After upgrading a server that already has data, before starting it: repair lineage and backfill derived indexes (idempotent)
cargo run -p scion -- admin reindex --lite --data-dir ./.scion-data
```

## Repository layout

```
proto/scion/v1/      protocol definitions (.proto), language-neutral; buf lint / breaking checks
SPEC.md              object model and canonical encoding
testdata/vectors/    golden test vectors every implementation must pass

common/              shared by server and client (may depend only on common)
  protocol/          scion-proto       Rust code generated from the protos
  object/            scion-object      ObjectId, canonical encoding, Tree / Changeset (pure, no IO)
  flight/            scion-flight      single-flight request coalescing (server caches and the daemon)

server/              server side (may depend on common and lower server crates)
  store/             scion-store       MetaStore / BlobStore traits, in-memory backend, conformance suite
  store-sql/         scion-store-sql   SQLite / PostgreSQL metadata store
  store-blob/        scion-store-blob  local directory / S3-compatible object store
  auth/              scion-auth        tokens, external issuers (JWKS / HS256), introspection, OIDC, ACL
  tree/              scion-tree        incremental tree building
  merge/             scion-merge       server-side rebase: replay a delta onto a newer tree with three-way merge
  land/              scion-land        changeset creation, stack rebase, LandCoordinator, leases
  query/             scion-query       history queries (log / diff / blame / merge-base) and reindexing
  cache/             scion-cache       regional object cache (scion cache): foyer, rendezvous, hedging, warm-up
  api/               scion-api         gRPC services and server bootstrap
  scion/             scion             server binary (scion server / scion cache / scion admin)

client/              client side (may depend on common and client, never on server)
  sdk/               scion-client      Rust SDK
  wc/                scion-wc          working copy (clone / status / commit / land / sync)
  daemon/            scion-daemon      local L1 cache (scn daemon): object cache, transparent forwarding, prefetch
  gitimport/         scion-gitimport   git history import
  cli/               scion-cli         the scn binary

xtask/               maintenance tasks (cargo xtask layers: dependency direction check)
deploy/dev/          local development dependencies (SeaweedFS, PostgreSQL)
docs/guide/          user guides
docs/design/         design docs and diagrams
```

Directory names describe location and role; crate names carry a `scion-` prefix to avoid clashing with crates.io packages such as `object`. Dependency direction is enforced in CI by `cargo xtask layers`.

## Development

```sh
cargo test --workspace                                    # all tests
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask layers                                        # dependency direction check
cargo llvm-cov --workspace                                # coverage (cargo install cargo-llvm-cov)
SCION_BLESS=1 cargo test -p scion-object --test vectors   # regenerate golden vectors after adding some
docker compose -f deploy/dev/docker-compose.yml up -d     # SeaweedFS (S3) + PostgreSQL
```

**Testing requirements** ([server design doc §12.1](docs/design/rust-server-architecture.md)): every function with logic has unit tests covering the normal path, edge cases and every error branch. Core crates need at least 90% line coverage, and new or changed code in a pull request needs at least 90%; CI enforces both.

## License

Apache-2.0
