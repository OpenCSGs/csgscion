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

**Design and specifications** (Chinese)
- Design docs: [server](docs/design/rust-server-architecture.md) · [client](docs/design/rust-client-architecture.md) · [starhub / CSGLite integration](docs/design/starhub-integration.md)
- Object format: [`SPEC.md`](SPEC.md), with golden test vectors in [`testdata/vectors/`](testdata/vectors/)
- Protocol: [`proto/scion/v1/`](proto/scion/v1/)

## Status

**M3 (high concurrency) is complete**:
- **Server-side rebase:** a stale commit is rebased with three-way merge when it lands, and conflicts are reported with base/ours/theirs ids.
- **Exactly-once lands:** idempotent request ids, plus one landing per change id per ref.
- **LandCoordinator:** lands to the same ref are batched.
- **Multi-node clusters:** per-ref leader leases and forwarding.
- **Overload protection:** load shedding and per-repo write rate limits.

One ref sustains 885 lands/s on PostgreSQL (p99 379 ms). Linearizability tests cover concurrent lands and killing the leader mid-run.

Earlier milestones: M2 added history queries (log / blame / diff / merge-base, p99 under 20 ms on 100k commits). M1 added the write path, Lite mode, the `scn` working copy and git import.

Next is M4: the cache layer.

## Performance

Benchmarks run on one machine (Apple M5, 10 cores) with objects in memory; client, cache and server share the machine. Each row links the benchmark so you can rerun it (`cargo test --release -p <crate> --test bench -- --ignored --nocapture`).

| Area | Workload | Result | Target |
|---|---|---|---|
| Cache hit rate ([`server/cache/tests/bench.rs`](server/cache/tests/bench.rs)) | 200 clients check out the same 20,000 objects (1–4 KiB) from a cold cache | 99.5% of reads served without L3; each object fetched from L3 exactly once | ≥ 99% |
| Cache throughput (same) | 64 clients, warm cache, batches of 64 objects over gRPC | 1.61M objects/s per node | ≥ 200k/s |
| Cache throughput (same) | 64 clients, warm cache, one object per request | 166k requests/s per node | — |
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

server/              server side (may depend on common and lower server crates)
  store/             scion-store       MetaStore / BlobStore traits, in-memory backend, conformance suite
  store-sql/         scion-store-sql   SQLite / PostgreSQL metadata store
  store-blob/        scion-store-blob  local directory / S3-compatible object store
  tree/              scion-tree        incremental tree building
  merge/             scion-merge       server-side rebase: replay a delta onto a newer tree with three-way merge
  land/              scion-land        changeset creation, stack rebase, LandCoordinator, leases
  query/             scion-query       history queries (log / diff / blame / merge-base) and reindexing
  api/               scion-api         gRPC services and server bootstrap
  scion/             scion             server binary (scion server --lite / --meta / --blobs)

client/              client side (may depend on common and client, never on server)
  sdk/               scion-client      Rust SDK
  wc/                scion-wc          working copy (clone / status / commit / land / sync)
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
