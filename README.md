# LakeStore

A distributed key-value store built from scratch: a **LevelDB-style LSM-tree
storage engine in C++17** underneath a **from-scratch Raft consensus layer in
Go**, exposed as a replicated KV service over **gRPC** with a CLI client.

```
                        ┌─────────────┐
              lakectl → │  gRPC (KV)  │  Put / Get / Delete / Scan / Status
                        └──────┬──────┘
   ┌───────────────────────────┼───────────────────────────────┐
   │ lakestored (node 1)       │ node 2            node 3      │
   │  ┌──────────────┐   Raft RPCs (gRPC)   ┌──────────────┐   │
   │  │  Raft (Go)   │◄────────────────────►│  Raft (Go)   │…  │
   │  │ election ·   │                      └──────────────┘   │
   │  │ replication ·│  committed entries                      │
   │  │ snapshots    ├────────────┐                            │
   │  └──────────────┘            ▼                            │
   │                     ┌────────────────┐                    │
   │                     │  FSM (cgo)     │                    │
   │                     │ ┌────────────┐ │                    │
   │                     │ │ LakeStore  │ │  C++ LSM engine:   │
   │                     │ │ engine C++ │ │  WAL · MemTable ·  │
   │                     │ └────────────┘ │  SSTables · Levels │
   │                     └────────────────┘                    │
   └───────────────────────────────────────────────────────────┘
```

**New to the project?** The [`docs/`](docs/) directory has a four-part
deep-dive written for readers starting from zero: [the big picture](docs/01-overview.md) →
[the storage engine](docs/02-storage-engine.md) → [Raft](docs/03-raft.md) →
[the distributed KV layer](docs/04-distributed-kv.md).

## Components

| Layer | Language | What it does |
|---|---|---|
| [`storage-engine/`](storage-engine/) | C++17 | LSM-tree: WAL with group commit, lock-free-read SkipList memtable, SSTables with bloom filters + block cache, leveled compaction, MVCC snapshots, MANIFEST recovery |
| [`raft/`](raft/) | Go | Raft consensus from scratch: leader election, log replication, append-only durable log, snapshot-based log compaction, InstallSnapshot for lagging followers |
| [`engine/`](engine/) | Go (cgo) | Go binding over the engine's C API |
| [`server/`](server/) | Go | Replicated state machine: batched proposals, linearizable reads, snapshot/restore, gRPC services |
| [`client/`](client/), [`cmd/lakectl`](cmd/lakectl/) | Go | Leader-following client library and CLI |
| [`cmd/lakestored`](cmd/lakestored/) | Go | The server daemon |

## Quick start

Prerequisites: CMake ≥ 3.20, a C++17 compiler, Go ≥ 1.22.

```sh
make build          # builds the C++ engine, then lakestored + lakectl into ./bin
make cluster-up     # starts a local 3-node cluster on 127.0.0.1:7101-7103

./bin/lakectl put hello world
./bin/lakectl get hello
./bin/lakectl scan "" 100
./bin/lakectl status
./bin/lakectl bench --n 20000 --concurrency 64

make cluster-down
```

Kill any node (including the leader) and the cluster keeps serving; restart
it and it catches up from the leader's log — or, if the log was compacted,
via a full snapshot install.

## Testing

```sh
make test
```

runs the full matrix:

- **C++ engine** — 56 GoogleTest cases: unit tests for every component
  (coding, CRC32C, bloom, blocks, SSTables, WAL, cache, comparators) plus
  end-to-end flush/compaction/recovery/iterator tests and a multi-threaded
  stress test. Also verified clean under AddressSanitizer and ThreadSanitizer.
- **Raft** — a 13-scenario battery in the style of MIT 6.824 (run with
  `-race`): elections, partitions, divergent-log repair, persistence across
  crashes, unreliable networks with message loss, snapshot install.
- **cgo binding** — CRUD, batches, iterators, reopen durability.
- **Cluster integration** — real 3-node clusters over gRPC: replication to
  all nodes, leader failover, node restart catch-up, full-cluster restart
  durability, snapshot catch-up, stale reads.

## Benchmarks

Storage engine (`make bench`, Apple Silicon, release build):

| Benchmark | Throughput | Notes |
|---|---|---|
| Sequential writes (WAL, async) | ~460K ops/s | |
| Writes (WAL disabled) | ~3.6M ops/s | pure memtable |
| Random reads (memtable) | ~3.0M ops/s | p99 ≈ 0.6µs |
| Random reads (SSTables) | ~1.2M ops/s | warm block cache |
| Full scan | ~12.6M entries/s | merged view |

Cluster, 3 nodes on one machine (`lakectl bench`):

| Operation | Throughput | p50 | p99 |
|---|---|---|---|
| Replicated writes (64 clients) | ~40K ops/s | 1.4ms | 2.7ms |
| Linearizable reads (64 clients) | ~72K ops/s | 0.6ms | 1.5ms |

Write throughput comes from batching at two levels: concurrent client
proposals coalesce into single Raft entries (one fsync + one replication
round per batch), and the Raft log itself is an append-only file, not a
rewritten blob.

## Design notes & guarantees

- **Writes** are linearizable: they go through the Raft log and are
  acknowledged only after commit + local apply.
- **Reads** (default) are served by the leader after a read barrier (apply
  catches up to the commit index at read start). A deposed leader can serve
  a stale read only within one election timeout; use `get-stale` for
  explicitly cheap follower reads. (Full ReadIndex/lease reads are the
  natural next step.)
- **Durability**: entries are fsync'd to the Raft log on a majority before
  commit. The engine applies them *without* fsync — its WAL replays after a
  crash, and anything lost below that is re-applied from the Raft log
  (applies are idempotent via a replicated applied-index marker stored
  atomically with each batch).
  On macOS, plain `fsync(2)` is used by default; set `LAKESTORE_FULLFSYNC=1`
  for `F_FULLFSYNC` power-loss guarantees at a large latency cost.
- **Log compaction**: after `--snapshot-threshold` applied entries (default
  8192), each node serializes its full KV state, hands it to Raft, and the
  log prefix is dropped. Followers that fall behind the compaction horizon
  are repaired with InstallSnapshot.
- **Known limitations** (deliberate scope): static membership (no joint
  consensus), no PreVote (a rejoining node can bump terms), snapshots are
  monolithic rather than streamed in chunks, and the storage engine runs
  single-node compactions on one background thread.

## Repository layout

```
storage-engine/   C++ LSM-tree engine (CMake; tests + benchmarks)
raft/             Raft consensus package (pure Go, no deps)
engine/           cgo binding for the engine's C API
server/           node wiring: FSM, gRPC services, batching, snapshots
client/           leader-following Go client
cmd/lakestored    server binary
cmd/lakectl       CLI client
api/              protobuf definitions + generated stubs
scripts/          local cluster helpers
```
