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

### Automated

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
  durability, snapshot catch-up, stale reads, inspector endpoints.

Individual suites:

```sh
make engine-test                          # C++ engine only
go test ./raft/ -race -v                  # raft battery, verbose
go test ./server/ -run TestClusterLeaderFailover -v
```

### Manual verification drills

Everything below runs against a local cluster. Small write buffers make
flushes/compactions happen fast enough to watch:

```sh
make build
WRITE_BUFFER=262144 make cluster-up      # 3 nodes; flush every 256KB
```

**1. Basic operations — put / get / delete / scan:**

```sh
./bin/lakectl put user:1 alice
./bin/lakectl put user:2 bob
./bin/lakectl get user:1                  # → alice
./bin/lakectl scan user: 10               # ordered listing
./bin/lakectl del user:1
./bin/lakectl get user:1                  # → (not found), exit code 1
./bin/lakectl status                      # roles, terms, commit/applied per node
```

**2. Replication — the write is on every node:** `commit` and `applied` in
`status` should match across all three nodes within a second, and a stale
read from any single node returns the value:

```sh
./bin/lakectl put replicated yes
./bin/lakectl status
./bin/lakectl --cluster 127.0.0.1:7102 get-stale replicated   # follower's local copy
```

**3. Leader crash / failover:** kill the node `status` shows as `Leader`
(say node 2), then keep using the cluster — reads and writes work after
~½s of re-election, and nothing written before the crash is lost:

```sh
./bin/lakectl put before-crash v1
kill $(cat data/node2.pid)
./bin/lakectl status                      # new leader, higher term
./bin/lakectl get before-crash            # → v1 (survived)
./bin/lakectl put after-crash v2          # cluster still accepts writes
```

**4. Crashed node catches up:** restart it and watch `applied` converge:

```sh
./bin/lakestored --id 2 --listen 127.0.0.1:7102 --debug-listen 127.0.0.1:8102 \
    --data-dir data/node2 --engine-write-buffer 262144 \
    --peers "1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103" >> data/node2.log 2>&1 &
echo $! > data/node2.pid
sleep 3 && ./bin/lakectl status           # node 2 back, same commit/applied
./bin/lakectl --cluster 127.0.0.1:7102 get-stale after-crash   # it has the write it missed
```

**5. No quorum = no writes (safety):** kill two of three nodes; writes must
*fail* rather than silently succeed on the survivor. Restart one node and
writes resume:

```sh
kill $(cat data/node1.pid) $(cat data/node3.pid)
./bin/lakectl put should-fail x           # errors/times out — correct!
```

**6. Full-cluster restart durability:**

```sh
./bin/lakectl put durable forever
make cluster-down && make cluster-up
sleep 3 && ./bin/lakectl get durable      # → forever
```

**7. Watch the storage engine work:** push enough data to force memtable
flushes and an L0→L1 compaction, then look at the files:

```sh
./bin/lakectl bench --n 8000 --concurrency 32
ls -lh data/node1/engine/                 # .ldb SSTables, one .log WAL, MANIFEST, CURRENT
```

Old WALs are deleted after each flush; `.ldb` files appear and are merged.
The inspector (next section) shows all of this live, decoded.

## Inspector UI — see where your data lives

Each node serves a **read-only web inspector** (`--debug-listen`, on by
default in the local cluster at http://127.0.0.1:8101/8102/8103). It reads
the node's live internals on every click — nothing is sampled or cached,
and it never touches the write path or block cache:

- **Cluster view** — every node's role, term, commit/applied index, engine
  sequence number; watch them converge after a failover.
- **Raft log, decoded** — each entry as `PUT key=value` / `DEL key` /
  `NO-OP`, with term, size, batching visible (one entry holding many
  commands), and uncommitted entries flagged.
- **MemTable / immutable memtable** — the actual internal keys: user key,
  MVCC sequence number, PUT vs DEL tombstone, value.
- **Levels & SSTables** — every `.ldb` file per level with its key range;
  **click a file to dump its contents** (decoded internal entries).
- **Files on disk** — the raw `engine/` and `raft/` directory listings with
  sizes: WALs, SSTables, MANIFEST, raft log/meta/snapshot.

A fun exercise: run `lakectl put k v1`, see it in the memtable; run a bench
to force a flush, see `k` move into an L0 file; `lakectl del k`, see the
tombstone in the memtable *above* the old value in L0; keep writing until
compaction merges them and both disappear. That's the whole LSM lifecycle,
observable. JSON endpoints for scripting: `GET /api/state`,
`GET /api/sst?file=N`.

## Benchmarking

### Storage engine (C++ microbenchmarks)

```sh
make bench            # = cd storage-engine/build && ./lakestore_bench
```

Suites and what they measure:

| Benchmark | What it exercises | Result (Apple Silicon) |
|---|---|---|
| `BM_Write_AsyncWAL` | WAL append + memtable insert | ~460K ops/s |
| `BM_Write_SyncWAL` | + fsync per write | ~79K ops/s |
| `BM_Write_InMemoryOnly` | pure SkipList (WAL off) | ~3.6M ops/s |
| `BM_Read_MemTable_p99` | memtable point reads | ~3.0M ops/s, p99 0.6µs |
| `BM_Read_SSTables_p99` | disk path: bloom→index→block+cache | ~1.2M ops/s, warm cache |
| `BM_Scan_FullDatabase` | merged iterator over memtable+SSTables | ~12.6M entries/s |

Useful flags: `./lakestore_bench --benchmark_filter=Read
--benchmark_min_time=2s --benchmark_repetitions=3`.

### Cluster (end-to-end, replicated)

```sh
make cluster-up
./bin/lakectl bench --n 20000 --concurrency 64                  # replicated writes
./bin/lakectl bench --n 20000 --concurrency 64 --read           # linearizable reads
./bin/lakectl bench --n 5000  --concurrency 1                   # single-client latency floor
```

Reports throughput and p50/p95/p99/max latency. Reference numbers, 3 nodes
on one machine:

| Operation | Throughput | p50 | p99 |
|---|---|---|---|
| Replicated writes (64 clients) | ~40K ops/s | 1.4ms | 2.7ms |
| Linearizable reads (64 clients) | ~72K ops/s | 0.6ms | 1.5ms |

Write throughput comes from batching at two levels: concurrent client
proposals coalesce into single Raft entries (one fsync + one replication
round per batch), and the Raft log itself is an append-only file, not a
rewritten blob. Watch it happen: run a bench and look at the raft log in
the inspector — entries carrying dozens of PUTs each.

Knobs that change the numbers: `--concurrency` (bigger batches),
`LAKESTORE_FULLFSYNC=1` (power-loss-proof fsync, ~10× slower writes),
`--engine-write-buffer` / `WRITE_BUFFER` (flush pressure), and
`--snapshot-threshold` (raft log compaction frequency).

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
