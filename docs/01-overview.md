# LakeStore — The Big Picture

> Start here. This document explains **what LakeStore is, why each layer
> exists, and how the pieces fit together**. The other three documents then
> go deep into each layer:
>
> 1. **[01-overview.md](01-overview.md)** — this file
> 2. **[02-storage-engine.md](02-storage-engine.md)** — the C++ LSM-tree engine (how one machine stores data)
> 3. **[03-raft.md](03-raft.md)** — the Raft consensus layer (how many machines agree)
> 4. **[04-distributed-kv.md](04-distributed-kv.md)** — the server that glues them into a distributed database

---

## 1. What problem are we solving?

We want a **key-value database** — you `Put(key, value)`, later you
`Get(key)` — with three hard requirements:

1. **Durability** — if the process crashes or the machine loses power, no
   acknowledged write is lost.
2. **Performance** — writes and reads should be fast even with millions of
   keys, far more data than fits in RAM.
3. **Fault tolerance** — if a whole *machine* dies, the database keeps
   serving with no data loss.

No single component gives you all three. So LakeStore is built as two
independent, individually testable systems composed together:

| Requirement | Solved by | Language | Directory |
|---|---|---|---|
| Durability + performance on ONE machine | LSM-tree storage engine | C++17 | `storage-engine/` |
| Agreement across MANY machines | Raft consensus | Go | `raft/` |
| Gluing them into one service | gRPC server + state machine | Go (cgo) | `server/`, `engine/` |

## 2. The stack, top to bottom

```
                      you / lakectl / client library
                                  │  gRPC: Put / Get / Delete / Scan / Status
                                  ▼
 ┌─────────────────────────── lakestored (one node) ───────────────────────────┐
 │                                                                             │
 │   kvService (server/server.go)                                              │
 │      │ writes: propose to Raft          │ reads: leader check + barrier     │
 │      ▼                                  ▼                                   │
 │   Raft (raft/raft.go) ◄──── gRPC ────► other nodes' Raft                    │
 │      │ "this entry is committed" (applyCh)                                  │
 │      ▼                                                                      │
 │   FSM (server/fsm.go)  — applies committed commands, exactly once           │
 │      │ cgo calls (engine/engine.go → storage-engine/src/c_api)              │
 │      ▼                                                                      │
 │   LakeStore engine (C++) — WAL, MemTable, SSTables, compaction              │
 │      │                                                                      │
 │      ▼                                                                      │
 │   disk: data/nodeN/engine/*  +  data/nodeN/raft/*                           │
 └─────────────────────────────────────────────────────────────────────────────┘
```

A cluster is 3 (or 5) identical `lakestored` processes. Exactly one is the
**leader** at any time; it orders all writes. The others are **followers**
holding identical copies. Clients can connect to any node — the client
library follows redirects to the leader automatically.

## 3. Life of a write (the 30-second version)

`lakectl put user:1 alice` →

1. The client sends `Put` to the node it believes is the leader (redirected
   if wrong).
2. The server **batches** this command with any other commands that arrived
   at the same moment, and hands the batch to Raft as one log entry.
3. Raft appends the entry to its **durable log** (fsync), sends it to the
   followers, who also append + fsync, and reply.
4. Once a **majority** (2 of 3) have the entry, it is **committed**. Raft
   announces this on a channel.
5. The **FSM** applies the entry: it writes `user:1 → alice` into the C++
   storage engine (plus a marker recording "I have applied entry #N").
6. Only now does the client get `OK`. At this point the write exists on a
   majority of machines and survives any single-machine failure.

Life of a read: the leader checks it is still leader, waits until it has
applied everything committed so far (the *read barrier*), then reads
straight from its local engine — no network round trip through the log.

## 4. Why this division of labor?

- **The engine knows nothing about the network.** It is a fast, crash-safe,
  single-node library — the same role RocksDB plays inside CockroachDB, or
  LevelDB inside Chrome. You can use it standalone.
- **Raft knows nothing about storage.** It agrees on an *ordered log of
  opaque byte blobs*. What the blobs mean is the application's business.
  You could reuse this Raft package for a replicated queue or lock service.
- **The server is the only place that knows both.** It translates client
  requests into log entries, and committed log entries into engine writes.

This separation is what makes the system testable: the engine has 56 unit +
integration tests (run under ASan/TSan), Raft has a 13-scenario
partition-and-crash simulator (run under Go's race detector), and the server
has real 3-node cluster tests over gRPC — each layer proven independently
before composing them.

## 5. Guarantees, precisely

- **Writes are linearizable.** Every write goes through the Raft log; a
  client acknowledgment means the write is committed on a majority and
  applied on the leader.
- **Default reads are leader reads with a barrier** (apply catches up to the
  commit index observed at the start of the read). This gives
  read-your-writes and monotonic reads. In the rare window where a deposed
  leader hasn't noticed a new election yet (bounded by one election timeout,
  ≤ 500ms), it could serve a slightly stale value — full ReadIndex/leases
  would close this and are noted as future work.
- **`get-stale`** reads from any node with no checks: fastest, may lag.
- **Durability floor**: Raft's log is fsync'd before commit. The engine's
  own writes are *not* fsync'd on apply — if a node crashes, the engine's
  WAL replays what it had, and anything newer is re-applied from the Raft
  log (applies are idempotent — see doc 04).

## 6. Where things live on disk

```
data/node1/
├── engine/                 ← the C++ storage engine's world (doc 02)
│   ├── CURRENT             which MANIFEST is live
│   ├── MANIFEST-000007     log of file-level state changes
│   ├── 000012.log          write-ahead log for the active memtable
│   └── 000009.ldb ...      SSTables (the actual data), by level
└── raft/                   ← Raft's world (doc 03)
    ├── meta.bin            current term, who we voted for, snapshot markers
    ├── log.bin             the replicated log (append-only)
    └── snapshot.bin        last full-state snapshot (for log compaction)
```

## 7. Try it in two minutes

```sh
make build          # C++ engine + Go binaries into ./bin
make cluster-up     # 3 nodes on 127.0.0.1:7101-7103

./bin/lakectl put hello world
./bin/lakectl get hello
./bin/lakectl status                  # who is leader, commit/applied indexes
./bin/lakectl bench --n 20000 --concurrency 64

kill $(cat data/node2.pid)            # murder a node — cluster keeps working
./bin/lakectl get hello

make cluster-down
```

And the full test matrix: `make test`.

## 8. Measured performance (Apple Silicon laptop)

| Layer | Metric | Result |
|---|---|---|
| Engine | writes with WAL (async) | ~460K ops/s |
| Engine | reads from SSTables (warm cache) | ~1.2M ops/s |
| Engine | full scan | ~12.6M entries/s |
| Cluster (3 nodes) | replicated writes, 64 clients | ~40K ops/s, p50 1.4ms |
| Cluster (3 nodes) | linearizable reads, 64 clients | ~72K ops/s, p50 0.6ms |

The cluster write number is the interesting one: a naive implementation
(one fsync + one replication round *per command*) measured **169 ops/s** on
the same hardware. Two changes got the 238× improvement — an append-only
Raft log (doc 03 §7) and command batching (doc 04 §5). Keep those in mind
as you read; they're good case studies of where distributed-system
performance actually comes from.
