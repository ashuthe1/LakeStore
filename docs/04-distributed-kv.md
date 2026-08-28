# The Distributed KV Layer — Gluing Raft to the Engine

> Doc 02 gave us a fast, durable single-node engine. Doc 03 gave us a way
> for machines to agree on an ordered log of opaque commands. This document
> explains the layer that turns those two into one distributed database:
> `engine/` (the cgo binding), `server/` (the node), `client/` + `cmd/`
> (how you talk to it). It also covers the end-to-end failure scenarios and
> the two batching tricks the performance depends on.

---

## 1. The replicated state machine pattern

The entire design reduces to one sentence:

> **The Raft log is the database.** The C++ engine on each node is just a
> *materialized view* of that log — the result of applying entries 1..N in
> order.

Everything else follows. Writes must go through the log (that's what makes
them replicated and durable). Reads can be served from the local view (fast)
*if* you can argue the view is current enough. Recovery means "make the view
catch up with the log". Helping a lagging node means "ship it the view"
(a snapshot) when shipping the log would be too much.

```
        client Put ──► kvService ──► proposer (batches) ──► raft.Start()
                                                                │
                     ┌── the Raft machinery replicates it ──────┘
                     ▼
        applyCh ──► applyLoop ──► FSM.Apply ──► C++ engine
                     │                             (cgo)
                     └──► notify the waiting client: "committed & applied, OK"
```

## 2. The cgo binding (`engine/engine.go`)

Go can't call C++ directly, so the engine exposes a C API
(`storage-engine/src/c_api/`) and `engine/engine.go` wraps it:

- cgo directives compile against `src/` headers and link
  `storage-engine/build/liblakestore_lib.a` (plus `-lc++`/`-lstdc++`) —
  which is why `make engine` must run before any Go build.
- Error convention: C functions return `NULL` or a `malloc`'d error string;
  the Go side converts to `error` and frees the C memory.
- Byte-slice handling: Go pointers are pinned across the call with
  `runtime.KeepAlive`; returned values are *copied* into Go memory
  (`C.GoBytes`) then freed, so no C memory ever escapes into Go's world.
- Exposes exactly what the FSM needs: `Open/Close/Flush`, `Put/Get/Delete`,
  **`WriteBatch`** (atomic multi-op — the FSM's workhorse), and `Iterator`
  (snapshot scans — the basis of Raft snapshots).

## 3. The FSM (`server/fsm.go`) — applying the log exactly once

`FSM.Apply(index, payload)` is where a committed log entry becomes engine
state. Two invariants make it correct:

**Idempotence.** Raft re-delivers entries after a restart (its replay floor
is the last snapshot, not the last applied entry). So the FSM persists an
**applied-index marker** — a reserved engine key
`\x00lakestore:applied_index` — written **in the same atomic engine batch**
as the entry's effects. The marker and the data can never disagree, even if
the process dies between two syscalls. On `Apply`: `index ≤ lastApplied` →
skip silently. On startup: read the marker back from the engine — that's
the whole recovery protocol.

**No double-fsync.** The engine write uses `sync=false`. Durability already
happened — in the Raft log, *before* commit. If the node crashes, whatever
the engine's own WAL didn't capture is simply re-applied from the Raft log
(idempotently). One durable log in the system, not two.

The `\x00` prefix marks a reserved key namespace: the server rejects user
keys starting with a zero byte, and `Scan` filters them out.

## 4. Life of a request, end to end

### Write path (`Put`/`Delete`)

```
1  client picks its cached leader address (or a random node)
2  kvService.Put validates, hands the command to propose()
3  the proposer goroutine batches it with concurrent commands (§5)
   and calls raft.Start(batch) → assigned log index i, term t
4  the proposer registers a waiter: index i → [response channels]
5  Raft replicates; entry i commits; applyLoop receives it
6  FSM.Apply writes the batch + marker into the engine
7  notifyWaiters(i): every batched client gets its answer.
   Term check: if the entry at index i carries a term ≠ t, a different
   leader's entry landed there — OUR command was lost in a leadership
   change → respond errNotLeader and the client retries. (Retrying a
   lost-in-election write is safe; retrying is the client's decision.)
8  gRPC reply OK
```

If this node isn't the leader (`Start` refuses), the reply is a gRPC
`FailedPrecondition` carrying `leader_id`/`leader_addr` — the client parses
the hint, re-targets, and retries (`client/client.go: do()`), with backoff
through elections. Timeouts (default 5s) cover the no-quorum case.

### Read path (`Get`)

Reads don't go through the log (that would double every read's latency).
Instead, `linearizableRead()` runs a **read barrier** on the leader:

```
1  am I the leader? no → redirect
2  readIndex = my commitIndex          (everything acknowledged so far)
3  wait until fsm.lastApplied ≥ readIndex
4  still the leader? no → redirect
5  read from the local engine
```

Why this works: any acknowledged write has index ≤ commitIndex, and step 3
guarantees the local view includes it — so you always read your own writes,
and reads are monotonic. The honest caveat (also in doc 01): between steps
4 and 5 a partition could depose us without our knowledge for up to one
election timeout; a full ReadIndex round (confirming leadership with a
majority heartbeat) or leader leases would close that window — documented
future work. `Get` with `allow_stale` (CLI: `get-stale`) skips all of it and
reads any node's local view.

`Scan` uses the same barrier, then iterates the engine (which is itself a
consistent MVCC snapshot — doc 02 §5.5) skipping reserved keys.

## 5. Batching — where the throughput comes from

First benchmark of the naive path: **169 writes/s**, p50 93ms. Each command
paid its own Raft entry: leader log fsync + replication round + follower
fsync, serially. The fix is one idea applied at two layers:

**Proposal batching (`server/server.go: proposer()`).** Client commands go
into a channel; a single goroutine takes one, then drains whatever else is
queued (up to 512), encodes them all into **one** Raft entry, and registers
one waiter group for all of them. 64 concurrent clients → a handful of
entries per round instead of 64. Amortization: one append, one fsync, one
AppendEntries round for the lot. This is the same group-commit shape as the
engine's WAL (doc 02 §4.1) and Raft's own storage — the pattern shows up at
every layer of a storage system.

**Append-only Raft log** (doc 03 §7) so each of those entries costs an
append, not a full-state rewrite.

Result on the same hardware: **~40,000 writes/s, p50 1.4ms** (238×), and
~72,000 barrier reads/s. The FSM applies a batched entry as one atomic
engine `WriteBatch`, so batching also *reduces* engine work.

`Command` encoding (`server/command.go`) is deliberately trivial:
`[1B op][4B klen][key][4B vlen][value]`, concatenated for batches, big-
endian lengths. A zero-length entry payload is a Raft leader no-op (doc 03
§4) — the FSM just advances its marker so read barriers can pass it.

## 6. Snapshots: keeping the Raft log finite

Every `SnapshotThreshold` (default 8192) applied entries, `maybeSnapshot`:

1. `FSM.SnapshotBytes()` — iterate the engine (a stable MVCC snapshot even
   while writes continue) and serialize all pairs + the applied index.
2. `raft.Snapshot(appliedIndex, bytes)` — Raft persists it and drops the
   log prefix (doc 03 §5).

When a follower is behind the compaction horizon, Raft ships that blob via
InstallSnapshot and it arrives on the follower's applyCh as a snapshot
message. `FSM.Restore` then **replaces** its state: close the engine, wipe
its directory, reopen fresh, write all pairs + marker in one batch. Wiping
matters — a key deleted since the follower went dark must not survive the
restore. Stale snapshots (index ≤ lastApplied) are ignored, which also makes
the snapshot/command ordering on applyCh safe.

Honest limitation: the snapshot is one in-memory blob and one RPC (send/recv
caps set to 64MB) — fine for this project's scale; a production system
streams chunks.

## 7. Failure scenarios, end to end

| Scenario | What happens |
|---|---|
| **Follower dies** | Nothing visible. Leader commits with the remaining majority; the dead node's replicator just fails quietly. On restart it reloads engine + raft state and the leader streams the missing entries (or a snapshot). Verified: `TestClusterNodeRestartCatchesUp`, `TestClusterSnapshotCatchUp`. |
| **Leader dies** | Clients see errors for ~½ second; a follower wins the election, appends its no-op, and the client library re-discovers the leader and retries. No acknowledged write is lost. Verified: `TestClusterLeaderFailover` (and live: kill -9 on a real cluster mid-benchmark). |
| **In-flight write during leader death** | If it committed → it survives (new leader must have it, doc 03 §3). If not → the waiter gets a term mismatch or timeout → client retries against the new leader. |
| **Whole cluster restarts** | Each node: engine recovers (doc 02 §5.4), raft reloads meta+log+snapshot, FSM re-applies any log suffix beyond its marker, election, business as usual. Verified: `TestClusterFullRestartDurability`. |
| **Network partition** | Minority side (even with the old leader on it): accepts nothing durable — writes time out. Majority side elects, continues. On heal: doc 03 §8, divergent entries truncated. |
| **Node crashes mid-apply** | Raft log has the entry (it committed); the engine batch either fully landed (marker says applied → skipped on replay) or didn't (marker unchanged → re-applied). The atomic marker is the linchpin. |

## 8. The server binary, client, and CLI

- **`cmd/lakestored`** — flags: `--id`, `--listen`, `--data-dir`,
  `--peers 1=host:port,2=...` (static membership; must include itself),
  `--snapshot-threshold`. One gRPC listener serves both the client KV
  service and the inter-node Raft transport.
- **`client/`** — leader-following: caches the leader address, parses
  redirect hints, falls back to random nodes, retries with backoff through
  elections. `Put/Get/GetStale/Delete/Scan/Status`.
- **`cmd/lakectl`** — thin CLI over the client: `put get get-stale del scan
  status bench`; cluster from `--cluster` or `LAKESTORE_CLUSTER`. `bench`
  runs a concurrent load generator and reports throughput + p50/p95/p99.
- **`scripts/run-cluster.sh` / `stop-cluster.sh`** — local 3-node cluster
  (ports 7101–7103, data + logs + pidfiles under `./data`); wrapped by
  `make cluster-up` / `cluster-down`. Set `WRITE_BUFFER=262144` to make
  engine flushes frequent enough to watch.
- **Inspector UI** (`server/debug.go` + `server/ui/`) — every node can serve
  a read-only debug page (`--debug-listen`, ports 8101–8103 locally) showing
  the decoded Raft log, memtable contents, SSTables per level (click a file
  to dump it), and the raw files on disk. It pins state exactly like a
  normal read and bypasses the block cache, so inspecting a node under load
  changes nothing. JSON endpoints: `/api/state`, `/api/sst?file=N`.

## 9. Testing this layer (`server/integration_test.go`)

These are *real* clusters — three `Node`s in one process, each with its own
C++ engine and gRPC listener on a random port, talking over actual sockets:
CRUD + ordered scans, replication convergence on all three engines, leader
failover with data-survival checks, follower restart catch-up, full-cluster
restart durability, snapshot-based catch-up (threshold lowered to 64 to
force it), and stale reads from every node. Plus unit tests for the cgo
binding (`engine/engine_test.go`): CRUD, batches+reopen durability,
iterators, empty values.

## 10. If you extend this — the natural next steps

1. **ReadIndex or leader leases** — close the stale-read window (§4).
2. **PreVote** — stop rejoining nodes from inflating terms (doc 03 §8).
3. **Membership changes** — joint consensus, so nodes can be added/removed
   live instead of the static `--peers` list.
4. **Chunked snapshot streaming** — replace the single-RPC blob.
5. **Client sessions + request IDs** — exactly-once semantics for client
   retries (today a timed-out-but-actually-committed write that the client
   retries applies twice; for a pure KV overwrite that's benign, but it
   matters the moment you add counters or transactions).
