# raft

A from-scratch implementation of the Raft consensus algorithm
([Ongaro & Ousterhout, 2014](https://raft.github.io/raft.pdf)) in pure Go,
following the paper's Figure 2 plus the standard production refinements.

## What's implemented

- **Leader election** with randomized timeouts and the §5.4.1 up-to-date-log
  voting restriction.
- **Log replication** with per-peer replicator goroutines (condition-variable
  driven — new entries are pushed immediately, not on the next heartbeat),
  fast conflict backtracking (§5.3 optimization: conflict term + first index
  hints), and the §5.4.2 current-term commit rule.
- **Leader no-op entry** on election, so committed-but-unapplied entries from
  previous terms surface without waiting for client traffic.
- **Persistence** on every state transition, split for performance:
  - `meta.bin` — term, vote, snapshot markers; tiny atomic rewrite.
  - `log.bin` — append-only, CRC-framed entries; appending N entries costs
    one fsync'd append. Truncations (rare) rewrite atomically. Torn or
    corrupt tails from crashes are detected and dropped on load.
- **Snapshot-based log compaction**: the application hands Raft a serialized
  state (`Snapshot(index, data)`); the log prefix is dropped, and followers
  that fall behind the compaction horizon are repaired via **InstallSnapshot**.
- Pluggable **Transport** (gRPC in `server/`, an in-memory partition-capable
  fake in the tests) and **Storage** (files, or in-memory for tests).

## Usage

```go
applyCh := make(chan raft.ApplyMsg, 256)
storage, _ := raft.NewFileStorage("./data/raft")
rf, _ := raft.New(myID, []uint64{1, 2, 3}, transport, storage, applyCh)

index, term, isLeader := rf.Start(command) // propose
for msg := range applyCh {                 // consume committed entries in order
    if msg.CommandValid { apply(msg.CommandIndex, msg.Command) }
    if msg.SnapshotValid { restore(msg.SnapshotIndex, msg.Snapshot) }
}
```

The consumer must drain `applyCh` promptly and treat deliveries idempotently
(after restarts, entries above the last snapshot are re-delivered).

## Testing

`go test ./raft/ -race` runs a 13-scenario harness modeled on the MIT
6.824/6.5840 battery: an in-process cluster whose transport supports
partitions, ~10% message loss, delays, and crash/restart with persistent
state. Scenarios include re-election, agreement with minority failures, no
agreement without quorum, concurrent proposals, divergent-log repair after
partitions (50-entry divergences), full-cluster restarts, unreliable
networks, snapshot compaction, and InstallSnapshot catch-up. Every apply is
cross-checked for agreement across nodes at every index.

## Known limitations

- Static membership — no joint-consensus reconfiguration.
- No PreVote: a partitioned node inflates terms and forces one election on
  rejoin (harmless, but noisy).
- Snapshots transfer in a single RPC rather than chunks.
