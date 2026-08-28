# Raft — Consensus from Scratch

> How several machines agree on one ordered history, even while machines
> crash and the network drops messages. This documents the `raft/` package —
> a from-scratch implementation of the
> [Raft paper](https://raft.github.io/raft.pdf) — with LakeStore-specific
> engineering decisions called out. Read §1–§5 for the algorithm, §6–§9 for
> how this codebase implements and tests it.

---

## 1. The problem consensus solves

Copy a database to 3 machines and you immediately have a question: when two
clients write concurrently, and a machine dies mid-write, **whose version is
the truth?**

Raft's answer: stop thinking about replicating *state*, replicate a **log**.
If every machine executes the *same commands in the same order*, starting
from the same empty state, they end at the same state (a "replicated state
machine"). So the only thing machines must agree on is the log's contents —
and Raft is the protocol for that agreement. Its contract:

> If any server has applied the log entry at index *i*, no other server will
> ever apply a different entry at index *i*.

...while remaining available as long as a **majority** (2 of 3, 3 of 5) of
servers can talk to each other. Majorities are the magic: any two majorities
share at least one server, so agreed-upon information can never be "voted
out" by a group that didn't know it.

## 2. The moving parts

Every node is in one of three states:

```
                 times out, starts election        wins majority vote
   FOLLOWER ──────────────────────────► CANDIDATE ────────────► LEADER
      ▲                                    │                       │
      │   sees a higher term, or a valid   │ sees current leader   │
      └────────────────────────────────────┴───────────────────────┘
```

- **Terms** — a logical clock. Each election starts a new term; there is at
  most one leader per term. Every RPC carries the sender's term; any node
  that sees a higher term than its own immediately becomes a follower and
  adopts it. Terms are how stale leaders discover they've been replaced.
- **Log entries** — `{Index, Term, Data}`. Index = position; Term = when a
  leader created it; Data = an opaque command (LakeStore puts batched KV
  writes in here — doc 04).
- Three RPCs: **RequestVote** (elections), **AppendEntries** (replication +
  heartbeats), **InstallSnapshot** (catch up a hopelessly-behind follower).

## 3. Leader election

Followers expect a heartbeat from the leader every 75ms. If a follower hears
nothing for a randomized 250–500ms (the **election timeout**), it assumes
the leader died:

1. Increment `currentTerm`, become candidate, vote for itself, send
   `RequestVote(term, myLastLogIndex, myLastLogTerm)` to everyone.
2. Each node grants at most **one vote per term** (persisted! see §7) —
   this is what makes two leaders in one term impossible.
3. Majority of votes → leader. Higher term seen → step down. Timeout with no
   winner (split vote) → new randomized timeout, try again with term+1.
   Randomization makes repeated splits improbable.

**The election restriction (§5.4.1)** — the one non-obvious rule: a node
refuses to vote for a candidate whose log is *less up-to-date* than its own
(compare last entry's term, then log length). Since a committed entry lives
on a majority, and a candidate needs a majority of votes, at least one voter
has every committed entry — and that voter blocks any candidate missing one.
**Elected leaders therefore always contain all committed entries.**

## 4. Log replication

The leader is the single serialization point:

```
client → leader: append entry (index i, term t) to own log, fsync
leader → all:    AppendEntries(prev=(i-1, term of i-1), entries=[...], leaderCommit)
follower:        does MY log match `prev`?  yes → append, fsync, ACK
                                            no  → reject (see repair below)
leader:          entry on a majority (counting itself)? → COMMITTED
                 → apply to state machine, answer client,
                   piggyback new commit index on next AppendEntries
followers:       learn commit index → apply the same entries, same order
```

**The consistency check** (`prev` matching) is inductive: if a follower's
log matches the leader's at `prev`, and it appends the same entries after
it, the logs match through the new end. So a successful AppendEntries
*proves* log agreement up to that point.

**Repair after divergence.** A follower that crashed or was partitioned may
have missing entries, or *extra uncommitted* ones from a dead leader. The
leader tracks `nextIndex` per follower; on rejection it backs up and
retries, and the follower **truncates** the conflicting suffix and takes the
leader's version. Only *uncommitted* entries can ever be truncated —
committed ones are on every possible leader (§3). Rather than backing up one
entry at a time (painful after long partitions), the rejection carries a
hint — the conflicting term and that term's first index — letting the leader
jump the whole term in one step (`ConflictTerm`/`ConflictIndex` in
`raft.go`; the paper's §5.3 fast-backtracking optimization).

**Two commit subtleties this codebase hit in practice:**

- **A leader only commits by counting replicas of entries from its *own*
  term** (paper §5.4.2 — Figure 8 shows how counting old-term entries loses
  data). Consequence: a new leader of a *quiet* cluster can't commit
  anything — the previous term's tail sits committed-on-a-majority but
  never *marked* committed. Fix: every new leader immediately appends an
  empty **no-op entry** in its own term (`becomeLeader` in `raft.go`).
  Committing the no-op commits everything before it. Our integration test
  `TestClusterLeaderFailover` failed with reads returning NotFound-after-
  failover until this was added — a nice demonstration of why it matters.
- Replication counting uses `matchIndex` sorted across peers; the median is
  what a majority provably stores (`maybeAdvanceCommit`).

## 5. Log compaction & snapshots

The log grows forever; replaying a year of writes on restart is absurd. So
periodically the *application* hands Raft a **snapshot** — "here is my full
state as of index N" (`Raft.Snapshot(index, data)`). Raft persists it and
drops every entry ≤ N. Two consequences:

- Restart = load snapshot + replay only the short remaining log tail.
- If a follower is so far behind that the entries it needs were compacted
  away, plain AppendEntries can't repair it. The leader sends
  **InstallSnapshot** instead: the follower replaces its entire state with
  the snapshot and continues from index N. (In LakeStore the snapshot is a
  serialized dump of the KV state — doc 04 §6.)

## 6. How this implementation is structured (`raft.go`)

One mutex guards all state. Four kinds of goroutines cooperate:

```
ticker (every 20ms)         — follower/candidate: election deadline passed → startElection
                              leader: 75ms since last heartbeat → signal replicators
replicator (one per peer)   — sleeps on a condition variable; woken by Start()
                              or the ticker; pushes AppendEntries (or a snapshot
                              if the peer is behind the compaction horizon);
                              keeps sending while there's more; on failure just
                              waits for the next heartbeat tick (built-in retry
                              + backoff, no hot loops)
applier                     — sleeps until commitIndex > lastApplied; delivers
                              committed entries **in order** on applyCh
RPC handlers                — HandleRequestVote / HandleAppendEntries /
                              HandleInstallSnapshot, called by the transport
```

Design points worth explaining to someone reading the code:

- **Condition-variable replication, not polling.** `Start()` signals every
  replicator, so a new entry is on the wire within microseconds; heartbeats
  are just the same signal on a 75ms timer (an AppendEntries with no
  entries).
- **The lock is dropped around every RPC** (`sendEntriesTo` builds its
  arguments under the lock, releases it, calls the transport, reacquires).
  Rule after reacquiring: *re-validate everything* — still leader? same
  term? not killed? — because the world may have changed. Stale replies are
  simply dropped.
- **Log indexing with a moving floor.** After compaction the in-memory log
  starts at `lastSnapshotIndex+1`; helpers (`termAt`, `entriesFrom`)
  translate logical indexes so the rest of the code never does slice
  arithmetic.
- **Interfaces at the edges.** `Transport` (send the three RPCs) and
  `Storage` (persistence) are interfaces. Production uses gRPC + files;
  tests swap in an in-memory network with partitions and an in-memory store
  — same Raft code, zero test hooks inside it.

## 7. Persistence — and why it's shaped this way

Raft's promises rest on three things surviving a crash *before* the node
answers an RPC: `currentTerm`, `votedFor`, and the log. (Vote twice in one
term because you forgot the first vote → two leaders → split brain.)

The naive approach — serialize all state into one blob and rewrite it on
every change — is what this project did first, and it produced **169
writes/s** end-to-end: every appended entry rewrote the entire log *and*
paid a full fsync (macOS `F_FULLFSYNC`, 20–30ms). The redesign
(`storage.go`) splits the state by write pattern:

```
raft/meta.bin      term, votedFor, snapshot markers — tiny, changes rarely
                   → atomic rewrite (tmp + fsync + rename)
raft/log.bin       the entries — appended constantly
                   → append-only file: [4B len][4B CRC32C][gob(entry)]...
                   → appending N entries = one write + one fsync
                   → truncation (conflict repair) or compaction = atomic rewrite (rare)
raft/snapshot.bin  opaque snapshot bytes — atomic rewrite
```

Recovery (`Load`) reads records until EOF or the first bad CRC — a torn
final record from a crash mid-append is detected and dropped, exactly like
the engine's WAL. If a crash lands between the snapshot files and the log
rewrite, `restore()` reconciles: drop log entries the snapshot already
covers; if there's a gap, drop the suffix (it's committed data — it
re-replicates from the leader harmlessly).

Durability knob: plain `fsync(2)` by default; `LAKESTORE_FULLFSYNC=1` opts
into macOS `F_FULLFSYNC` (power-loss-proof, ~10× slower) — the same
trade-off SQLite exposes.

## 8. Scenario walkthroughs

**Normal write** — see §4. One round trip to a majority; the slowest
minority member never delays anyone.

**Leader crashes.** Followers time out (≤500ms), one wins an election (only
a fully-up-to-date node *can* win, §3), appends its no-op, and the moment
the no-op commits, every entry the old leader committed is applied
everywhere. Clients retry against the new leader (doc 04 §4). Uncommitted
entries from the old leader may be lost — that's correct: they were never
acknowledged.

**Network partition (the split-brain test).** Old leader isolated with a
minority: it still *accepts* writes but can never commit them (no
majority). Majority side elects a new leader (higher term) and moves on.
When the partition heals, the old leader sees the higher term, steps down,
and the consistency check truncates its never-committed entries, replacing
them with the majority's history. No acknowledged write is lost — verified
by `TestRejoinWithDivergentLog` and `TestBackup` (50-entry divergences).

**Node restarts.** Load meta + log + snapshot; rejoin as follower; the
leader's `nextIndex` probing finds where its log ends and streams the rest —
or InstallSnapshot if the gap was compacted away.

**Term inflation (known quirk).** A partitioned node keeps timing out and
incrementing its term; on rejoin its inflated term forces one harmless
re-election. The standard fix (PreVote) is deliberately out of scope and
noted in the README.

## 9. How it's tested (`harness_test.go`, `raft_test.go`)

The harness runs a whole cluster in one process with a fake transport that
can **partition nodes, drop ~10% of messages, add delays, and lose replies
after delivery** (the cruelest case — the RPC *happened* but the sender
thinks it didn't), plus crash/restart that reboots a node from its persisted
storage. A collector drains every node's applyCh and asserts the core
invariant on every delivery: *no two nodes ever apply different commands at
the same index*.

The 13 scenarios (modeled on MIT 6.824's grader): initial election,
re-election, no leader without quorum, basic agreement, agreement with a
failed follower, *no* agreement without a majority (and recovery after),
concurrent proposals, divergent-log repair, 50-entry backup/repair,
persistence across full-cluster restarts, agreement under an unreliable
network, snapshot compaction, InstallSnapshot catch-up, and
leader-completeness across crash-restart cycles. All run under `go test
-race`, repeatedly.
