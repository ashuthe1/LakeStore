# Architecture Diagrams (Mermaid)

Render these at [mermaid.live](https://mermaid.live) to export PNG/SVG for
the README, or let GitHub render the fenced blocks directly.

## 1. Storage engine (C++ LSM-tree)

```mermaid
flowchart TB
    subgraph API["Public API — db_impl.h / C API"]
        PUT["Put / Delete / Write(batch)"]
        GET["Get(key)"]
        ITER["NewIterator — snapshot scan"]
    end

    subgraph CORE["DBImpl — orchestrator: mutex, sequence numbers, write stalls"]
        WB["WriteBatch encoder<br/>seq | op count | ops"]
        LK["LookupKey<br/>(user_key, snapshot seq)"]
        DBI["DBIter — MVCC collapse:<br/>skip old versions and tombstones"]
    end

    subgraph MEMORY["In-memory"]
        MT["Active MemTable<br/>SkipList — lock-free reads,<br/>serialized writes"]
        IMM["Immutable MemTable<br/>(frozen, being flushed)"]
    end

    subgraph DURABILITY["Durability"]
        WAL["WAL — 32KB blocks, CRC32C,<br/>group commit"]
        LOGF[("N.log files<br/>(one per memtable)")]
    end

    subgraph BG["Background thread"]
        FLUSH["Minor compaction:<br/>flush imm to L0 SSTable"]
        COMPACT["Major compaction:<br/>level L + overlapping L+1,<br/>MergingIterator + ShouldDrop<br/>(GC old versions and tombstones)"]
    end

    subgraph DISK["SSTables on disk (.ldb)"]
        L0[("Level 0<br/>overlapping files")]
        LN[("Levels 1..6<br/>sorted, non-overlapping,<br/>10x size budget per level")]
        SST["SSTable layout:<br/>prefix-compressed data blocks<br/>+ bloom filter + index block<br/>(last key per block) + footer"]
    end

    subgraph CACHES["Caches"]
        TC["TableCache<br/>open readers: index + bloom pinned"]
        BC["BlockCache<br/>sharded LRU of data blocks"]
    end

    subgraph METADATA["Metadata"]
        VER["Version — immutable snapshot:<br/>files per level + key ranges"]
        MAN["Manifest — fsync'd log<br/>of VersionEdits"]
        CUR[("CURRENT → MANIFEST-N<br/>(atomic rename)")]
    end

    PUT --> WB
    WB -->|"1: append + optional fsync"| WAL
    WAL --> LOGF
    WB -->|"2: insert with seq"| MT
    MT -->|"write_buffer_size exceeded:<br/>rotate memtable + new WAL"| IMM
    IMM --> FLUSH
    FLUSH --> L0
    L0 -->|"4+ files trigger"| COMPACT
    COMPACT --> LN
    FLUSH -->|"VersionEdit"| MAN
    COMPACT -->|"VersionEdit"| MAN
    MAN --> CUR
    MAN -->|"apply edit, swap pointer"| VER

    GET --> LK
    LK -->|"1"| MT
    LK -->|"2"| IMM
    LK -->|"3: L0 newest first,<br/>then one file per deeper level"| TC
    VER -.->|"which files cover this key"| LK
    TC --> SST
    TC --> BC
    L0 --- SST
    LN --- SST

    ITER --> DBI
    DBI -->|"merge mem + imm + every table"| TC
```

## 2. Raft + complete distributed KV store

```mermaid
flowchart TB
    subgraph CLIENTS["Clients"]
        CLI["lakectl / Go client library<br/>leader-following, redirect hints,<br/>retry with backoff through elections"]
    end

    subgraph NODE1["lakestored — node 1 (leader)"]
        KV["gRPC KV service<br/>Put / Get / Delete / Scan / Status"]
        PROP["Proposer<br/>batches concurrent commands<br/>into ONE Raft entry"]
        RB["Read barrier<br/>leader check + wait until<br/>lastApplied ≥ commitIndex"]
        RAFT1["Raft core<br/>election · replication · commit ·<br/>no-op on election · snapshots"]
        RSTORE[("raft/<br/>meta.bin · log.bin (append-only,<br/>CRC-framed) · snapshot.bin")]
        APPLY["applyLoop<br/>consumes committed entries,<br/>notifies waiting clients"]
        FSM["FSM — idempotent apply:<br/>applied-index marker written in<br/>the same atomic engine batch"]
        CGO["engine/ — cgo binding<br/>over the C API"]
        ENG["C++ LSM storage engine<br/>(see storage-engine diagram)"]
        EDISK[("engine/<br/>WAL · SSTables ·<br/>MANIFEST · CURRENT")]
    end

    subgraph NODE2["node 2 (follower)"]
        RAFT2["Raft"]
        FSM2["FSM → engine"]
    end

    subgraph NODE3["node 3 (follower)"]
        RAFT3["Raft"]
        FSM3["FSM → engine"]
    end

    CLI -->|"gRPC"| KV
    KV -->|"write"| PROP
    PROP -->|"Start(batched entry)"| RAFT1
    KV -->|"consistent read"| RB
    RB -->|"local Get after barrier"| FSM

    RAFT1 -->|"append + fsync<br/>BEFORE replying"| RSTORE
    RAFT1 <-->|"AppendEntries / RequestVote /<br/>InstallSnapshot (gRPC)"| RAFT2
    RAFT1 <-->|"same RPCs"| RAFT3
    RAFT2 -->|"committed"| FSM2
    RAFT3 -->|"committed"| FSM3

    RAFT1 -->|"committed entries, in order<br/>(applyCh)"| APPLY
    APPLY --> FSM
    APPLY -->|"notify waiter: OK to client"| KV
    FSM --> CGO
    CGO --> ENG
    ENG --> EDISK
    FSM -->|"every N applied entries:<br/>serialize state, compact the log"| RAFT1
```
