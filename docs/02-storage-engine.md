# The Storage Engine — a C++ LSM-Tree from Scratch

> How one machine stores key-value data durably and fast. Everything here
> lives under `storage-engine/src/`. No networking, no replication — that's
> docs 03/04. By the end you should be able to explain every file in the
> data directory and trace any operation through the code.

---

## 1. Why an LSM-tree?

The naive design — update a big on-disk B-tree in place — makes every write
a random disk write. Random writes are the slowest thing storage does.

The **Log-Structured Merge tree** (used by LevelDB, RocksDB, Cassandra,
HBase) turns all writes into *sequential* ones:

1. **Never modify data on disk.** Writes go to an in-memory table.
2. **Log first.** Before touching memory, append the write to a sequential
   log file (the WAL), so a crash loses nothing.
3. **Flush in bulk.** When the memory table is full, write it to disk as one
   immutable, sorted file (an SSTable) in a single sequential pass.
4. **Clean up in the background.** Periodically merge SSTables
   ("compaction") to throw away overwritten values and deleted keys.

The price: a read may need to look in several places (memory, then several
files). The engine spends most of its design budget making reads cheap
anyway — sorted files, sparse indexes, bloom filters, caches.

## 2. The component map

```
            Put/Get/Delete/Write/NewIterator
                        │
                 DBImpl (db/db_impl.*)  ← orchestrates everything
                        │
   ┌──────────┬─────────┼─────────────┬──────────────┐
   ▼          ▼         ▼             ▼              ▼
 WAL      MemTable   Manifest      TableCache    Compaction
(wal/)   (memtable/) + Version     (db/table_    (db/compaction.*)
  │          │       (db/manifest.*)  cache.*)        │
  │          │          │             │               │
  ▼          ▼          │             ▼               ▼
 *.log   SkipList       │        SSTableReader   SSTableWriter
                        │        (sstable/)      (sstable/)
                        ▼             │               │
                  MANIFEST-N,         ▼               ▼
                  CURRENT        Block, BloomFilter, *.ldb files
                                 LRUCache (cache/)
```

Foundations used by everything (`types.h`, `util/`):

- **`Slice`** — a `{pointer, length}` view of bytes. Passing data around
  without copying strings.
- **`Status`** — every operation returns one: `OK`, `NotFound`, `IOError`,
  `Corruption`. No exceptions across component boundaries.
- **`coding.h`** — varint32/64 (small integers in 1–5 bytes) and fixed32/64
  little-endian encoders. All file formats are built from these.
- **`crc32c.h`** — CRC32C checksums (table generated at startup). Every WAL
  record is checksummed so recovery can detect a torn write from a crash.

## 3. The most important idea: the InternalKey

The engine never stores your key directly. It stores an **internal key**:

```
  encoded = user_key  +  8 bytes: (sequence_number << 8 | type)
                                   ─────────56 bits────  ──8 bits──
  type: 1 = Put ("this key has this value")
        0 = Deletion ("this key was deleted") — a tombstone
```

Every write in the engine's life gets a globally increasing **sequence
number**. So overwriting a key doesn't replace anything — it adds a *newer
version*:

```
  ("user:1", seq=105, Put)    → "alice-v2"     ← newest
  ("user:1", seq=88,  Delete) →                ← tombstone
  ("user:1", seq=51,  Put)    → "alice-v1"     ← oldest
```

**Sort order** (`CompareInternalKeys` / `CompareEncodedInternalKeys` in
`types.h`): user key ascending, then sequence **descending** — the newest
version of a key sorts *first*. This single rule powers everything:

- **Reads = a seek.** To read `user:1` "as of now", seek to
  `("user:1", MY_SEQUENCE)`. The first entry you land on is the newest
  version visible to you. If it's a tombstone → NotFound. Done.
- **MVCC for free.** A reader that captured sequence 100 simply skips
  entries with seq > 100 — writers never disturb it (this is how iterators
  give a stable snapshot while writes continue).
- **Deletes are writes.** A `Delete` inserts a tombstone; the old value is
  physically removed only later, by compaction.

⚠️ One subtle trap this codebase learned the hard way: encoded internal keys
must **never** be compared with `memcmp`. The 8 tag bytes are little-endian
(byte order ≠ numeric order) and a short user key's tag bytes would be
compared against a longer key's characters (`"app"+tag` vs `"apple"+tag`).
Every on-disk structure (blocks, indexes, merges) goes through the
comparator function instead.

## 4. Component deep dives

### 4.1 WAL — the write-ahead log (`wal/wal.{h,cc}`)

**Purpose:** make a write durable *before* it is applied to memory, so a
crash never loses acknowledged data. It is a generic record log — the DB
writes batches into one (`N.log`), and the Manifest reuses the same code for
its own log.

**Physical format:** the file is a sequence of 32KB **blocks**. Each record
is stored as one or more *fragments*, each with a 7-byte header:

```
  [4B CRC32C][2B length][1B type]  payload...
  type: FULL | FIRST | MIDDLE | LAST   (records larger than a block are split)
```

Fragments never straddle a block boundary; if fewer than 7 bytes remain in a
block, they're zero-padded. This is exactly LevelDB's format, and it's what
makes recovery robust: read blocks, verify each CRC, and on the first bad
checksum — that's the torn tail of a crashed write — stop cleanly.

**Group commit:** concurrent appenders queue up; the first becomes the
*leader*, drains the whole queue, and does **one** `write()` for everyone
(and one fsync if requested). Followers just wait on a condition variable.
Ten threads writing concurrently ≈ the disk cost of one.

**What the DB puts in each record:** `[fixed64 sequence][fixed32 op_count]
[operations]` — so recovery can restore not just the data but the exact
sequence numbers (essential: after restart, new writes must get *higher*
sequences than recovered ones, or they would be invisible to reads).

### 4.2 MemTable + SkipList (`memtable/`)

**Purpose:** the sorted, in-memory buffer that absorbs all writes.

Why a **skip list** (not a hash map or `std::map`)? It's sorted (needed for
flushing and scans), O(log n), and — the real reason — supports **lock-free
concurrent reads**. A reader walks `next` pointers (atomic loads with
acquire semantics) while a writer links in new nodes (release stores under
the DB mutex). Readers never take a lock; a half-visible node is never
observable because the bottom-level link is published last.

Details worth knowing:

- Nodes get a random height (P=1/4 per level, max 12) — the probabilistic
  balancing that replaces tree rotations.
- Nodes are allocated with placement-new into raw memory sized exactly for
  their height (`sizeof(Node) + (height-1) * sizeof(atomic<Node*>)`).
- `MemTable::Get(LookupKey)` does the seek-and-inspect described in §3:
  found + Put → value; found + tombstone → *authoritative* NotFound (stop
  searching!); user key mismatch → not in this table, keep looking.
- Memory usage is tracked so `DBImpl` knows when to rotate
  (`Options::write_buffer_size`, default 64MB).
- MemTables are handed around as `shared_ptr` — a reader can hold one while
  the background thread flushes and drops it; the memory lives until the
  last user lets go.

### 4.3 SSTable — the on-disk format (`sstable/`)

**Purpose:** an immutable, sorted file of internal-key → value entries.
Written once by a flush or compaction, then only ever read.

```
  ┌─────────────────┐
  │ data block 0    │  ~4KB of sorted entries, prefix-compressed
  │ data block 1    │
  │ ...             │
  ├─────────────────┤
  │ filter block    │  bloom filter over all USER keys in the file
  ├─────────────────┤
  │ index block     │  one entry per data block:
  │                 │    key   = LAST key in that block
  │                 │    value = block's (offset, size)
  ├─────────────────┤
  │ footer (40B)    │  fixed64 ×4: filter/index handles + magic number
  └─────────────────┘
```

**Data blocks** (`block.cc`, `BlockBuilder` in `sstable_writer.cc`): entries
are `[shared][non_shared][value_len][key suffix][value]` — each key stores
only the bytes that differ from the previous key (*prefix compression*;
sorted keys share long prefixes). Every 16th entry is a full **restart
point**; the block ends with an array of restart offsets. Lookups binary
search the restart points, then scan forward decompressing at most 15 keys.

**Why the index maps each block's *last* key:** to find key K, seek the
index for the first entry with key ≥ K. That names the unique block whose
range could contain K. (An earlier version of this code keyed the index by
the *next* block's first key and stored the *previous* block's handle — a
scheme that fetches the wrong block for the boundary key. The tests in
`tests/components_test.cc` caught it; `SSTableTest.WriteReadManyBlocks`
exists so it can never come back.)

**Bloom filter** (`bloom_filter.cc`): a bitmap (10 bits per key) that
answers "is this user key *possibly* in this file?" — with ~1% false
positives and **zero false negatives**. One hash is computed per key and
double-hashing (`h += delta`) simulates k probes. A negative answer lets a
read skip a file without any disk I/O — crucial, because a Get may have to
consider many files.

**Reading** (`sstable_reader.cc`): `Get(internal_key)` = bloom check → index
seek → fetch that one data block (via the cache) → block seek → verify the
user key matches and inspect the type. Returns a three-way answer the read
path depends on: `kFound` / `kDeleted` (tombstone — stop searching older
files!) / `kAbsent` (keep looking). `TableIterator` is a two-level iterator:
walk the index, lazily load one data block at a time.

### 4.4 The caches (`cache/lru_cache.cc`, `db/table_cache.cc`)

- **Block cache** — a sharded (16×) LRU of decompressed data blocks, keyed
  by `(file_number, offset)`, default 256MB. Shards cut lock contention;
  entries are `shared_ptr<Block>` so eviction never invalidates a block a
  reader is still using.
- **Table cache** — an LRU of *open file handles* + their parsed index and
  bloom filter. Without it, every Get would `open()` a file and re-read its
  index. Also `shared_ptr`-based: compaction can delete a file while an
  iterator still reads the old one safely.

### 4.5 Manifest & Version — the file-level metadata (`db/manifest.*`)

The engine's state is "which SSTable files exist, at which level, covering
which key range". That must survive restarts too.

- **`Version`** — an immutable in-memory snapshot of that state: 7 levels
  (`files_[0..6]`), each a list of `FileMetaData{number, size, smallest,
  largest}`. Immutability is the concurrency story: a reader grabs
  `shared_ptr<const Version>` and works with a frozen view; any change
  (flush, compaction) builds a *copy*, applies a `VersionEdit`, and swaps
  the pointer.
- **`VersionEdit`** — a delta: "add file 12 to L0, delete files 7 and 9 from
  L1, log number is now 13, last sequence is N".
- **`Manifest`** — a WAL of encoded VersionEdits (`MANIFEST-N`), plus the
  counters (next file number, last sequence, WAL boundary). Every state
  change is appended + fsync'd *before* it takes effect. The file
  **`CURRENT`** contains the name of the live manifest and is replaced
  atomically (write temp → fsync → rename) — the classic pattern for
  atomically switching between metadata generations.
- On every open, a fresh manifest is started with a full **snapshot** of the
  current Version (so old manifests can be deleted and replay stays short).

One boundary rule worth knowing (`Version::GetOverlappingInputs` /
`FindFileInLevel`): "does file F contain key K" is decided on **user keys
only**. Internal-key comparison here is subtly wrong at the edges — a file
whose largest key is `(K, seq=42)` *does* contain K, yet sorts *before*
`(K, seq=0)` in internal order.

### 4.6 Compaction (`db/compaction.*`, driven from `db_impl.cc`)

Two kinds:

- **Minor** (`CompactMemTable`/`WriteLevel0Table`): frozen memtable → one
  sorted L0 file. L0 files can overlap each other (each is just a memtable
  dump).
- **Major** (`DoMajorCompaction`): pick a level (L0 when it hits 4 files;
  L≥1 when its total bytes exceed `10MB × 10^(L-1)`), take its files plus
  every overlapping file in the next level, merge-sort them all
  (`MergingIterator` — a min-heap over per-file iterators, heap-ordered by
  the internal comparator so newest versions pop first), and write fresh
  non-overlapping files into the next level, rotating output files at
  `target_file_size`.

While merging, `Compaction::ShouldDrop` discards:
- any version of a key older than a version already written (nothing can
  see it — the engine has no external snapshot API pinning it), and
- **tombstones**, but only if no *deeper* level might still hold an older
  value for that key — drop the tombstone too early and the old value
  "resurrects". (The check is conservative: file-range overlap in deeper
  levels keeps the tombstone.)

The result of a compaction is just a VersionEdit (delete inputs, add
outputs) — logged to the manifest, then the input files are unlinked.

### 4.7 DBImpl — the conductor (`db/db_impl.*`)

Owns the mutex, the memtables (`mem_`, `imm_`), the WAL, the current
Version, the caches, and **one background thread** that does all flushes and
compactions. Writers signal it; it signals back (`bg_cv_`) when it makes
room. Write stalls: if the immutable memtable is still flushing, or L0 has
≥ 12 files, incoming writes wait — backpressure, so reads never face
unbounded numbers of overlapping L0 files.

## 5. Data flow — every scenario traced

### 5.1 Put (and Delete — same path, different type byte)

```
Put("k","v")
 → encode into WriteBatch payload: [type][klen][k][vlen][v]
 → lock; MakeRoomForWrite (maybe wait / maybe rotate memtable, §5.3)
 → allocate sequence numbers (last_sequence += op_count)
 → WAL.AppendRecord([seq][count][payload])       ── sequential disk write
     (opts.sync ⇒ fsync; opts.disable_wal ⇒ skipped)
 → decode payload, insert each op into the SkipList with its sequence
 → unlock; return OK
```

### 5.2 Get

```
Get("k")
 → snapshot = current last_sequence; grab shared_ptrs to mem, imm, Version; unlock
   (the actual searching happens WITHOUT the lock — pointers keep everything alive)
 → probe mem        — found value → return it; found tombstone → NotFound
 → probe imm        — same
 → L0: every file whose [smallest,largest] user-key range covers "k",
       newest file first (they overlap!); TableCache.Get on each:
       kFound → value; kDeleted → NotFound; kAbsent → next file
 → L1..L6: levels don't self-overlap, so binary-search for THE one candidate
       file per level; probe it; same three-way logic
 → NotFound
```

The three-way `kDeleted` answer is what makes deletes correct: a tombstone
in a newer file must *hide* a value in an older one.

### 5.3 Memtable rotation and flush

```
write finds mem_ ≥ write_buffer_size:
 → create WAL #(n+1); imm_ = mem_; mem_ = new MemTable   (writes continue!)
 → background thread: iterate imm_ in order → SSTableWriter → new L0 .ldb (fsync)
 → manifest.LogAndApply(edit{add L0 file, log_number = n+1})   (fsync)
 → imm_ = nullptr; delete WAL #n and older     ← safe: its data is in the SSTable
 → wake stalled writers; maybe cascade into a major compaction
```

The WAL is rotated *with* the memtable so each log file corresponds to
exactly one memtable — the moment that memtable is safely an SSTable, its
log is garbage.

### 5.4 Crash recovery / reopen (`DBImpl::Open`)

```
1. read CURRENT → replay MANIFEST-N → rebuild Version + counters
     (now we know every SSTable, the last sequence, and the WAL boundary)
2. replay every .log file ≥ the boundary into a fresh memtable,
     restoring original sequence numbers; last_sequence = max seen
3. flush that memtable to L0 immediately; log a new WAL boundary
4. start a fresh manifest (full snapshot) + fresh WAL
5. delete obsolete files: old logs, old manifests, unreferenced .ldb files
```

Idempotent by construction: crash during recovery ⇒ next open replays the
same logs again.

### 5.5 Scan / iterator (`NewIterator`, `db/db_iter.h`)

Build one `MergingIterator` over: mem, imm, every L0 table, every deeper
table — all in internal-key order. `DBIter` wraps it and turns the
*internal* stream into the *user* view at a frozen snapshot sequence:

```
internal:  a/105/Put  a/51/Put  b/88/Del  b/12/Put  c/7/Put
                │        skip      │        skip      │
user view:   a="v105"          (b deleted)          c="v7"
```

Skip rules: entries newer than the snapshot; older versions of a user key
already emitted; every version at or below a tombstone. The iterator pins
its memtables/Version/table readers via shared_ptrs, so it stays valid and
consistent no matter what flushes or compactions happen meanwhile.

### 5.6 Lifecycle of a deleted key (the full story)

```
t1  Put("k","v")      → (k, 51, Put) in memtable
t2  flush             → (k, 51, Put) in L0 file A
t3  Delete("k")       → (k, 88, Del) in memtable        Get(k): sees 88 first → NotFound ✓
t4  flush             → (k, 88, Del) in L0 file B
t5  compaction A+B→L1 → sees 88(Del) then 51(Put):
                        51 is shadowed → dropped;
                        88 is a tombstone — deeper levels empty for k → dropped too
t6  L1 file contains no trace of "k". Space reclaimed.
```

## 6. The C API (`src/c_api/`)

A thin `extern "C"` wrapper so non-C++ code (our Go layer) can use the
engine: open/close with options, put/get/delete with a sync flag, atomic
write batches, snapshot iterators, and a flush call. Convention: functions
return `NULL` on success or a `malloc`'d error string (free with
`lakestore_free`). Doc 04 §2 shows the Go side.

## 7. How it's tested (`tests/`)

- `components_test.cc` — each piece in isolation: coding round-trips, CRC32C
  against published test vectors, bloom FP rates, block round-trips incl.
  restart points and the single-entry edge case, SSTable lookups across
  block boundaries, MVCC/tombstones at the file level, WAL fragmentation +
  torn-tail + corrupted-record recovery, LRU eviction, comparator laws
  (encoded vs struct comparison must agree — including the prefix trap).
- `db_test.cc` — the original CRUD/MVCC/recovery/concurrency suite.
- `db_disk_test.cc` — the disk path end-to-end with a tiny (16KB) write
  buffer so everything flushes and compacts: reopen chains, WAL cleanup,
  tombstones across flushes, snapshot-stable iterators, a 4-writer/3-reader/
  1-scanner concurrent stress test, 10k-key mixed workloads.
- The whole suite also runs clean under **AddressSanitizer** and
  **ThreadSanitizer** (`build-asan/`, `build-tsan/` CMake configs).
