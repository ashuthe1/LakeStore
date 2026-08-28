# LakeStore Storage Engine

A LevelDB-style LSM-tree storage engine in C++17. Single-node, embeddable,
exposed to Go through a C API ([`src/c_api/`](src/c_api/)).

## Architecture

### Write path
1. `DBImpl::Put/Delete/Write` encodes the operations into a `WriteBatch`.
2. The batch gets a sequence-number block and is appended to the **WAL** as
   one record (`[fixed64 seq][fixed32 count][ops]`), CRC32C-protected, with
   group commit coalescing concurrent appends into one `write(2)`.
   `WriteOptions::sync` adds an fsync; `disable_wal` skips the log.
3. Operations are inserted into the **MemTable** — a custom SkipList with
   lock-free concurrent reads and mutex-serialized writes. Every entry is an
   `InternalKey` (user key + 56-bit sequence + type tag) enabling MVCC.
4. When the memtable exceeds `Options::write_buffer_size`, it is frozen, a
   **fresh WAL is started**, and a background thread flushes it to a Level-0
   **SSTable**. The old WAL is deleted once the flush commits.

### SSTables
- Prefix-compressed data blocks with restart points, an index block keyed by
  each block's last key, a **bloom filter** over user keys, and a
  fixed-width footer.
- All on-disk ordering goes through the internal-key comparator
  (user key ascending, sequence descending) — never raw `memcmp`.
- Readers are cached in a **table cache**; data blocks in a sharded
  **LRU block cache** (`Options::block_cache_size`).

### Compaction
- L0 files may overlap; when `Options::l0_compaction_trigger` files
  accumulate, all of L0 is merged with the overlapping L1 files.
- Levels ≥ 1 are non-overlapping and compact into the next level when they
  exceed `max_bytes_for_level_base * 10^(L-1)` bytes.
- Compaction drops shadowed versions and garbage-collects tombstones that
  cannot hide anything in deeper levels.

### Recovery
1. `CURRENT` names the live **MANIFEST**; replaying its `VersionEdit`
   records rebuilds the file-level state (which SSTable lives at which
   level) plus the sequence/file-number counters.
2. WAL files at or above the manifest's log boundary are replayed into a
   fresh memtable (restoring original sequence numbers), flushed to L0, and
   the obsolete logs are removed.
3. Every reopen writes a fresh manifest with a full snapshot and swaps
   `CURRENT` atomically.

### Reads & iteration
- `Get` probes memtable → immutable memtable → L0 (newest file first) → one
  candidate file per deeper level; a tombstone stops the descent.
- `NewIterator` returns a merged, MVCC-collapsed view over memtables and all
  SSTables at a consistent snapshot. Versions are immutable and reference
  counted, so reads never block flushes or compactions.

## Building & running

```sh
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(sysctl -n hw.ncpu)   # or -j$(nproc) on Linux

./lakestore_main    # tiny smoke-test playground
./lakestore_test    # 56 GoogleTest cases
./lakestore_bench   # Google Benchmark suite
```

The test suite covers every component in isolation (varint/fixed coding,
CRC32C test vectors, bloom filter FP rates, block round-trips, SSTable
lookups across block boundaries, WAL fragmentation/corruption handling, LRU
eviction, comparator laws) and the full engine (flush, compaction, manifest
recovery, WAL rotation, tombstones across flushes, snapshot iterators, a
concurrent reader/writer/scanner stress test, and 10k-key mixed workloads
with reopen). CI-worthy extras: the suite runs clean under ASan and TSan.

## Benchmarks (Apple Silicon, Release)

| Benchmark | Result |
|---|---|
| `BM_Write_AsyncWAL` | ~460K items/s |
| `BM_Write_SyncWAL` | ~79K items/s |
| `BM_Write_InMemoryOnly` | ~3.6M items/s |
| `BM_Read_MemTable_p99` | ~3.0M items/s, p99 0.6µs |
| `BM_Read_SSTables_p99` | ~1.2M items/s (warm cache) |
| `BM_Scan_FullDatabase` | ~12.6M entries/s |
