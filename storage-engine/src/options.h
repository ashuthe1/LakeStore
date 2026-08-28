#pragma once
#include <cstdint>
#include <cstddef>

namespace lakestore {
struct Options {
    // --- SSTable layout ---
    size_t block_size = 4096;
    int block_restart_interval = 16;
    int bloom_bits_per_key = 10;

    // --- Memtable / flush ---
    // The active memtable is rotated to immutable and flushed to an L0
    // SSTable once it exceeds this many bytes.
    size_t write_buffer_size = 64 * 1024 * 1024;

    // --- Compaction ---
    // Number of L0 files that triggers an L0 -> L1 compaction.
    int l0_compaction_trigger = 4;
    // Number of L0 files at which incoming writes are stalled until the
    // backlog drains (protects read amplification from growing unbounded).
    int l0_stall_trigger = 12;
    // A level L (>= 1) is compacted into L+1 once its total size exceeds
    // max_bytes_for_level_base * 10^(L-1).
    uint64_t max_bytes_for_level_base = 10 * 1024 * 1024;
    // Target size for a single SSTable produced by compaction.
    uint64_t target_file_size = 2 * 1024 * 1024;

    // --- Caching ---
    // Capacity of the shared block cache in bytes. 0 disables the cache.
    size_t block_cache_size = 256 * 1024 * 1024;
    // Maximum number of SSTable readers kept open in the table cache.
    int table_cache_size = 512;
};
}
