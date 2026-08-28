#pragma once

#include "../types.h"
#include "../options.h"
#include "../wal/wal.h"
#include "../memtable/memtable.h"
#include "../cache/lru_cache.h"
#include "../sstable/sstable_reader.h"
#include "manifest.h"
#include "table_cache.h"
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <memory>

namespace lakestore {

struct WriteOptions {
    bool sync = false; // If true, fsync the WAL before returning
    bool disable_wal = false; // will use this for running some benchmarks
};

class DBImpl {
public:
    static Status Open(const Options& options, const std::string& dbpath, DBImpl** dbptr);
    ~DBImpl();

    // Prevent copy/move
    DBImpl(const DBImpl&) = delete;
    DBImpl& operator=(const DBImpl&) = delete;

    Status Put(const WriteOptions& opts, const Slice& key, const Slice& value);
    Status Delete(const WriteOptions& opts, const Slice& key);
    Status Get(const ReadOptions& opts, const Slice& key, std::string* value);
    Status Write(const WriteOptions& opts, WriteBatch* batch);

    // User-facing scan: yields user keys/values at a consistent snapshot,
    // with tombstones and stale MVCC versions collapsed. Caller owns it.
    lakestore::Iterator* NewIterator();

    // Iterator over the ACTIVE memtable's raw internal keys (tests/tooling).
    lakestore::Iterator* NewInternalIterator();

    // Blocks until every memtable is flushed and no compaction is pending.
    // Primarily for tests and orderly shutdown.
    Status FlushAll();

    // Read-only introspection (db_debug.cc): serializes internal state
    // (memtables, levels, caches) as JSON for the inspector UI. Uses the
    // same snapshot pinning as reads; never disturbs the data path.
    void DebugJSON(std::string* out);
    // Lists the (decoded) entries of one SSTable, bypassing the block cache.
    Status DebugSSTableJSON(uint64_t file_number, std::string* out);

    // Encode/decode a WriteBatch to/from a WAL record:
    // [fixed64 sequence][fixed32 op_count][ops payload]
    static void EncodeBatchRecord(SequenceNumber seq, uint32_t count,
                                  const std::string& payload, std::string* record);
    static bool DecodeBatchRecord(const Slice& record, SequenceNumber* seq,
                                  uint32_t* count, Slice* payload);

private:
    DBImpl(const Options& options, const std::string& dbpath);

    Status Recover();

    static uint32_t CountOps(const WriteBatch* batch);
    static void DecodeAndInsert(MemTable* mem, SequenceNumber seq, const Slice& payload);

    // Waits/rotates until the active memtable can accept the write.
    // Called with mutex_ held.
    Status MakeRoomForWrite(std::unique_lock<std::mutex>& lock);

    // Background tasks
    void MaybeScheduleCompaction(); // mutex_ held
    void BackgroundThreadMain();
    Status BackgroundCompaction(std::unique_lock<std::mutex>& lock);
    Status CompactMemTable(std::unique_lock<std::mutex>& lock);
    Status DoMajorCompaction(std::unique_lock<std::mutex>& lock, int level);
    Status WriteLevel0Table(MemTable* mem, VersionEdit* edit);
    void RemoveObsoleteFiles(); // mutex_ held

    // Concurrency control
    std::mutex mutex_;
    std::condition_variable bg_cv_;      // signaled when background state changes
    std::condition_variable bg_work_cv_; // wakes the background thread
    bool bg_work_pending_ = false;
    Status bg_error_; // Catches errors from background thread
    bool shutting_down_ = false;
    std::thread bg_thread_;

    Options options_;
    std::string dbname_;

    // Memory and Disk structures. Memtables are refcounted because reads
    // hold them across the mutex being released.
    std::shared_ptr<MemTable> mem_;
    std::shared_ptr<MemTable> imm_;
    std::unique_ptr<WAL> wal_;
    std::unique_ptr<Manifest> manifest_;
    std::shared_ptr<const Version> current_version_;
    std::unique_ptr<LRUCache> block_cache_;
    std::unique_ptr<TableCache> table_cache_;

    // State
    std::atomic<SequenceNumber> last_sequence_{0};
    uint64_t log_file_number_ = 0;     // log backing mem_
    uint64_t imm_log_file_number_ = 0; // log backing imm_ (obsolete after flush)
};

} // namespace lakestore
