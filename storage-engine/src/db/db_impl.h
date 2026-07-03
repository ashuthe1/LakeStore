#pragma once

#include "../types.h"
#include "../options.h"
#include "../wal/wal.h"
#include "../memtable/memtable.h"
#include "../cache/lru_cache.h"
#include "../sstable/sstable_reader.h"
#include "manifest.h"
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

namespace lakestore {

struct WriteOptions {
    bool sync = false; // If true, fsync the WAL before returning
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

    lakestore::Iterator* NewInternalIterator();

private:
    DBImpl(const Options& options, const std::string& dbpath);

    Status Recover();
    Status RecoverLogFile(uint64_t log_number, SequenceNumber* max_sequence);

    uint32_t CountOps(const WriteBatch* batch);
    void DecodeAndInsert(MemTable* mem, SequenceNumber seq, const std::string& payload);

    // Background tasks
    void MaybeScheduleCompaction();
    void BGWork();
    void BackgroundCall();
    Status BackgroundCompaction();
    Status WriteLevel0Table(MemTable* mem, VersionEdit* edit);

    // Concurrency control
    std::mutex mutex_;
    std::condition_variable bg_cv_;
    bool bg_compaction_scheduled_ = false;
    Status bg_error_; // Catches errors from background thread
    bool shutting_down_ = false;

    Options options_;
    std::string dbname_;
    
    // Memory and Disk structures
    MemTable* mem_ = nullptr;
    MemTable* imm_ = nullptr;
    std::unique_ptr<WAL> wal_;
    std::unique_ptr<Manifest> manifest_;
    std::unique_ptr<Version> current_version_;
    std::unique_ptr<LRUCache> block_cache_;

    // State
    std::atomic<SequenceNumber> last_sequence_{0};
    uint64_t log_file_number_ = 0;
};

} // namespace lakestore