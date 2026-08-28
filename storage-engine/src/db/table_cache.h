#pragma once

#include "../types.h"
#include "../options.h"
#include "../sstable/sstable_reader.h"
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace lakestore {

// Caches open SSTableReader handles keyed by file number so that Gets and
// iterators don't pay an open()+index-read on every probe. Readers are held
// by shared_ptr, so evicting one while an iterator still uses it is safe —
// the reader is destroyed when the last user releases it.
class TableCache {
public:
    TableCache(std::string dbname, const Options& options, int capacity);

    // Returns (and caches) the reader for file_number.
    Status GetReader(uint64_t file_number, std::shared_ptr<SSTableReader>* reader);

    // Probe one SSTable for internal_key. See TableLookup for semantics.
    Status Get(const ReadOptions& opts, uint64_t file_number,
               const Slice& internal_key, std::string* value, TableLookup* result);

    // Iterator over the whole table; keeps the reader pinned for its lifetime.
    Iterator* NewIterator(const ReadOptions& opts, uint64_t file_number);

    // Drop the cached handle after the file is deleted by compaction.
    void Evict(uint64_t file_number);

    static std::string TableFileName(const std::string& dbname, uint64_t file_number);

private:
    std::string dbname_;
    Options options_;
    size_t capacity_;

    std::mutex mu_;
    // LRU list of file numbers, most-recent at front, plus the handle map.
    std::list<uint64_t> lru_;
    struct Entry {
        std::shared_ptr<SSTableReader> reader;
        std::list<uint64_t>::iterator lru_pos;
    };
    std::unordered_map<uint64_t, Entry> map_;
};

} // namespace lakestore
