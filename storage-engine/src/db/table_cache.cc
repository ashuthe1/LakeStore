#include "table_cache.h"

namespace lakestore {

std::string TableCache::TableFileName(const std::string& dbname, uint64_t file_number) {
    return dbname + "/" + std::to_string(file_number) + ".ldb";
}

TableCache::TableCache(std::string dbname, const Options& options, int capacity)
    : dbname_(std::move(dbname)), options_(options),
      capacity_(capacity > 0 ? capacity : 1) {}

Status TableCache::GetReader(uint64_t file_number, std::shared_ptr<SSTableReader>* reader) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = map_.find(file_number);
    if (it != map_.end()) {
        // Refresh LRU position
        lru_.erase(it->second.lru_pos);
        lru_.push_front(file_number);
        it->second.lru_pos = lru_.begin();
        *reader = it->second.reader;
        return Status::OK();
    }

    SSTableReader* raw = nullptr;
    Status s = SSTableReader::Open(TableFileName(dbname_, file_number), file_number, &raw);
    if (!s.ok()) return s;

    std::shared_ptr<SSTableReader> shared(raw);
    lru_.push_front(file_number);
    map_[file_number] = Entry{shared, lru_.begin()};

    // Evict the least-recently-used handles over capacity. In-flight users
    // keep evicted readers alive through their own shared_ptr copies.
    while (map_.size() > capacity_) {
        uint64_t victim = lru_.back();
        lru_.pop_back();
        map_.erase(victim);
    }

    *reader = std::move(shared);
    return Status::OK();
}

Status TableCache::Get(const ReadOptions& opts, uint64_t file_number,
                       const Slice& internal_key, std::string* value,
                       TableLookup* result) {
    std::shared_ptr<SSTableReader> reader;
    Status s = GetReader(file_number, &reader);
    if (!s.ok()) {
        *result = TableLookup::kAbsent;
        return s;
    }
    return reader->Get(opts, internal_key, value, result);
}

Iterator* TableCache::NewIterator(const ReadOptions& opts, uint64_t file_number) {
    std::shared_ptr<SSTableReader> reader;
    Status s = GetReader(file_number, &reader);
    if (!s.ok()) return nullptr;
    return new TableIterator(reader.get(), reader, opts);
}

void TableCache::Evict(uint64_t file_number) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = map_.find(file_number);
    if (it != map_.end()) {
        lru_.erase(it->second.lru_pos);
        map_.erase(it);
    }
}

} // namespace lakestore
