#pragma once

#include "../types.h"
#include "block.h"
#include "bloom_filter.h"
#include "../options.h"
#include "../iterator.h"
#include <string>
#include <memory>

namespace lakestore {

class LRUCache; // Forward declaration so we don't need to include the full cache header

struct ReadOptions {
    bool verify_checksums = false;
    bool fill_cache = true;
    LRUCache* block_cache = nullptr;
};

// Result of probing a single SSTable for a key. A tombstone is authoritative:
// the search must STOP and report NotFound rather than fall through to older
// levels that may still hold a stale value.
enum class TableLookup {
    kFound,   // value written to *value
    kDeleted, // newest visible version is a tombstone
    kAbsent   // no visible version in this table; keep searching
};

class SSTableReader {
public:
    static Status Open(const std::string& filename, uint64_t file_number,
                       SSTableReader** reader);
    ~SSTableReader();

    // internal_key must be an ENCODED internal key (user_key, snapshot, kTypeValue).
    Status Get(const ReadOptions& opts, const Slice& internal_key,
               std::string* value, TableLookup* result);

    // Iterates every entry (encoded internal keys) in internal-key order.
    // The reader must outlive the iterator; use TableCache/shared_ptr pinning.
    Iterator* NewIterator(const ReadOptions& opts) const;

    uint64_t FileSize() const { return file_size_; }
    uint64_t FileNumber() const { return file_number_; }

    // Read-only introspection: physical structure (data blocks, index,
    // bloom filter statistics) plus up to max_entries decoded entries.
    // Bypasses the block cache entirely.
    Status DebugJSON(std::string* out, size_t max_entries) const;

private:
    friend class TableIterator;
    SSTableReader() = default;

    Status ReadBlock(uint64_t offset, size_t size, Block** block) const;
    // Cache-aware block fetch.
    Status GetBlock(const ReadOptions& opts, uint64_t offset, size_t size,
                    std::shared_ptr<Block>* block) const;

    int fd_ = -1;
    uint64_t file_size_ = 0;
    uint64_t file_number_ = 0; // Cache identification — must be unique per file

    Block* index_block_ = nullptr;
    std::string filter_data_;
};

// Two-level iterator: walks the index block, lazily loading one data block
// at a time. Keeps the reader alive through a shared_ptr when constructed
// via TableCache.
class TableIterator : public Iterator {
public:
    TableIterator(const SSTableReader* reader, std::shared_ptr<const SSTableReader> pin,
                  const ReadOptions& opts);
    ~TableIterator() override = default;

    bool Valid() const override;
    void SeekToFirst() override;
    void Seek(const Slice& target) override;
    void Next() override;

    Slice key() const override;
    Slice value() const override;
    Status status() const override { return status_; }

private:
    void LoadDataBlock();
    void SkipEmptyBlocksForward();

    const SSTableReader* reader_;
    std::shared_ptr<const SSTableReader> pin_; // may be null when caller guarantees lifetime
    ReadOptions opts_;

    std::unique_ptr<Iterator> index_iter_;
    std::shared_ptr<Block> data_block_;
    std::unique_ptr<Iterator> data_iter_;
    Status status_;
};

} // namespace lakestore
