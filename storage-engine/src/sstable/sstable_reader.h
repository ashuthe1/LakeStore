#pragma once
#include "../types.h"
#include "block.h"
#include "bloom_filter.h"
#include "../options.h"
#include <string>

namespace lakestore {

struct ReadOptions {
    bool verify_checksums = false;
    bool fill_cache = true;
    LRUCache* block_cache = nullptr;
};

class SSTableReader {
public:
    static Status Open(const std::string& filename, SSTableReader** reader);
    ~SSTableReader();

    // Point query
    Status Get(const ReadOptions& opts, const Slice& key, std::string* value);
    
    uint64_t FileSize() const { return file_size_; }

private:
    SSTableReader() = default;
    
    Status ReadBlock(uint64_t offset, size_t size, Block** block);

    int fd_ = -1;
    uint64_t file_size_ = 0;
    
    // Always in memory
    Block* index_block_ = nullptr;
    std::string filter_data_;
};

} // namespace lakestore