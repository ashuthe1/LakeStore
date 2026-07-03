#pragma once
#include "../types.h"
#include "../options.h"
#include "block_builder.h"
#include <string>
#include "bloom_filter.h"

namespace lakestore {

struct BlockHandle {
    uint64_t offset;
    uint64_t size;
};

class SSTableWriter {
public:
    SSTableWriter(const std::string& filename, const Options& opts);
    ~SSTableWriter();

    void Add(const Slice& key, const Slice& value);
    Status Finish();
    
    uint64_t FileSize() const { return offset_; }
    Slice SmallestKey() const { return smallest_key_; }
    Slice LargestKey() const { return largest_key_; }

private:

    void FlushDataBlock();
    BloomFilter filter_block_;
    Options options_;
    std::string filename_;
    int fd_;
    uint64_t offset_;
    
    BlockBuilder data_block_;
    BlockBuilder index_block_;
    
    std::string last_key_;
    std::string smallest_key_;
    std::string largest_key_;
    BlockHandle pending_handle_;
    bool pending_index_entry_;
};

} // namespace lakestore