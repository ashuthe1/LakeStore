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

// SSTable physical layout:
//   [data block 0] ... [data block N]
//   [filter block]
//   [index block]
//   [footer: fixed64 x4 (filter/index handles) + fixed64 magic]  = 40 bytes
constexpr size_t kFooterSize = 40;
constexpr uint64_t kTableMagicNumber = 0xdb4775248b80fb57ULL;

class SSTableWriter {
public:
    SSTableWriter(const std::string& filename, const Options& opts);
    ~SSTableWriter();

    // Keys must be ENCODED internal keys, added in internal-key order.
    void Add(const Slice& key, const Slice& value);
    Status Finish();

    uint64_t FileSize() const { return offset_; }
    uint64_t NumEntries() const { return num_entries_; }
    Slice SmallestKey() const { return smallest_key_; }
    Slice LargestKey() const { return largest_key_; }

private:

    void FlushDataBlock();
    void WriteRaw(const char* data, size_t n);

    BloomFilter filter_block_;
    Options options_;
    std::string filename_;
    int fd_;
    uint64_t offset_;
    uint64_t num_entries_ = 0;
    Status io_status_;

    BlockBuilder data_block_;
    BlockBuilder index_block_;

    std::string last_key_;
    std::string smallest_key_;
    std::string largest_key_;
};

} // namespace lakestore
