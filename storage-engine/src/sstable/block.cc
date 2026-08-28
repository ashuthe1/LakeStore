#include "block.h"
#include "../util/coding.h"
#include <stdexcept>

namespace lakestore {

Block::Block(const char* data, size_t size) : data_(data), size_(size) {
    if (size_ < sizeof(uint32_t)) throw std::runtime_error("Block too small");
    num_restarts_ = DecodeFixed32(data_ + size_ - sizeof(uint32_t));
    restart_offset_ = size_ - (1 + num_restarts_) * sizeof(uint32_t);
}

Block::~Block() {
    delete[] data_;
}

lakestore::Iterator* Block::NewIterator() const {
    return new Iterator(data_, restart_offset_, num_restarts_);
}

Block::Iterator::Iterator(const char* data, uint32_t restarts, uint32_t num_restarts)
    : data_(data), restarts_(restarts), num_restarts_(num_restarts),
      current_(restarts), next_offset_(restarts), restart_index_(num_restarts) {}

void Block::Iterator::ParseNextKey() {
    current_ = next_offset_;
    if (current_ >= restarts_) {
        valid_ = false;
        return;
    }

    uint32_t shared, non_shared, value_length;
    const char* p = data_ + current_;
    const char* limit = data_ + restarts_;

    p = GetVarint32Ptr(p, limit, &shared);
    if (p) p = GetVarint32Ptr(p, limit, &non_shared);
    if (p) p = GetVarint32Ptr(p, limit, &value_length);
    if (p == nullptr || shared > key_.size()) {
        valid_ = false; // Corrupt entry
        return;
    }

    // Delta decoding: the first `shared` bytes are inherited from the
    // previous key, so ParseNextKey must only ever move forward from a
    // restart point.
    key_.resize(shared);
    key_.append(p, non_shared);

    value_ = Slice(p + non_shared, value_length);
    next_offset_ = (value_.data + value_.size) - data_;
    valid_ = true;
}

void Block::Iterator::SeekToRestartPoint(uint32_t index) {
    key_.clear();
    restart_index_ = index;
    next_offset_ = DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
    ParseNextKey();
}

void Block::Iterator::SeekToFirst() { SeekToRestartPoint(0); }
void Block::Iterator::Next() { ParseNextKey(); }

void Block::Iterator::Seek(const Slice& target) {
    // Both data and index blocks store ENCODED internal keys, so ordering
    // must go through the internal key comparator — a raw byte compare gives
    // a different order (little-endian tag bytes + the prefix problem).
    uint32_t left = 0;
    uint32_t right = num_restarts_ > 0 ? num_restarts_ - 1 : 0;

    while (left < right) {
        uint32_t mid = (left + right + 1) / 2;
        uint32_t region_offset = DecodeFixed32(data_ + restarts_ + mid * sizeof(uint32_t));

        uint32_t shared, non_shared, value_len;
        const char* p = data_ + region_offset;
        const char* limit = data_ + restarts_;
        p = GetVarint32Ptr(p, limit, &shared);
        p = GetVarint32Ptr(p, limit, &non_shared);
        p = GetVarint32Ptr(p, limit, &value_len);

        Slice mid_key(p, non_shared); // shared == 0 at restart points
        if (CompareEncodedInternalKeys(mid_key, target) < 0) left = mid;
        else right = mid - 1;
    }

    SeekToRestartPoint(left);
    while (Valid() && CompareEncodedInternalKeys(key(), target) < 0) Next();
}

} // namespace lakestore