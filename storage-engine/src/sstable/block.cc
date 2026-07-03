#include "block.h"
#include "../util/coding.h"
#include <stdexcept>

namespace lakestore {

inline uint32_t DecodeFixed32(const char* ptr) {
    return (static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24);
}

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
      current_(restarts), restart_index_(num_restarts) {}

void Block::Iterator::ParseNextKey() {
    current_ += value_.size; 
    if (current_ >= restarts_) {
        current_ = restarts_;
        return;
    }

    uint32_t shared, non_shared, value_length;
    const char* p = data_ + current_;
    const char* limit = data_ + restarts_;

    p = GetVarint32Ptr(p, limit, &shared);
    p = GetVarint32Ptr(p, limit, &non_shared);
    p = GetVarint32Ptr(p, limit, &value_length);

    key_.resize(shared); 
    key_.append(p, non_shared); 

    value_ = Slice(p + non_shared, value_length);
    current_ = (value_.data + value_.size) - data_;
}

void Block::Iterator::SeekToRestartPoint(uint32_t index) {
    key_.clear();
    restart_index_ = index;
    uint32_t offset = DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
    current_ = offset;
    value_ = Slice(data_ + current_, 0); 
    ParseNextKey();
}

void Block::Iterator::SeekToFirst() { SeekToRestartPoint(0); }
void Block::Iterator::Next() { ParseNextKey(); }

void Block::Iterator::Seek(const Slice& target) {
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
        
        Slice mid_key(p, non_shared);
        if (mid_key.compare(target) < 0) left = mid;
        else right = mid - 1;
    }
    
    SeekToRestartPoint(left);
    while (Valid() && key().compare(target) < 0) Next();
}

} // namespace lakestore