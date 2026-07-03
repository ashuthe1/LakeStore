#include "bloom_filter.h"
#include "../util/hash.h"

namespace lakestore {

BloomFilter::BloomFilter(int bits_per_key) 
    : bits_per_key_(bits_per_key) {
    // k = bits_per_key * ln(2)
    k_ = static_cast<int>(bits_per_key * 0.69);
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
}

void BloomFilter::AddKey(const Slice& key) {
    // We only hash once per key and store it. 
    // Double-hashing is used during Finish() to simulate k hashes.
    hashes_.push_back(Hash(key.data, key.size, 0xbc9f1d34));
}

Slice BloomFilter::Finish() {
    size_t bits = hashes_.size() * bits_per_key_;
    // Minimum 64 bits to avoid high false positive rates for small files
    if (bits < 64) bits = 64;

    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    result_.assign(bytes, 0);

    for (uint32_t h : hashes_) {
        // Double hashing: h_i = h1 + i * h2
        // We use the higher bits of the 32-bit hash as the delta (h2)
        uint32_t delta = (h >> 17) | (h << 15);
        for (int j = 0; j < k_; j++) {
            uint32_t bit_pos = h % bits;
            result_[bit_pos / 8] |= (1 << (bit_pos % 8));
            h += delta;
        }
    }

    // Append k_ as the last byte of the filter block so the reader knows how many probes to do
    result_.push_back(static_cast<char>(k_));
    return Slice(result_);
}

bool BloomFilter::MayContain(const Slice& filter, const Slice& key) {
    if (filter.size < 2) return false;

    size_t bits = (filter.size - 1) * 8;
    // Extract k_ from the last byte
    int k = static_cast<uint8_t>(filter.data[filter.size - 1]);
    if (k > 30) return true; // Safety check for corrupted filters

    uint32_t h = Hash(key.data, key.size, 0xbc9f1d34);
    uint32_t delta = (h >> 17) | (h << 15);

    for (int j = 0; j < k; j++) {
        uint32_t bit_pos = h % bits;
        if ((filter.data[bit_pos / 8] & (1 << (bit_pos % 8))) == 0) {
            return false; // Definitely not present
        }
        h += delta;
    }
    
    return true; // Maybe present
}

} // namespace lakestore