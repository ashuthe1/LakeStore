#pragma once
#include "../types.h"
#include <vector>
#include <string>

namespace lakestore {

class BloomFilter {
public:
    explicit BloomFilter(int bits_per_key);

    // Call for each key during SSTable construction
    void AddKey(const Slice& key);
    
    // Generate the filter bytes, including the k_ value appended at the end
    Slice Finish();
    
    // Returns false if key is DEFINITELY not present
    static bool MayContain(const Slice& filter_data, const Slice& key);

private:
    int bits_per_key_;
    int k_; // Number of hash probes
    std::vector<uint32_t> hashes_;
    std::string result_;
};

} // namespace lakestore