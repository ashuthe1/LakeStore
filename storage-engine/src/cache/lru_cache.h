#pragma once

#include "../sstable/block.h"
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <memory>
#include <vector>

namespace lakestore {

// A unique identifier for a block on disk.
struct CacheKey {
    uint64_t file_number;
    uint64_t offset;

    bool operator==(const CacheKey& other) const {
        return file_number == other.file_number && offset == other.offset;
    }
};

struct CacheKeyHasher {
    std::size_t operator()(const CacheKey& k) const {
        // Simple hash combining file_number and offset
        return std::hash<uint64_t>()(k.file_number) ^ (std::hash<uint64_t>()(k.offset) << 1);
    }
};

// A single shard of the LRU Cache.
class LRUCacheShard {
public:
    explicit LRUCacheShard(size_t capacity);
    ~LRUCacheShard();

    std::shared_ptr<Block> Lookup(const CacheKey& key);
    void Insert(const CacheKey& key, std::shared_ptr<Block> block, size_t charge);
    size_t TotalCharge() const;

private:
    struct LRUEntry {
        CacheKey key;
        std::shared_ptr<Block> value;
        size_t charge;
        LRUEntry* prev;
        LRUEntry* next;
    };

    void Evict();
    void Remove(LRUEntry* e);
    void Append(LRUEntry* e); // Appends to MRU end (head)

    size_t capacity_;
    size_t usage_;
    mutable std::mutex mu_;

    std::unordered_map<CacheKey, LRUEntry*, CacheKeyHasher> table_;
    
    // Dummy head for the doubly-linked list.
    // lru_.next is MRU, lru_.prev is LRU.
    LRUEntry lru_; 
};

// The top-level sharded cache.
class LRUCache {
public:
    explicit LRUCache(size_t capacity_bytes);
    ~LRUCache() = default;

    // Prevent copy and move
    LRUCache(const LRUCache&) = delete;
    LRUCache& operator=(const LRUCache&) = delete;

    std::shared_ptr<Block> Lookup(uint64_t file_number, uint64_t offset);
    void Insert(uint64_t file_number, uint64_t offset, std::shared_ptr<Block> block, size_t charge);
    
    size_t TotalCharge() const;
    size_t Capacity() const { return capacity_; }

private:
    static constexpr int kNumShards = 16;
    
    inline uint32_t Shard(uint64_t file_number, uint64_t offset) const {
        CacheKeyHasher hasher;
        return hasher(CacheKey{file_number, offset}) % kNumShards;
    }

    size_t capacity_;
    std::vector<std::unique_ptr<LRUCacheShard>> shards_;
};

} // namespace lakestore