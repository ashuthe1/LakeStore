#include "lru_cache.h"

namespace lakestore {

// --- LRUCacheShard Implementation ---

LRUCacheShard::LRUCacheShard(size_t capacity) 
    : capacity_(capacity), usage_(0) {
    // Initialize circular doubly-linked list
    lru_.next = &lru_;
    lru_.prev = &lru_;
}

LRUCacheShard::~LRUCacheShard() {
    // Clean up all entries
    for (LRUEntry* e = lru_.next; e != &lru_; ) {
        LRUEntry* next = e->next;
        delete e;
        e = next;
    }
}

void LRUCacheShard::Remove(LRUEntry* e) {
    e->prev->next = e->next;
    e->next->prev = e->prev;
}

void LRUCacheShard::Append(LRUEntry* e) {
    // Insert before lru_ (making it the new MRU)
    // lru_.next points to the MRU element
    e->next = lru_.next;
    e->prev = &lru_;
    e->prev->next = e;
    e->next->prev = e;
}

std::shared_ptr<Block> LRUCacheShard::Lookup(const CacheKey& key) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = table_.find(key);
    if (it == table_.end()) {
        return nullptr;
    }
    
    LRUEntry* e = it->second;
    // Move to MRU position
    Remove(e);
    Append(e);
    
    return e->value;
}

void LRUCacheShard::Insert(const CacheKey& key, std::shared_ptr<Block> block, size_t charge) {
    std::lock_guard<std::mutex> lock(mu_);
    
    auto it = table_.find(key);
    if (it != table_.end()) {
        // Key already exists, update it
        LRUEntry* e = it->second;
        usage_ -= e->charge;
        e->value = std::move(block);
        e->charge = charge;
        usage_ += charge;
        Remove(e);
        Append(e);
    } else {
        // New entry
        LRUEntry* e = new LRUEntry{key, std::move(block), charge, nullptr, nullptr};
        table_[key] = e;
        Append(e);
        usage_ += charge;
    }

    // Evict if over capacity
    Evict();
}

void LRUCacheShard::Evict() {
    // lru_.prev is the Least Recently Used element
    while (usage_ > capacity_ && lru_.prev != &lru_) {
        LRUEntry* old = lru_.prev;
        Remove(old);
        table_.erase(old->key);
        usage_ -= old->charge;
        delete old;
    }
}

size_t LRUCacheShard::TotalCharge() const {
    std::lock_guard<std::mutex> lock(mu_);
    return usage_;
}

// --- Top-Level LRUCache Implementation ---

LRUCache::LRUCache(size_t capacity_bytes) : capacity_(capacity_bytes) {
    const size_t per_shard = (capacity_bytes + (kNumShards - 1)) / kNumShards;
    for (int i = 0; i < kNumShards; i++) {
        shards_.push_back(std::make_unique<LRUCacheShard>(per_shard));
    }
}

std::shared_ptr<Block> LRUCache::Lookup(uint64_t file_number, uint64_t offset) {
    return shards_[Shard(file_number, offset)]->Lookup({file_number, offset});
}

void LRUCache::Insert(uint64_t file_number, uint64_t offset, std::shared_ptr<Block> block, size_t charge) {
    shards_[Shard(file_number, offset)]->Insert({file_number, offset}, std::move(block), charge);
}

size_t LRUCache::TotalCharge() const {
    size_t total = 0;
    for (const auto& shard : shards_) {
        total += shard->TotalCharge();
    }
    return total;
}

} // namespace lakestore