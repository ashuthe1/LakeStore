#pragma once

#include "skiplist.h"
#include <string>

namespace lakestore {

// A helper to format keys for SkipList lookups.
class LookupKey {
public:
    // We encode the snapshot sequence number with kTypeValue.
    // Because seq numbers sort descending, seeking for (user_key, snapshot_seq)
    // will land us on the newest version of the key <= snapshot_seq.
    LookupKey(const Slice& user_key, SequenceNumber snapshot_seq) {
        ik_.user_key = user_key.ToString();
        ik_.seq = snapshot_seq;
        ik_.type = kTypeValue; 
    }

    const InternalKey& internal_key() const { return ik_; }
    Slice user_key() const { return ik_.user_key; }

private:
    InternalKey ik_;
};

class MemTable {
public:
    MemTable() : memory_usage_(0) {}

    // Add a new entry to the MemTable
    void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);
    
    // Lookup a key. 
    // Returns true if the key was found (either as a Value or a Deletion tombstone).
    // If found and it's a Value, populates *value and sets *status to OK.
    // If found and it's a Deletion, sets *status to NotFound.
    // Returns false if not present at all.
    bool Get(const LookupKey& key, std::string* value, Status* s) const;
    
    // Used by DBImpl to check against kMemTableSizeThreshold
    size_t ApproximateMemoryUsage() const { return memory_usage_.load(std::memory_order_relaxed); }
    
    bool IsEmpty() const { return memory_usage_.load(std::memory_order_relaxed) == 0; }
    
    // Ownership transferred to caller
    SkipList::Iterator* NewIterator() const { return table_.NewIterator(); }

private:
    SkipList table_;
    std::atomic<size_t> memory_usage_;
};

} // namespace lakestore