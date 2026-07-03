#include "memtable.h"

namespace lakestore {

void MemTable::Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value) {
    InternalKey ik;
    ik.user_key = key.ToString();
    ik.seq = seq;
    ik.type = type;

    table_.Insert(ik, value.ToString());

    // Approximate size: key size + value size + internal metadata + pointer overhead
    size_t entry_size = key.size + value.size + 8 /* tag */ + 32 /* node overhead estimate */;
    memory_usage_.fetch_add(entry_size, std::memory_order_relaxed);
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) const {
    std::unique_ptr<SkipList::Iterator> iter(table_.NewIterator());
    iter->Seek(key.internal_key());

    if (iter->Valid()) {
        const InternalKey& found_key = iter->key();
        
        // 1. Ensure the user keys actually match (we didn't just seek to the next entirely different key)
        if (found_key.user_key == key.user_key().ToString()) {
            // 2. Check type (Tombstone vs Value)
            if (found_key.type == kTypeValue) {
                *value = iter->value();
                *s = Status::OK();
                return true;
            } else if (found_key.type == kTypeDeletion) {
                *s = Status::NotFound("Key was deleted");
                return true;
            }
        }
    }
    
    return false; // Not found in this MemTable
}

} // namespace lakestore