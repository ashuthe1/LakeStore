#include "memtable.h"

namespace lakestore {

// --- MemTable Implementation ---

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
        
        // 1. Ensure the user keys actually match (Zero-copy comparison using Slice)
        if (Slice(found_key.user_key) == key.user_key()) {
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

Iterator* MemTable::NewIterator() const { 
    return new MemTableIterator(&table_); 
}

// --- MemTableIterator Implementation ---

MemTableIterator::MemTableIterator(const SkipList* list) 
    : iter_(list->NewIterator()) {}

bool MemTableIterator::Valid() const { 
    return iter_->Valid(); 
}

void MemTableIterator::SeekToFirst() {
    iter_->SeekToFirst();
    if (Valid()) encoded_key_cache_ = iter_->key().Encode();
}

void MemTableIterator::Seek(const Slice& target) {
    iter_->Seek(InternalKey::Decode(target));
    if (Valid()) encoded_key_cache_ = iter_->key().Encode();
}

void MemTableIterator::Next() {
    iter_->Next();
    if (Valid()) encoded_key_cache_ = iter_->key().Encode();
}

Slice MemTableIterator::key() const { 
    return Slice(encoded_key_cache_); 
}

Slice MemTableIterator::value() const { 
    return Slice(iter_->value()); 
}

Status MemTableIterator::status() const { 
    return Status::OK(); 
}

} // namespace lakestore