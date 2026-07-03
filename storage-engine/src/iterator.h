#pragma once
#include "types.h"

namespace lakestore {

// A unified interface for iterating over MemTables, Data Blocks, and SSTables.
class Iterator {
public:
    virtual ~Iterator() = default;
    
    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void Seek(const Slice& target) = 0;
    virtual void Next() = 0;
    
    virtual Slice key() const = 0;
    virtual Slice value() const = 0;
    virtual Status status() const = 0;
};

} // namespace lakestore