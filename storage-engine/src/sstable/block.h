#pragma once

#include "../types.h"
#include "../iterator.h" // MUST INCLUDE THIS
#include <vector>
#include <string>

namespace lakestore {

class Block {
public:
    explicit Block(const char* data, size_t size);
    ~Block();
    Block(const Block&) = delete;
    Block& operator=(const Block&) = delete;

    size_t size() const { return size_; }

    class Iterator : public lakestore::Iterator { 
    public:
        Iterator(const char* data, uint32_t restarts, uint32_t num_restarts);
        ~Iterator() override = default;
        
        bool Valid() const override { return valid_; }
        Slice key() const override { return Slice(key_); }
        Slice value() const override { return value_; }
        Status status() const override { return Status::OK(); }
        
        void SeekToFirst() override;
        void Seek(const Slice& target) override;
        void Next() override;

    private:
        void SeekToRestartPoint(uint32_t index);
        void ParseNextKey();

        const char* data_;
        uint32_t restarts_;
        uint32_t num_restarts_;
        
        uint32_t current_;     // offset of the entry we are positioned on
        uint32_t next_offset_; // offset where the next entry starts
        uint32_t restart_index_;
        bool valid_ = false;

        std::string key_;
        Slice value_;
    };

    lakestore::Iterator* NewIterator() const;

private:
    const char* data_;
    size_t size_;
    uint32_t restart_offset_;
    uint32_t num_restarts_;
};

} // namespace lakestore