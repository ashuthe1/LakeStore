#pragma once
#include "../types.h"
#include <vector>
#include <string>

namespace lakestore {

class Block {
public:
    // Takes ownership of the dynamically allocated buffer
    explicit Block(const char* data, size_t size);
    ~Block();

    // Prevent copying
    Block(const Block&) = delete;
    Block& operator=(const Block&) = delete;

    size_t size() const { return size_; }

    class Iterator : public lakestore::Iterator { // Inherit here!
    public:
        Iterator(const char* data, uint32_t restarts, uint32_t num_restarts);
        ~Iterator() override = default;
        
        bool Valid() const override { return current_ < restarts_; }
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
        uint32_t restarts_;     // Offset in data_ where restart array begins
        uint32_t num_restarts_;
        
        uint32_t current_;      // Current offset in data_
        uint32_t restart_index_;// Current restart block index
        
        std::string key_;       // Reconstructed key
        Slice value_;           // Pointer to value in data_
    };

    lakestore::Iterator* NewIterator() const; // Update return type

private:
    const char* data_;
    size_t size_;
    uint32_t restart_offset_;
    uint32_t num_restarts_;
};

} // namespace lakestore