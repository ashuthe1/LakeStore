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

    class Iterator {
    public:
        Iterator(const char* data, uint32_t restarts, uint32_t num_restarts);
        
        bool Valid() const { return current_ < restarts_; }
        Slice key() const { return Slice(key_); }
        Slice value() const { return value_; }
        
        void SeekToFirst();
        void Seek(const Slice& target);
        void Next();

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

    Iterator* NewIterator() const;

private:
    const char* data_;
    size_t size_;
    uint32_t restart_offset_;
    uint32_t num_restarts_;
};

} // namespace lakestore