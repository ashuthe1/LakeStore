#pragma once

#include <cstdint>
#include <string>
#include <cstring>
#include <stdexcept>

namespace lakestore {

using SequenceNumber = uint64_t;

enum ValueType : uint8_t {
    kTypeDeletion = 0x0,
    kTypeValue    = 0x1,
};

// Slice = non-owning view into a buffer.
// Using standard C++17 string_view would also work, but we adhere to the classic
// struct format requested to ensure explicit control over the pointer and size.
struct Slice {
    const char* data;
    size_t size;

    Slice() : data(""), size(0) {}
    Slice(const char* d, size_t s) : data(d), size(s) {}
    Slice(const std::string& s) : data(s.data()), size(s.size()) {}
    Slice(const char* s) : data(s), size(strlen(s)) {}

    std::string ToString() const { return std::string(data, size); }
};

// Status returned by every operation.
class Status {
public:
    enum Code { kOk, kNotFound, kIOError, kCorruption };

    Status() : code_(kOk) {}
    Status(Code code, std::string msg) : code_(code), msg_(std::move(msg)) {}
    
    static Status OK() { return Status(); }
    static Status NotFound(const std::string& msg) { return Status(kNotFound, msg); }
    static Status IOError(const std::string& msg) { return Status(kIOError, msg); }
    static Status Corruption(const std::string& msg) { return Status(kCorruption, msg); }
    
    bool ok() const { return code_ == kOk; }
    bool IsNotFound() const { return code_ == kNotFound; }
    
    std::string ToString() const {
        if (ok()) return "OK";
        const char* type;
        switch(code_) {
            case kNotFound: type = "NotFound: "; break;
            case kIOError: type = "IOError: "; break;
            case kCorruption: type = "Corruption: "; break;
            default: type = "Unknown: "; break;
        }
        return std::string(type) + msg_;
    }

private:
    Code code_;
    std::string msg_;
};

// InternalKey = user_key + (sequence_number << 8 | type)
struct InternalKey {
    std::string user_key;
    SequenceNumber seq;
    ValueType type;
    
    std::string Encode() const;
    static InternalKey Decode(const Slice& s);
};

// Minimal WriteBatch struct to support WAL group commit interface.
struct WriteBatch {
    SequenceNumber sequence_number = 0;
    std::string payload; // Encoded Puts/Deletes (op_count + operations)
};

} // namespace lakestore