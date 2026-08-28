#pragma once

#include <cstdint>
#include <string>
#include <cstring>
#include <stdexcept>

namespace lakestore {

using SequenceNumber = uint64_t;

// Sequence numbers share a uint64 with the 1-byte type tag when packed,
// so only 56 bits are usable. This is the "read everything" snapshot.
static constexpr SequenceNumber kMaxSequenceNumber = ((1ULL << 56) - 1);

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

    int compare(const Slice& b) const {
        const size_t min_len = (size < b.size) ? size : b.size;
        int r = std::memcmp(data, b.data, min_len);
        if (r == 0) {
            if (size < b.size) r = -1;
            else if (size > b.size) r = +1;
        }
        return r;
    }

    bool operator==(const Slice& b) const { return compare(b) == 0; }
    bool operator!=(const Slice& b) const { return compare(b) != 0; }
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

inline int CompareInternalKeys(const InternalKey& a, const InternalKey& b) {
    int r = a.user_key.compare(b.user_key);
    if (r != 0) return r;
    // Newer first: compare the full packed tag (seq << 8 | type) descending,
    // exactly like the encoded form, so both comparators agree on ties.
    uint64_t atag = (a.seq << 8) | a.type;
    uint64_t btag = (b.seq << 8) | b.type;
    if (atag > btag) return -1;
    if (atag < btag) return 1;
    return 0;
}

// The user-key portion of an encoded internal key (everything except the
// trailing 8-byte packed tag).
inline Slice ExtractUserKey(const Slice& internal_key) {
    return Slice(internal_key.data, internal_key.size >= 8 ? internal_key.size - 8 : 0);
}

// Compare two ENCODED internal keys without heap allocation.
// Order matches CompareInternalKeys: user key ascending, sequence descending.
// A plain memcmp of encoded keys is WRONG (the little-endian tag bytes and
// the prefix problem break the ordering), so every on-disk structure must
// order through this function.
inline int CompareEncodedInternalKeys(const Slice& a, const Slice& b) {
    int r = ExtractUserKey(a).compare(ExtractUserKey(b));
    if (r != 0) return r;

    auto decode_tag = [](const Slice& s) {
        uint64_t packed = 0;
        std::memcpy(&packed, s.data + s.size - 8, 8);
        return packed;
    };
    // packed = (seq << 8) | type. Larger seq (newer) must sort FIRST.
    uint64_t atag = decode_tag(a);
    uint64_t btag = decode_tag(b);
    if (atag > btag) return -1;
    if (atag < btag) return 1;
    return 0;
}

// Minimal WriteBatch struct to support WAL group commit interface.
struct WriteBatch {
    SequenceNumber sequence_number = 0;
    std::string payload; // Encoded Puts/Deletes (op_count + operations)
};

} // namespace lakestore