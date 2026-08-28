#pragma once
#include "../types.h"
#include <string>
#include <cstdint>
#include <cstring>

namespace lakestore {
// Fast variable-length integer encoding (varint).
// Uses 7 bits of each byte for data, 1 bit for continuation.
inline void PutVarint32(std::string* dst, uint32_t v) {
    uint8_t buf[5];
    int ptr = 0;
    while (v >= 128) {
        buf[ptr++] = v | 128;
        v >>= 7;
    }
    buf[ptr++] = static_cast<uint8_t>(v);
    dst->append(reinterpret_cast<const char*>(buf), ptr);
}

inline void PutVarint64(std::string* dst, uint64_t v) {
    uint8_t buf[10];
    int ptr = 0;
    while (v >= 128) {
        buf[ptr++] = static_cast<uint8_t>(v) | 128;
        v >>= 7;
    }
    buf[ptr++] = static_cast<uint8_t>(v);
    dst->append(reinterpret_cast<const char*>(buf), ptr);
}

inline const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value) {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *(reinterpret_cast<const uint8_t*>(p));
        p++;
        if (byte & 128) {
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return nullptr; // Corruption or truncated
}

inline const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = *(reinterpret_cast<const uint8_t*>(p));
        p++;
        if (byte & 128) {
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return nullptr; // Corruption or truncated
}

inline bool GetVarint32(Slice* input, uint32_t* value) {
    const char* p = GetVarint32Ptr(input->data, input->data + input->size, value);
    if (p == nullptr) {
        return false;
    }

    // Calculate how many bytes the varint took (MUST do this before moving input->data)
    size_t bytes_read = p - input->data;

    // Now safely update the Slice
    input->data = p;
    input->size -= bytes_read;

    return true;
}

inline bool GetVarint64(Slice* input, uint64_t* value) {
    const char* p = GetVarint64Ptr(input->data, input->data + input->size, value);
    if (p == nullptr) {
        return false;
    }
    size_t bytes_read = p - input->data;
    input->data = p;
    input->size -= bytes_read;
    return true;
}

// Fixed-width little-endian encoding. Used where the size must be predictable
// (WAL batch headers, SSTable footers, block restart arrays).
inline void EncodeFixed32(char* dst, uint32_t v) {
    dst[0] = static_cast<char>(v & 0xff);
    dst[1] = static_cast<char>((v >> 8) & 0xff);
    dst[2] = static_cast<char>((v >> 16) & 0xff);
    dst[3] = static_cast<char>((v >> 24) & 0xff);
}

inline void EncodeFixed64(char* dst, uint64_t v) {
    for (int i = 0; i < 8; i++) {
        dst[i] = static_cast<char>((v >> (i * 8)) & 0xff);
    }
}

inline void PutFixed32(std::string* dst, uint32_t v) {
    char buf[4];
    EncodeFixed32(buf, v);
    dst->append(buf, 4);
}

inline void PutFixed64(std::string* dst, uint64_t v) {
    char buf[8];
    EncodeFixed64(buf, v);
    dst->append(buf, 8);
}

inline uint32_t DecodeFixed32(const char* ptr) {
    return (static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24);
}

inline uint64_t DecodeFixed64(const char* ptr) {
    uint64_t result = 0;
    for (int i = 7; i >= 0; i--) {
        result = (result << 8) | static_cast<unsigned char>(ptr[i]);
    }
    return result;
}
}
