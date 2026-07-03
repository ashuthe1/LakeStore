#pragma once
#include <string>
#include <cstdint>

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
}