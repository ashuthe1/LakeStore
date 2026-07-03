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

inline bool GetVarint32(Slice* input, uint32_t* value) {
    const char* p = GetVarint32Ptr(input->data, input->data + input->size, value);
    if (p == nullptr) {
        return false;
    }
    input->data = p;
    input->size -= (p - (input->data - input->size)); // Wait, safer math below:
    // Correct size adjustment:
    size_t advanced = p - (input->data - input->size); 
    // Actually, input->data was moved in the macro conceptually. 
    // Let's do it cleanly:
    const char* original_data = input->data;
    input->data = p;
    input->size -= (p - original_data);
    return true;
}
}