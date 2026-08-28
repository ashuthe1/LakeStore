#pragma once
#include <cstdint>
#include <cstddef>

namespace lakestore {
namespace crc32c {

// CRC32C (Castagnoli polynomial, reflected: 0x82F63B78).
// The lookup table is generated once at startup, which guarantees every
// entry is correct instead of embedding 256 hand-copied constants.
class Table {
public:
    Table() {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t crc = i;
            for (int j = 0; j < 8; j++) {
                crc = (crc & 1) ? (crc >> 1) ^ 0x82F63B78u : crc >> 1;
            }
            entries[i] = crc;
        }
    }
    uint32_t entries[256];
};

inline const uint32_t* TableEntries() {
    static const Table table;
    return table.entries;
}

// Extend `crc` (state, NOT the finalized value) with `data[0, n)`.
inline uint32_t Extend(uint32_t crc, const char* data, size_t n) {
    const uint32_t* t = TableEntries();
    uint32_t c = crc ^ 0xFFFFFFFFu;
    for (size_t i = 0; i < n; i++) {
        c = (c >> 8) ^ t[(c ^ static_cast<uint8_t>(data[i])) & 0xFF];
    }
    return c ^ 0xFFFFFFFFu;
}

inline uint32_t Value(const char* data, size_t n) {
    return Extend(0, data, n);
}

} // namespace crc32c
} // namespace lakestore
