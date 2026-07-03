#pragma once
#include <cstdint>
#include <cstddef>

namespace lakestore {

// MurmurHash3 (32-bit) implementation
inline uint32_t Hash(const char* data, size_t n, uint32_t seed) {
    const uint32_t m = 0xcc9e2d51;
    const uint32_t n_const = 0x1b873593;
    uint32_t hash = seed;

    const int nblocks = n / 4;
    const uint32_t* blocks = reinterpret_cast<const uint32_t*>(data);

    for (int i = 0; i < nblocks; i++) {
        uint32_t k = blocks[i];
        k *= m;
        k = (k << 15) | (k >> 17);
        k *= n_const;

        hash ^= k;
        hash = (hash << 13) | (hash >> 19);
        hash = hash * 5 + 0xe6546b64;
    }

    const uint8_t* tail = reinterpret_cast<const uint8_t*>(data + nblocks * 4);
    uint32_t k1 = 0;

    switch (n & 3) {
        case 3: k1 ^= tail[2] << 16;
        case 2: k1 ^= tail[1] << 8;
        case 1: k1 ^= tail[0];
                k1 *= m;
                k1 = (k1 << 15) | (k1 >> 17);
                k1 *= n_const;
                hash ^= k1;
    }

    hash ^= n;
    hash ^= hash >> 16;
    hash *= 0x85ebca6b;
    hash ^= hash >> 13;
    hash *= 0xc2b2ae35;
    hash ^= hash >> 16;

    return hash;
}

} // namespace lakestore