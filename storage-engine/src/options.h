#pragma once
#include <cstdint>

namespace lakestore {
struct Options {
    size_t block_size = 4096;
    int block_restart_interval = 16;
    int bloom_bits_per_key = 10;
};
}