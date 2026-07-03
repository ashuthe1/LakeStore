cmake_minimum_required(VERSION 3.20)
project(lakestore VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable maximum performance for release builds
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native")

# 1. Fetch Google Benchmark
include(FetchContent)
FetchContent_Declare(
  googlebenchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.8.3
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googlebenchmark)

# 2. Define the core Lakestore Library
set(LAKESTORE_SOURCES
    src/types.cc
    src/wal/wal.cc
    src/memtable/memtable.cc
    src/sstable/block.cc
    src/sstable/sstable_writer.cc
    src/sstable/sstable_reader.cc
    src/sstable/bloom_filter.cc
    src/cache/lru_cache.cc
    src/db/manifest.cc
    src/db/compaction.cc
    src/db/db_impl.cc
)

add_library(lakestore_lib STATIC ${LAKESTORE_SOURCES})
target_include_directories(lakestore_lib PUBLIC src)
# Link against pthread for threading
find_package(Threads REQUIRED)
target_link_libraries(lakestore_lib Threads::Threads)

# 3. Define the Benchmark Executable
add_executable(lakestore_bench bench/bench.cc)
target_link_libraries(lakestore_bench PRIVATE lakestore_lib benchmark::benchmark)