#include <benchmark/benchmark.h>
#include "src/db/db_impl.h"
#include "src/options.h"
#include <string>
#include <filesystem>
#include <random>

using namespace lakestore;

// Helper to generate a zero-padded string key (e.g., "key_0000152")
std::string MakeKey(int i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%07d", i);
    return std::string(buf);
}

// Helper to create a fresh DB directory for a benchmark
std::string SetupDB(const std::string& name, DBImpl** db) {
    std::string path = "/tmp/lakestore_bench_" + name;
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    
    Options opts;
    opts.block_size = 4096;
    opts.bloom_bits_per_key = 10;
    
    Status s = DBImpl::Open(opts, path, db);
    if (!s.ok()) {
        throw std::runtime_error("Failed to open DB: " + s.ToString());
    }
    return path;
}

// --- Benchmark 1: Sequential Writes ---
static void BM_SequentialWrite(benchmark::State& state) {
    DBImpl* db = nullptr;
    std::string path = SetupDB("seq_write", &db);
    WriteOptions wopts;
    wopts.sync = false; // standard async WAL writes
    
    std::string value(100, 'x'); // 100 byte payload
    int i = 0;
    
    for (auto _ : state) {
        db->Put(wopts, MakeKey(i++), value);
    }
    
    state.SetBytesProcessed(state.iterations() * (15 + 100)); // key + value size
    state.SetItemsProcessed(state.iterations());
    
    delete db;
    std::filesystem::remove_all(path);
}
BENCHMARK(BM_SequentialWrite);

// --- Benchmark 2: Random Writes ---
static void BM_RandomWrite(benchmark::State& state) {
    DBImpl* db = nullptr;
    std::string path = SetupDB("rand_write", &db);
    WriteOptions wopts;
    
    std::string value(100, 'x');
    
    // Fast RNG
    std::mt19937 gen(42);
    std::uniform_int_distribution<> dist(0, 1000000);
    
    for (auto _ : state) {
        db->Put(wopts, MakeKey(dist(gen)), value);
    }
    
    state.SetBytesProcessed(state.iterations() * (15 + 100));
    state.SetItemsProcessed(state.iterations());
    
    delete db;
    std::filesystem::remove_all(path);
}
BENCHMARK(BM_RandomWrite);

// --- Benchmark 3: Random Reads (Warm Cache) ---
static void BM_RandomRead(benchmark::State& state) {
    DBImpl* db = nullptr;
    std::string path = SetupDB("rand_read", &db);
    
    // Pre-populate the DB with 100,000 keys
    WriteOptions wopts;
    std::string value(100, 'x');
    const int kNumKeys = 100000;
    
    for (int i = 0; i < kNumKeys; i++) {
        db->Put(wopts, MakeKey(i), value);
    }
    
    ReadOptions ropts;
    std::string out_value;
    
    std::mt19937 gen(42);
    std::uniform_int_distribution<> dist(0, kNumKeys - 1);
    
    for (auto _ : state) {
        Status s = db->Get(ropts, MakeKey(dist(gen)), &out_value);
        benchmark::DoNotOptimize(s);
        benchmark::DoNotOptimize(out_value);
    }
    
    state.SetItemsProcessed(state.iterations());
    
    delete db;
    std::filesystem::remove_all(path);
}
BENCHMARK(BM_RandomRead);

BENCHMARK_MAIN();