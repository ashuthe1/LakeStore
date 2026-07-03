#include <benchmark/benchmark.h>
#include <filesystem>
#include <chrono>
#include <vector>
#include <algorithm>
#include "db/db_impl.h"
#include "options.h"

using namespace lakestore;

// Helper to set up a fresh database for each benchmark
DBImpl* SetupFreshDB(const std::string& path) {
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    Options opts;
    DBImpl* db = nullptr;
    DBImpl::Open(opts, path, &db);
    return db;
}

// ---------------------------------------------------------
// Benchmark 1: Normal Async Writes (WAL + OS Buffer)
// ---------------------------------------------------------
static void BM_Write_AsyncWAL(benchmark::State& state) {
    DBImpl* db = SetupFreshDB("/tmp/lakestore_bench_async");
    WriteOptions wopts;
    wopts.sync = false;
    wopts.disable_wal = false;

    int i = 0;
    for (auto _ : state) {
        std::string key = "key_" + std::to_string(i);
        db->Put(wopts, key, "value_payload");
        i++;
    }
    state.SetBytesProcessed(state.iterations() * 25);
    state.SetItemsProcessed(state.iterations());
    delete db;
}

// ---------------------------------------------------------
// Benchmark 2: Strict Sync Writes (WAL + Physical Disk Flush)
// ---------------------------------------------------------
static void BM_Write_SyncWAL(benchmark::State& state) {
    DBImpl* db = SetupFreshDB("/tmp/lakestore_bench_sync");
    WriteOptions wopts;
    wopts.sync = true; // Forces disk physical write
    wopts.disable_wal = false;

    int i = 0;
    for (auto _ : state) {
        std::string key = "key_" + std::to_string(i);
        db->Put(wopts, key, "value_payload");
        i++;
    }
    state.SetItemsProcessed(state.iterations());
    delete db;
}

// ---------------------------------------------------------
// Benchmark 3: In-Memory Only (Bypass WAL - Target: ~800k/s)
// ---------------------------------------------------------
static void BM_Write_InMemoryOnly(benchmark::State& state) {
    DBImpl* db = SetupFreshDB("/tmp/lakestore_bench_mem");
    WriteOptions wopts;
    wopts.disable_wal = true; // Pure SkipList speed

    int i = 0;
    for (auto _ : state) {
        std::string key = "key_" + std::to_string(i);
        db->Put(wopts, key, "value_payload");
        i++;
    }
    state.SetBytesProcessed(state.iterations() * 25);
    state.SetItemsProcessed(state.iterations());
    delete db;
}

// ---------------------------------------------------------
// Benchmark 4: Random Reads + p99 Latency (Target: < 1ms)
// ---------------------------------------------------------
static void BM_Read_p99(benchmark::State& state) {
    DBImpl* db = SetupFreshDB("/tmp/lakestore_bench_read");
    WriteOptions wopts;
    wopts.disable_wal = true; // Fast setup
    ReadOptions ropts;

    // Populate data
    for (int i = 0; i < 100000; ++i) {
        db->Put(wopts, "key_" + std::to_string(i), "value_payload");
    }

    std::vector<double> latencies;
    latencies.reserve(state.max_iterations);

    for (auto _ : state) {
        int random_id = rand() % 100000;
        std::string key = "key_" + std::to_string(random_id);
        std::string value;

        auto start = std::chrono::high_resolution_clock::now();
        db->Get(ropts, key, &value);
        auto end = std::chrono::high_resolution_clock::now();

        double elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        latencies.push_back(elapsed_ns);
    }

    std::sort(latencies.begin(), latencies.end());
    double p99_ns = 0;
    if (!latencies.empty()) {
        size_t p99_index = static_cast<size_t>(latencies.size() * 0.99);
        p99_ns = latencies[p99_index];
    }

    state.counters["p99_latency_ms"] = p99_ns / 1000000.0;
    state.SetItemsProcessed(state.iterations());
    delete db;
}

// Register the benchmarks
BENCHMARK(BM_Write_AsyncWAL);
BENCHMARK(BM_Write_SyncWAL);
BENCHMARK(BM_Write_InMemoryOnly);
BENCHMARK(BM_Read_p99);

BENCHMARK_MAIN();