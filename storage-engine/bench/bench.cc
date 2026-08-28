#include <benchmark/benchmark.h>
#include <filesystem>
#include <chrono>
#include <vector>
#include <algorithm>
#include "db/db_impl.h"
#include "options.h"

using namespace lakestore;

// Helper to set up a fresh database for each benchmark
static DBImpl* SetupFreshDB(const std::string& path, const Options& opts = Options()) {
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
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
// Benchmark 3: In-Memory Only (Bypass WAL)
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
// Benchmark 4: Random Reads from the MemTable + p99 Latency
// ---------------------------------------------------------
static void BM_Read_MemTable_p99(benchmark::State& state) {
    DBImpl* db = SetupFreshDB("/tmp/lakestore_bench_read");
    WriteOptions wopts;
    wopts.disable_wal = true; // Fast setup
    ReadOptions ropts;

    // Populate data (stays in the memtable: default 64MB write buffer)
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
    if (!latencies.empty()) {
        size_t p99_index = static_cast<size_t>(latencies.size() * 0.99);
        state.counters["p99_latency_ms"] = latencies[p99_index] / 1000000.0;
    }
    state.SetItemsProcessed(state.iterations());
    delete db;
}

// ---------------------------------------------------------
// Benchmark 5: Random Reads from SSTables (block cache warm)
// ---------------------------------------------------------
static void BM_Read_SSTables_p99(benchmark::State& state) {
    Options opts;
    opts.write_buffer_size = 2 * 1024 * 1024; // force flushes + compactions
    DBImpl* db = SetupFreshDB("/tmp/lakestore_bench_sst", opts);
    WriteOptions wopts;
    wopts.disable_wal = true;
    ReadOptions ropts;

    const int N = 200000;
    for (int i = 0; i < N; ++i) {
        db->Put(wopts, "key_" + std::to_string(i), "value_payload_from_disk");
    }
    db->FlushAll(); // everything on disk, memtable empty

    std::vector<double> latencies;
    latencies.reserve(state.max_iterations);

    for (auto _ : state) {
        int random_id = rand() % N;
        std::string key = "key_" + std::to_string(random_id);
        std::string value;

        auto start = std::chrono::high_resolution_clock::now();
        benchmark::DoNotOptimize(db->Get(ropts, key, &value));
        auto end = std::chrono::high_resolution_clock::now();

        latencies.push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    }

    std::sort(latencies.begin(), latencies.end());
    if (!latencies.empty()) {
        size_t p99_index = static_cast<size_t>(latencies.size() * 0.99);
        state.counters["p99_latency_ms"] = latencies[p99_index] / 1000000.0;
    }
    state.SetItemsProcessed(state.iterations());
    delete db;
}

// ---------------------------------------------------------
// Benchmark 6: Full scan throughput over a merged view
// ---------------------------------------------------------
static void BM_Scan_FullDatabase(benchmark::State& state) {
    Options opts;
    opts.write_buffer_size = 2 * 1024 * 1024;
    DBImpl* db = SetupFreshDB("/tmp/lakestore_bench_scan", opts);
    WriteOptions wopts;
    wopts.disable_wal = true;

    const int N = 100000;
    for (int i = 0; i < N; ++i) {
        db->Put(wopts, "key_" + std::to_string(i), "value_payload");
    }
    db->FlushAll();
    // Leave some fresh writes in the memtable so the scan really merges.
    for (int i = 0; i < 1000; ++i) {
        db->Put(wopts, "memkey_" + std::to_string(i), "value_payload");
    }

    for (auto _ : state) {
        std::unique_ptr<Iterator> it(db->NewIterator());
        int64_t count = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            benchmark::DoNotOptimize(it->value());
            count++;
        }
        state.counters["entries"] = static_cast<double>(count);
        state.SetItemsProcessed(state.items_processed() + count);
    }
    delete db;
}

// Register the benchmarks
BENCHMARK(BM_Write_AsyncWAL);
BENCHMARK(BM_Write_SyncWAL);
BENCHMARK(BM_Write_InMemoryOnly);
BENCHMARK(BM_Read_MemTable_p99);
BENCHMARK(BM_Read_SSTables_p99);
BENCHMARK(BM_Scan_FullDatabase);

BENCHMARK_MAIN();
