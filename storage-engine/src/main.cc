#include "db/db_impl.h"
#include "options.h"
#include <iostream>
#include <string>
#include <filesystem>

using namespace lakestore;

int main() {
    // 1. Setup a temporary directory for our test database
    std::string db_path = "/tmp/lakestore_simple_test";
    std::filesystem::remove_all(db_path);
    std::filesystem::create_directories(db_path);

    Options opts;
    opts.block_size = 4096;
    opts.bloom_bits_per_key = 10;

    // 2. Open the database
    DBImpl* db = nullptr;
    Status s = DBImpl::Open(opts, db_path, &db);
    if (!s.ok()) {
        std::cerr << "Failed to open DB: " << s.ToString() << "\n";
        return 1;
    }

    std::cout << "=== Successfully opened Lakestore at " << db_path << " ===\n\n";

    WriteOptions wopts;
    ReadOptions ropts;
    std::string value;

    // --- TEST 1: Basic Put and Get ---
    std::cout << "[Test 1] Put('hello') -> 'world'\n";
    db->Put(wopts, "hello", "world");
    
    s = db->Get(ropts, "hello", &value);
    if (s.ok() && value == "world") {
        std::cout << "  -> PASS: Read back '" << value << "'\n\n";
    } else {
        std::cout << "  -> FAIL: Expected 'world', got: " << s.ToString() << "\n\n";
    }

    // --- TEST 2: Overwrite (MVCC check) ---
    std::cout << "[Test 2] Overwrite('hello') -> 'lakestore'\n";
    db->Put(wopts, "hello", "lakestore");
    
    s = db->Get(ropts, "hello", &value);
    if (s.ok() && value == "lakestore") {
        std::cout << "  -> PASS: Read back '" << value << "'\n\n";
    } else {
        std::cout << "  -> FAIL: Expected 'lakestore', got: " << s.ToString() << "\n\n";
    }

    // --- TEST 3: Delete (Tombstone check) ---
    std::cout << "[Test 3] Delete('hello')\n";
    db->Delete(wopts, "hello");
    
    s = db->Get(ropts, "hello", &value);
    if (s.IsNotFound()) {
        std::cout << "  -> PASS: Key correctly reported as NotFound.\n\n";
    } else {
        std::cout << "  -> FAIL: Expected NotFound, got: " << s.ToString() << "\n\n";
    }

    std::cout << "=== All basic operations finished! ===\n";

    // 3. Clean up
    delete db;
    std::filesystem::remove_all(db_path);
    
    return 0;
}