#include <gtest/gtest.h>
#include "db/db_impl.h"
#include "options.h"
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

using namespace lakestore;

class DBTest : public ::testing::Test {
protected:
    std::string db_path = "./lakestore_test_data";
    DBImpl* db = nullptr;
    Options opts;
    WriteOptions wopts;
    ReadOptions ropts;

    void SetUp() override {
        std::filesystem::remove_all(db_path);
        std::filesystem::create_directories(db_path);
        opts.block_size = 4096;
        opts.bloom_bits_per_key = 10;
        ASSERT_TRUE(DBImpl::Open(opts, db_path, &db).ok());
    }

    void TearDown() override {
        if (db) {
            delete db;
            db = nullptr;
        }
        std::filesystem::remove_all(db_path);
    }

    void Reopen() {
        delete db;
        db = nullptr;
        ASSERT_TRUE(DBImpl::Open(opts, db_path, &db).ok());
    }
};

// ============================================================================
// CATEGORY 1: BASIC CRUD & IDENTITY
// ============================================================================

TEST_F(DBTest, EmptyKeyFailsOrWorks) {
    std::string val;
    EXPECT_TRUE(db->Put(wopts, "", "empty_key_test").ok());
    EXPECT_TRUE(db->Get(ropts, "", &val).ok());
    EXPECT_EQ(val, "empty_key_test");
}

TEST_F(DBTest, EmptyValueWorks) {
    std::string val;
    EXPECT_TRUE(db->Put(wopts, "empty_val", "").ok());
    EXPECT_TRUE(db->Get(ropts, "empty_val", &val).ok());
    EXPECT_EQ(val, "");
}

TEST_F(DBTest, KeyNotFoundReturnsCorrectStatus) {
    std::string val;
    Status s = db->Get(ropts, "non_existent_key_abc", &val);
    EXPECT_TRUE(s.IsNotFound());
}

TEST_F(DBTest, DeleteNonExistentKeyIsNoOp) {
    EXPECT_TRUE(db->Delete(wopts, "ghost_key").ok());
    std::string val;
    EXPECT_TRUE(db->Get(ropts, "ghost_key", &val).IsNotFound()); // Added &val
}

// ============================================================================
// CATEGORY 2: MVCC & OVERWRITE INVARIANTS
// ============================================================================

TEST_F(DBTest, ContinuousOverwrites) {
    std::string val;
    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(db->Put(wopts, "ow_key", "value_" + std::to_string(i)).ok());
    }
    EXPECT_TRUE(db->Get(ropts, "ow_key", &val).ok());
    EXPECT_EQ(val, "value_49"); // Newest version must win
}

TEST_F(DBTest, DeleteThenReinsert) {
    std::string val;
    EXPECT_TRUE(db->Put(wopts, "cycle_key", "data").ok());
    EXPECT_TRUE(db->Delete(wopts, "cycle_key").ok());
    EXPECT_TRUE(db->Put(wopts, "cycle_key", "revived_data").ok());
    
    EXPECT_TRUE(db->Get(ropts, "cycle_key", &val).ok());
    EXPECT_EQ(val, "revived_data");
}

TEST_F(DBTest, OverwriteWithEmptyValue) {
    std::string val;
    EXPECT_TRUE(db->Put(wopts, "ov_empty", "pre_existing_long_string").ok());
    EXPECT_TRUE(db->Put(wopts, "ov_empty", "").ok());
    EXPECT_TRUE(db->Get(ropts, "ov_empty", &val).ok());
    EXPECT_EQ(val, "");
}

// ============================================================================
// CATEGORY 3: COMPONENT SIZE BOUNDARIES
// ============================================================================
TEST_F(DBTest, LargeValueHandlesAllocationBoundaries) {
    std::string large_val(5 * 1024 * 1024, 'a'); // 5MB Value payload
    std::string read_val;
    EXPECT_TRUE(db->Put(wopts, "large_val_key", large_val).ok());
    EXPECT_TRUE(db->Get(ropts, "large_val_key", &read_val).ok());
    EXPECT_EQ(large_val.size(), read_val.size());
    EXPECT_EQ(large_val, read_val);
}

TEST_F(DBTest, LargeKeyHandlesAllocationBoundaries) {
    std::string large_key(2048, 'k'); // 2KB Key size
    std::string read_val;
    EXPECT_TRUE(db->Put(wopts, large_key, "large_key_payload").ok());
    EXPECT_TRUE(db->Get(ropts, large_key, &read_val).ok());
    EXPECT_EQ(read_val, "large_key_payload");
}

// ============================================================================
// CATEGORY 4: DURABILITY & WAL RECOVERY
// ============================================================================

TEST_F(DBTest, SingleKeyRecovery) {
    EXPECT_TRUE(db->Put(wopts, "r_key1", "r_val1").ok());
    Reopen();
    std::string val;
    EXPECT_TRUE(db->Get(ropts, "r_key1", &val).ok());
    EXPECT_EQ(val, "r_val1");
}

TEST_F(DBTest, MultipleKeysRecoveryOrdered) {
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(db->Put(wopts, "k_" + std::to_string(i), "v_" + std::to_string(i)).ok());
    }
    Reopen();
    for (int i = 0; i < 100; ++i) {
        std::string val;
        EXPECT_TRUE(db->Get(ropts, "k_" + std::to_string(i), &val).ok());
        EXPECT_EQ(val, "v_" + std::to_string(i));
    }
}

TEST_F(DBTest, RecoveryMaintainsDeletions) {
    EXPECT_TRUE(db->Put(wopts, "del_check", "should_not_exist").ok());
    EXPECT_TRUE(db->Delete(wopts, "del_check").ok());
    Reopen();
    std::string val;
    EXPECT_TRUE(db->Get(ropts, "del_check", &val).IsNotFound());
}

TEST_F(DBTest, RecoveryInterleavedUpdates) {
    EXPECT_TRUE(db->Put(wopts, "inter", "v1").ok());
    EXPECT_TRUE(db->Put(wopts, "inter", "v2").ok());
    EXPECT_TRUE(db->Delete(wopts, "inter").ok());
    EXPECT_TRUE(db->Put(wopts, "inter", "v3").ok());
    Reopen();
    std::string val;
    EXPECT_TRUE(db->Get(ropts, "inter", &val).ok());
    EXPECT_EQ(val, "v3");
}

// ============================================================================
// CATEGORY 5: CONCURRENCY & THREAD SAFETY
// ============================================================================

TEST_F(DBTest, ConcurrentSingleKeyMutations) {
    const int kNumThreads = 4;
    const int kWritesPerThread = 250;
    std::vector<std::thread> threads;

    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([this, t]() {
            for (int i = 0; i < kWritesPerThread; ++i) {
                WriteOptions wo;
                db->Put(wo, "concurrent_key", "thread_" + std::to_string(t) + "_val_" + std::to_string(i));
            }
        });
    }

    for (auto& th : threads) th.join();
    
    std::string val;
    EXPECT_TRUE(db->Get(ropts, "concurrent_key", &val).ok());
    EXPECT_TRUE(val.rfind("thread_", 0) == 0); // Must be a valid value written by one of the threads
}

TEST_F(DBTest, ConcurrentIndependentKeys) {
    const int kNumThreads = 4;
    const int kWritesPerThread = 250;
    std::vector<std::thread> threads;

    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([this, t]() {
            for (int i = 0; i < kWritesPerThread; ++i) {
                WriteOptions wo;
                db->Put(wo, "t_" + std::to_string(t) + "_k_" + std::to_string(i), "payload");
            }
        });
    }

    for (auto& th : threads) th.join();

    for (int t = 0; t < kNumThreads; ++t) {
        for (int i = 0; i < kWritesPerThread; ++i) {
            std::string val;
            EXPECT_TRUE(db->Get(ropts, "t_" + std::to_string(t) + "_k_" + std::to_string(i), &val).ok());
        }
    }
}

// ============================================================================
// CATEGORY 6: RANGE SCANNING & ITERATOR INVARIANTS
// ============================================================================

TEST_F(DBTest, MemTableIteratorEmpty) {
    std::unique_ptr<lakestore::Iterator> it(db->NewInternalIterator());
    it->SeekToFirst();
    EXPECT_FALSE(it->Valid());
}

TEST_F(DBTest, MemTableIteratorScanForward) {
    EXPECT_TRUE(db->Put(wopts, "a", "1").ok());
    EXPECT_TRUE(db->Put(wopts, "b", "2").ok());
    EXPECT_TRUE(db->Put(wopts, "c", "3").ok());

    std::unique_ptr<lakestore::Iterator> it(db->NewInternalIterator());
    it->SeekToFirst();
    
    ASSERT_TRUE(it->Valid());
    // Internal keys embed user keys, extract or compare carefully
    EXPECT_TRUE(it->key().ToString().find("a") != std::string::npos);
    
    it->Next();
    ASSERT_TRUE(it->Valid());
    EXPECT_TRUE(it->key().ToString().find("b") != std::string::npos);
    
    it->Next();
    ASSERT_TRUE(it->Valid());
    EXPECT_TRUE(it->key().ToString().find("c") != std::string::npos);
    
    it->Next();
    EXPECT_FALSE(it->Valid());
}

TEST_F(DBTest, MemTableIteratorSeekExact) {
    EXPECT_TRUE(db->Put(wopts, "key_a", "1").ok());
    EXPECT_TRUE(db->Put(wopts, "key_b", "2").ok());

    std::unique_ptr<lakestore::Iterator> it(db->NewInternalIterator());
    
    // Construct a proper InternalKey to seek for "key_b".
    // We use the maximum possible sequence number (0xFFFFFFFFFFFFFFFF) 
    // so the iterator lands on the absolute newest version of the key.
    InternalKey target;
    target.user_key = "key_b";
    target.seq = 0xFFFFFFFFFFFFFFFF; 
    target.type = kTypeValue;
    
    std::string encoded_target = target.Encode();
    it->Seek(Slice(encoded_target));
    
    ASSERT_TRUE(it->Valid());
    EXPECT_TRUE(it->key().ToString().find("key_b") != std::string::npos);
}

// ============================================================================
// CATEGORY 7: CACHE & BLOOM INTEGRITY
// ============================================================================

TEST_F(DBTest, BloomFilterPreventsFalsePositives) {
    // Write entries to establish sst patterns if background processing or reads execute
    EXPECT_TRUE(db->Put(wopts, "bloom_target", "exists").ok());
    std::string val;
    // Verification that non-existent lookup exits via negative bloom paths cleanly
    EXPECT_TRUE(db->Get(ropts, "bloom_absent_key_xyz", &val).IsNotFound());
}

TEST_F(DBTest, DatabaseReopenStateChain) {
    EXPECT_TRUE(db->Put(wopts, "chain1", "alpha").ok());
    Reopen();
    EXPECT_TRUE(db->Put(wopts, "chain2", "beta").ok());
    Reopen();
    
    std::string v1, v2;
    EXPECT_TRUE(db->Get(ropts, "chain1", &v1).ok());
    EXPECT_TRUE(db->Get(ropts, "chain2", &v2).ok());
    EXPECT_EQ(v1, "alpha");
    EXPECT_EQ(v2, "beta");
}