#include <gtest/gtest.h>
#include "db/db_impl.h"
#include "options.h"
#include <filesystem>
#include <string>

using namespace lakestore;

class DBTest : public ::testing::Test {
protected:
    std::string db_path = "/tmp/lakestore_gtest";
    DBImpl* db = nullptr;
    Options opts;
    WriteOptions wopts;
    ReadOptions ropts;

    void SetUp() override {
        std::filesystem::remove_all(db_path);
        std::filesystem::create_directories(db_path);
        ASSERT_TRUE(DBImpl::Open(opts, db_path, &db).ok());
    }

    void TearDown() override {
        delete db;
        db = nullptr;
        std::filesystem::remove_all(db_path);
    }

    // Helper to reopen DB (useful for testing recovery)
    void Reopen() {
        delete db;
        db = nullptr;
        ASSERT_TRUE(DBImpl::Open(opts, db_path, &db).ok());
    }
};

TEST_F(DBTest, BasicPutAndGet) {
    std::string value;
    
    // Key should not exist initially
    Status s = db->Get(ropts, "key1", &value);
    EXPECT_TRUE(s.IsNotFound());

    // Put and verify
    EXPECT_TRUE(db->Put(wopts, "key1", "value1").ok());
    
    s = db->Get(ropts, "key1", &value);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(value, "value1");
}

TEST_F(DBTest, DeleteAndOverwrite) {
    std::string value;
    
    EXPECT_TRUE(db->Put(wopts, "key2", "value2").ok());
    EXPECT_TRUE(db->Delete(wopts, "key2").ok());
    
    EXPECT_TRUE(db->Get(ropts, "key2", &value).IsNotFound());

    // Overwrite the tombstone
    EXPECT_TRUE(db->Put(wopts, "key2", "new_value2").ok());
    EXPECT_TRUE(db->Get(ropts, "key2", &value).ok());
    EXPECT_EQ(value, "new_value2");
}

TEST_F(DBTest, WALRecovery) {
    // 1. Write data
    EXPECT_TRUE(db->Put(wopts, "persist_key", "survive_crash").ok());
    
    // 2. Simulate a crash / shutdown by deleting the DB object
    // This forces the MemTable to drop from RAM. The only record is in the WAL.
    Reopen();
    
    // 3. Verify data was recovered from the WAL
    std::string value;
    Status s = db->Get(ropts, "persist_key", &value);
    EXPECT_TRUE(s.ok()) << "Failed to recover data from WAL!";
    EXPECT_EQ(value, "survive_crash");
}