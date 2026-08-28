// End-to-end tests that exercise the DISK path: memtable flushes, SSTable
// reads, compaction, manifest recovery, WAL rotation and cleanup. The
// fixture uses a tiny write buffer so a handful of writes reach L0.
#include <gtest/gtest.h>
#include "db/db_impl.h"
#include "options.h"
#include <atomic>
#include <filesystem>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace lakestore;

namespace {

int CountFiles(const std::string& dir, const std::string& ext) {
    int count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().extension() == ext) count++;
    }
    return count;
}

} // namespace

class DBDiskTest : public ::testing::Test {
protected:
    std::string db_path = "./lakestore_disk_test_data";
    DBImpl* db = nullptr;
    Options opts;
    WriteOptions wopts;
    ReadOptions ropts;

    void SetUp() override {
        std::filesystem::remove_all(db_path);
        opts.write_buffer_size = 16 * 1024; // tiny: flush after ~16KB
        opts.block_size = 1024;
        opts.l0_compaction_trigger = 4;
        opts.max_bytes_for_level_base = 64 * 1024;
        opts.target_file_size = 16 * 1024;
        ASSERT_TRUE(DBImpl::Open(opts, db_path, &db).ok());
    }

    void TearDown() override {
        delete db;
        db = nullptr;
        std::filesystem::remove_all(db_path);
    }

    void Reopen() {
        delete db;
        db = nullptr;
        ASSERT_TRUE(DBImpl::Open(opts, db_path, &db).ok());
    }

    static std::string Key(int i) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key_%06d", i);
        return buf;
    }
    static std::string Value(int i, int gen = 0) {
        return "value_" + std::to_string(i) + "_gen" + std::to_string(gen) +
               std::string(100, 'p'); // padding so flushes trigger quickly
    }
};

// ---------------------------------------------------------------------------
// Flush path
// ---------------------------------------------------------------------------

TEST_F(DBDiskTest, FlushCreatesSSTableAndDataStaysReadable) {
    const int N = 500;
    for (int i = 0; i < N; i++) {
        ASSERT_TRUE(db->Put(wopts, Key(i), Value(i)).ok());
    }
    ASSERT_TRUE(db->FlushAll().ok());

    EXPECT_GE(CountFiles(db_path, ".ldb"), 1);

    std::string val;
    for (int i = 0; i < N; i++) {
        ASSERT_TRUE(db->Get(ropts, Key(i), &val).ok()) << Key(i);
        EXPECT_EQ(val, Value(i));
    }
}

TEST_F(DBDiskTest, OverwriteAcrossFlushReturnsNewest) {
    ASSERT_TRUE(db->Put(wopts, "k", "v1").ok());
    ASSERT_TRUE(db->FlushAll().ok());
    ASSERT_TRUE(db->Put(wopts, "k", "v2").ok());
    ASSERT_TRUE(db->FlushAll().ok());

    std::string val;
    ASSERT_TRUE(db->Get(ropts, "k", &val).ok());
    EXPECT_EQ(val, "v2");
}

TEST_F(DBDiskTest, TombstoneAcrossFlushHidesOlderValue) {
    ASSERT_TRUE(db->Put(wopts, "doomed", "value").ok());
    ASSERT_TRUE(db->FlushAll().ok());
    ASSERT_TRUE(db->Delete(wopts, "doomed").ok());
    ASSERT_TRUE(db->FlushAll().ok());

    std::string val;
    EXPECT_TRUE(db->Get(ropts, "doomed", &val).IsNotFound());

    Reopen();
    EXPECT_TRUE(db->Get(ropts, "doomed", &val).IsNotFound());
}

// ---------------------------------------------------------------------------
// Recovery / manifest
// ---------------------------------------------------------------------------

TEST_F(DBDiskTest, SSTablesSurviveReopenViaManifest) {
    const int N = 500;
    for (int i = 0; i < N; i++) {
        ASSERT_TRUE(db->Put(wopts, Key(i), Value(i)).ok());
    }
    ASSERT_TRUE(db->FlushAll().ok());

    Reopen();

    // After FlushAll the data lives ONLY in SSTables (old WALs deleted),
    // so this proves the MANIFEST recovery path, not WAL replay.
    std::string val;
    for (int i = 0; i < N; i++) {
        ASSERT_TRUE(db->Get(ropts, Key(i), &val).ok()) << Key(i);
        EXPECT_EQ(val, Value(i));
    }
}

TEST_F(DBDiskTest, SequenceNumbersRestoredAfterReopen) {
    ASSERT_TRUE(db->Put(wopts, "seq_key", "v1").ok());
    Reopen();
    // If last_sequence_ were reset, this write would get an OLDER sequence
    // than the recovered "v1" and become invisible.
    ASSERT_TRUE(db->Put(wopts, "seq_key", "v2").ok());

    std::string val;
    ASSERT_TRUE(db->Get(ropts, "seq_key", &val).ok());
    EXPECT_EQ(val, "v2");

    Reopen();
    ASSERT_TRUE(db->Get(ropts, "seq_key", &val).ok());
    EXPECT_EQ(val, "v2");
}

TEST_F(DBDiskTest, MixedMemtableAndSSTableStateSurvivesReopen) {
    // Generation 0: flushed to disk
    for (int i = 0; i < 200; i++) {
        ASSERT_TRUE(db->Put(wopts, Key(i), Value(i, 0)).ok());
    }
    ASSERT_TRUE(db->FlushAll().ok());
    // Generation 1: half the keys overwritten, still in WAL/memtable
    for (int i = 0; i < 100; i++) {
        ASSERT_TRUE(db->Put(wopts, Key(i), Value(i, 1)).ok());
    }
    // A few deletions on top
    for (int i = 190; i < 200; i++) {
        ASSERT_TRUE(db->Delete(wopts, Key(i)).ok());
    }

    Reopen();

    std::string val;
    for (int i = 0; i < 100; i++) {
        ASSERT_TRUE(db->Get(ropts, Key(i), &val).ok()) << Key(i);
        EXPECT_EQ(val, Value(i, 1));
    }
    for (int i = 100; i < 190; i++) {
        ASSERT_TRUE(db->Get(ropts, Key(i), &val).ok()) << Key(i);
        EXPECT_EQ(val, Value(i, 0));
    }
    for (int i = 190; i < 200; i++) {
        EXPECT_TRUE(db->Get(ropts, Key(i), &val).IsNotFound()) << Key(i);
    }
}

TEST_F(DBDiskTest, ObsoleteWALFilesAreDeleted) {
    for (int i = 0; i < 1000; i++) {
        ASSERT_TRUE(db->Put(wopts, Key(i), Value(i)).ok());
    }
    ASSERT_TRUE(db->FlushAll().ok());

    // Everything is flushed; only the single active WAL should remain.
    EXPECT_EQ(CountFiles(db_path, ".log"), 1);
}

TEST_F(DBDiskTest, RepeatedReopenChain) {
    for (int round = 0; round < 5; round++) {
        ASSERT_TRUE(db->Put(wopts, "round_" + std::to_string(round), "v").ok());
        Reopen();
        std::string val;
        for (int r = 0; r <= round; r++) {
            ASSERT_TRUE(db->Get(ropts, "round_" + std::to_string(r), &val).ok())
                << "round " << r << " lost after reopen " << round;
        }
    }
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

TEST_F(DBDiskTest, CompactionMergesL0IntoL1AndKeepsData) {
    // Write enough to trigger several flushes AND at least one L0->L1
    // compaction (l0_compaction_trigger = 4).
    std::map<std::string, std::string> expected;
    for (int gen = 0; gen < 3; gen++) {
        for (int i = 0; i < 600; i++) {
            std::string v = Value(i, gen);
            ASSERT_TRUE(db->Put(wopts, Key(i), v).ok());
            expected[Key(i)] = v;
        }
    }
    ASSERT_TRUE(db->FlushAll().ok());

    std::string val;
    for (const auto& [k, v] : expected) {
        ASSERT_TRUE(db->Get(ropts, k, &val).ok()) << k;
        EXPECT_EQ(val, v);
    }

    Reopen();
    for (const auto& [k, v] : expected) {
        ASSERT_TRUE(db->Get(ropts, k, &val).ok()) << k << " after reopen";
        EXPECT_EQ(val, v);
    }
}

TEST_F(DBDiskTest, CompactionDropsDeletedKeys) {
    for (int i = 0; i < 300; i++) {
        ASSERT_TRUE(db->Put(wopts, Key(i), Value(i)).ok());
    }
    for (int i = 0; i < 300; i += 2) {
        ASSERT_TRUE(db->Delete(wopts, Key(i)).ok());
    }
    ASSERT_TRUE(db->FlushAll().ok());

    std::string val;
    for (int i = 0; i < 300; i++) {
        if (i % 2 == 0) {
            EXPECT_TRUE(db->Get(ropts, Key(i), &val).IsNotFound()) << Key(i);
        } else {
            ASSERT_TRUE(db->Get(ropts, Key(i), &val).ok()) << Key(i);
        }
    }

    Reopen();
    for (int i = 0; i < 300; i++) {
        if (i % 2 == 0) {
            EXPECT_TRUE(db->Get(ropts, Key(i), &val).IsNotFound()) << Key(i);
        } else {
            ASSERT_TRUE(db->Get(ropts, Key(i), &val).ok()) << Key(i);
        }
    }
}

// ---------------------------------------------------------------------------
// DB-wide iterator
// ---------------------------------------------------------------------------

TEST_F(DBDiskTest, IteratorScansMergedViewInOrder) {
    // Data spread across SSTables (flushed) and the memtable.
    std::map<std::string, std::string> expected;
    for (int i = 0; i < 400; i++) {
        ASSERT_TRUE(db->Put(wopts, Key(i), Value(i, 0)).ok());
        expected[Key(i)] = Value(i, 0);
    }
    ASSERT_TRUE(db->FlushAll().ok());
    for (int i = 100; i < 200; i++) { // overwrite in memtable
        ASSERT_TRUE(db->Put(wopts, Key(i), Value(i, 1)).ok());
        expected[Key(i)] = Value(i, 1);
    }
    for (int i = 300; i < 350; i++) { // delete in memtable
        ASSERT_TRUE(db->Delete(wopts, Key(i)).ok());
        expected.erase(Key(i));
    }

    std::unique_ptr<Iterator> it(db->NewIterator());
    auto exp_it = expected.begin();
    int count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next(), ++exp_it) {
        ASSERT_NE(exp_it, expected.end());
        EXPECT_EQ(it->key().ToString(), exp_it->first);
        EXPECT_EQ(it->value().ToString(), exp_it->second);
        count++;
    }
    EXPECT_EQ(exp_it, expected.end());
    EXPECT_EQ(count, static_cast<int>(expected.size()));
}

TEST_F(DBDiskTest, IteratorSeekLandsOnCorrectKey) {
    for (int i = 0; i < 100; i++) {
        ASSERT_TRUE(db->Put(wopts, Key(i * 2), "even").ok()); // only even keys
    }
    ASSERT_TRUE(db->FlushAll().ok());

    std::unique_ptr<Iterator> it(db->NewIterator());

    it->Seek(Key(50)); // exists
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->key().ToString(), Key(50));

    it->Seek(Key(51)); // gap -> lands on next key
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->key().ToString(), Key(52));

    it->Seek(Key(9999)); // past the end
    EXPECT_FALSE(it->Valid());
}

TEST_F(DBDiskTest, IteratorIsStableSnapshot) {
    for (int i = 0; i < 50; i++) {
        ASSERT_TRUE(db->Put(wopts, Key(i), "before").ok());
    }
    std::unique_ptr<Iterator> it(db->NewIterator());

    // Mutations after iterator creation must not be visible to it.
    for (int i = 0; i < 50; i++) {
        ASSERT_TRUE(db->Put(wopts, Key(i), "after").ok());
    }
    ASSERT_TRUE(db->Delete(wopts, Key(0)).ok());

    int count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        EXPECT_EQ(it->value().ToString(), "before") << it->key().ToString();
        count++;
    }
    EXPECT_EQ(count, 50);
}

// ---------------------------------------------------------------------------
// Larger-scale sanity
// ---------------------------------------------------------------------------

TEST_F(DBDiskTest, ConcurrentReadersWritersDuringFlushes) {
    // Hammer the DB from multiple threads while tiny memtables force
    // continuous flushes and compactions underneath.
    std::atomic<bool> stop{false};
    std::atomic<int> errors{0};

    std::vector<std::thread> writers;
    for (int t = 0; t < 4; t++) {
        writers.emplace_back([&, t]() {
            WriteOptions w;
            for (int i = 0; i < 2000; i++) {
                int k = t * 2000 + i;
                if (!db->Put(w, Key(k), Value(k)).ok()) errors++;
            }
        });
    }

    std::vector<std::thread> readers;
    for (int t = 0; t < 3; t++) {
        readers.emplace_back([&]() {
            ReadOptions r;
            std::string val;
            while (!stop.load()) {
                int k = rand() % 8000;
                Status s = db->Get(r, Key(k), &val);
                // NotFound is fine (not written yet); errors are not.
                if (!s.ok() && !s.IsNotFound()) errors++;
                if (s.ok() && val != Value(k)) errors++;
            }
        });
    }

    // A scanning thread creating iterators mid-churn.
    std::thread scanner([&]() {
        while (!stop.load()) {
            std::unique_ptr<Iterator> it(db->NewIterator());
            std::string prev;
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                std::string cur = it->key().ToString();
                if (!prev.empty() && prev >= cur) errors++;
                prev = cur;
            }
        }
    });

    for (auto& w : writers) w.join();
    stop = true;
    for (auto& r : readers) r.join();
    scanner.join();

    EXPECT_EQ(errors.load(), 0);

    ASSERT_TRUE(db->FlushAll().ok());
    std::string val;
    for (int k = 0; k < 8000; k++) {
        ASSERT_TRUE(db->Get(ropts, Key(k), &val).ok()) << Key(k);
        EXPECT_EQ(val, Value(k));
    }
}

TEST_F(DBDiskTest, TenThousandKeysWithOverwritesAndDeletes) {
    const int N = 10000;
    for (int i = 0; i < N; i++) {
        ASSERT_TRUE(db->Put(wopts, Key(i), Value(i, 0)).ok());
    }
    for (int i = 0; i < N; i += 3) {
        ASSERT_TRUE(db->Put(wopts, Key(i), Value(i, 1)).ok());
    }
    for (int i = 0; i < N; i += 7) {
        ASSERT_TRUE(db->Delete(wopts, Key(i)).ok());
    }
    ASSERT_TRUE(db->FlushAll().ok());
    Reopen();

    std::string val;
    for (int i = 0; i < N; i++) {
        Status s = db->Get(ropts, Key(i), &val);
        if (i % 7 == 0) {
            EXPECT_TRUE(s.IsNotFound()) << Key(i);
        } else if (i % 3 == 0) {
            ASSERT_TRUE(s.ok()) << Key(i) << ": " << s.ToString();
            EXPECT_EQ(val, Value(i, 1));
        } else {
            ASSERT_TRUE(s.ok()) << Key(i) << ": " << s.ToString();
            EXPECT_EQ(val, Value(i, 0));
        }
    }
}
