// Unit tests for the individual engine components: coding, CRC32C, bloom
// filter, block builder/reader, WAL record log, LRU cache, comparators.
#include <gtest/gtest.h>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "util/coding.h"
#include "util/crc32c.h"
#include "types.h"
#include "options.h"
#include "sstable/block.h"
#include "sstable/block_builder.h"
#include "sstable/bloom_filter.h"
#include "sstable/sstable_writer.h"
#include "sstable/sstable_reader.h"
#include "cache/lru_cache.h"
#include "wal/wal.h"
#include "db/merging_iterator.h"
#include "memtable/memtable.h"

using namespace lakestore;

// ============================================================================
// Coding
// ============================================================================

TEST(Coding, Varint32RoundTrip) {
    std::string buf;
    std::vector<uint32_t> values = {0, 1, 127, 128, 16383, 16384, 1u << 30, UINT32_MAX};
    for (uint32_t v : values) PutVarint32(&buf, v);

    Slice input(buf);
    for (uint32_t expected : values) {
        uint32_t v = 0;
        ASSERT_TRUE(GetVarint32(&input, &v));
        EXPECT_EQ(v, expected);
    }
    EXPECT_EQ(input.size, 0u);
}

TEST(Coding, Varint64RoundTrip) {
    std::string buf;
    std::vector<uint64_t> values = {0, 1, 127, 128, 1ull << 32, 1ull << 56, UINT64_MAX};
    for (uint64_t v : values) PutVarint64(&buf, v);

    Slice input(buf);
    for (uint64_t expected : values) {
        uint64_t v = 0;
        ASSERT_TRUE(GetVarint64(&input, &v));
        EXPECT_EQ(v, expected);
    }
    EXPECT_EQ(input.size, 0u);
}

TEST(Coding, FixedRoundTrip) {
    std::string buf;
    PutFixed32(&buf, 0xDEADBEEF);
    PutFixed64(&buf, 0x0123456789ABCDEFULL);
    EXPECT_EQ(DecodeFixed32(buf.data()), 0xDEADBEEFu);
    EXPECT_EQ(DecodeFixed64(buf.data() + 4), 0x0123456789ABCDEFULL);
}

TEST(Coding, TruncatedVarintFails) {
    std::string buf;
    PutVarint32(&buf, 300000); // multi-byte varint
    Slice truncated(buf.data(), 1);
    uint32_t v;
    EXPECT_FALSE(GetVarint32(&truncated, &v));
}

// ============================================================================
// CRC32C
// ============================================================================

TEST(CRC32C, KnownVectors) {
    // Standard check value for CRC-32C: crc("123456789") = 0xE3069283
    EXPECT_EQ(crc32c::Value("123456789", 9), 0xE3069283u);
    // 32 bytes of zero: 0x8A9136AA (iSCSI test vector, RFC 3720)
    char zeros[32] = {0};
    EXPECT_EQ(crc32c::Value(zeros, 32), 0x8A9136AAu);
}

TEST(CRC32C, ExtendMatchesWhole) {
    std::string data = "hello lakestore crc32c";
    uint32_t whole = crc32c::Value(data.data(), data.size());
    uint32_t partial = crc32c::Value(data.data(), 5);
    partial = crc32c::Extend(partial, data.data() + 5, data.size() - 5);
    EXPECT_EQ(whole, partial);
}

// ============================================================================
// Internal key comparator
// ============================================================================

TEST(InternalKeyCompare, EncodedMatchesStruct) {
    auto make = [](const std::string& uk, SequenceNumber seq, ValueType t) {
        InternalKey ik; ik.user_key = uk; ik.seq = seq; ik.type = t;
        return ik;
    };
    std::vector<InternalKey> keys = {
        make("app", 5, kTypeValue),
        make("apple", 100, kTypeValue),
        make("apple", 3, kTypeDeletion),
        make("banana", 1, kTypeValue),
        make("apple", 3, kTypeValue),
    };
    for (const auto& a : keys) {
        for (const auto& b : keys) {
            std::string ea = a.Encode(), eb = b.Encode();
            int expected = CompareInternalKeys(a, b);
            int actual = CompareEncodedInternalKeys(Slice(ea), Slice(eb));
            // Normalize signs
            expected = (expected > 0) - (expected < 0);
            actual = (actual > 0) - (actual < 0);
            EXPECT_EQ(actual, expected)
                << a.user_key << "/" << a.seq << " vs " << b.user_key << "/" << b.seq;
        }
    }
}

TEST(InternalKeyCompare, PrefixProblem) {
    // "app" < "apple" as user keys, but a raw memcmp of encoded internal keys
    // would compare the tag bytes of "app" against the letters of "apple".
    InternalKey a; a.user_key = "app"; a.seq = 1; a.type = kTypeValue;
    InternalKey b; b.user_key = "apple"; b.seq = kMaxSequenceNumber; b.type = kTypeValue;
    std::string ea = a.Encode(), eb = b.Encode();
    EXPECT_LT(CompareEncodedInternalKeys(Slice(ea), Slice(eb)), 0);
}

// ============================================================================
// Bloom filter
// ============================================================================

TEST(Bloom, NoFalseNegatives) {
    BloomFilter bf(10);
    for (int i = 0; i < 1000; i++) {
        std::string key = "bloom_key_" + std::to_string(i);
        bf.AddKey(key);
    }
    std::string filter = bf.Finish().ToString();
    for (int i = 0; i < 1000; i++) {
        std::string key = "bloom_key_" + std::to_string(i);
        EXPECT_TRUE(BloomFilter::MayContain(Slice(filter), key)) << key;
    }
}

TEST(Bloom, LowFalsePositiveRate) {
    BloomFilter bf(10);
    for (int i = 0; i < 10000; i++) {
        bf.AddKey("present_" + std::to_string(i));
    }
    std::string filter = bf.Finish().ToString();
    int false_positives = 0;
    for (int i = 0; i < 10000; i++) {
        if (BloomFilter::MayContain(Slice(filter), "absent_" + std::to_string(i))) {
            false_positives++;
        }
    }
    // 10 bits/key should give ~1% FP; allow generous headroom.
    EXPECT_LT(false_positives, 500);
}

// ============================================================================
// Block builder / reader
// ============================================================================

static std::string EncodeIK(const std::string& uk, SequenceNumber seq, ValueType t) {
    InternalKey ik; ik.user_key = uk; ik.seq = seq; ik.type = t;
    return ik.Encode();
}

TEST(BlockTest, RoundTripWithRestarts) {
    Options opts;
    opts.block_restart_interval = 4; // force several restart points
    BlockBuilder builder(&opts);

    std::vector<std::pair<std::string, std::string>> entries;
    for (int i = 0; i < 100; i++) {
        char buf[16];
        snprintf(buf, sizeof(buf), "key_%04d", i);
        entries.push_back({EncodeIK(buf, 10, kTypeValue), "value_" + std::to_string(i)});
    }
    for (const auto& [k, v] : entries) builder.Add(Slice(k), Slice(v));

    Slice raw = builder.Finish();
    char* copy = new char[raw.size];
    memcpy(copy, raw.data, raw.size);
    Block block(copy, raw.size); // Block owns `copy`

    // Full scan sees every entry in order
    std::unique_ptr<Iterator> it(block.NewIterator());
    size_t idx = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ASSERT_LT(idx, entries.size());
        EXPECT_EQ(it->key().ToString(), entries[idx].first);
        EXPECT_EQ(it->value().ToString(), entries[idx].second);
        idx++;
    }
    EXPECT_EQ(idx, entries.size()); // Including the LAST entry of the block

    // Seek to each key lands exactly on it
    for (const auto& [k, v] : entries) {
        it->Seek(Slice(k));
        ASSERT_TRUE(it->Valid()) << k;
        EXPECT_EQ(it->key().ToString(), k);
        EXPECT_EQ(it->value().ToString(), v);
    }

    // Seek past the end is invalid
    std::string past = EncodeIK("zzzz", kMaxSequenceNumber, kTypeValue);
    it->Seek(Slice(past));
    EXPECT_FALSE(it->Valid());
}

TEST(BlockTest, SingleEntryBlockIsVisible) {
    Options opts;
    BlockBuilder builder(&opts);
    std::string k = EncodeIK("only", 7, kTypeValue);
    builder.Add(Slice(k), Slice("value"));
    Slice raw = builder.Finish();
    char* copy = new char[raw.size];
    memcpy(copy, raw.data, raw.size);
    Block block(copy, raw.size);

    std::unique_ptr<Iterator> it(block.NewIterator());
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->key().ToString(), k);
    it->Next();
    EXPECT_FALSE(it->Valid());
}

// ============================================================================
// SSTable writer + reader
// ============================================================================

class SSTableTest : public ::testing::Test {
protected:
    std::string path = "./sstable_test_dir";
    void SetUp() override {
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
    }
    void TearDown() override { std::filesystem::remove_all(path); }
};

TEST_F(SSTableTest, WriteReadManyBlocks) {
    Options opts;
    opts.block_size = 512; // force many data blocks
    std::string file = path + "/1.ldb";

    SSTableWriter writer(file, opts);
    const int N = 2000;
    for (int i = 0; i < N; i++) {
        char buf[16];
        snprintf(buf, sizeof(buf), "key_%05d", i);
        std::string k = EncodeIK(buf, 42, kTypeValue);
        writer.Add(Slice(k), Slice("val_" + std::to_string(i)));
    }
    ASSERT_TRUE(writer.Finish().ok());
    EXPECT_EQ(writer.NumEntries(), static_cast<uint64_t>(N));

    SSTableReader* reader = nullptr;
    ASSERT_TRUE(SSTableReader::Open(file, 1, &reader).ok());

    ReadOptions ropts;
    // Point lookups on every key
    for (int i = 0; i < N; i++) {
        char buf[16];
        snprintf(buf, sizeof(buf), "key_%05d", i);
        std::string lookup = EncodeIK(buf, kMaxSequenceNumber, kTypeValue);
        std::string val;
        TableLookup result;
        ASSERT_TRUE(reader->Get(ropts, Slice(lookup), &val, &result).ok());
        ASSERT_EQ(result, TableLookup::kFound) << buf;
        EXPECT_EQ(val, "val_" + std::to_string(i));
    }

    // Missing keys are absent
    std::string missing = EncodeIK("nope", kMaxSequenceNumber, kTypeValue);
    std::string val;
    TableLookup result;
    ASSERT_TRUE(reader->Get(ropts, Slice(missing), &val, &result).ok());
    EXPECT_EQ(result, TableLookup::kAbsent);

    // Full scan yields all entries in order
    std::unique_ptr<Iterator> it(reader->NewIterator(ropts));
    int count = 0;
    std::string prev;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string cur = it->key().ToString();
        if (!prev.empty()) {
            EXPECT_LT(CompareEncodedInternalKeys(Slice(prev), Slice(cur)), 0);
        }
        prev = cur;
        count++;
    }
    EXPECT_EQ(count, N);
    delete reader;
}

TEST_F(SSTableTest, TombstonesAndMVCC) {
    Options opts;
    std::string file = path + "/2.ldb";

    SSTableWriter writer(file, opts);
    // Internal order for one user key: newest (highest seq) first.
    std::string k_del = EncodeIK("k1", 20, kTypeDeletion);
    std::string k_old = EncodeIK("k1", 10, kTypeValue);
    writer.Add(Slice(k_del), Slice());
    writer.Add(Slice(k_old), Slice("old_value"));
    ASSERT_TRUE(writer.Finish().ok());

    SSTableReader* reader = nullptr;
    ASSERT_TRUE(SSTableReader::Open(file, 2, &reader).ok());
    ReadOptions ropts;
    std::string val;
    TableLookup result;

    // Snapshot >= 20 sees the tombstone
    std::string lookup_new = EncodeIK("k1", kMaxSequenceNumber, kTypeValue);
    ASSERT_TRUE(reader->Get(ropts, Slice(lookup_new), &val, &result).ok());
    EXPECT_EQ(result, TableLookup::kDeleted);

    // Snapshot 15 (between versions) still sees the old value
    std::string lookup_mid = EncodeIK("k1", 15, kTypeValue);
    ASSERT_TRUE(reader->Get(ropts, Slice(lookup_mid), &val, &result).ok());
    EXPECT_EQ(result, TableLookup::kFound);
    EXPECT_EQ(val, "old_value");
    delete reader;
}

TEST_F(SSTableTest, BlockCacheServesRepeatReads) {
    Options opts;
    std::string file = path + "/3.ldb";
    SSTableWriter writer(file, opts);
    std::string k = EncodeIK("cached", 5, kTypeValue);
    writer.Add(Slice(k), Slice("v"));
    ASSERT_TRUE(writer.Finish().ok());

    SSTableReader* reader = nullptr;
    ASSERT_TRUE(SSTableReader::Open(file, 3, &reader).ok());

    LRUCache cache(1 << 20);
    ReadOptions ropts;
    ropts.block_cache = &cache;

    std::string lookup = EncodeIK("cached", kMaxSequenceNumber, kTypeValue);
    std::string val;
    TableLookup result;
    ASSERT_TRUE(reader->Get(ropts, Slice(lookup), &val, &result).ok());
    EXPECT_EQ(result, TableLookup::kFound);
    EXPECT_GT(cache.TotalCharge(), 0u); // block landed in the cache

    ASSERT_TRUE(reader->Get(ropts, Slice(lookup), &val, &result).ok());
    EXPECT_EQ(result, TableLookup::kFound);
    delete reader;
}

// ============================================================================
// WAL record log
// ============================================================================

class WALTest : public ::testing::Test {
protected:
    std::string path = "./wal_test_dir";
    void SetUp() override {
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
    }
    void TearDown() override { std::filesystem::remove_all(path); }
};

TEST_F(WALTest, RoundTripSmallRecords) {
    std::string file = path + "/1.log";
    std::vector<std::string> records = {"alpha", "beta", "", "gamma delta"};
    {
        WAL wal(file, 1);
        for (const auto& r : records) {
            ASSERT_TRUE(wal.AppendRecord(Slice(r)).ok());
        }
        ASSERT_TRUE(wal.Sync().ok());
    }

    WAL wal(file, 1);
    std::vector<std::string> recovered;
    ASSERT_TRUE(wal.Recover([&](const Slice& rec) {
        recovered.push_back(rec.ToString());
        return Status::OK();
    }).ok());
    EXPECT_EQ(recovered, records);
}

TEST_F(WALTest, RoundTripLargeRecordSpanningBlocks) {
    std::string file = path + "/2.log";
    // > 32KB forces First/Middle/Last fragmentation
    std::string big(100 * 1024, 'x');
    for (size_t i = 0; i < big.size(); i += 997) big[i] = 'A' + (i % 26);

    {
        WAL wal(file, 2);
        ASSERT_TRUE(wal.AppendRecord(Slice(big)).ok());
        ASSERT_TRUE(wal.AppendRecord(Slice("after_big")).ok());
    }

    WAL wal(file, 2);
    std::vector<std::string> recovered;
    ASSERT_TRUE(wal.Recover([&](const Slice& rec) {
        recovered.push_back(rec.ToString());
        return Status::OK();
    }).ok());
    ASSERT_EQ(recovered.size(), 2u);
    EXPECT_EQ(recovered[0], big);
    EXPECT_EQ(recovered[1], "after_big");
}

TEST_F(WALTest, TruncatedTailIsIgnored) {
    std::string file = path + "/3.log";
    {
        WAL wal(file, 3);
        ASSERT_TRUE(wal.AppendRecord(Slice("complete_record")).ok());
    }
    // Simulate a crash mid-write: chop bytes off the end.
    auto size = std::filesystem::file_size(file);
    std::filesystem::resize_file(file, size - 3);

    WAL wal(file, 3);
    std::vector<std::string> recovered;
    ASSERT_TRUE(wal.Recover([&](const Slice& rec) {
        recovered.push_back(rec.ToString());
        return Status::OK();
    }).ok());
    // The damaged record must not surface; recovery stops cleanly.
    EXPECT_TRUE(recovered.empty());
}

TEST_F(WALTest, CorruptedRecordStopsRecovery) {
    std::string file = path + "/4.log";
    {
        WAL wal(file, 4);
        ASSERT_TRUE(wal.AppendRecord(Slice("good_record")).ok());
    }
    // Flip a payload byte -> CRC mismatch
    {
        std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(10);
        f.put('X');
    }
    WAL wal(file, 4);
    std::vector<std::string> recovered;
    ASSERT_TRUE(wal.Recover([&](const Slice& rec) {
        recovered.push_back(rec.ToString());
        return Status::OK();
    }).ok());
    EXPECT_TRUE(recovered.empty());
}

// ============================================================================
// LRU cache
// ============================================================================

static std::shared_ptr<Block> MakeTestBlock() {
    Options opts;
    BlockBuilder b(&opts);
    std::string k = EncodeIK("k", 1, kTypeValue);
    b.Add(Slice(k), Slice("v"));
    Slice raw = b.Finish();
    char* copy = new char[raw.size];
    memcpy(copy, raw.data, raw.size);
    return std::make_shared<Block>(copy, raw.size);
}

TEST(LRUCacheTest, InsertLookupEvict) {
    LRUCache cache(16 * 100); // tiny: 16 shards x 100 bytes
    for (uint64_t i = 0; i < 64; i++) {
        cache.Insert(1, i * 4096, MakeTestBlock(), 50);
    }
    // Capacity per shard is ~100 bytes -> at most 2 entries of charge 50 each.
    EXPECT_LE(cache.TotalCharge(), 16u * 100u);

    cache.Insert(2, 0, MakeTestBlock(), 50);
    EXPECT_NE(cache.Lookup(2, 0), nullptr);
    EXPECT_EQ(cache.Lookup(99, 12345), nullptr);
}

// ============================================================================
// Merging iterator
// ============================================================================

TEST(MergingIteratorTest, MergesSortedStreamsNewestFirst) {
    // Two memtables with overlapping keys and different sequences.
    MemTable m1, m2;
    m1.Add(10, kTypeValue, "a", "a_new");
    m1.Add(11, kTypeValue, "c", "c_only");
    m2.Add(5, kTypeValue, "a", "a_old");
    m2.Add(6, kTypeValue, "b", "b_only");

    std::vector<Iterator*> children = {m1.NewIterator(), m2.NewIterator()};
    MergingIterator merged(children);

    std::vector<std::pair<std::string, SequenceNumber>> seen;
    for (merged.SeekToFirst(); merged.Valid(); merged.Next()) {
        InternalKey ik = InternalKey::Decode(merged.key());
        seen.push_back({ik.user_key, ik.seq});
    }
    // Expected internal order: a/10 (newest first), a/5, b/6, c/11
    ASSERT_EQ(seen.size(), 4u);
    EXPECT_EQ(seen[0], (std::pair<std::string, SequenceNumber>{"a", 10}));
    EXPECT_EQ(seen[1], (std::pair<std::string, SequenceNumber>{"a", 5}));
    EXPECT_EQ(seen[2], (std::pair<std::string, SequenceNumber>{"b", 6}));
    EXPECT_EQ(seen[3], (std::pair<std::string, SequenceNumber>{"c", 11}));
}
