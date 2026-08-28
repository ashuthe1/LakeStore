#include "sstable_reader.h"
#include "sstable_writer.h" // for kFooterSize / kTableMagicNumber
#include "../util/coding.h"
#include "../util/json_builder.h"
#include "../cache/lru_cache.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cmath>
#include <cstring>
#include <memory>

namespace lakestore {

Status SSTableReader::ReadBlock(uint64_t offset, size_t size, Block** block) const {
    char* buf = new char[size];
    ssize_t bytes_read = ::pread(fd_, buf, size, offset);

    if (bytes_read != static_cast<ssize_t>(size)) {
        delete[] buf;
        return Status::IOError("Failed to read complete block");
    }

    *block = new Block(buf, size);
    return Status::OK();
}

Status SSTableReader::GetBlock(const ReadOptions& opts, uint64_t offset, size_t size,
                               std::shared_ptr<Block>* block) const {
    if (opts.block_cache) {
        *block = opts.block_cache->Lookup(file_number_, offset);
        if (*block) return Status::OK();
    }

    Block* raw_block = nullptr;
    Status s = ReadBlock(offset, size, &raw_block);
    if (!s.ok()) return s;

    block->reset(raw_block);
    if (opts.block_cache && opts.fill_cache) {
        opts.block_cache->Insert(file_number_, offset, *block, size);
    }
    return Status::OK();
}

Status SSTableReader::Open(const std::string& filename, uint64_t file_number,
                           SSTableReader** reader) {
    *reader = nullptr;

    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd < 0) return Status::IOError("Could not open SSTable: " + filename);

    struct stat st;
    ::fstat(fd, &st);
    uint64_t size = st.st_size;

    if (size < kFooterSize) {
        ::close(fd);
        return Status::Corruption("File too short: " + filename);
    }

    char footer_buf[kFooterSize];
    if (::pread(fd, footer_buf, kFooterSize, size - kFooterSize) !=
        static_cast<ssize_t>(kFooterSize)) {
        ::close(fd);
        return Status::IOError("Failed to read footer: " + filename);
    }

    uint64_t magic = DecodeFixed64(footer_buf + 32);
    if (magic != kTableMagicNumber) {
        ::close(fd);
        return Status::Corruption("Bad magic number: " + filename);
    }

    uint64_t filter_offset = DecodeFixed64(footer_buf);
    uint64_t filter_size   = DecodeFixed64(footer_buf + 8);
    uint64_t index_offset  = DecodeFixed64(footer_buf + 16);
    uint64_t index_size    = DecodeFixed64(footer_buf + 24);

    SSTableReader* r = new SSTableReader();
    r->fd_ = fd;
    r->file_size_ = size;
    r->file_number_ = file_number;

    Status s = r->ReadBlock(index_offset, index_size, &r->index_block_);
    if (!s.ok()) {
        delete r;
        return s;
    }

    if (filter_size > 0) {
        char* fbuf = new char[filter_size];
        if (::pread(fd, fbuf, filter_size, filter_offset) ==
            static_cast<ssize_t>(filter_size)) {
            r->filter_data_ = std::string(fbuf, filter_size);
        }
        delete[] fbuf;
    }

    *reader = r;
    return Status::OK();
}

SSTableReader::~SSTableReader() {
    if (fd_ >= 0) ::close(fd_);
    delete index_block_;
}

Status SSTableReader::Get(const ReadOptions& opts, const Slice& internal_key,
                          std::string* value, TableLookup* result) {
    *result = TableLookup::kAbsent;
    Slice user_key = ExtractUserKey(internal_key);

    if (!filter_data_.empty() && !BloomFilter::MayContain(filter_data_, user_key)) {
        return Status::OK(); // Definitely absent
    }

    std::unique_ptr<lakestore::Iterator> iiter(index_block_->NewIterator());
    iiter->Seek(internal_key);

    if (!iiter->Valid()) return Status::OK(); // Past the largest key

    Slice handle_val = iiter->value();
    uint64_t block_offset, block_size;
    if (!GetVarint64(&handle_val, &block_offset) ||
        !GetVarint64(&handle_val, &block_size)) {
        return Status::Corruption("Bad block handle in index");
    }

    std::shared_ptr<Block> data_block;
    Status s = GetBlock(opts, block_offset, block_size, &data_block);
    if (!s.ok()) return s;

    std::unique_ptr<lakestore::Iterator> diter(data_block->NewIterator());
    diter->Seek(internal_key);

    if (diter->Valid()) {
        // Seek landed on the newest entry with the same user key whose
        // sequence <= snapshot, OR on a later user key entirely.
        InternalKey found = InternalKey::Decode(diter->key());
        if (Slice(found.user_key) == user_key) {
            if (found.type == kTypeValue) {
                *value = diter->value().ToString();
                *result = TableLookup::kFound;
            } else {
                *result = TableLookup::kDeleted;
            }
        }
    }
    return Status::OK();
}

lakestore::Iterator* SSTableReader::NewIterator(const ReadOptions& opts) const {
    return new TableIterator(this, nullptr, opts);
}

namespace {
// One decoded internal entry for the inspector.
void AppendEntryJSON(std::string* out, const Slice& encoded_key, const Slice& value) {
    constexpr size_t kMaxValuePreview = 128;
    InternalKey ik = InternalKey::Decode(encoded_key);
    out->append("{");
    json::KVStr(out, "key", Slice(ik.user_key));
    out->append(",");
    json::KV(out, "seq", ik.seq);
    out->append(",\"type\":\"");
    out->append(ik.type == kTypeValue ? "PUT" : "DEL");
    out->append("\",");
    json::KV(out, "value_len", value.size);
    out->append(",");
    Slice preview(value.data, value.size < kMaxValuePreview ? value.size : kMaxValuePreview);
    json::KVStr(out, "value", preview);
    if (value.size > kMaxValuePreview) {
        out->append(",\"value_truncated\":true");
    }
    out->append("}");
}
} // namespace

Status SSTableReader::DebugJSON(std::string* out, size_t max_entries) const {
    constexpr size_t kMaxBlocksListed = 500;
    ReadOptions no_cache; // never pollute the cache from the inspector
    no_cache.block_cache = nullptr;
    no_cache.fill_cache = false;

    out->clear();
    out->append("{");
    json::KV(out, "file_number", file_number_);
    out->append(",");
    json::KV(out, "file_size", file_size_);

    // --- Bloom filter statistics ---
    out->append(",\"bloom\":{");
    if (!filter_data_.empty()) {
        size_t bytes = filter_data_.size();
        int k = static_cast<uint8_t>(filter_data_[bytes - 1]);
        size_t bits = (bytes - 1) * 8;
        size_t set = 0;
        for (size_t i = 0; i + 1 < bytes; i++) {
            set += __builtin_popcount(static_cast<uint8_t>(filter_data_[i]));
        }
        double fill = bits > 0 ? static_cast<double>(set) / bits : 0.0;
        json::KV(out, "size_bytes", bytes);
        out->append(",");
        json::KV(out, "bits", bits);
        out->append(",");
        json::KV(out, "bits_set", set);
        out->append(",");
        json::KV(out, "hash_probes_k", k);
        out->append(",");
        json::KVDouble(out, "fill_ratio", fill);
        out->append(",");
        // A lookup false-positives only if all k probed bits are set.
        json::KVDouble(out, "est_false_positive_rate", std::pow(fill, k));
    } else {
        json::KV(out, "size_bytes", 0);
    }
    out->append("}");

    // --- Data block map (via the index block) ---
    out->append(",\"blocks\":[");
    size_t block_count = 0;
    uint64_t data_bytes = 0;
    {
        std::unique_ptr<lakestore::Iterator> iiter(index_block_->NewIterator());
        for (iiter->SeekToFirst(); iiter->Valid(); iiter->Next()) {
            Slice handle_val = iiter->value();
            uint64_t offset, size;
            if (!GetVarint64(&handle_val, &offset) || !GetVarint64(&handle_val, &size)) {
                break;
            }
            data_bytes += size;
            if (block_count < kMaxBlocksListed) {
                if (block_count > 0) out->append(",");
                // Read the block (uncached) for its first key + entry count.
                std::string first_key;
                size_t entries = 0;
                Block* raw = nullptr;
                if (ReadBlock(offset, size, &raw).ok()) {
                    std::unique_ptr<lakestore::Iterator> diter(raw->NewIterator());
                    for (diter->SeekToFirst(); diter->Valid(); diter->Next()) {
                        if (entries == 0) {
                            first_key = ExtractUserKey(diter->key()).ToString();
                        }
                        entries++;
                    }
                    delete raw;
                }
                InternalKey last = InternalKey::Decode(iiter->key());
                out->append("{");
                json::KV(out, "offset", offset);
                out->append(",");
                json::KV(out, "size", size);
                out->append(",");
                json::KV(out, "entries", entries);
                out->append(",");
                json::KVStr(out, "first_key", Slice(first_key));
                out->append(",");
                json::KVStr(out, "last_key", Slice(last.user_key));
                out->append("}");
            }
            block_count++;
        }
    }
    out->append("],");
    json::KV(out, "block_count", block_count);
    out->append(",");
    json::KVBool(out, "blocks_capped", block_count > kMaxBlocksListed);
    out->append(",");
    json::KV(out, "data_bytes", data_bytes);

    // --- Decoded entries ---
    out->append(",\"entries\":[");
    size_t count = 0;
    std::unique_ptr<lakestore::Iterator> it(NewIterator(no_cache));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        if (count < max_entries) {
            if (count > 0) out->append(",");
            AppendEntryJSON(out, it->key(), it->value());
        }
        count++;
    }
    out->append("],");
    json::KV(out, "entry_count", count);
    out->append(",");
    json::KVBool(out, "entries_capped", count > max_entries);
    out->append("}");
    return Status::OK();
}

// --- TableIterator ---

TableIterator::TableIterator(const SSTableReader* reader,
                             std::shared_ptr<const SSTableReader> pin,
                             const ReadOptions& opts)
    : reader_(reader), pin_(std::move(pin)), opts_(opts),
      index_iter_(reader->index_block_->NewIterator()) {}

bool TableIterator::Valid() const {
    return data_iter_ != nullptr && data_iter_->Valid();
}

void TableIterator::LoadDataBlock() {
    data_iter_.reset();
    data_block_.reset();
    if (!index_iter_->Valid()) return;

    Slice handle_val = index_iter_->value();
    uint64_t block_offset, block_size;
    if (!GetVarint64(&handle_val, &block_offset) ||
        !GetVarint64(&handle_val, &block_size)) {
        status_ = Status::Corruption("Bad block handle in index");
        return;
    }

    Status s = reader_->GetBlock(opts_, block_offset, block_size, &data_block_);
    if (!s.ok()) {
        status_ = s;
        return;
    }
    data_iter_.reset(data_block_->NewIterator());
}

void TableIterator::SkipEmptyBlocksForward() {
    while ((data_iter_ == nullptr || !data_iter_->Valid()) &&
           status_.ok() && index_iter_->Valid()) {
        index_iter_->Next();
        LoadDataBlock();
        if (data_iter_) data_iter_->SeekToFirst();
    }
}

void TableIterator::SeekToFirst() {
    index_iter_->SeekToFirst();
    LoadDataBlock();
    if (data_iter_) data_iter_->SeekToFirst();
    SkipEmptyBlocksForward();
}

void TableIterator::Seek(const Slice& target) {
    index_iter_->Seek(target);
    LoadDataBlock();
    if (data_iter_) data_iter_->Seek(target);
    SkipEmptyBlocksForward();
}

void TableIterator::Next() {
    if (data_iter_) data_iter_->Next();
    SkipEmptyBlocksForward();
}

Slice TableIterator::key() const { return data_iter_->key(); }
Slice TableIterator::value() const { return data_iter_->value(); }

} // namespace lakestore
