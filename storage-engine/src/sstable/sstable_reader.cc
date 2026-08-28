#include "sstable_reader.h"
#include "sstable_writer.h" // for kFooterSize / kTableMagicNumber
#include "../util/coding.h"
#include "../cache/lru_cache.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
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
