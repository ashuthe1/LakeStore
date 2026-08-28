#include "sstable_writer.h"
#include "../util/coding.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>

namespace lakestore {

// --- BlockBuilder Implementation ---

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), counter_(0), finished_(false) {
    restarts_.push_back(0); // First restart point is at offset 0
}

void BlockBuilder::Reset() {
    buffer_.clear();
    restarts_.clear();
    restarts_.push_back(0);
    counter_ = 0;
    finished_ = false;
    last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
    // buffer size + restart array size + restart count (4 bytes)
    return (buffer_.size() + restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t));
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
    size_t shared = 0;
    if (counter_ < options_->block_restart_interval) {
        size_t min_len = std::min(last_key_.size(), key.size);
        while (shared < min_len && last_key_[shared] == key.data[shared]) {
            shared++;
        }
    } else {
        // Restart interval hit, store full key
        restarts_.push_back(buffer_.size());
        counter_ = 0;
    }

    size_t non_shared = key.size - shared;
    PutVarint32(&buffer_, shared);
    PutVarint32(&buffer_, non_shared);
    PutVarint32(&buffer_, value.size);
    buffer_.append(key.data + shared, non_shared);
    buffer_.append(value.data, value.size);

    last_key_ = key.ToString();
    counter_++;
}

Slice BlockBuilder::Finish() {
    for (uint32_t restart : restarts_) {
        PutFixed32(&buffer_, restart);
    }
    PutFixed32(&buffer_, static_cast<uint32_t>(restarts_.size()));
    finished_ = true;
    return Slice(buffer_);
}

// --- SSTableWriter Implementation ---

SSTableWriter::SSTableWriter(const std::string& filename, const Options& opts)
    : filter_block_(opts.bloom_bits_per_key),
      options_(opts), filename_(filename), offset_(0),
      data_block_(&options_), index_block_(&options_) {

    fd_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) {
        io_status_ = Status::IOError("Failed to open SSTable file: " + filename_);
    }
}

SSTableWriter::~SSTableWriter() {
    if (fd_ >= 0) ::close(fd_);
}

void SSTableWriter::WriteRaw(const char* data, size_t n) {
    if (!io_status_.ok()) return;
    ssize_t written = ::write(fd_, data, n);
    if (written != static_cast<ssize_t>(n)) {
        io_status_ = Status::IOError("Short write to SSTable: " + filename_);
    }
}

void SSTableWriter::Add(const Slice& key, const Slice& value) {
    if (smallest_key_.empty()) {
        smallest_key_ = key.ToString();
    }
    largest_key_ = key.ToString();
    num_entries_++;

    // Add user_key to bloom filter (strip sequence number and type)
    // We only filter on the user key, not the MVCC version!
    filter_block_.AddKey(ExtractUserKey(key));

    data_block_.Add(key, value);
    last_key_ = key.ToString();

    if (data_block_.CurrentSizeEstimate() >= options_.block_size) {
        FlushDataBlock();
    }
}

void SSTableWriter::FlushDataBlock() {
    if (data_block_.empty()) return;

    Slice raw_block = data_block_.Finish();

    WriteRaw(raw_block.data, raw_block.size);

    // Index entry: key = the LAST key stored in this block, value = the
    // block's handle. A lookup seeks the index for the first entry whose
    // key >= target, which is exactly the block that could contain it.
    std::string handle_encoding;
    PutVarint64(&handle_encoding, offset_);
    PutVarint64(&handle_encoding, raw_block.size);
    index_block_.Add(Slice(last_key_), handle_encoding);

    offset_ += raw_block.size;
    data_block_.Reset();
}

Status SSTableWriter::Finish() {
    FlushDataBlock();

    // Write Filter Block
    Slice filter_slice = filter_block_.Finish();
    WriteRaw(filter_slice.data, filter_slice.size);
    BlockHandle filter_handle = {offset_, filter_slice.size};
    offset_ += filter_slice.size;

    // Write Index Block
    Slice index_slice = index_block_.Finish();
    WriteRaw(index_slice.data, index_slice.size);
    BlockHandle index_handle = {offset_, index_slice.size};
    offset_ += index_slice.size;

    // Write Footer (40 bytes, fixed-width so the reader can locate it
    // unambiguously at file_size - kFooterSize).
    std::string footer;
    PutFixed64(&footer, filter_handle.offset);
    PutFixed64(&footer, filter_handle.size);
    PutFixed64(&footer, index_handle.offset);
    PutFixed64(&footer, index_handle.size);
    PutFixed64(&footer, kTableMagicNumber);
    WriteRaw(footer.data(), footer.size());
    offset_ += footer.size();

    if (io_status_.ok() && ::fsync(fd_) != 0) {
        io_status_ = Status::IOError("Failed to fsync SSTable: " + filename_);
    }
    ::close(fd_);
    fd_ = -1;

    return io_status_;
}

} // namespace lakestore
