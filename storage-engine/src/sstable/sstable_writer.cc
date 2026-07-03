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
        buffer_.append(reinterpret_cast<const char*>(&restart), 4);
    }
    uint32_t num_restarts = restarts_.size();
    buffer_.append(reinterpret_cast<const char*>(&num_restarts), 4);
    finished_ = true;
    return Slice(buffer_);
}

// --- SSTableWriter Implementation ---

SSTableWriter::SSTableWriter(const std::string& filename, const Options& opts)
    : options_(opts), filename_(filename), offset_(0),
      data_block_(&options_), index_block_(&options_),
      filter_block_(opts.bloom_bits_per_key),
      pending_index_entry_(false) {
    
    fd_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("Failed to open SSTable file");
    }
}

SSTableWriter::~SSTableWriter() {
    if (fd_ >= 0) ::close(fd_);
}

void SSTableWriter::Add(const Slice& key, const Slice& value) {
    if (smallest_key_.empty()) {
        smallest_key_ = key.ToString();
    }
    largest_key_ = key.ToString();

    // Add user_key to bloom filter (strip sequence number and type)
    // We only filter on the user key, not the MVCC version!
    InternalKey ik = InternalKey::Decode(key);
    filter_block_.AddKey(ik.user_key);

    if (pending_index_entry_) {
        // Find a short separator between last_key_ and current key to save index space
        // For simplicity in Phase 1, we just use the current key as the separator.
        std::string handle_encoding;
        PutVarint32(&handle_encoding, pending_handle_.offset);
        PutVarint32(&handle_encoding, pending_handle_.size);
        
        index_block_.Add(key, handle_encoding);
        pending_index_entry_ = false;
    }

    data_block_.Add(key, value);
    last_key_ = key.ToString();

    if (data_block_.CurrentSizeEstimate() >= options_.block_size) {
        FlushDataBlock();
    }
}

void SSTableWriter::FlushDataBlock() {
    if (data_block_.empty()) return;

    Slice raw_block = data_block_.Finish();
    
    // In a full implementation, you would compress (e.g., Snappy) and append a CRC32 here.
    ::write(fd_, raw_block.data, raw_block.size);
    
    pending_handle_.offset = offset_;
    pending_handle_.size = raw_block.size;
    offset_ += raw_block.size;

    pending_index_entry_ = true;
    data_block_.Reset();
}

Status SSTableWriter::Finish() {
    FlushDataBlock();
    
    if (pending_index_entry_) {
        std::string handle_encoding;
        PutVarint32(&handle_encoding, pending_handle_.offset);
        PutVarint32(&handle_encoding, pending_handle_.size);
        index_block_.Add(last_key_, handle_encoding);
        pending_index_entry_ = false;
    }

    // --- NEW: Write Filter Block ---
    Slice filter_slice = filter_block_.Finish();
    ::write(fd_, filter_slice.data, filter_slice.size);
    BlockHandle filter_handle = {offset_, filter_slice.size};
    offset_ += filter_slice.size;

    // Write Index Block
    Slice index_slice = index_block_.Finish();
    ::write(fd_, index_slice.data, index_slice.size);
    BlockHandle index_handle = {offset_, index_slice.size};
    offset_ += index_slice.size;

    // Write Footer (48 bytes)
    std::string footer;
    PutVarint32(&footer, filter_handle.offset);
    PutVarint32(&footer, filter_handle.size);
    PutVarint32(&footer, index_handle.offset);
    PutVarint32(&footer, index_handle.size);
    
    footer.resize(40, 0); 
    
    uint64_t magic = 0xdb4775248b80fb57ULL;
    footer.append(reinterpret_cast<const char*>(&magic), 8);
    
    ::write(fd_, footer.data(), footer.size());
    ::fsync(fd_);
    ::close(fd_);
    fd_ = -1;

    return Status::OK();
}

} // namespace lakestore