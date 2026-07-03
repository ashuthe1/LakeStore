#include "sstable_reader.h"
#include "../util/coding.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace lakestore {

Status SSTableReader::ReadBlock(uint64_t offset, size_t size, Block** block) {
    char* buf = new char[size];
    ssize_t bytes_read = ::pread(fd_, buf, size, offset);
    
    if (bytes_read != static_cast<ssize_t>(size)) {
        delete[] buf;
        return Status::IOError("Failed to read complete block");
    }
    
    *block = new Block(buf, size);
    return Status::OK();
}

Status SSTableReader::Open(const std::string& filename, SSTableReader** reader) {
    *reader = nullptr;
    
    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd < 0) return Status::IOError("Could not open SSTable: " + filename);

    struct stat st;
    ::fstat(fd, &st);
    uint64_t size = st.st_size;

    if (size < 48) { // Footer size
        ::close(fd);
        return Status::Corruption("File too short to be an SSTable");
    }

    // Read footer
    char footer_buf[48];
    ::pread(fd, footer_buf, 48, size - 48);
    
    // Verify magic number (0xdb4775248b80fb57)
    uint64_t magic;
    std::memcpy(&magic, footer_buf + 40, 8);
    if (magic != 0xdb4775248b80fb57ULL) {
        ::close(fd);
        return Status::Corruption("Bad magic number");
    }

    Slice footer_input(footer_buf, 40);
    uint32_t filter_offset, filter_size, index_offset, index_size;
    GetVarint32(&footer_input, &filter_offset);
    GetVarint32(&footer_input, &filter_size);
    GetVarint32(&footer_input, &index_offset);
    GetVarint32(&footer_input, &index_size);

    SSTableReader* r = new SSTableReader();
    r->fd_ = fd;
    r->file_size_ = size;

    // Load Index Block
    Status s = r->ReadBlock(index_offset, index_size, &r->index_block_);
    if (!s.ok()) {
        delete r;
        return s;
    }

    // Load Filter Block
    if (filter_size > 0) {
        char* fbuf = new char[filter_size];
        ::pread(fd, fbuf, filter_size, filter_offset);
        r->filter_data_ = std::string(fbuf, filter_size);
        delete[] fbuf;
    }

    *reader = r;
    return Status::OK();
}

SSTableReader::~SSTableReader() {
    if (fd_ >= 0) ::close(fd_);
    delete index_block_;
}

Status SSTableReader::Get(const ReadOptions& opts, const Slice& key, std::string* value) {
    // 1. Check Bloom Filter First (Skip disk I/O if possible)
    if (!filter_data_.empty()) {
        InternalKey ik = InternalKey::Decode(key);
        if (!BloomFilter::MayContain(filter_data_, ik.user_key)) {
            return Status::NotFound("Not in bloom filter");
        }
    }

    // 2. Binary search the index block to find the correct data block handle
    std::unique_ptr<Block::Iterator> iiter(index_block_->NewIterator());
    iiter->Seek(key);
    
    if (iiter->Valid()) {
        Slice handle_val = iiter->value();
        uint32_t block_offset, block_size;
        GetVarint32(&handle_val, &block_offset);
        GetVarint32(&handle_val, &block_size);

        // 3. Read the data block (using BlockCache if available)
        std::shared_ptr<Block> data_block;
        
        // Extract the file number from the fd_ or store it on the reader.
        // Assuming we added `uint64_t file_number_` to SSTableReader:
        uint64_t file_num = file_number_; 

        if (opts.block_cache) {
            data_block = opts.block_cache->Lookup(file_num, block_offset);
        }

        if (!data_block) {
            // Cache miss (or cache disabled), read from disk
            Block* raw_block = nullptr;
            Status s = ReadBlock(block_offset, block_size, &raw_block);
            if (!s.ok()) return s;
            
            data_block.reset(raw_block);

            if (opts.block_cache && opts.fill_cache) {
                opts.block_cache->Insert(file_num, block_offset, data_block, block_size);
            }
        }

        // 4. Binary search inside the data block
        std::unique_ptr<Block::Iterator> diter(data_block->NewIterator());
        diter->Seek(key);

        if (diter->Valid() && diter->key().compare(key) == 0) {
            *value = diter->value().ToString();
            return Status::OK();
        }
    }

    return Status::NotFound("Key not found in block");
}

} // namespace lakestore