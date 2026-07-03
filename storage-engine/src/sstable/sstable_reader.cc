#include "sstable_reader.h"
#include "../util/coding.h"
#include "../cache/lru_cache.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <memory>

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

    if (size < 48) { 
        ::close(fd);
        return Status::Corruption("File too short");
    }

    char footer_buf[48];
    ::pread(fd, footer_buf, 48, size - 48);
    
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
    // For Phase 1 simple test, default file_number to 0. 
    // In a real system, you'd extract this from the filename string.
    r->file_number_ = 0; 

    Status s = r->ReadBlock(index_offset, index_size, &r->index_block_);
    if (!s.ok()) {
        delete r;
        return s;
    }

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
    if (!filter_data_.empty()) {
        InternalKey ik = InternalKey::Decode(key);
        if (!BloomFilter::MayContain(filter_data_, ik.user_key)) {
            return Status::NotFound("Not in bloom filter");
        }
    }

    std::unique_ptr<lakestore::Iterator> iiter(index_block_->NewIterator());
    iiter->Seek(key);
    
    if (iiter->Valid()) {
        Slice handle_val = iiter->value();
        uint32_t block_offset, block_size;
        GetVarint32(&handle_val, &block_offset);
        GetVarint32(&handle_val, &block_size);

        std::shared_ptr<Block> data_block;
        uint64_t file_num = file_number_; 

        if (opts.block_cache) {
            data_block = opts.block_cache->Lookup(file_num, block_offset);
        }

        if (!data_block) {
            Block* raw_block = nullptr;
            Status s = ReadBlock(block_offset, block_size, &raw_block);
            if (!s.ok()) return s;
            
            data_block.reset(raw_block);

            if (opts.block_cache && opts.fill_cache) {
                opts.block_cache->Insert(file_num, block_offset, data_block, block_size);
            }
        }

        std::unique_ptr<lakestore::Iterator> diter(data_block->NewIterator());
        diter->Seek(key);

        if (diter->Valid() && diter->key().compare(key) == 0) {
            *value = diter->value().ToString();
            return Status::OK();
        }
    }

    return Status::NotFound("Key not found in block");
}

} // namespace lakestore