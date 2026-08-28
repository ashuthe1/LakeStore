#include "wal.h"
#include "../util/crc32c.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace lakestore {

uint32_t WAL::ComputeCRC(RecordType type, const char* data, size_t n) {
    // Hash the type byte first so a record whose payload moves between
    // fragment types is detected, then extend over the payload.
    char type_byte = static_cast<char>(type);
    uint32_t crc = crc32c::Value(&type_byte, 1);
    return crc32c::Extend(crc, data, n);
}

WAL::WAL(const std::string& filename, uint64_t file_number)
    : filename_(filename), file_number_(file_number), block_offset_(0) {

    // O_APPEND ensures writes are atomic up to PIPE_BUF, though our group
    // commit lock inherently serializes writes anyway.
    fd_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("Failed to open WAL file: " + filename_);
    }

    // If we are re-opening an existing log (e.g. the MANIFEST), continue
    // block accounting from the current physical size.
    struct stat st;
    if (::fstat(fd_, &st) == 0 && st.st_size > 0) {
        block_offset_ = static_cast<size_t>(st.st_size) % kBlockSize;
    }
}

WAL::~WAL() {
    if (fd_ >= 0) {
        Sync();
        ::close(fd_);
    }
}

Status WAL::Sync() {
    #ifdef __APPLE__
    if (::fsync(fd_) != 0) {
        return Status::IOError("Failed to fsync WAL");
    }
    #else
        if (::fdatasync(fd_) != 0) {
            return Status::IOError("Failed to fdatasync WAL");
        }
    #endif
    return Status::OK();
}

Status WAL::AppendRecord(const Slice& record) {
    Writer w;
    w.record = &record;
    w.done = false;

    std::unique_lock<std::mutex> lock(writers_mu_);
    writers_.push_back(&w);

    // Wait until this thread is either made leader, or a leader finishes our work
    while (!w.done && &w != writers_.front()) {
        w.cv.wait(lock);
    }

    if (w.done) {
        return w.status; // A leader handled our batch
    }

    // -- LEADER PATH --
    // Drain the queue to form a group commit
    std::vector<Writer*> group;
    for (auto* writer : writers_) {
        group.push_back(writer);
    }

    // Build the concatenated physical blocks in memory
    std::string buffer;

    for (auto* writer : group) {
        size_t left = writer->record->size;
        const char* ptr = writer->record->data;
        bool begin = true;

        // Emit at least one fragment even for an empty record.
        do {
            size_t leftover = kBlockSize - block_offset_;

            // If less than header size remains in block, pad it with zeroes and start new block
            if (leftover < kHeaderSize) {
                if (leftover > 0) {
                    static const char zeroes[kHeaderSize] = {0};
                    buffer.append(zeroes, leftover);
                }
                block_offset_ = 0;
                leftover = kBlockSize;
            }

            size_t avail = leftover - kHeaderSize;
            size_t fragment_length = (left < avail) ? left : avail;

            RecordType type;
            bool end = (left == fragment_length);
            if (begin && end) {
                type = kFullType;
            } else if (begin) {
                type = kFirstType;
            } else if (end) {
                type = kLastType;
            } else {
                type = kMiddleType;
            }

            // Write 7-byte header
            uint32_t crc = ComputeCRC(type, ptr, fragment_length);
            char header[kHeaderSize];
            header[0] = crc & 0xff;
            header[1] = (crc >> 8) & 0xff;
            header[2] = (crc >> 16) & 0xff;
            header[3] = (crc >> 24) & 0xff;
            header[4] = fragment_length & 0xff;
            header[5] = (fragment_length >> 8) & 0xff;
            header[6] = type;

            buffer.append(header, kHeaderSize);
            buffer.append(ptr, fragment_length);

            ptr += fragment_length;
            left -= fragment_length;
            block_offset_ += kHeaderSize + fragment_length;
            begin = false;
        } while (left > 0);
    }

    // Unlock during expensive I/O operations so other threads can queue up
    lock.unlock();

    Status s = Status::OK();
    if (!buffer.empty()) {
        ssize_t written = ::write(fd_, buffer.data(), buffer.size());
        if (written != static_cast<ssize_t>(buffer.size())) {
            s = Status::IOError("Incomplete write to WAL");
        }
        // DBImpl::Write will now handle it via wal_->Sync() when opts.sync == true.
    }

    // Re-acquire lock to update followers and remove from queue
    lock.lock();
    for (auto* writer : group) {
        writer->status = s;
        writer->done = true;
        writer->cv.notify_one();
        writers_.pop_front(); // Safe since we process exactly from the front
    }

    return s;
}

Status WAL::Recover(const std::function<Status(const Slice& record)>& apply_fn) {
    int read_fd = ::open(filename_.c_str(), O_RDONLY);
    if (read_fd < 0) {
        // If file doesn't exist, there's nothing to recover.
        return Status::OK();
    }

    char block[kBlockSize];
    std::string fragmented_payload;
    bool in_fragmented_record = false;

    while (true) {
        ssize_t bytes_read = ::read(read_fd, block, kBlockSize);
        if (bytes_read <= 0) break; // EOF or error

        size_t offset = 0;
        while (offset + kHeaderSize <= static_cast<size_t>(bytes_read)) {
            // 1. Read 7-byte header
            uint32_t expected_crc =
                (static_cast<uint8_t>(block[offset])) |
                (static_cast<uint8_t>(block[offset+1]) << 8) |
                (static_cast<uint8_t>(block[offset+2]) << 16) |
                (static_cast<uint8_t>(block[offset+3]) << 24);

            uint16_t length =
                (static_cast<uint8_t>(block[offset+4])) |
                (static_cast<uint8_t>(block[offset+5]) << 8);

            RecordType type = static_cast<RecordType>(block[offset+6]);

            // Zero type indicates padding to the end of the block
            if (type == kZeroType) {
                break; // Skip to next block
            }

            if (offset + kHeaderSize + length > static_cast<size_t>(bytes_read)) {
                break; // Incomplete record, end of readable log
            }

            const char* data = block + offset + kHeaderSize;

            // 2. Verify CRC
            uint32_t actual_crc = ComputeCRC(type, data, length);
            if (actual_crc != expected_crc) {
                // Bad CRC implies crash mid-write. Stop reading cleanly.
                ::close(read_fd);
                return Status::OK();
            }

            // 3. Reassemble and apply
            Status s;
            if (type == kFullType) {
                s = apply_fn(Slice(data, length));
            } else if (type == kFirstType) {
                fragmented_payload = std::string(data, length);
                in_fragmented_record = true;
            } else if (type == kMiddleType && in_fragmented_record) {
                fragmented_payload.append(data, length);
            } else if (type == kLastType && in_fragmented_record) {
                fragmented_payload.append(data, length);
                s = apply_fn(Slice(fragmented_payload));
                in_fragmented_record = false;
                fragmented_payload.clear();
            }
            if (!s.ok()) {
                ::close(read_fd);
                return s;
            }

            offset += kHeaderSize + length;
        }
    }

    ::close(read_fd);
    return Status::OK();
}

} // namespace lakestore
