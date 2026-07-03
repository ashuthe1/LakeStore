#pragma once

#include "../types.h"
#include <string>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <functional>
#include <vector>

namespace lakestore {

enum RecordType : uint8_t {
    kZeroType   = 0, // Used for block padding
    kFullType   = 1,
    kFirstType  = 2,
    kMiddleType = 3,
    kLastType   = 4
};

class WAL {
public:
    explicit WAL(const std::string& filename, uint64_t file_number);
    ~WAL();

    // Prevent copying
    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    // Called by multiple threads concurrently.
    Status Append(WriteBatch* batch);
    
    // Called on startup to replay records.
    Status Recover(std::function<Status(const WriteBatch&)> apply_fn);
    
    Status Sync();
    uint64_t FileNumber() const { return file_number_; }

private:
    struct Writer {
        WriteBatch* batch;
        bool done;
        Status status;
        std::condition_variable cv;
        
        Writer() : batch(nullptr), done(false) {}
    };

    std::string filename_;
    uint64_t file_number_;
    int fd_;
    size_t block_offset_;

    std::mutex writers_mu_;
    std::deque<Writer*> writers_;

    static constexpr size_t kBlockSize = 32768; // 32KB blocks
    static constexpr size_t kHeaderSize = 7;    // 4B CRC + 2B Len + 1B Type

    static uint32_t ComputeCRC(RecordType type, const char* data, size_t n);
};

} // namespace lakestore