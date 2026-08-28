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

// A length-prefixed, CRC-protected record log. Records are opaque byte
// strings; the DB layer frames WriteBatches ([seq][count][ops]) on top of
// this, and the Manifest frames VersionEdits on top of it.
class WAL {
public:
    explicit WAL(const std::string& filename, uint64_t file_number);
    ~WAL();

    // Prevent copying
    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    // Append one record. Called by multiple threads concurrently; a group
    // commit protocol coalesces concurrent appends into a single write().
    Status AppendRecord(const Slice& record);

    // Called on startup to replay records.
    Status Recover(const std::function<Status(const Slice& record)>& apply_fn);

    // Read-only record scan of any WAL-format file (data logs, MANIFEST).
    // Unlike constructing a WAL, this never opens the file for writing and
    // never creates it — safe for inspection of live files.
    static Status ReadAll(const std::string& filename,
                          const std::function<Status(const Slice& record)>& apply_fn);

    Status Sync();
    uint64_t FileNumber() const { return file_number_; }
    const std::string& Filename() const { return filename_; }

private:
    struct Writer {
        const Slice* record;
        bool done;
        Status status;
        std::condition_variable cv;

        Writer() : record(nullptr), done(false) {}
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
