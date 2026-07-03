#pragma once

#include "../types.h"
#include "../wal/wal.h"
#include <vector>
#include <optional>
#include <string>
#include <memory>
#include <set>

namespace lakestore {

constexpr int kNumLevels = 7;

// Tracks a single SSTable file on disk
struct FileMetaData {
    int level;
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest;
    InternalKey largest;
};

// Represents a delta (change) to the MANIFEST
class VersionEdit {
public:
    void SetLogNumber(uint64_t num) { log_number_ = num; }
    void SetNextFileNumber(uint64_t num) { next_file_number_ = num; }
    void SetLastSequence(SequenceNumber seq) { last_sequence_ = seq; }
    
    void AddFile(int level, uint64_t file_number, uint64_t file_size,
                 const InternalKey& smallest, const InternalKey& largest);
    
    void DeleteFile(int level, uint64_t file_number);

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(const Slice& src);

    // Public for Version to apply
    std::optional<uint64_t> log_number_;
    std::optional<uint64_t> next_file_number_;
    std::optional<SequenceNumber> last_sequence_;
    std::vector<FileMetaData> new_files_;
    std::vector<std::pair<int, uint64_t>> deleted_files_;
};

// Represents a snapshot of the database state (which files live where)
class Version {
public:
    Version() = default;

    // Apply an edit to this version to produce a new state.
    // (In a full implementation, Versions form a linked list for lock-free reads,
    // but we mutate directly for Phase 1 simplicity).
    void Apply(const VersionEdit& edit);

    // Find all L0 files that overlap with [begin, end]
    void GetOverlappingL0(const Slice& begin, const Slice& end, 
                          std::vector<FileMetaData*>* result);

    // Find the single file in Level 1+ that could contain the key
    FileMetaData* FindFileInLevel(int level, const Slice& key);
    
    const std::vector<FileMetaData>& Files(int level) const { return files_[level]; }

private:
    std::vector<FileMetaData> files_[kNumLevels];
};

// Manages the MANIFEST file and CURRENT pointer
class Manifest {
public:
    Manifest(const std::string& dbname);
    ~Manifest() = default;

    Status Open(); // Reads CURRENT and recovers the Version
    
    // Logs the edit, fsyncs, and updates the in-memory version
    Status LogAndApply(VersionEdit* edit, Version* version);
    
    uint64_t NextFileNumber() { return next_file_number_++; }
    uint64_t LogNumber() const { return log_number_; }
    SequenceNumber LastSequence() const { return last_sequence_; }

private:
    Status SetCurrentFile(uint64_t manifest_file_number);

    std::string dbname_;
    std::unique_ptr<WAL> manifest_log_;
    
    uint64_t manifest_file_number_ = 0;
    uint64_t next_file_number_ = 1;
    uint64_t log_number_ = 0;
    SequenceNumber last_sequence_ = 0;
};

} // namespace lakestore