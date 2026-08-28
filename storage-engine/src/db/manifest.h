#pragma once

#include "../types.h"
#include "../options.h"
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

// Represents a snapshot of the database state (which files live where).
// Treated as IMMUTABLE once published: DBImpl holds a shared_ptr<const Version>
// and every mutation copies the current version, applies an edit, and swaps
// the pointer. Readers that grabbed the old pointer keep a consistent view.
class Version {
public:
    Version() = default;

    // Apply an edit to this (still-private) copy.
    void Apply(const VersionEdit& edit);

    // Find all L0 files whose USER-KEY range overlaps [begin, end]
    void GetOverlappingL0(const Slice& begin, const Slice& end,
                          std::vector<FileMetaData>* result) const;

    // Find all files in a level (>= 1) whose user-key range overlaps [begin, end]
    void GetOverlappingInputs(int level, const Slice& begin, const Slice& end,
                              std::vector<FileMetaData>* result) const;

    // Find the single file in Level 1+ that could contain the user key
    const FileMetaData* FindFileInLevel(int level, const Slice& user_key) const;

    // Total bytes stored in a level
    uint64_t LevelSize(int level) const;

    // Returns the level most in need of compaction, or -1 if none.
    int PickCompactionLevel(const Options& options) const;

    const std::vector<FileMetaData>& Files(int level) const { return files_[level]; }

private:
    std::vector<FileMetaData> files_[kNumLevels];
};

// Manages the MANIFEST file and CURRENT pointer
class Manifest {
public:
    Manifest(const std::string& dbname);
    ~Manifest() = default;

    // Reads CURRENT and replays the MANIFEST into *base. On a fresh
    // database this is a no-op that leaves the defaults in place.
    Status Open(Version* base);

    // Logs the edit, fsyncs, and publishes a new Version built from
    // *current + edit. Caller must hold the DB mutex.
    Status LogAndApply(VersionEdit* edit, std::shared_ptr<const Version>* current);

    uint64_t NextFileNumber() { return next_file_number_++; }
    uint64_t LogNumber() const { return log_number_; }
    SequenceNumber LastSequence() const { return last_sequence_; }
    void SetLastSequence(SequenceNumber seq) { last_sequence_ = seq; }
    uint64_t ManifestFileNumber() const { return manifest_file_number_; }

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
