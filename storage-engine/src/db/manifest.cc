#include "manifest.h"
#include "../util/coding.h"
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>

namespace lakestore {

// --- VersionEdit Encoding/Decoding ---
// Tags for serialization
enum EditTag : uint32_t {
    kLogNumber = 1,
    kNextFileNumber = 2,
    kLastSequence = 3,
    kNewFile = 4,
    kDeletedFile = 5
};

void VersionEdit::AddFile(int level, uint64_t file_number, uint64_t file_size,
                          const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.level = level;
    f.number = file_number;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(f);
}

void VersionEdit::DeleteFile(int level, uint64_t file_number) {
    deleted_files_.push_back({level, file_number});
}

void VersionEdit::EncodeTo(std::string* dst) const {
    if (log_number_) {
        PutVarint32(dst, kLogNumber);
        PutVarint64(dst, *log_number_);
    }
    if (next_file_number_) {
        PutVarint32(dst, kNextFileNumber);
        PutVarint64(dst, *next_file_number_);
    }
    if (last_sequence_) {
        PutVarint32(dst, kLastSequence);
        PutVarint64(dst, *last_sequence_);
    }
    for (const auto& del : deleted_files_) {
        PutVarint32(dst, kDeletedFile);
        PutVarint32(dst, del.first); // level
        PutVarint64(dst, del.second); // file number
    }
    for (const auto& f : new_files_) {
        PutVarint32(dst, kNewFile);
        PutVarint32(dst, f.level);
        PutVarint64(dst, f.number);
        PutVarint64(dst, f.file_size);

        std::string s_enc = f.smallest.Encode();
        PutVarint32(dst, s_enc.size());
        dst->append(s_enc);

        std::string l_enc = f.largest.Encode();
        PutVarint32(dst, l_enc.size());
        dst->append(l_enc);
    }
}

Status VersionEdit::DecodeFrom(const Slice& src) {
    Slice input = src;
    uint32_t tag, level, len;
    uint64_t val, num, size;

    while (input.size > 0) {
        if (!GetVarint32(&input, &tag)) return Status::Corruption("Manifest tag");

        switch (tag) {
            case kLogNumber:
                if (GetVarint64(&input, &val)) log_number_ = val;
                break;
            case kNextFileNumber:
                if (GetVarint64(&input, &val)) next_file_number_ = val;
                break;
            case kLastSequence:
                if (GetVarint64(&input, &val)) last_sequence_ = val;
                break;
            case kDeletedFile:
                if (GetVarint32(&input, &level) && GetVarint64(&input, &num)) {
                    deleted_files_.push_back({static_cast<int>(level), num});
                }
                break;
            case kNewFile: {
                FileMetaData f;
                if (GetVarint32(&input, &level) && GetVarint64(&input, &num) && GetVarint64(&input, &size)) {
                    f.level = level;
                    f.number = num;
                    f.file_size = size;

                    if (GetVarint32(&input, &len) && input.size >= len) {
                        f.smallest = InternalKey::Decode(Slice(input.data, len));
                        input.data += len; input.size -= len;

                        if (GetVarint32(&input, &len) && input.size >= len) {
                            f.largest = InternalKey::Decode(Slice(input.data, len));
                            input.data += len; input.size -= len;
                            new_files_.push_back(f);
                            break;
                        }
                    }
                }
                return Status::Corruption("Manifest new file entry");
            }
            default:
                return Status::Corruption("Unknown manifest tag");
        }
    }
    return Status::OK();
}

// --- Version Implementation ---

void Version::Apply(const VersionEdit& edit) {
    // 1. Remove deleted files
    for (const auto& del : edit.deleted_files_) {
        int level = del.first;
        uint64_t number = del.second;
        auto& level_files = files_[level];
        level_files.erase(
            std::remove_if(level_files.begin(), level_files.end(),
                           [number](const FileMetaData& f) { return f.number == number; }),
            level_files.end()
        );
    }

    // 2. Add new files
    for (const auto& f : edit.new_files_) {
        files_[f.level].push_back(f);
    }

    // 3. Sort files in levels > 0 by smallest key. L0 stays sorted by file
    // number (newest last) since its files overlap.
    for (int i = 1; i < kNumLevels; i++) {
        std::sort(files_[i].begin(), files_[i].end(),
                  [](const FileMetaData& a, const FileMetaData& b) {
                      return CompareInternalKeys(a.smallest, b.smallest) < 0;
                  });
    }
    std::sort(files_[0].begin(), files_[0].end(),
              [](const FileMetaData& a, const FileMetaData& b) {
                  return a.number < b.number;
              });
}

void Version::GetOverlappingL0(const Slice& begin, const Slice& end,
                               std::vector<FileMetaData>* result) const {
    GetOverlappingInputs(0, begin, end, result);
}

void Version::GetOverlappingInputs(int level, const Slice& begin, const Slice& end,
                                   std::vector<FileMetaData>* result) const {
    // Overlap is decided on USER keys only. Comparing internal keys here is
    // subtly wrong at the boundaries: a file whose largest key is
    // (begin, seq=42) still contains `begin`, but compares below
    // (begin, seq=0) in internal-key order.
    for (const auto& f : files_[level]) {
        Slice f_smallest(f.smallest.user_key);
        Slice f_largest(f.largest.user_key);
        if (f_largest.compare(begin) >= 0 && f_smallest.compare(end) <= 0) {
            result->push_back(f);
        }
    }
}

const FileMetaData* Version::FindFileInLevel(int level, const Slice& user_key) const {
    if (level == 0 || files_[level].empty()) return nullptr;

    // Binary search for the first file whose largest USER key is >= target.
    auto it = std::lower_bound(files_[level].begin(), files_[level].end(), user_key,
        [](const FileMetaData& f, const Slice& target) {
            return Slice(f.largest.user_key).compare(target) < 0;
        });

    if (it != files_[level].end() && Slice(it->smallest.user_key).compare(user_key) <= 0) {
        return &(*it);
    }
    return nullptr;
}

uint64_t Version::LevelSize(int level) const {
    uint64_t total = 0;
    for (const auto& f : files_[level]) total += f.file_size;
    return total;
}

int Version::PickCompactionLevel(const Options& options) const {
    if (static_cast<int>(files_[0].size()) >= options.l0_compaction_trigger) {
        return 0;
    }
    uint64_t max_bytes = options.max_bytes_for_level_base;
    for (int level = 1; level < kNumLevels - 1; level++) {
        if (LevelSize(level) > max_bytes) return level;
        max_bytes *= 10;
    }
    return -1;
}

// --- Manifest Implementation ---

Manifest::Manifest(const std::string& dbname) : dbname_(dbname) {}

Status Manifest::SetCurrentFile(uint64_t manifest_number) {
    std::string tmp_name = dbname_ + "/CURRENT.tmp";
    int fd = ::open(tmp_name.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return Status::IOError("Failed to create CURRENT.tmp");

    std::string content = "MANIFEST-" + std::to_string(manifest_number) + "\n";
    ::write(fd, content.data(), content.size());
    ::fsync(fd);
    ::close(fd);

    std::string current_name = dbname_ + "/CURRENT";
    if (::rename(tmp_name.c_str(), current_name.c_str()) != 0) {
        return Status::IOError("Failed to rename CURRENT.tmp");
    }
    return Status::OK();
}

Status Manifest::LogAndApply(VersionEdit* edit, std::shared_ptr<const Version>* current) {
    if (edit->log_number_) log_number_ = *edit->log_number_;
    if (edit->last_sequence_) last_sequence_ = *edit->last_sequence_;

    // Always persist the counters' high-water marks with every edit.
    edit->SetNextFileNumber(next_file_number_);
    edit->SetLastSequence(last_sequence_);
    edit->SetLogNumber(log_number_);

    // Create the MANIFEST lazily. A brand-new manifest starts with a
    // snapshot of the current version so it is self-contained.
    if (!manifest_log_) {
        manifest_file_number_ = next_file_number_++;
        edit->SetNextFileNumber(next_file_number_);
        std::string manifest_name = dbname_ + "/MANIFEST-" + std::to_string(manifest_file_number_);
        manifest_log_ = std::make_unique<WAL>(manifest_name, manifest_file_number_);

        VersionEdit snapshot;
        for (int level = 0; level < kNumLevels; level++) {
            for (const auto& f : (*current)->Files(level)) {
                snapshot.AddFile(f.level, f.number, f.file_size, f.smallest, f.largest);
            }
        }
        snapshot.SetNextFileNumber(next_file_number_);
        snapshot.SetLastSequence(last_sequence_);
        snapshot.SetLogNumber(log_number_);

        std::string snap_record;
        snapshot.EncodeTo(&snap_record);
        Status s = manifest_log_->AppendRecord(Slice(snap_record));
        if (!s.ok()) return s;

        s = SetCurrentFile(manifest_file_number_);
        if (!s.ok()) return s;
    }

    // 1. Serialize and append the edit, then fsync — the edit must be
    // durable before the files it references become part of the state.
    std::string record;
    edit->EncodeTo(&record);

    Status s = manifest_log_->AppendRecord(Slice(record));
    if (!s.ok()) return s;
    s = manifest_log_->Sync();
    if (!s.ok()) return s;

    // 2. Publish a new immutable Version (copy-on-write).
    auto next = std::make_shared<Version>(**current);
    next->Apply(*edit);
    *current = std::move(next);

    return Status::OK();
}

Status Manifest::Open(Version* base) {
    // 1. Read CURRENT to find the active manifest. Absence = fresh DB.
    std::ifstream current_file(dbname_ + "/CURRENT");
    if (!current_file.is_open()) {
        return Status::OK();
    }
    std::string manifest_name;
    std::getline(current_file, manifest_name);
    current_file.close();
    if (manifest_name.rfind("MANIFEST-", 0) != 0) {
        return Status::Corruption("CURRENT does not name a manifest: " + manifest_name);
    }

    // 2. Replay every VersionEdit in the manifest into *base.
    WAL reader(dbname_ + "/" + manifest_name, 0);
    Status s = reader.Recover([&](const Slice& record) {
        VersionEdit edit;
        Status ds = edit.DecodeFrom(record);
        if (!ds.ok()) return ds;

        base->Apply(edit);
        if (edit.log_number_) log_number_ = *edit.log_number_;
        if (edit.next_file_number_) next_file_number_ = *edit.next_file_number_;
        if (edit.last_sequence_) last_sequence_ = *edit.last_sequence_;
        return Status::OK();
    });

    // NOTE: manifest_log_ stays null. The first LogAndApply after recovery
    // creates a fresh MANIFEST containing a full snapshot, and CURRENT is
    // switched atomically. The old manifest is removed as an obsolete file.
    return s;
}

} // namespace lakestore
