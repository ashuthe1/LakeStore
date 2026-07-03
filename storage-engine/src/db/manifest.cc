#include "manifest.h"
#include "../util/coding.h"
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <iostream>

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
        PutVarint32(dst, *log_number_);
    }
    if (next_file_number_) {
        PutVarint32(dst, kNextFileNumber);
        PutVarint32(dst, *next_file_number_);
    }
    if (last_sequence_) {
        PutVarint32(dst, kLastSequence);
        PutVarint32(dst, *last_sequence_);
    }
    for (const auto& del : deleted_files_) {
        PutVarint32(dst, kDeletedFile);
        PutVarint32(dst, del.first); // level
        PutVarint32(dst, del.second); // file number
    }
    for (const auto& f : new_files_) {
        PutVarint32(dst, kNewFile);
        PutVarint32(dst, f.level);
        PutVarint32(dst, f.number);
        PutVarint32(dst, f.file_size);
        
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
    uint32_t tag, val, level, num, size, len;
    
    while (input.size > 0) {
        if (!GetVarint32(&input, &tag)) return Status::Corruption("Manifest tag");
        
        switch (tag) {
            case kLogNumber:
                if (GetVarint32(&input, &val)) log_number_ = val;
                break;
            case kNextFileNumber:
                if (GetVarint32(&input, &val)) next_file_number_ = val;
                break;
            case kLastSequence:
                if (GetVarint32(&input, &val)) last_sequence_ = val;
                break;
            case kDeletedFile:
                if (GetVarint32(&input, &level) && GetVarint32(&input, &num)) {
                    deleted_files_.push_back({level, num});
                }
                break;
            case kNewFile: {
                FileMetaData f;
                if (GetVarint32(&input, &level) && GetVarint32(&input, &num) && GetVarint32(&input, &size)) {
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
    
    // 3. Sort files in levels > 0 by smallest key (L0 is sorted by file number implicitly)
    for (int i = 1; i < kNumLevels; i++) {
        std::sort(files_[i].begin(), files_[i].end(),
                  [](const FileMetaData& a, const FileMetaData& b) {
                      return CompareInternalKeys(a.smallest, b.smallest) < 0;
                  });
    }
}

void Version::GetOverlappingL0(const Slice& begin, const Slice& end, std::vector<FileMetaData*>* result) {
    for (auto& f : files_[0]) {
        // L0 files can overlap. We check if the file's range overlaps with [begin, end]
        // overlap = !(f.largest < begin || f.smallest > end)
        InternalKey b_key; b_key.user_key = begin.ToString(); b_key.seq = 0; b_key.type = kTypeValue;
        InternalKey e_key; e_key.user_key = end.ToString(); e_key.seq = ~(0ULL); e_key.type = kTypeValue;
        
        if (CompareInternalKeys(f.largest, b_key) >= 0 && CompareInternalKeys(f.smallest, e_key) <= 0) {
            result->push_back(&f);
        }
    }
}

FileMetaData* Version::FindFileInLevel(int level, const Slice& key) {
    if (level == 0 || files_[level].empty()) return nullptr;
    
    // Binary search to find the file whose largest key is >= the target key
    InternalKey target; 
    target.user_key = key.ToString(); 
    target.seq = 0; 
    target.type = kTypeValue;

    auto it = std::lower_bound(files_[level].begin(), files_[level].end(), target,
        [](const FileMetaData& f, const InternalKey& t) {
            return CompareInternalKeys(f.largest, t) < 0;
        });
        
    if (it != files_[level].end() && CompareInternalKeys(it->smallest, target) <= 0) {
        return &(*it);
    }
    return nullptr;
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

Status Manifest::LogAndApply(VersionEdit* edit, Version* version) {
    if (edit->log_number_) log_number_ = *edit->log_number_;
    if (edit->next_file_number_) next_file_number_ = *edit->next_file_number_;
    if (edit->last_sequence_) last_sequence_ = *edit->last_sequence_;

    // Always ensure next_file_number and last_sequence are saved
    edit->SetNextFileNumber(next_file_number_);
    edit->SetLastSequence(last_sequence_);

    // 1. Serialize
    std::string record;
    edit->EncodeTo(&record);
    
    WriteBatch batch;
    batch.payload = record;

    // 2. Append to log and fsync
    if (!manifest_log_) {
        manifest_file_number_ = next_file_number_++;
        std::string manifest_name = dbname_ + "/MANIFEST-" + std::to_string(manifest_file_number_);
        manifest_log_ = std::make_unique<WAL>(manifest_name, manifest_file_number_);
        
        // When creating a new MANIFEST, we must write a snapshot of the current Version first
        // (Omitted for brevity, but you'd iterate `version->Files()` and inject them into `edit`)
        SetCurrentFile(manifest_file_number_);
    }

    Status s = manifest_log_->Append(&batch);
    if (!s.ok()) return s;
    
    // 3. Update memory state
    version->Apply(*edit);
    
    return Status::OK();
}

Status Manifest::Open() {
    // TODO: Phase 1 recovery reads CURRENT, parses the MANIFEST file name, 
    // loops through WAL::Recover on it, decodes VersionEdits, and builds the baseline Version.
    // We will hook this up in Step 11 (DBImpl).
    return Status::OK();
}

} // namespace lakestore