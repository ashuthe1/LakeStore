#include "compaction.h"
#include "merging_iterator.h"
#include "../sstable/sstable_reader.h"
#include <iostream>

namespace lakestore {

Compaction::Compaction(const Options& options, int level, Version* version)
    : options_(options), level_(level), input_version_(version) {}

void Compaction::AddInputFiles(int level_index, const std::vector<FileMetaData*>& files) {
    for (auto* f : files) {
        inputs_[level_index].push_back(f);
    }
}

void Compaction::AddOutputFile(uint64_t file_number, uint64_t file_size, 
                               const InternalKey& smallest, const InternalKey& largest) {
    edit_.AddFile(level_ + 1, file_number, file_size, smallest, largest);
}

Iterator* Compaction::MakeInputIterator() {
    std::vector<Iterator*> list;
    ReadOptions r_opts;
    r_opts.fill_cache = false; // Don't pollute cache during compaction

    for (int i = 0; i < 2; i++) {
        for (FileMetaData* f : inputs_[i]) {
            // Note: In a real implementation, you'd track file names via the MANIFEST logic.
            // Assuming we can derive filename from number:
            std::string filename = "sst_" + std::to_string(f->number) + ".ldb";
            SSTableReader* reader = nullptr;
            if (SSTableReader::Open(filename, &reader).ok()) {
                // Assuming SSTableReader provides a NewIterator() method returning our generic Iterator*
                // list.push_back(reader->NewIterator(r_opts));
            }
        }
    }
    return new MergingIterator(list);
}

bool Compaction::ShouldDrop(const InternalKey& ikey, SequenceNumber smallest_snapshot) {
    // 1. Is this a newer version of a key we've already seen in this compaction?
    if (has_current_user_key_ && current_user_key_ == ikey.user_key) {
        // We've already written a newer version of this key to the output.
        // We can drop this older version, UNLESS it's required for an active snapshot.
        if (last_sequence_for_key_ <= smallest_snapshot) {
            return true; // Drop it
        }
    } else {
        // First time seeing this user_key in the current merge
        current_user_key_ = ikey.user_key;
        has_current_user_key_ = true;
        last_sequence_for_key_ = ikey.seq;
    }

    // 2. Is it a tombstone (deletion) that can be garbage collected?
    if (ikey.type == kTypeDeletion && ikey.seq <= smallest_snapshot) {
        // We can safely drop a tombstone ONLY if there are no older versions of this key
        // lurking in levels strictly greater than our target output level (level_ + 1).
        
        bool has_older_version = false;
        for (int l = level_ + 2; l < kNumLevels; l++) {
            FileMetaData* f = input_version_->FindFileInLevel(l, ikey.user_key);
            if (f != nullptr) {
                has_older_version = true;
                break;
            }
        }

        if (!has_older_version) {
            return true; // Safe to drop the tombstone entirely!
        }
    }

    return false; // Keep it
}

} // namespace lakestore