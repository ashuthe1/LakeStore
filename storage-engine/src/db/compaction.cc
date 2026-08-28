#include "compaction.h"
#include <algorithm>

namespace lakestore {

Compaction::Compaction(const Options& options, int level, const Version* version)
    : options_(options), level_(level), input_version_(version) {}

bool Compaction::PickInputs() {
    const auto& level_files = input_version_->Files(level_);
    if (level_files.empty()) return false;

    if (level_ == 0) {
        // L0 files overlap each other, so take them ALL — merging a subset
        // could surface an older version of a key above a newer one.
        inputs_[0] = level_files;
    } else {
        // Compact the whole level for simplicity. Levels are kept small by
        // the size thresholds, and this avoids tracking a compaction cursor.
        inputs_[0] = level_files;
    }

    // Compute the combined user-key range of the inputs.
    std::string smallest = inputs_[0][0].smallest.user_key;
    std::string largest = inputs_[0][0].largest.user_key;
    for (const auto& f : inputs_[0]) {
        if (Slice(f.smallest.user_key).compare(Slice(smallest)) < 0) smallest = f.smallest.user_key;
        if (Slice(f.largest.user_key).compare(Slice(largest)) > 0) largest = f.largest.user_key;
    }

    // Pull in every overlapping file from the next level.
    input_version_->GetOverlappingInputs(level_ + 1, Slice(smallest), Slice(largest), &inputs_[1]);

    // Register the inputs for deletion in the resulting edit.
    for (int which = 0; which < 2; which++) {
        for (const auto& f : inputs_[which]) {
            edit_.DeleteFile(f.level, f.number);
        }
    }
    return true;
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
    }
    last_sequence_for_key_ = ikey.seq;

    // 2. Is it a tombstone (deletion) that can be garbage collected?
    if (ikey.type == kTypeDeletion && ikey.seq <= smallest_snapshot) {
        // We can safely drop a tombstone ONLY if there are no older versions of this key
        // lurking in levels strictly greater than our target output level (level_ + 1).

        bool has_older_version = false;
        for (int l = level_ + 2; l < kNumLevels; l++) {
            if (input_version_->FindFileInLevel(l, Slice(ikey.user_key)) != nullptr) {
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
