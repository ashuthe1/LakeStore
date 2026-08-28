#pragma once
#include "manifest.h"
#include "../options.h"
#include "../iterator.h"
#include <vector>
#include <memory>

namespace lakestore {

// Describes one compaction: files from `level` merged with the overlapping
// files of `level + 1`, producing new files in `level + 1`. The driving loop
// (iteration, output writing) lives in DBImpl, which owns the table cache.
class Compaction {
public:
    Compaction(const Options& options, int level, const Version* version);

    // Selects the input files from the version. Returns false if the level
    // has nothing to compact.
    bool PickInputs();

    int level() const { return level_; }
    const std::vector<FileMetaData>& inputs(int i) const { return inputs_[i]; }

    // Evaluates if a key can be safely dropped from the database.
    // Entries MUST be fed in internal-key order (user key asc, seq desc).
    bool ShouldDrop(const InternalKey& ikey, SequenceNumber smallest_snapshot);

    // Builds the VersionEdit to atomically apply this compaction
    VersionEdit* edit() { return &edit_; }

private:
    Options options_;
    int level_;
    const Version* input_version_;

    std::vector<FileMetaData> inputs_[2];
    VersionEdit edit_;

    // State for ShouldDrop()
    std::string current_user_key_;
    bool has_current_user_key_ = false;
    SequenceNumber last_sequence_for_key_ = 0;
};

} // namespace lakestore
