#pragma once

#include "../iterator.h"
#include "../types.h"
#include "../memtable/memtable.h"
#include "../sstable/sstable_reader.h"
#include "merging_iterator.h"
#include <memory>
#include <vector>

namespace lakestore {

// Wraps a merged internal iterator (memtables + SSTables, encoded internal
// keys) and exposes the USER view at a fixed snapshot: newest visible version
// per user key, tombstones and stale versions skipped.
//
// Also pins everything the underlying iterators reference: the memtables and
// the version stay alive for the iterator's lifetime, and SSTable readers are
// pinned inside their TableIterators.
class DBIter : public Iterator {
public:
    DBIter(Iterator* internal_iter, SequenceNumber snapshot,
           std::vector<std::shared_ptr<const void>> pins)
        : iter_(internal_iter), snapshot_(snapshot), pins_(std::move(pins)) {}

    bool Valid() const override { return valid_; }

    void SeekToFirst() override {
        iter_->SeekToFirst();
        FindNextUserEntry();
    }

    void Seek(const Slice& target) override {
        // Seek to the newest visible version of `target`.
        InternalKey ik;
        ik.user_key = target.ToString();
        ik.seq = snapshot_;
        ik.type = kTypeValue;
        std::string encoded = ik.Encode();
        iter_->Seek(Slice(encoded));
        FindNextUserEntry();
    }

    void Next() override {
        if (!valid_) return;
        iter_->Next();
        FindNextUserEntry();
    }

    Slice key() const override { return Slice(user_key_); }
    Slice value() const override { return Slice(value_); }
    Status status() const override { return iter_->status(); }

private:
    // Advance to the next user key whose newest visible version is a value.
    void FindNextUserEntry() {
        valid_ = false;
        while (iter_->Valid()) {
            InternalKey ik = InternalKey::Decode(iter_->key());

            // Invisible to this snapshot: a newer write than our read point.
            if (ik.seq > snapshot_) {
                iter_->Next();
                continue;
            }
            // An older duplicate of a user key we already resolved.
            if (ik.user_key == last_emitted_or_skipped_ && has_last_) {
                iter_->Next();
                continue;
            }

            // Newest visible version of a NEW user key.
            has_last_ = true;
            last_emitted_or_skipped_ = ik.user_key;
            if (ik.type == kTypeDeletion) {
                iter_->Next();
                continue; // Deleted — skip all remaining versions too.
            }

            user_key_ = ik.user_key;
            value_ = iter_->value().ToString();
            valid_ = true;
            return;
        }
    }

    std::unique_ptr<Iterator> iter_;
    SequenceNumber snapshot_;
    std::vector<std::shared_ptr<const void>> pins_;

    bool valid_ = false;
    bool has_last_ = false;
    std::string last_emitted_or_skipped_;
    std::string user_key_;
    std::string value_;
};

} // namespace lakestore
