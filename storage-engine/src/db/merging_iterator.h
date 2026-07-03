#pragma once
#include "../iterator.h"
#include <vector>
#include <queue>

namespace lakestore {

class MergingIterator : public Iterator {
public:
    explicit MergingIterator(const std::vector<Iterator*>& children) 
        : children_(children) {}

    ~MergingIterator() override {
        for (auto* child : children_) {
            delete child;
        }
    }

    bool Valid() const override { return current_ != nullptr; }

    void SeekToFirst() override {
        while (!heap_.empty()) heap_.pop();
        
        for (size_t i = 0; i < children_.size(); ++i) {
            children_[i]->SeekToFirst();
            if (children_[i]->Valid()) {
                heap_.push({children_[i], i});
            }
        }
        FindSmallest();
    }

    void Seek(const Slice& target) override {
        while (!heap_.empty()) heap_.pop();
        
        for (size_t i = 0; i < children_.size(); ++i) {
            children_[i]->Seek(target);
            if (children_[i]->Valid()) {
                heap_.push({children_[i], i});
            }
        }
        FindSmallest();
    }

    void Next() override {
        if (current_ != nullptr) {
            current_->Next();
            if (current_->Valid()) {
                heap_.push({current_, current_index_});
            }
            FindSmallest();
        }
    }

    Slice key() const override { return current_->key(); }
    Slice value() const override { return current_->value(); }
    Status status() const override { return Status::OK(); }

private:
    struct HeapItem {
        Iterator* iter;
        size_t index;

        // Min-heap: We want the smallest key at the top.
        // If keys match, the one with the higher sequence number (newer) comes first,
        // which matches our CompareInternalKeys logic.
        bool operator<(const HeapItem& other) const {
            InternalKey a = InternalKey::Decode(iter->key());
            InternalKey b = InternalKey::Decode(other.iter->key());
            return CompareInternalKeys(a, b) > 0; // > because std::priority_queue is a max-heap by default
        }
    };

    void FindSmallest() {
        if (heap_.empty()) {
            current_ = nullptr;
        } else {
            HeapItem top = heap_.top();
            heap_.pop();
            current_ = top.iter;
            current_index_ = top.index;
        }
    }

    std::vector<Iterator*> children_;
    std::priority_queue<HeapItem> heap_;
    Iterator* current_ = nullptr;
    size_t current_index_ = 0;
};

} // namespace lakestore