#pragma once

#include "../types.h"
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include <string>

namespace lakestore {

class SkipList {
private:
    static constexpr int kMaxHeight = 12;

    struct Node {
        InternalKey key;
        std::string value;
        std::atomic<Node*> next[1]; 
        
        // Explicit constructor prevents aggregate initialization memory corruption
        Node(const InternalKey& k, const std::string& v) : key(k), value(v) {}

        Node* Next(int level) {
            return next[level].load(std::memory_order_acquire);
        }

        void SetNext(int level, Node* x) {
            next[level].store(x, std::memory_order_release);
        }
    };

    Node* head_;
    std::atomic<int> max_height_;

    // Helper to allocate a node with enough space for `height` next pointers
    Node* AllocateNode(const InternalKey& key, const std::string& value, int height) {
        size_t size = sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1);
        void* mem = std::malloc(size);
        
        // Safely invoke the constructor using placement new
        Node* node = new (mem) Node(key, value);
        
        // Safely initialize the atomic pointers in the raw memory
        for (int i = 0; i < height; i++) {
            new (&node->next[i]) std::atomic<Node*>(nullptr);
        }
        return node;
    }

    int RandomHeight() {
        int height = 1;
        // P = 0.25 branching factor
        while (height < kMaxHeight && (std::rand() % 4 == 0)) {
            height++;
        }
        return height;
    }

public:
    SkipList() : max_height_(1) {
        // Dummy head node. Key and value don't matter.
        head_ = AllocateNode(InternalKey{}, "", kMaxHeight);
        for (int i = 0; i < kMaxHeight; i++) {
            head_->SetNext(i, nullptr);
        }
    }

    ~SkipList() {
        Node* current = head_;
        while (current != nullptr) {
            Node* next = current->Next(0);
            current->~Node(); // Call destructor for std::string etc.
            std::free(current);
            current = next;
        }
    }

    // Insert is called by a single writer thread (protected by DB mutex).
    void Insert(const InternalKey& key, const std::string& value) {
        Node* update[kMaxHeight];
        Node* current = head_;

        // 1. Find the insert position at each level
        for (int i = max_height_.load(std::memory_order_relaxed) - 1; i >= 0; i--) {
            while (true) {
                Node* next = current->Next(i);
                if (next != nullptr && CompareInternalKeys(next->key, key) < 0) {
                    current = next;
                } else {
                    break;
                }
            }
            update[i] = current;
        }

        // 2. Generate random height for new node
        int height = RandomHeight();
        if (height > max_height_.load(std::memory_order_relaxed)) {
            for (int i = max_height_.load(std::memory_order_relaxed); i < height; i++) {
                update[i] = head_;
            }
            // Safe to use relaxed here; readers check max_height_ or just follow nulls
            max_height_.store(height, std::memory_order_relaxed);
        }

        // 3. Create node and link it in
        Node* x = AllocateNode(key, value, height);
        for (int i = 0; i < height; i++) {
            // x's next pointers don't need release semantics yet since x isn't reachable
            x->next[i].store(update[i]->Next(i), std::memory_order_relaxed);
            // This release store makes x visible to concurrent readers at level i
            update[i]->SetNext(i, x);
        }
    }

    // Iterator for scanning / flushing the MemTable
    class Iterator {
    private:
        const SkipList* list_;
        Node* current_;

    public:
        explicit Iterator(const SkipList* list) : list_(list), current_(nullptr) {}

        bool Valid() const { return current_ != nullptr; }
        const InternalKey& key() const { return current_->key; }
        const std::string& value() const { return current_->value; }

        void SeekToFirst() {
            current_ = list_->head_->Next(0);
        }

        void Next() {
            if (Valid()) {
                current_ = current_->Next(0);
            }
        }

        // Seek to the first key that is >= target
        void Seek(const InternalKey& target) {
            Node* current = list_->head_;
            int level = list_->max_height_.load(std::memory_order_acquire) - 1;
            
            while (true) {
                Node* next = current->Next(level);
                if (next != nullptr && CompareInternalKeys(next->key, target) < 0) {
                    current = next; // Keep moving forward in this level
                } else {
                    if (level == 0) {
                        current_ = next; // Reached the bottom level, this is our node
                        return;
                    } else {
                        level--; // Drop down a level
                    }
                }
            }
        }
    };

    Iterator* NewIterator() const {
        return new Iterator(this);
    }
};

} // namespace lakestore