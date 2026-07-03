#include "db_impl.h"
#include "../sstable/sstable_writer.h"
#include "../sstable/sstable_reader.h"
#include "../util/coding.h"
#include <iostream>

namespace lakestore {

DBImpl::DBImpl(const Options& options, const std::string& dbpath)
    : options_(options), dbname_(dbpath) {
    block_cache_ = std::make_unique<LRUCache>(256 * 1024 * 1024); // 256MB cache
    manifest_ = std::make_unique<Manifest>(dbname_);
    current_version_ = std::make_unique<Version>();
}

DBImpl::~DBImpl() {
    // 1. Signal background thread to shut down
    std::unique_lock<std::mutex> lock(mutex_);
    shutting_down_ = true;
    bg_cv_.notify_all();
    
    // Wait for background thread to exit (in a real impl, you'd join() the actual thread object here)
    while (bg_compaction_scheduled_) {
        bg_cv_.wait(lock);
    }

    delete mem_;
    delete imm_;
}

Status DBImpl::Open(const Options& options, const std::string& dbpath, DBImpl** dbptr) {
    *dbptr = nullptr;
    DBImpl* impl = new DBImpl(options, dbpath);
    
    impl->mutex_.lock();
    // 1. Read MANIFEST to restore state (Step 9)
    Status s = impl->manifest_->Open();
    // For Phase 1 startup, we assume a fresh DB if open fails/doesn't exist
    
    // 2. Recover WAL files (Step 2)
    s = impl->Recover();
    
    // 3. Setup active MemTable and WAL
    if (s.ok()) {
        impl->log_file_number_ = impl->manifest_->NextFileNumber();
        std::string wal_name = impl->dbname_ + "/" + std::to_string(impl->log_file_number_) + ".log";
        impl->wal_ = std::make_unique<WAL>(wal_name, impl->log_file_number_);
        impl->mem_ = new MemTable();
    }
    
    impl->mutex_.unlock();

    if (s.ok()) {
        *dbptr = impl;
    } else {
        delete impl;
    }
    return s;
}

Status DBImpl::Recover() {
    // Phase 1 shortcut: assuming fresh DB, recovery is a no-op unless MANIFEST loaded successfully.
    // In full impl, you read current_version_'s log_number and replay all WALs >= that number.
    return Status::OK();
}

Status DBImpl::Write(const WriteOptions& opts, WriteBatch* batch) {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!bg_error_.ok()) {
        return bg_error_; 
    }

    while (imm_ != nullptr) {
        bg_cv_.wait(lock);
    }

    SequenceNumber seq = last_sequence_.fetch_add(1, std::memory_order_relaxed) + 1;
    batch->sequence_number = seq;

    lock.unlock();
    Status s = wal_->Append(batch);
    if (opts.sync && s.ok()) {
        s = wal_->Sync();
    }
    lock.lock();

    if (!s.ok()) return s;

    // --- ACTUAL MEMTABLE INSERTION LOGIC ---
    Slice input(batch->payload);
    while (input.size > 0) {
        char type = input.data[0];
        input.data++; input.size--;
        
        uint32_t key_len = 0;
        if (!GetVarint32(&input, &key_len) || input.size < key_len) {
            return Status::Corruption("Corrupted WriteBatch key");
        }
        
        Slice key(input.data, key_len);
        input.data += key_len; input.size -= key_len;
        
        if (type == kTypeValue) {
            uint32_t val_len = 0;
            if (!GetVarint32(&input, &val_len) || input.size < val_len) {
                return Status::Corruption("Corrupted WriteBatch value");
            }
            Slice val(input.data, val_len);
            input.data += val_len; input.size -= val_len;
            mem_->Add(seq, kTypeValue, key, val);
        } else if (type == kTypeDeletion) {
            mem_->Add(seq, kTypeDeletion, key, Slice());
        }
    }
    // ---------------------------------------

    if (mem_->ApproximateMemoryUsage() > 64 * 1024 * 1024) {
        imm_ = mem_;
        mem_ = new MemTable();
        MaybeScheduleCompaction();
    }

    return Status::OK();
}

Status DBImpl::Put(const WriteOptions& opts, const Slice& key, const Slice& value) {
    WriteBatch batch;
    // Encode: [Type] [Key Length] [Key] [Value Length] [Value]
    batch.payload.push_back(static_cast<char>(kTypeValue));
    PutVarint32(&batch.payload, key.size);
    batch.payload.append(key.data, key.size);
    PutVarint32(&batch.payload, value.size);
    batch.payload.append(value.data, value.size);
    
    return Write(opts, &batch);
}

Status DBImpl::Delete(const WriteOptions& opts, const Slice& key) {
    WriteBatch batch;
    // Encode: [Type] [Key Length] [Key]
    batch.payload.push_back(static_cast<char>(kTypeDeletion));
    PutVarint32(&batch.payload, key.size);
    batch.payload.append(key.data, key.size);
    
    return Write(opts, &batch);
}

Status DBImpl::Get(const ReadOptions& opts, const Slice& key, std::string* value) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    MemTable* mem = mem_;
    MemTable* imm = imm_;
    Version* current = current_version_.get();
    
    // Grab current snapshot sequence number for MVCC
    SequenceNumber snapshot = last_sequence_.load(std::memory_order_acquire);
    LookupKey lkey(key, snapshot);
    
    lock.unlock();

    Status s;
    // 1. Probe Active MemTable
    if (mem->Get(lkey, value, &s)) {
        return s;
    }
    
    // 2. Probe Immutable MemTable
    if (imm != nullptr && imm->Get(lkey, value, &s)) {
        return s;
    }
    
    // 3. Probe L0 SSTables
    std::vector<FileMetaData*> l0_files;
    current->GetOverlappingL0(key, key, &l0_files);
    
    // L0 files overlap, must check in order from newest to oldest
    std::sort(l0_files.begin(), l0_files.end(), [](FileMetaData* a, FileMetaData* b) {
        return a->number > b->number;
    });

    for (FileMetaData* f : l0_files) {
        std::string filename = dbname_ + "/" + std::to_string(f->number) + ".ldb";
        SSTableReader* reader;
        if (SSTableReader::Open(filename, &reader).ok()) {
            s = reader->Get(opts, key, value);
            delete reader;
            if (s.ok() || !s.IsNotFound()) return s; // Found or IOError
        }
    }

    // 4. Probe Level 1 -> N
    for (int level = 1; level < kNumLevels; level++) {
        FileMetaData* f = current->FindFileInLevel(level, key);
        if (f != nullptr) {
            std::string filename = dbname_ + "/" + std::to_string(f->number) + ".ldb";
            SSTableReader* reader;
            if (SSTableReader::Open(filename, &reader).ok()) {
                s = reader->Get(opts, key, value);
                delete reader;
                if (s.ok() || !s.IsNotFound()) return s;
            }
        }
    }

    return Status::NotFound("Key not found");
}

void DBImpl::MaybeScheduleCompaction() {
    if (bg_compaction_scheduled_ || shutting_down_) return;
    if (imm_ == nullptr) return; // Only schedule if there's an immutable memtable to flush

    bg_compaction_scheduled_ = true;
    
    // In C++11+, you'd typically manage a persistent thread pool.
    // For this example, we detach a thread.
    std::thread([this]() { this->BackgroundCall(); }).detach();
}

void DBImpl::BackgroundCall() {
    std::unique_lock<std::mutex> lock(mutex_);
    
    if (!shutting_down_ && bg_error_.ok()) {
        bg_error_ = BackgroundCompaction();
    }
    
    bg_compaction_scheduled_ = false;
    MaybeScheduleCompaction(); // See if more work is needed
    bg_cv_.notify_all(); // Wake up writers waiting on imm_
}

Status DBImpl::BackgroundCompaction() {
    // Minor Compaction: Flush Immutable MemTable to Level 0
    if (imm_ != nullptr) {
        VersionEdit edit;
        Status s = WriteLevel0Table(imm_, &edit);
        
        if (s.ok()) {
            s = manifest_->LogAndApply(&edit, current_version_.get());
        }
        
        if (s.ok()) {
            delete imm_;
            imm_ = nullptr;
        }
        return s;
    }
    
    // Major Compaction (L0 -> L1, etc) would go here based on Step 10 logic.
    return Status::OK();
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit) {
    uint64_t file_number = manifest_->NextFileNumber();
    std::string filename = dbname_ + "/" + std::to_string(file_number) + ".ldb";
    
    SSTableWriter writer(filename, options_);
    
    std::unique_ptr<lakestore::Iterator> iter(mem->NewIterator());
    iter->SeekToFirst();
    
    while (iter->Valid()) {
        writer.Add(iter->key(), iter->value());
        iter->Next();
    }
    
    Status s = writer.Finish();
    
    if (s.ok() && writer.FileSize() > 0) {
        InternalKey smallest = InternalKey::Decode(writer.SmallestKey());
        InternalKey largest = InternalKey::Decode(writer.LargestKey());
        edit->AddFile(0, file_number, writer.FileSize(), smallest, largest);
    }
    
    return s;
}

} // namespace lakestore