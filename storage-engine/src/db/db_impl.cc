#include "db_impl.h"
#include "../sstable/sstable_writer.h"
#include "../sstable/sstable_reader.h"
#include "../util/coding.h"
#include <iostream>
#include <filesystem>

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
    if (imm_ != nullptr) delete imm_;
}

Status DBImpl::Open(const Options& options, const std::string& dbpath, DBImpl** dbptr) {
    *dbptr = nullptr;
    DBImpl* impl = new DBImpl(options, dbpath);
    
    impl->mutex_.lock();
    
    // 1. Read MANIFEST
    Status s = impl->manifest_->Open();
    if (!s.ok()) {
        impl->mutex_.unlock();
        delete impl;
        return s;
    }
    
    // 2. CRITICAL FIX: Allocate MemTable BEFORE recovery!
    impl->mem_ = new MemTable();
    
    // 3. Recover WAL files into the MemTable
    s = impl->Recover();
    
    // 4. Setup the active WAL for new incoming writes
    if (s.ok()) {
        impl->log_file_number_ = impl->manifest_->NextFileNumber();
        std::string wal_name = impl->dbname_ + "/" + std::to_string(impl->log_file_number_) + ".log";
        impl->wal_ = std::make_unique<WAL>(wal_name, impl->log_file_number_);
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
    std::vector<uint64_t> logs;
    for (const auto& entry : std::filesystem::directory_iterator(dbname_)) {
        if (entry.path().extension() == ".log") {
            std::string stem = entry.path().stem().string();
            logs.push_back(std::stoull(stem));
        }
    }
    std::sort(logs.begin(), logs.end());

    SequenceNumber max_seq = 0;
    for (uint64_t log_num : logs) {
        std::unique_ptr<WAL> log(new WAL(dbname_ + "/" + std::to_string(log_num) + ".log", log_num));
        log->Recover([&](const WriteBatch& batch) {
            DecodeAndInsert(mem_, batch.sequence_number, batch.payload);
            
            // Cast away constness only for the helper call
            max_seq = std::max(max_seq, batch.sequence_number + CountOps(const_cast<WriteBatch*>(&batch)) - 1);
            return Status::OK();
        });
    }
    return Status::OK();
}

Status DBImpl::Write(const WriteOptions& opts, WriteBatch* batch) {
    if (!bg_error_.ok()) return bg_error_;
    if (!wal_) return Status::IOError("WAL not initialized - DB might be corrupted");

    uint32_t op_count = CountOps(batch);
    SequenceNumber base_seq = last_sequence_.fetch_add(op_count) + 1;
    batch->sequence_number = base_seq;

    // 1. Lock the mutex for the ENTIRE write process
    std::unique_lock<std::mutex> lock(mutex_);
    
    // Wait if the immutable memtable is still waiting to be flushed
    while (imm_ != nullptr) {
        bg_cv_.wait(lock);
    }

    // 2. Serialized WAL Append
    Status s = wal_->Append(batch);
    
    // Only touch the WAL if it is NOT disabled
    if (!opts.disable_wal) {
        s = wal_->Append(batch);
        if (opts.sync && s.ok()) s = wal_->Sync();
    }

    if (!s.ok()) {
        return s;
    }

    // 3. Serialized MemTable Insertion (Prevents the infinite loop!)
    DecodeAndInsert(mem_, base_seq, batch->payload);

    // 4. Check Compaction Threshold
    if (mem_->ApproximateMemoryUsage() > 64 * 1024 * 1024) {
        imm_ = mem_;
        mem_ = new MemTable();
        MaybeScheduleCompaction();
    }
    
    return Status::OK();
}

Status DBImpl::Put(const WriteOptions& opts, const Slice& key, const Slice& value) {
    WriteBatch batch;
    batch.payload.push_back(static_cast<char>(kTypeValue));
    PutVarint32(&batch.payload, key.size);
    batch.payload.append(key.data, key.size);
    PutVarint32(&batch.payload, value.size);
    batch.payload.append(value.data, value.size);
    
    // std::cout << "  [Put] Constructed payload of size: " << batch.payload.size() << std::endl;
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
    // Current threshold: L0 file count >= 4
    if (bg_compaction_scheduled_ || shutting_down_) return;
    
    // Check files in Version (Step 9)
    if (imm_ != nullptr || current_version_->Files(0).size() >= 4) {
        bg_compaction_scheduled_ = true;
        std::thread([this]() { this->BackgroundCall(); }).detach();
    }
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

uint32_t DBImpl::CountOps(const WriteBatch* batch) {
    uint32_t count = 0;
    Slice input(batch->payload);
    
    // std::cout << "    [CountOps] Input size: " << input.size << " | Bytes: ";
    // for(size_t i = 0; i < input.size; i++) {
    //     printf("%02x ", (unsigned char)input.data[i]);
    // }
    // std::cout << std::endl;

    while (input.size > 0) {
        char type = input.data[0];
        // std::cout << "    [CountOps] Parsed Type: " << (int)type << std::endl;
        input.data++; input.size--;
        
        uint32_t len = 0;
        if (!GetVarint32(&input, &len)) {
            // std::cout << "    [CountOps] ERROR: Failed to decode key varint!" << std::endl;
            break; 
        }
        if (input.size < len) {
            // std::cout << "    [CountOps] ERROR: Key length " << len << " > remaining size " << input.size << std::endl;
            break;
        }
        
        // std::cout << "    [CountOps] Parsed Key Length: " << len << std::endl;
        input.data += len; input.size -= len; 
        
        if (type == kTypeValue) {
            if (!GetVarint32(&input, &len)) {
                // std::cout << "    [CountOps] ERROR: Failed to decode value varint!" << std::endl;
                break;
            }
            if (input.size < len) {
                // std::cout << "    [CountOps] ERROR: Value length " << len << " > remaining size " << input.size << std::endl;
                break;
            }
            // std::cout << "    [CountOps] Parsed Value Length: " << len << std::endl;
            input.data += len; input.size -= len;
        }
        count++;
    }
    return count;
}

void DBImpl::DecodeAndInsert(MemTable* mem, SequenceNumber seq, const std::string& payload) {
    Slice input(payload);
    SequenceNumber current_seq = seq;
    
    while (input.size > 0) {
        char type = input.data[0];
        input.data++; input.size--;
        
        uint32_t key_len = 0;
        // Strict bounds checking
        if (!GetVarint32(&input, &key_len) || input.size < key_len) break;
        
        Slice key(input.data, key_len);
        input.data += key_len; input.size -= key_len;
        
        if (type == kTypeValue) {
            uint32_t val_len = 0;
            if (!GetVarint32(&input, &val_len) || input.size < val_len) break;
            Slice val(input.data, val_len);
            input.data += val_len; input.size -= val_len;
            
            mem->Add(current_seq, kTypeValue, key, val);
        } else {
            mem->Add(current_seq, kTypeDeletion, key, Slice());
        }
        current_seq++;
    }
}

lakestore::Iterator* DBImpl::NewInternalIterator() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (mem_ != nullptr) {
        return mem_->NewIterator();
    }
    return nullptr;
}

} // namespace lakestore