#include "db_impl.h"
#include "db_iter.h"
#include "compaction.h"
#include "merging_iterator.h"
#include "../sstable/sstable_writer.h"
#include "../util/coding.h"
#include <algorithm>
#include <filesystem>

namespace lakestore {

DBImpl::DBImpl(const Options& options, const std::string& dbpath)
    : options_(options), dbname_(dbpath) {
    if (options_.block_cache_size > 0) {
        block_cache_ = std::make_unique<LRUCache>(options_.block_cache_size);
    }
    manifest_ = std::make_unique<Manifest>(dbname_);
    table_cache_ = std::make_unique<TableCache>(dbname_, options_, options_.table_cache_size);
    current_version_ = std::make_shared<Version>();
}

DBImpl::~DBImpl() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        shutting_down_ = true;
        bg_work_cv_.notify_all();
    }
    if (bg_thread_.joinable()) {
        bg_thread_.join();
    }
}

// --- Batch record framing ---
// [fixed64 sequence][fixed32 op_count][ops payload]
void DBImpl::EncodeBatchRecord(SequenceNumber seq, uint32_t count,
                               const std::string& payload, std::string* record) {
    record->clear();
    PutFixed64(record, seq);
    PutFixed32(record, count);
    record->append(payload);
}

bool DBImpl::DecodeBatchRecord(const Slice& record, SequenceNumber* seq,
                               uint32_t* count, Slice* payload) {
    if (record.size < 12) return false;
    *seq = DecodeFixed64(record.data);
    *count = DecodeFixed32(record.data + 8);
    *payload = Slice(record.data + 12, record.size - 12);
    return true;
}

Status DBImpl::Open(const Options& options, const std::string& dbpath, DBImpl** dbptr) {
    *dbptr = nullptr;

    std::error_code ec;
    std::filesystem::create_directories(dbpath, ec);
    if (ec) return Status::IOError("Cannot create db directory: " + dbpath);

    DBImpl* impl = new DBImpl(options, dbpath);

    {
        std::unique_lock<std::mutex> lock(impl->mutex_);

        // 1. Read the MANIFEST: rebuilds the Version (which SSTables live at
        // which level) plus the file-number / sequence / log-number counters.
        auto base = std::make_shared<Version>();
        Status s = impl->manifest_->Open(base.get());
        if (!s.ok()) {
            lock.unlock();
            delete impl;
            return s;
        }
        impl->current_version_ = base;
        impl->last_sequence_.store(impl->manifest_->LastSequence());

        // 2. Allocate the active MemTable, then replay live WAL files into it.
        impl->mem_ = std::make_shared<MemTable>();
        s = impl->Recover();

        // 3. Setup the active WAL for new incoming writes.
        if (s.ok()) {
            impl->log_file_number_ = impl->manifest_->NextFileNumber();
            std::string wal_name = impl->dbname_ + "/" + std::to_string(impl->log_file_number_) + ".log";
            impl->wal_ = std::make_unique<WAL>(wal_name, impl->log_file_number_);
        }

        // 4. If recovery replayed anything, flush it to an L0 table so the
        // old logs become obsolete, and record the new log number so a
        // future recovery ignores them.
        if (s.ok() && !impl->mem_->IsEmpty()) {
            VersionEdit edit;
            s = impl->WriteLevel0Table(impl->mem_.get(), &edit);
            if (s.ok()) {
                edit.SetLogNumber(impl->log_file_number_);
                edit.SetLastSequence(impl->last_sequence_.load());
                s = impl->manifest_->LogAndApply(&edit, &impl->current_version_);
            }
            if (s.ok()) {
                impl->mem_ = std::make_shared<MemTable>();
            }
        } else if (s.ok()) {
            // Nothing replayed; still persist the new log number so stale
            // logs (already flushed in a previous run) are ignored.
            VersionEdit edit;
            edit.SetLogNumber(impl->log_file_number_);
            edit.SetLastSequence(impl->last_sequence_.load());
            s = impl->manifest_->LogAndApply(&edit, &impl->current_version_);
        }

        if (!s.ok()) {
            lock.unlock();
            delete impl;
            return s;
        }

        impl->RemoveObsoleteFiles();

        // 5. Start the background worker and kick off any pending compaction.
        impl->bg_thread_ = std::thread([impl]() { impl->BackgroundThreadMain(); });
        impl->MaybeScheduleCompaction();
    }

    *dbptr = impl;
    return Status::OK();
}

Status DBImpl::Recover() {
    std::vector<uint64_t> logs;
    for (const auto& entry : std::filesystem::directory_iterator(dbname_)) {
        if (entry.path().extension() == ".log") {
            uint64_t num = std::stoull(entry.path().stem().string());
            // Logs below the manifest's log number were flushed already.
            if (num >= manifest_->LogNumber()) {
                logs.push_back(num);
            }
        }
    }
    std::sort(logs.begin(), logs.end());

    SequenceNumber max_seq = last_sequence_.load();
    for (uint64_t log_num : logs) {
        WAL log(dbname_ + "/" + std::to_string(log_num) + ".log", log_num);
        Status s = log.Recover([&](const Slice& record) {
            SequenceNumber seq;
            uint32_t count;
            Slice payload;
            if (!DecodeBatchRecord(record, &seq, &count, &payload)) {
                return Status::Corruption("Malformed WAL record");
            }
            DecodeAndInsert(mem_.get(), seq, payload);
            if (count > 0) {
                max_seq = std::max(max_seq, seq + count - 1);
            }
            return Status::OK();
        });
        if (!s.ok()) return s;
    }
    last_sequence_.store(max_seq);
    return Status::OK();
}

Status DBImpl::MakeRoomForWrite(std::unique_lock<std::mutex>& lock) {
    while (true) {
        if (!bg_error_.ok()) return bg_error_;

        if (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size) {
            return Status::OK();
        }
        if (imm_ != nullptr) {
            // Previous memtable still flushing — wait for the background
            // thread to finish before rotating again.
            bg_cv_.wait(lock);
            continue;
        }
        if (static_cast<int>(current_version_->Files(0).size()) >= options_.l0_stall_trigger) {
            // Too many L0 files: stall writes until compaction catches up.
            bg_cv_.wait(lock);
            continue;
        }

        // Rotate: freeze the current memtable and switch to a fresh WAL so
        // the frozen data's log can be deleted after the flush.
        uint64_t new_log_number = manifest_->NextFileNumber();
        std::string wal_name = dbname_ + "/" + std::to_string(new_log_number) + ".log";
        auto new_wal = std::make_unique<WAL>(wal_name, new_log_number);

        imm_ = mem_;
        imm_log_file_number_ = log_file_number_;
        mem_ = std::make_shared<MemTable>();
        wal_ = std::move(new_wal);
        log_file_number_ = new_log_number;

        MaybeScheduleCompaction();
        return Status::OK();
    }
}

Status DBImpl::Write(const WriteOptions& opts, WriteBatch* batch) {
    uint32_t op_count = CountOps(batch);

    std::unique_lock<std::mutex> lock(mutex_);

    Status s = MakeRoomForWrite(lock);
    if (!s.ok()) return s;

    SequenceNumber base_seq = last_sequence_.load(std::memory_order_relaxed) + 1;
    last_sequence_.store(base_seq + op_count - 1, std::memory_order_release);
    batch->sequence_number = base_seq;

    // 1. WAL append (durability first), unless explicitly disabled.
    if (!opts.disable_wal) {
        std::string record;
        EncodeBatchRecord(base_seq, op_count, batch->payload, &record);
        s = wal_->AppendRecord(Slice(record));
        if (opts.sync && s.ok()) s = wal_->Sync();
        if (!s.ok()) return s;
    }

    // 2. Serialized MemTable insertion.
    DecodeAndInsert(mem_.get(), base_seq, Slice(batch->payload));

    return Status::OK();
}

Status DBImpl::Put(const WriteOptions& opts, const Slice& key, const Slice& value) {
    WriteBatch batch;
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
    std::shared_ptr<MemTable> mem;
    std::shared_ptr<MemTable> imm;
    std::shared_ptr<const Version> current;
    SequenceNumber snapshot;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        mem = mem_;
        imm = imm_;
        current = current_version_;
        snapshot = last_sequence_.load(std::memory_order_acquire);
    }

    LookupKey lkey(key, snapshot);

    Status s;
    // 1. Probe Active MemTable
    if (mem->Get(lkey, value, &s)) {
        return s;
    }

    // 2. Probe Immutable MemTable
    if (imm != nullptr && imm->Get(lkey, value, &s)) {
        return s;
    }

    // 3. Probe the SSTables, newest data first.
    ReadOptions r_opts = opts;
    r_opts.block_cache = block_cache_.get();
    std::string encoded_lookup = lkey.internal_key().Encode();
    Slice internal_key(encoded_lookup);

    // L0 files may overlap: check newest to oldest (highest file number first).
    std::vector<FileMetaData> l0_files;
    current->GetOverlappingL0(key, key, &l0_files);
    std::sort(l0_files.begin(), l0_files.end(),
              [](const FileMetaData& a, const FileMetaData& b) {
                  return a.number > b.number;
              });

    for (const FileMetaData& f : l0_files) {
        TableLookup result;
        s = table_cache_->Get(r_opts, f.number, internal_key, value, &result);
        if (!s.ok()) return s;
        if (result == TableLookup::kFound) return Status::OK();
        if (result == TableLookup::kDeleted) return Status::NotFound("Key not found");
    }

    // 4. Probe Level 1 -> N (at most one candidate file per level).
    for (int level = 1; level < kNumLevels; level++) {
        const FileMetaData* f = current->FindFileInLevel(level, key);
        if (f != nullptr) {
            TableLookup result;
            s = table_cache_->Get(r_opts, f->number, internal_key, value, &result);
            if (!s.ok()) return s;
            if (result == TableLookup::kFound) return Status::OK();
            if (result == TableLookup::kDeleted) return Status::NotFound("Key not found");
        }
    }

    return Status::NotFound("Key not found");
}

// --- Background work ---

void DBImpl::MaybeScheduleCompaction() {
    if (shutting_down_) return;
    if (imm_ != nullptr || current_version_->PickCompactionLevel(options_) >= 0) {
        bg_work_pending_ = true;
        bg_work_cv_.notify_one();
    }
}

void DBImpl::BackgroundThreadMain() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!shutting_down_) {
        if (!bg_work_pending_) {
            bg_work_cv_.wait(lock);
            continue;
        }
        bg_work_pending_ = false;

        if (bg_error_.ok()) {
            Status s = BackgroundCompaction(lock);
            if (!s.ok()) bg_error_ = s;
        }

        MaybeScheduleCompaction(); // More work? (e.g. cascading compactions)
        bg_cv_.notify_all();       // Wake writers waiting on imm_ / stalls
    }
    // Final flush attempt on clean shutdown so no committed data lives only
    // in the WAL longer than necessary (recovery would replay it anyway).
    if (imm_ != nullptr && bg_error_.ok()) {
        CompactMemTable(lock);
        bg_cv_.notify_all();
    }
}

Status DBImpl::BackgroundCompaction(std::unique_lock<std::mutex>& lock) {
    // Minor compaction first: it unblocks writers.
    if (imm_ != nullptr) {
        return CompactMemTable(lock);
    }

    int level = current_version_->PickCompactionLevel(options_);
    if (level >= 0) {
        return DoMajorCompaction(lock, level);
    }
    return Status::OK();
}

Status DBImpl::CompactMemTable(std::unique_lock<std::mutex>& lock) {
    std::shared_ptr<MemTable> imm = imm_;
    uint64_t log_boundary = log_file_number_;

    VersionEdit edit;
    // Writing the table is long I/O — drop the lock. imm_ stays set, so
    // writers keep waiting and no second flush can start (single bg thread).
    lock.unlock();
    Status s = WriteLevel0Table(imm.get(), &edit);
    lock.lock();

    if (s.ok()) {
        // Everything in imm_ (and older) is now in an SSTable: logs below
        // the current one are obsolete.
        edit.SetLogNumber(log_boundary);
        edit.SetLastSequence(last_sequence_.load());
        s = manifest_->LogAndApply(&edit, &current_version_);
    }

    if (s.ok()) {
        imm_ = nullptr;
        imm_log_file_number_ = 0;
        RemoveObsoleteFiles();
    }
    return s;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit) {
    uint64_t file_number = manifest_->NextFileNumber();
    std::string filename = TableCache::TableFileName(dbname_, file_number);

    SSTableWriter writer(filename, options_);

    std::unique_ptr<lakestore::Iterator> iter(mem->NewIterator());
    iter->SeekToFirst();

    while (iter->Valid()) {
        writer.Add(iter->key(), iter->value());
        iter->Next();
    }

    Status s = writer.Finish();

    if (s.ok() && writer.NumEntries() > 0) {
        InternalKey smallest = InternalKey::Decode(writer.SmallestKey());
        InternalKey largest = InternalKey::Decode(writer.LargestKey());
        edit->AddFile(0, file_number, writer.FileSize(), smallest, largest);
    } else if (s.ok()) {
        std::filesystem::remove(filename);
    }

    return s;
}

Status DBImpl::DoMajorCompaction(std::unique_lock<std::mutex>& lock, int level) {
    // The version is immutable and pinned by shared_ptr; only this thread
    // performs flushes/compactions, so the inputs cannot change under us.
    std::shared_ptr<const Version> base = current_version_;
    SequenceNumber smallest_snapshot = last_sequence_.load();

    Compaction compaction(options_, level, base.get());
    if (!compaction.PickInputs()) return Status::OK();

    ReadOptions r_opts;
    r_opts.fill_cache = false; // Don't pollute the cache during compaction
    r_opts.block_cache = block_cache_.get();

    // Build the merged input iterator (long I/O follows — release the lock).
    std::vector<lakestore::Iterator*> children;
    for (int which = 0; which < 2; which++) {
        for (const FileMetaData& f : compaction.inputs(which)) {
            lakestore::Iterator* it = table_cache_->NewIterator(r_opts, f.number);
            if (it == nullptr) {
                for (auto* c : children) delete c;
                return Status::IOError("Cannot open compaction input table");
            }
            children.push_back(it);
        }
    }

    lock.unlock();

    MergingIterator merged(children); // Takes ownership of children
    merged.SeekToFirst();

    Status s;
    std::unique_ptr<SSTableWriter> out;
    uint64_t out_number = 0;
    auto finish_output = [&]() {
        if (!out) return;
        Status fs = out->Finish();
        if (fs.ok() && out->NumEntries() > 0) {
            compaction.edit()->AddFile(
                level + 1, out_number, out->FileSize(),
                InternalKey::Decode(out->SmallestKey()),
                InternalKey::Decode(out->LargestKey()));
        } else {
            std::filesystem::remove(TableCache::TableFileName(dbname_, out_number));
            if (!fs.ok() && s.ok()) s = fs;
        }
        out.reset();
    };

    while (merged.Valid() && s.ok()) {
        InternalKey ikey = InternalKey::Decode(merged.key());

        if (!compaction.ShouldDrop(ikey, smallest_snapshot)) {
            if (!out) {
                // File numbers come from the manifest counter; take the
                // mutex briefly for the allocation.
                lock.lock();
                out_number = manifest_->NextFileNumber();
                lock.unlock();
                out = std::make_unique<SSTableWriter>(
                    TableCache::TableFileName(dbname_, out_number), options_);
            }
            out->Add(merged.key(), merged.value());
            if (out->FileSize() >= options_.target_file_size) {
                finish_output();
            }
        }
        merged.Next();
    }
    finish_output();

    lock.lock();

    if (s.ok()) {
        s = manifest_->LogAndApply(compaction.edit(), &current_version_);
    }
    if (s.ok()) {
        RemoveObsoleteFiles();
    } else {
        // Clean up any orphaned outputs; the edit was never applied.
        for (const auto& f : compaction.edit()->new_files_) {
            std::filesystem::remove(TableCache::TableFileName(dbname_, f.number));
            table_cache_->Evict(f.number);
        }
    }
    return s;
}

void DBImpl::RemoveObsoleteFiles() {
    // Collect the live set under the current state.
    std::set<uint64_t> live_tables;
    for (int level = 0; level < kNumLevels; level++) {
        for (const auto& f : current_version_->Files(level)) {
            live_tables.insert(f.number);
        }
    }

    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(dbname_, ec)) {
        std::string name = entry.path().filename().string();
        std::string ext = entry.path().extension().string();

        bool remove = false;
        if (ext == ".ldb") {
            uint64_t num = std::stoull(entry.path().stem().string());
            if (live_tables.find(num) == live_tables.end()) {
                remove = true;
                table_cache_->Evict(num);
            }
        } else if (ext == ".log") {
            uint64_t num = std::stoull(entry.path().stem().string());
            // Keep the active log, the imm log, and anything newer than the
            // manifest's recovery boundary.
            if (num < manifest_->LogNumber() &&
                num != log_file_number_ &&
                (imm_ == nullptr || num != imm_log_file_number_)) {
                remove = true;
            }
        } else if (name.rfind("MANIFEST-", 0) == 0) {
            // Only prune old manifests once a new one exists.
            uint64_t current_manifest = manifest_->ManifestFileNumber();
            if (current_manifest > 0 &&
                name != "MANIFEST-" + std::to_string(current_manifest)) {
                remove = true;
            }
        } else if (name == "CURRENT.tmp") {
            remove = true;
        }

        if (remove) {
            std::filesystem::remove(entry.path(), ec);
        }
    }
}

Status DBImpl::FlushAll() {
    std::unique_lock<std::mutex> lock(mutex_);

    // Force-rotate a non-empty active memtable.
    if (!mem_->IsEmpty()) {
        while (imm_ != nullptr && bg_error_.ok()) bg_cv_.wait(lock);
        if (!bg_error_.ok()) return bg_error_;

        uint64_t new_log_number = manifest_->NextFileNumber();
        std::string wal_name = dbname_ + "/" + std::to_string(new_log_number) + ".log";
        auto new_wal = std::make_unique<WAL>(wal_name, new_log_number);

        imm_ = mem_;
        imm_log_file_number_ = log_file_number_;
        mem_ = std::make_shared<MemTable>();
        wal_ = std::move(new_wal);
        log_file_number_ = new_log_number;
        MaybeScheduleCompaction();
    }

    // Wait for the flush and any cascading compactions to settle.
    while (bg_error_.ok() &&
           (imm_ != nullptr || bg_work_pending_ ||
            current_version_->PickCompactionLevel(options_) >= 0)) {
        MaybeScheduleCompaction();
        bg_cv_.wait(lock);
    }
    return bg_error_;
}

// --- Batch decoding helpers ---

uint32_t DBImpl::CountOps(const WriteBatch* batch) {
    uint32_t count = 0;
    Slice input(batch->payload);

    while (input.size > 0) {
        char type = input.data[0];
        input.data++; input.size--;

        uint32_t len = 0;
        if (!GetVarint32(&input, &len)) break;
        if (input.size < len) break;

        input.data += len; input.size -= len;

        if (type == kTypeValue) {
            if (!GetVarint32(&input, &len)) break;
            if (input.size < len) break;
            input.data += len; input.size -= len;
        }
        count++;
    }
    return count;
}

void DBImpl::DecodeAndInsert(MemTable* mem, SequenceNumber seq, const Slice& payload) {
    Slice input = payload;
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

// --- Iterators ---

lakestore::Iterator* DBImpl::NewInternalIterator() {
    std::lock_guard<std::mutex> lock(mutex_);
    return mem_->NewIterator();
}

lakestore::Iterator* DBImpl::NewIterator() {
    std::shared_ptr<MemTable> mem;
    std::shared_ptr<MemTable> imm;
    std::shared_ptr<const Version> current;
    SequenceNumber snapshot;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        mem = mem_;
        imm = imm_;
        current = current_version_;
        snapshot = last_sequence_.load(std::memory_order_acquire);
    }

    ReadOptions r_opts;
    r_opts.block_cache = block_cache_.get();

    std::vector<lakestore::Iterator*> children;
    children.push_back(mem->NewIterator());
    if (imm) children.push_back(imm->NewIterator());
    for (int level = 0; level < kNumLevels; level++) {
        for (const auto& f : current->Files(level)) {
            lakestore::Iterator* it = table_cache_->NewIterator(r_opts, f.number);
            if (it != nullptr) children.push_back(it);
        }
    }

    // Pin the memtables and the version for the iterator's lifetime.
    std::vector<std::shared_ptr<const void>> pins;
    pins.push_back(std::static_pointer_cast<const void>(mem));
    if (imm) pins.push_back(std::static_pointer_cast<const void>(imm));
    pins.push_back(std::static_pointer_cast<const void>(current));

    return new DBIter(new MergingIterator(children), snapshot, std::move(pins));
}

} // namespace lakestore
