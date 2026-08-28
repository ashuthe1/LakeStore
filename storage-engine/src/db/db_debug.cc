// Read-only introspection for the inspector UI / debugging: serializes the
// engine's internal state (memtables, levels, files, caches, stats) and
// decodes on-disk files (WAL records, MANIFEST edits; SSTable structure
// lives in sstable_reader.cc). Deliberately zero impact on the normal data
// path: it uses the same snapshot mechanisms as reads (shared_ptr pins),
// never takes the mutex for longer than a pointer copy, opens files
// strictly read-only, and bypasses the block cache.
#include "db_impl.h"
#include "db_iter.h"
#include "../util/coding.h"
#include "../util/json_builder.h"
#include <filesystem>

namespace lakestore {

namespace {

constexpr size_t kMaxValuePreview = 128;  // bytes of a value shown per entry
constexpr size_t kMaxMemEntries = 2000;   // memtable entries listed
constexpr size_t kMaxSSTEntries = 5000;   // SSTable entries listed per dump
constexpr size_t kMaxWALRecords = 2000;   // WAL records listed per dump

// One decoded internal entry: {"key":..,"seq":..,"type":"PUT|DEL","value":..}
void AppendEntry(std::string* out, const Slice& encoded_key, const Slice& value) {
    InternalKey ik = InternalKey::Decode(encoded_key);
    out->append("{");
    json::KVStr(out, "key", Slice(ik.user_key));
    out->append(",");
    json::KV(out, "seq", ik.seq);
    out->append(",\"type\":\"");
    out->append(ik.type == kTypeValue ? "PUT" : "DEL");
    out->append("\",");
    json::KV(out, "value_len", value.size);
    out->append(",");
    Slice preview(value.data, value.size < kMaxValuePreview ? value.size : kMaxValuePreview);
    json::KVStr(out, "value", preview);
    if (value.size > kMaxValuePreview) {
        out->append(",\"value_truncated\":true");
    }
    out->append("}");
}

void AppendMemTableJSON(std::string* out, const MemTable* mem) {
    out->append("{");
    json::KV(out, "memory_bytes", mem->ApproximateMemoryUsage());
    out->append(",\"entries\":[");
    std::unique_ptr<Iterator> it(mem->NewIterator());
    size_t count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        if (count < kMaxMemEntries) {
            if (count > 0) out->append(",");
            AppendEntry(out, it->key(), it->value());
        }
        count++;
    }
    out->append("],");
    json::KV(out, "entry_count", count);
    out->append(",");
    json::KVBool(out, "entries_capped", count > kMaxMemEntries);
    out->append("}");
}

// Decode a WriteBatch ops payload ([type][klen][key]([vlen][val]))* to JSON.
void AppendOpsJSON(std::string* out, Slice input) {
    out->append("[");
    bool first = true;
    while (input.size > 0) {
        char type = input.data[0];
        input.data++; input.size--;

        uint32_t key_len = 0;
        if (!GetVarint32(&input, &key_len) || input.size < key_len) break;
        Slice key(input.data, key_len);
        input.data += key_len; input.size -= key_len;

        if (!first) out->append(",");
        first = false;
        out->append("{\"op\":\"");
        if (type == kTypeValue) {
            uint32_t val_len = 0;
            if (!GetVarint32(&input, &val_len) || input.size < val_len) {
                out->append("CORRUPT\"}");
                break;
            }
            Slice val(input.data, val_len);
            input.data += val_len; input.size -= val_len;
            out->append("PUT\",");
            json::KVStr(out, "key", key);
            out->append(",");
            json::KV(out, "value_len", val.size);
            out->append(",");
            Slice preview(val.data, val.size < kMaxValuePreview ? val.size : kMaxValuePreview);
            json::KVStr(out, "value", preview);
            if (val.size > kMaxValuePreview) out->append(",\"value_truncated\":true");
        } else {
            out->append("DEL\",");
            json::KVStr(out, "key", key);
        }
        out->append("}");
    }
    out->append("]");
}

void AppendInternalKeyJSON(std::string* out, const char* name, const InternalKey& ik) {
    out->append("\"");
    out->append(name);
    out->append("\":{");
    json::KVStr(out, "key", Slice(ik.user_key));
    out->append(",");
    json::KV(out, "seq", ik.seq);
    out->append("}");
}

} // namespace

void DBImpl::DebugJSON(std::string* out) {
    // Pin a consistent view exactly like Get() does, then build the JSON
    // without holding the mutex.
    std::shared_ptr<MemTable> mem;
    std::shared_ptr<MemTable> imm;
    std::shared_ptr<const Version> current;
    SequenceNumber last_seq;
    uint64_t log_num, imm_log_num;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        mem = mem_;
        imm = imm_;
        current = current_version_;
        last_seq = last_sequence_.load();
        log_num = log_file_number_;
        imm_log_num = imm_log_file_number_;
    }

    out->clear();
    out->append("{");
    json::KV(out, "last_sequence", last_seq);
    out->append(",");
    json::KV(out, "active_wal_file", log_num);
    out->append(",");
    json::KV(out, "imm_wal_file", imm_log_num);
    out->append(",");
    json::KV(out, "write_buffer_size", options_.write_buffer_size);
    out->append(",");
    json::KV(out, "l0_compaction_trigger", options_.l0_compaction_trigger);
    out->append(",");
    json::KV(out, "l0_stall_trigger", options_.l0_stall_trigger);
    out->append(",");
    json::KV(out, "target_file_size", options_.target_file_size);

    out->append(",\"stats\":{");
    json::KV(out, "memtable_flushes", stat_flushes_.load(std::memory_order_relaxed));
    out->append(",");
    json::KV(out, "major_compactions", stat_major_compactions_.load(std::memory_order_relaxed));
    out->append(",");
    json::KV(out, "write_stalls", stat_write_stalls_.load(std::memory_order_relaxed));
    out->append(",");
    json::KV(out, "compacted_bytes", stat_compacted_bytes_.load(std::memory_order_relaxed));
    out->append("}");

    out->append(",\"block_cache\":{");
    if (block_cache_) {
        json::KV(out, "used_bytes", block_cache_->TotalCharge());
        out->append(",");
        json::KV(out, "capacity_bytes", block_cache_->Capacity());
    } else {
        json::KV(out, "used_bytes", 0);
        out->append(",");
        json::KV(out, "capacity_bytes", 0);
    }
    out->append("}");

    out->append(",\"memtable\":");
    AppendMemTableJSON(out, mem.get());
    out->append(",\"immutable_memtable\":");
    if (imm) {
        AppendMemTableJSON(out, imm.get());
    } else {
        out->append("null");
    }

    // Per level: files, total size, and the compaction threshold so the UI
    // can show "how close is this level to compacting".
    out->append(",\"levels\":[");
    uint64_t max_bytes = options_.max_bytes_for_level_base;
    for (int level = 0; level < kNumLevels; level++) {
        if (level > 0) out->append(",");
        out->append("{\"level\":");
        out->append(std::to_string(level));
        out->append(",");
        json::KV(out, "total_bytes", current->LevelSize(level));
        out->append(",");
        if (level == 0) {
            json::KV(out, "compaction_trigger_files", options_.l0_compaction_trigger);
        } else {
            json::KV(out, "max_bytes", max_bytes);
            max_bytes *= 10;
        }
        out->append(",\"files\":[");
        const auto& files = current->Files(level);
        for (size_t i = 0; i < files.size(); i++) {
            if (i > 0) out->append(",");
            const FileMetaData& f = files[i];
            out->append("{");
            json::KV(out, "number", f.number);
            out->append(",");
            json::KV(out, "size_bytes", f.file_size);
            out->append(",");
            json::KVStr(out, "smallest_key", Slice(f.smallest.user_key));
            out->append(",");
            json::KVStr(out, "largest_key", Slice(f.largest.user_key));
            out->append(",");
            json::KV(out, "smallest_seq", f.smallest.seq);
            out->append(",");
            json::KV(out, "largest_seq", f.largest.seq);
            out->append("}");
        }
        out->append("]}");
    }
    out->append("]}");
}

Status DBImpl::DebugSSTableJSON(uint64_t file_number, std::string* out) {
    std::shared_ptr<SSTableReader> reader;
    Status s = table_cache_->GetReader(file_number, &reader);
    if (!s.ok()) {
        return Status::NotFound("SSTable " + std::to_string(file_number) + " not found");
    }
    return reader->DebugJSON(out, kMaxSSTEntries);
}

Status DBImpl::DebugWALJSON(uint64_t file_number, std::string* out) {
    std::string filename = dbname_ + "/" + std::to_string(file_number) + ".log";
    if (!std::filesystem::exists(filename)) {
        return Status::NotFound("WAL " + filename + " not found");
    }

    std::error_code ec;
    out->clear();
    out->append("{");
    json::KV(out, "file_number", file_number);
    out->append(",");
    json::KV(out, "file_size", std::filesystem::file_size(filename, ec));
    out->append(",\"records\":[");

    size_t count = 0;
    WAL::ReadAll(filename, [&](const Slice& record) {
        if (count < kMaxWALRecords) {
            if (count > 0) out->append(",");
            out->append("{");
            SequenceNumber seq;
            uint32_t op_count;
            Slice payload;
            if (DecodeBatchRecord(record, &seq, &op_count, &payload)) {
                json::KV(out, "seq", seq);
                out->append(",");
                json::KV(out, "op_count", op_count);
                out->append(",");
                json::KV(out, "bytes", record.size);
                out->append(",\"ops\":");
                AppendOpsJSON(out, payload);
            } else {
                json::KVBool(out, "unparseable", true);
                out->append(",");
                json::KV(out, "bytes", record.size);
            }
            out->append("}");
        }
        count++;
        return Status::OK();
    });

    out->append("],");
    json::KV(out, "record_count", count);
    out->append(",");
    json::KVBool(out, "records_capped", count > kMaxWALRecords);
    out->append("}");
    return Status::OK();
}

Status DBImpl::DebugManifestJSON(uint64_t file_number, std::string* out) {
    std::string filename = dbname_ + "/MANIFEST-" + std::to_string(file_number);
    if (!std::filesystem::exists(filename)) {
        return Status::NotFound(filename + " not found");
    }

    out->clear();
    out->append("{");
    json::KV(out, "file_number", file_number);
    out->append(",\"edits\":[");

    size_t count = 0;
    WAL::ReadAll(filename, [&](const Slice& record) {
        VersionEdit edit;
        if (count > 0) out->append(",");
        out->append("{");
        if (edit.DecodeFrom(record).ok()) {
            bool first = true;
            auto sep = [&]() { if (!first) out->append(","); first = false; };
            if (edit.log_number_) {
                sep(); json::KV(out, "log_number", *edit.log_number_);
            }
            if (edit.next_file_number_) {
                sep(); json::KV(out, "next_file_number", *edit.next_file_number_);
            }
            if (edit.last_sequence_) {
                sep(); json::KV(out, "last_sequence", *edit.last_sequence_);
            }
            sep();
            out->append("\"added_files\":[");
            for (size_t i = 0; i < edit.new_files_.size(); i++) {
                if (i > 0) out->append(",");
                const FileMetaData& f = edit.new_files_[i];
                out->append("{");
                json::KV(out, "level", f.level);
                out->append(",");
                json::KV(out, "number", f.number);
                out->append(",");
                json::KV(out, "size_bytes", f.file_size);
                out->append(",");
                AppendInternalKeyJSON(out, "smallest", f.smallest);
                out->append(",");
                AppendInternalKeyJSON(out, "largest", f.largest);
                out->append("}");
            }
            out->append("],\"deleted_files\":[");
            for (size_t i = 0; i < edit.deleted_files_.size(); i++) {
                if (i > 0) out->append(",");
                out->append("{");
                json::KV(out, "level", edit.deleted_files_[i].first);
                out->append(",");
                json::KV(out, "number", edit.deleted_files_[i].second);
                out->append("}");
            }
            out->append("]");
        } else {
            json::KVBool(out, "unparseable", true);
        }
        out->append("}");
        count++;
        return Status::OK();
    });

    out->append("],");
    json::KV(out, "edit_count", count);
    out->append("}");
    return Status::OK();
}

} // namespace lakestore
