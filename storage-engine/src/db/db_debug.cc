// Read-only introspection for the inspector UI / debugging: serializes the
// engine's internal state (memtables, levels, files, caches) as JSON.
// Deliberately zero impact on the normal data path: it uses the same
// snapshot mechanisms as reads (shared_ptr pins), never takes the mutex for
// longer than a pointer copy, and SSTable dumps bypass the block cache.
#include "db_impl.h"
#include "db_iter.h"
#include <cstdio>
#include <filesystem>

namespace lakestore {

namespace {

constexpr size_t kMaxValuePreview = 128;  // bytes of a value shown per entry
constexpr size_t kMaxMemEntries = 2000;   // memtable entries listed
constexpr size_t kMaxSSTEntries = 5000;   // SSTable entries listed per dump

void JsonEscapeTo(std::string* out, const Slice& s) {
    for (size_t i = 0; i < s.size; i++) {
        unsigned char c = static_cast<unsigned char>(s.data[i]);
        switch (c) {
            case '"':  out->append("\\\""); break;
            case '\\': out->append("\\\\"); break;
            case '\n': out->append("\\n"); break;
            case '\r': out->append("\\r"); break;
            case '\t': out->append("\\t"); break;
            default:
                if (c < 0x20 || c >= 0x7f) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out->append(buf);
                } else {
                    out->push_back(static_cast<char>(c));
                }
        }
    }
}

void AppendKV(std::string* out, const char* name, uint64_t v) {
    out->append("\"");
    out->append(name);
    out->append("\":");
    out->append(std::to_string(v));
}

void AppendKVStr(std::string* out, const char* name, const Slice& v) {
    out->append("\"");
    out->append(name);
    out->append("\":\"");
    JsonEscapeTo(out, v);
    out->append("\"");
}

// One decoded internal entry: {"key":..,"seq":..,"type":"PUT|DEL","value":..}
void AppendEntry(std::string* out, const Slice& encoded_key, const Slice& value) {
    InternalKey ik = InternalKey::Decode(encoded_key);
    out->append("{");
    AppendKVStr(out, "key", Slice(ik.user_key));
    out->append(",");
    AppendKV(out, "seq", ik.seq);
    out->append(",\"type\":\"");
    out->append(ik.type == kTypeValue ? "PUT" : "DEL");
    out->append("\",");
    AppendKV(out, "value_len", value.size);
    out->append(",");
    Slice preview(value.data, value.size < kMaxValuePreview ? value.size : kMaxValuePreview);
    AppendKVStr(out, "value", preview);
    if (value.size > kMaxValuePreview) {
        out->append(",\"value_truncated\":true");
    }
    out->append("}");
}

void AppendMemTableJSON(std::string* out, const MemTable* mem) {
    out->append("{");
    AppendKV(out, "memory_bytes", mem->ApproximateMemoryUsage());
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
    AppendKV(out, "entry_count", count);
    out->append(",\"entries_capped\":");
    out->append(count > kMaxMemEntries ? "true" : "false");
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
    AppendKV(out, "last_sequence", last_seq);
    out->append(",");
    AppendKV(out, "active_wal_file", log_num);
    out->append(",");
    AppendKV(out, "imm_wal_file", imm_log_num);
    out->append(",");
    AppendKV(out, "write_buffer_size", options_.write_buffer_size);
    out->append(",");
    AppendKV(out, "l0_compaction_trigger", options_.l0_compaction_trigger);

    out->append(",\"block_cache\":{");
    if (block_cache_) {
        AppendKV(out, "used_bytes", block_cache_->TotalCharge());
        out->append(",");
        AppendKV(out, "capacity_bytes", block_cache_->Capacity());
    } else {
        AppendKV(out, "used_bytes", 0);
        out->append(",");
        AppendKV(out, "capacity_bytes", 0);
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

    out->append(",\"levels\":[");
    for (int level = 0; level < kNumLevels; level++) {
        if (level > 0) out->append(",");
        out->append("{\"level\":");
        out->append(std::to_string(level));
        out->append(",");
        AppendKV(out, "total_bytes", current->LevelSize(level));
        out->append(",\"files\":[");
        const auto& files = current->Files(level);
        for (size_t i = 0; i < files.size(); i++) {
            if (i > 0) out->append(",");
            const FileMetaData& f = files[i];
            out->append("{");
            AppendKV(out, "number", f.number);
            out->append(",");
            AppendKV(out, "size_bytes", f.file_size);
            out->append(",");
            AppendKVStr(out, "smallest_key", Slice(f.smallest.user_key));
            out->append(",");
            AppendKVStr(out, "largest_key", Slice(f.largest.user_key));
            out->append(",");
            AppendKV(out, "smallest_seq", f.smallest.seq);
            out->append(",");
            AppendKV(out, "largest_seq", f.largest.seq);
            out->append("}");
        }
        out->append("]}");
    }
    out->append("]}");
}

Status DBImpl::DebugSSTableJSON(uint64_t file_number, std::string* out) {
    ReadOptions r_opts;
    r_opts.fill_cache = false; // never pollute the cache from the inspector
    r_opts.block_cache = nullptr;

    std::unique_ptr<Iterator> it(table_cache_->NewIterator(r_opts, file_number));
    if (!it) {
        return Status::NotFound("SSTable " + std::to_string(file_number) + " not found");
    }

    out->clear();
    out->append("{");
    AppendKV(out, "file_number", file_number);
    out->append(",\"entries\":[");
    size_t count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        if (count < kMaxSSTEntries) {
            if (count > 0) out->append(",");
            AppendEntry(out, it->key(), it->value());
        }
        count++;
    }
    out->append("],");
    AppendKV(out, "entry_count", count);
    out->append(",\"entries_capped\":");
    out->append(count > kMaxSSTEntries ? "true" : "false");
    out->append("}");
    return it->status();
}

} // namespace lakestore
