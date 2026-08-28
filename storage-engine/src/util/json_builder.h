#pragma once
// Tiny JSON-building helpers shared by the debug/introspection code
// (db_debug.cc, sstable_reader.cc). Not a general-purpose JSON library —
// just enough to emit valid documents with safely escaped binary strings.
#include "../types.h"
#include <cstdio>
#include <string>

namespace lakestore {
namespace json {

inline void EscapeTo(std::string* out, const Slice& s) {
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

inline void KV(std::string* out, const char* name, uint64_t v) {
    out->append("\"");
    out->append(name);
    out->append("\":");
    out->append(std::to_string(v));
}

inline void KVDouble(std::string* out, const char* name, double v) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.6f", v);
    out->append("\"");
    out->append(name);
    out->append("\":");
    out->append(buf);
}

inline void KVStr(std::string* out, const char* name, const Slice& v) {
    out->append("\"");
    out->append(name);
    out->append("\":\"");
    EscapeTo(out, v);
    out->append("\"");
}

inline void KVBool(std::string* out, const char* name, bool v) {
    out->append("\"");
    out->append(name);
    out->append("\":");
    out->append(v ? "true" : "false");
}

} // namespace json
} // namespace lakestore
