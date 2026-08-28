// src/c_api/c_api.cc
#include "c_api.h"
#include "../db/db_impl.h"
#include "../options.h"
#include "../util/coding.h"
#include <string.h>
#include <stdlib.h>

using namespace lakestore;

// The structs that wrap our C++ objects
struct lakestore_t {
    DBImpl* rep;
};
struct lakestore_options_t {
    Options rep;
};
struct lakestore_writebatch_t {
    WriteBatch rep;
};
struct lakestore_iterator_t {
    Iterator* rep;
};

// Helper to pass C++ strings safely to Go
static char* CopyString(const std::string& str) {
    char* result = (char*)malloc(str.size() + 1);
    memcpy(result, str.data(), str.size());
    result[str.size()] = '\0';
    return result;
}

// --- Options ---

lakestore_options_t* lakestore_options_create(void) {
    return new lakestore_options_t;
}

void lakestore_options_destroy(lakestore_options_t* opts) {
    delete opts;
}

void lakestore_options_set_write_buffer_size(lakestore_options_t* opts, size_t bytes) {
    opts->rep.write_buffer_size = bytes;
}

void lakestore_options_set_block_cache_size(lakestore_options_t* opts, size_t bytes) {
    opts->rep.block_cache_size = bytes;
}

void lakestore_options_set_block_size(lakestore_options_t* opts, size_t bytes) {
    opts->rep.block_size = bytes;
}

void lakestore_options_set_bloom_bits_per_key(lakestore_options_t* opts, int bits) {
    opts->rep.bloom_bits_per_key = bits;
}

// --- Database lifecycle ---

static char* OpenInternal(const char* path, const Options& opts, lakestore_t** db) {
    *db = nullptr;
    DBImpl* db_impl = nullptr;

    Status s = DBImpl::Open(opts, path, &db_impl);
    if (!s.ok()) {
        return CopyString(s.ToString());
    }

    *db = new lakestore_t;
    (*db)->rep = db_impl;
    return nullptr;
}

char* lakestore_open(const char* path, lakestore_t** db) {
    Options opts;
    return OpenInternal(path, opts, db);
}

char* lakestore_open_with_options(const char* path, const lakestore_options_t* opts,
                                  lakestore_t** db) {
    return OpenInternal(path, opts->rep, db);
}

void lakestore_close(lakestore_t* db) {
    if (db) {
        delete db->rep;
        delete db;
    }
}

char* lakestore_flush(lakestore_t* db) {
    Status s = db->rep->FlushAll();
    if (!s.ok()) return CopyString(s.ToString());
    return nullptr;
}

// --- Point operations ---

char* lakestore_put(lakestore_t* db, const char* key, size_t key_len,
                    const char* val, size_t val_len, int sync) {
    WriteOptions wopts;
    wopts.sync = (sync != 0);
    Status s = db->rep->Put(wopts, Slice(key, key_len), Slice(val, val_len));
    if (!s.ok()) return CopyString(s.ToString());
    return nullptr;
}

char* lakestore_get(lakestore_t* db, const char* key, size_t key_len,
                    char** val, size_t* val_len) {
    ReadOptions ropts;
    std::string tmp_val;
    Status s = db->rep->Get(ropts, Slice(key, key_len), &tmp_val);

    if (!s.ok()) {
        *val = nullptr;
        *val_len = 0;
        if (s.IsNotFound()) return nullptr; // Not an error, just missing
        return CopyString(s.ToString());
    }

    *val = (char*)malloc(tmp_val.size() > 0 ? tmp_val.size() : 1);
    memcpy(*val, tmp_val.data(), tmp_val.size());
    *val_len = tmp_val.size();
    return nullptr;
}

char* lakestore_delete(lakestore_t* db, const char* key, size_t key_len, int sync) {
    WriteOptions wopts;
    wopts.sync = (sync != 0);
    Status s = db->rep->Delete(wopts, Slice(key, key_len));
    if (!s.ok()) return CopyString(s.ToString());
    return nullptr;
}

// --- Atomic write batches ---

lakestore_writebatch_t* lakestore_writebatch_create(void) {
    return new lakestore_writebatch_t;
}

void lakestore_writebatch_destroy(lakestore_writebatch_t* batch) {
    delete batch;
}

void lakestore_writebatch_put(lakestore_writebatch_t* batch,
                              const char* key, size_t key_len,
                              const char* val, size_t val_len) {
    std::string& payload = batch->rep.payload;
    payload.push_back(static_cast<char>(kTypeValue));
    PutVarint32(&payload, key_len);
    payload.append(key, key_len);
    PutVarint32(&payload, val_len);
    payload.append(val, val_len);
}

void lakestore_writebatch_delete(lakestore_writebatch_t* batch,
                                 const char* key, size_t key_len) {
    std::string& payload = batch->rep.payload;
    payload.push_back(static_cast<char>(kTypeDeletion));
    PutVarint32(&payload, key_len);
    payload.append(key, key_len);
}

char* lakestore_write(lakestore_t* db, lakestore_writebatch_t* batch, int sync) {
    WriteOptions wopts;
    wopts.sync = (sync != 0);
    Status s = db->rep->Write(wopts, &batch->rep);
    if (!s.ok()) return CopyString(s.ToString());
    return nullptr;
}

// --- Iteration ---

lakestore_iterator_t* lakestore_iter_create(lakestore_t* db) {
    auto* it = new lakestore_iterator_t;
    it->rep = db->rep->NewIterator();
    return it;
}

void lakestore_iter_destroy(lakestore_iterator_t* it) {
    if (it) {
        delete it->rep;
        delete it;
    }
}

int lakestore_iter_valid(const lakestore_iterator_t* it) {
    return it->rep->Valid() ? 1 : 0;
}

void lakestore_iter_seek_to_first(lakestore_iterator_t* it) {
    it->rep->SeekToFirst();
}

void lakestore_iter_seek(lakestore_iterator_t* it, const char* key, size_t key_len) {
    it->rep->Seek(Slice(key, key_len));
}

void lakestore_iter_next(lakestore_iterator_t* it) {
    it->rep->Next();
}

const char* lakestore_iter_key(const lakestore_iterator_t* it, size_t* len) {
    Slice k = it->rep->key();
    *len = k.size;
    return k.data;
}

const char* lakestore_iter_value(const lakestore_iterator_t* it, size_t* len) {
    Slice v = it->rep->value();
    *len = v.size;
    return v.data;
}

char* lakestore_debug_json(lakestore_t* db) {
    std::string json;
    db->rep->DebugJSON(&json);
    return CopyString(json);
}

char* lakestore_debug_sst_json(lakestore_t* db, uint64_t file_number, char** json) {
    *json = nullptr;
    std::string out;
    Status s = db->rep->DebugSSTableJSON(file_number, &out);
    if (!s.ok()) return CopyString(s.ToString());
    *json = CopyString(out);
    return nullptr;
}

char* lakestore_debug_wal_json(lakestore_t* db, uint64_t file_number, char** json) {
    *json = nullptr;
    std::string out;
    Status s = db->rep->DebugWALJSON(file_number, &out);
    if (!s.ok()) return CopyString(s.ToString());
    *json = CopyString(out);
    return nullptr;
}

char* lakestore_debug_manifest_json(lakestore_t* db, uint64_t file_number, char** json) {
    *json = nullptr;
    std::string out;
    Status s = db->rep->DebugManifestJSON(file_number, &out);
    if (!s.ok()) return CopyString(s.ToString());
    *json = CopyString(out);
    return nullptr;
}

void lakestore_free(void* ptr) {
    if (ptr) {
        free(ptr);
    }
}
