// src/c_api/c_api.cc
#include "c_api.h"
#include "../db/db_impl.h"
#include "../options.h"
#include <string.h>
#include <stdlib.h>

using namespace lakestore;

// The struct that wraps our C++ pointer
struct lakestore_t {
    DBImpl* rep;
};

// Helper to pass C++ strings safely to Go
static char* CopyString(const std::string& str) {
    char* result = (char*)malloc(str.size() + 1);
    memcpy(result, str.data(), str.size());
    result[str.size()] = '\0';
    return result;
}

char* lakestore_open(const char* path, lakestore_t** db) {
    *db = nullptr;
    DBImpl* db_impl = nullptr;
    Options opts; // We can expand this later to pass options from Go
    
    Status s = DBImpl::Open(opts, path, &db_impl);
    if (!s.ok()) {
        return CopyString(s.ToString());
    }
    
    *db = new lakestore_t;
    (*db)->rep = db_impl;
    return nullptr;
}

void lakestore_close(lakestore_t* db) {
    if (db) {
        delete db->rep;
        delete db;
    }
}

char* lakestore_put(lakestore_t* db, const char* key, size_t key_len, const char* val, size_t val_len) {
    WriteOptions wopts;
    Status s = db->rep->Put(wopts, Slice(key, key_len), Slice(val, val_len));
    if (!s.ok()) return CopyString(s.ToString());
    return nullptr;
}

char* lakestore_get(lakestore_t* db, const char* key, size_t key_len, char** val, size_t* val_len) {
    ReadOptions ropts;
    std::string tmp_val;
    Status s = db->rep->Get(ropts, Slice(key, key_len), &tmp_val);
    
    if (!s.ok()) {
        *val = nullptr;
        *val_len = 0;
        if (s.IsNotFound()) return nullptr; // Not an error, just missing
        return CopyString(s.ToString());
    }
    
    *val = (char*)malloc(tmp_val.size());
    memcpy(*val, tmp_val.data(), tmp_val.size());
    *val_len = tmp_val.size();
    return nullptr;
}

char* lakestore_delete(lakestore_t* db, const char* key, size_t key_len) {
    WriteOptions wopts;
    Status s = db->rep->Delete(wopts, Slice(key, key_len));
    if (!s.ok()) return CopyString(s.ToString());
    return nullptr;
}

void lakestore_free(void* ptr) {
    if (ptr) {
        free(ptr);
    }
}