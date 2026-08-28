// src/c_api/c_api.h
//
// C bindings for the LakeStore storage engine, consumed by the Go layer
// through cgo. Error convention: functions returning char* give NULL on
// success and a malloc'd error string on failure (free with lakestore_free).
#ifndef LAKESTORE_C_API_H
#define LAKESTORE_C_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handles
typedef struct lakestore_t lakestore_t;
typedef struct lakestore_options_t lakestore_options_t;
typedef struct lakestore_writebatch_t lakestore_writebatch_t;
typedef struct lakestore_iterator_t lakestore_iterator_t;

// --- Options ---
lakestore_options_t* lakestore_options_create(void);
void lakestore_options_destroy(lakestore_options_t* opts);
void lakestore_options_set_write_buffer_size(lakestore_options_t* opts, size_t bytes);
void lakestore_options_set_block_cache_size(lakestore_options_t* opts, size_t bytes);
void lakestore_options_set_block_size(lakestore_options_t* opts, size_t bytes);
void lakestore_options_set_bloom_bits_per_key(lakestore_options_t* opts, int bits);

// --- Database lifecycle ---
// Opens the database. Returns NULL on success, or a malloc'd error string on failure.
char* lakestore_open(const char* path, lakestore_t** db);
char* lakestore_open_with_options(const char* path, const lakestore_options_t* opts,
                                  lakestore_t** db);

// Closes and frees the database object.
void lakestore_close(lakestore_t* db);

// Blocks until all memtables are flushed and compactions settle.
char* lakestore_flush(lakestore_t* db);

// --- Point operations ---
// Writes a key-value pair. sync != 0 forces an fsync of the WAL.
char* lakestore_put(lakestore_t* db, const char* key, size_t key_len,
                    const char* val, size_t val_len, int sync);

// Reads a value. If found, returns NULL and allocates *val.
// If not found, returns NULL and *val is set to NULL.
// If actual error, returns error string.
char* lakestore_get(lakestore_t* db, const char* key, size_t key_len,
                    char** val, size_t* val_len);

// Deletes a key. Returns NULL on success, or an error string.
char* lakestore_delete(lakestore_t* db, const char* key, size_t key_len, int sync);

// --- Atomic write batches ---
lakestore_writebatch_t* lakestore_writebatch_create(void);
void lakestore_writebatch_destroy(lakestore_writebatch_t* batch);
void lakestore_writebatch_put(lakestore_writebatch_t* batch,
                              const char* key, size_t key_len,
                              const char* val, size_t val_len);
void lakestore_writebatch_delete(lakestore_writebatch_t* batch,
                                 const char* key, size_t key_len);
// Applies the whole batch atomically (one WAL record, one sequence block).
char* lakestore_write(lakestore_t* db, lakestore_writebatch_t* batch, int sync);

// --- Iteration ---
// Snapshot iterator over user keys (tombstones and old versions collapsed).
lakestore_iterator_t* lakestore_iter_create(lakestore_t* db);
void lakestore_iter_destroy(lakestore_iterator_t* it);
int lakestore_iter_valid(const lakestore_iterator_t* it);
void lakestore_iter_seek_to_first(lakestore_iterator_t* it);
void lakestore_iter_seek(lakestore_iterator_t* it, const char* key, size_t key_len);
void lakestore_iter_next(lakestore_iterator_t* it);
// Returned pointers are valid until the next mutation of the iterator.
const char* lakestore_iter_key(const lakestore_iterator_t* it, size_t* len);
const char* lakestore_iter_value(const lakestore_iterator_t* it, size_t* len);

// Frees memory allocated by lakestore_get (the value) or returned error strings.
void lakestore_free(void* ptr);

#ifdef __cplusplus
}
#endif

#endif // LAKESTORE_C_API_H
