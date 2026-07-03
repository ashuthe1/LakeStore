// src/c_api/c_api.h
#ifndef LAKESTORE_C_API_H
#define LAKESTORE_C_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handle for the database
typedef struct lakestore_t lakestore_t;

// Opens the database. Returns NULL on success, or a malloc'd error string on failure.
char* lakestore_open(const char* path, lakestore_t** db);

// Closes and frees the database object.
void lakestore_close(lakestore_t* db);

// Writes a key-value pair. Returns NULL on success, or an error string.
char* lakestore_put(lakestore_t* db, const char* key, size_t key_len, const char* val, size_t val_len);

// Reads a value. If found, returns NULL and allocates *val. 
// If not found, returns NULL and *val is set to NULL.
// If actual error, returns error string.
char* lakestore_get(lakestore_t* db, const char* key, size_t key_len, char** val, size_t* val_len);

// Deletes a key. Returns NULL on success, or an error string.
char* lakestore_delete(lakestore_t* db, const char* key, size_t key_len);

// Frees memory allocated by lakestore_get (the value) or returned error strings.
void lakestore_free(void* ptr);

#ifdef __cplusplus
}
#endif

#endif // LAKESTORE_C_API_H