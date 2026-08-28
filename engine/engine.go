// Package engine is the Go binding for the LakeStore C++ LSM-tree storage
// engine, via the C API in storage-engine/src/c_api. Build the static
// library first (make -C storage-engine, or the root Makefile's `engine`
// target); cgo links it in.
package engine

/*
#cgo CFLAGS: -I${SRCDIR}/../storage-engine/src
#cgo LDFLAGS: -L${SRCDIR}/../storage-engine/build -llakestore_lib
#cgo darwin LDFLAGS: -lc++
#cgo linux LDFLAGS: -lstdc++ -lpthread

#include <stdlib.h>
#include "c_api/c_api.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"
)

// ErrClosed is returned for operations on a closed DB.
var ErrClosed = errors.New("engine: database is closed")

// DB is a handle to an open LakeStore database.
type DB struct {
	ptr *C.lakestore_t
}

// Options configures a database at open time. Zero values keep the engine's
// defaults.
type Options struct {
	WriteBufferSize int // memtable size before flush, bytes
	BlockCacheSize  int // shared block cache, bytes
	BlockSize       int // SSTable block size, bytes
	BloomBitsPerKey int
}

func toErr(cerr *C.char) error {
	if cerr == nil {
		return nil
	}
	defer C.lakestore_free(unsafe.Pointer(cerr))
	return errors.New(C.GoString(cerr))
}

// Open opens (creating if needed) the database at path.
func Open(path string, opts *Options) (*DB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	var ptr *C.lakestore_t
	var cerr *C.char

	if opts == nil {
		cerr = C.lakestore_open(cpath, &ptr)
	} else {
		copts := C.lakestore_options_create()
		defer C.lakestore_options_destroy(copts)
		if opts.WriteBufferSize > 0 {
			C.lakestore_options_set_write_buffer_size(copts, C.size_t(opts.WriteBufferSize))
		}
		if opts.BlockCacheSize > 0 {
			C.lakestore_options_set_block_cache_size(copts, C.size_t(opts.BlockCacheSize))
		}
		if opts.BlockSize > 0 {
			C.lakestore_options_set_block_size(copts, C.size_t(opts.BlockSize))
		}
		if opts.BloomBitsPerKey > 0 {
			C.lakestore_options_set_bloom_bits_per_key(copts, C.int(opts.BloomBitsPerKey))
		}
		cerr = C.lakestore_open_with_options(cpath, copts, &ptr)
	}

	if err := toErr(cerr); err != nil {
		return nil, fmt.Errorf("engine: open %s: %w", path, err)
	}
	return &DB{ptr: ptr}, nil
}

// Close shuts the database down. Safe to call once; the handle is unusable
// afterwards.
func (db *DB) Close() {
	if db.ptr != nil {
		C.lakestore_close(db.ptr)
		db.ptr = nil
	}
}

// Flush blocks until all memtables reach disk and compactions settle.
func (db *DB) Flush() error {
	if db.ptr == nil {
		return ErrClosed
	}
	return toErr(C.lakestore_flush(db.ptr))
}

func bytePtr(b []byte) *C.char {
	if len(b) == 0 {
		// C API tolerates any non-nil pointer with length 0.
		return (*C.char)(unsafe.Pointer(&placeholder))
	}
	return (*C.char)(unsafe.Pointer(&b[0]))
}

var placeholder byte

// Put writes a key-value pair. sync forces an fsync of the WAL.
func (db *DB) Put(key, value []byte, sync bool) error {
	if db.ptr == nil {
		return ErrClosed
	}
	syncFlag := C.int(0)
	if sync {
		syncFlag = 1
	}
	err := toErr(C.lakestore_put(db.ptr, bytePtr(key), C.size_t(len(key)),
		bytePtr(value), C.size_t(len(value)), syncFlag))
	runtime.KeepAlive(key)
	runtime.KeepAlive(value)
	return err
}

// Get reads a key. Returns (nil, false, nil) when the key does not exist.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	if db.ptr == nil {
		return nil, false, ErrClosed
	}
	var cval *C.char
	var clen C.size_t
	cerr := C.lakestore_get(db.ptr, bytePtr(key), C.size_t(len(key)), &cval, &clen)
	runtime.KeepAlive(key)
	if err := toErr(cerr); err != nil {
		return nil, false, err
	}
	if cval == nil {
		return nil, false, nil
	}
	defer C.lakestore_free(unsafe.Pointer(cval))
	return C.GoBytes(unsafe.Pointer(cval), C.int(clen)), true, nil
}

// Delete removes a key. Deleting a missing key is a no-op.
func (db *DB) Delete(key []byte, sync bool) error {
	if db.ptr == nil {
		return ErrClosed
	}
	syncFlag := C.int(0)
	if sync {
		syncFlag = 1
	}
	err := toErr(C.lakestore_delete(db.ptr, bytePtr(key), C.size_t(len(key)), syncFlag))
	runtime.KeepAlive(key)
	return err
}

// WriteBatch accumulates operations applied atomically by Write.
type WriteBatch struct {
	ptr *C.lakestore_writebatch_t
}

func NewWriteBatch() *WriteBatch {
	return &WriteBatch{ptr: C.lakestore_writebatch_create()}
}

func (b *WriteBatch) Destroy() {
	if b.ptr != nil {
		C.lakestore_writebatch_destroy(b.ptr)
		b.ptr = nil
	}
}

func (b *WriteBatch) Put(key, value []byte) {
	C.lakestore_writebatch_put(b.ptr, bytePtr(key), C.size_t(len(key)),
		bytePtr(value), C.size_t(len(value)))
	runtime.KeepAlive(key)
	runtime.KeepAlive(value)
}

func (b *WriteBatch) Delete(key []byte) {
	C.lakestore_writebatch_delete(b.ptr, bytePtr(key), C.size_t(len(key)))
	runtime.KeepAlive(key)
}

// Write applies the batch atomically: one WAL record, one sequence block.
func (db *DB) Write(b *WriteBatch, sync bool) error {
	if db.ptr == nil {
		return ErrClosed
	}
	syncFlag := C.int(0)
	if sync {
		syncFlag = 1
	}
	return toErr(C.lakestore_write(db.ptr, b.ptr, syncFlag))
}

// Iterator scans user keys in order at a stable snapshot, with old versions
// and tombstones collapsed.
type Iterator struct {
	ptr *C.lakestore_iterator_t
}

// NewIterator creates a snapshot iterator. Must be Closed.
func (db *DB) NewIterator() (*Iterator, error) {
	if db.ptr == nil {
		return nil, ErrClosed
	}
	return &Iterator{ptr: C.lakestore_iter_create(db.ptr)}, nil
}

func (it *Iterator) Close() {
	if it.ptr != nil {
		C.lakestore_iter_destroy(it.ptr)
		it.ptr = nil
	}
}

func (it *Iterator) Valid() bool { return C.lakestore_iter_valid(it.ptr) != 0 }

func (it *Iterator) SeekToFirst() { C.lakestore_iter_seek_to_first(it.ptr) }

func (it *Iterator) Seek(key []byte) {
	C.lakestore_iter_seek(it.ptr, bytePtr(key), C.size_t(len(key)))
	runtime.KeepAlive(key)
}

func (it *Iterator) Next() { C.lakestore_iter_next(it.ptr) }

// DebugJSON returns a JSON document describing the engine's internal state
// (memtables, levels, files, caches). Read-only; safe under load.
func (db *DB) DebugJSON() (string, error) {
	if db.ptr == nil {
		return "", ErrClosed
	}
	cjson := C.lakestore_debug_json(db.ptr)
	defer C.lakestore_free(unsafe.Pointer(cjson))
	return C.GoString(cjson), nil
}

// DebugSSTableJSON lists the decoded entries + physical structure (blocks,
// index, bloom filter statistics) of one SSTable as JSON.
func (db *DB) DebugSSTableJSON(fileNumber uint64) (string, error) {
	if db.ptr == nil {
		return "", ErrClosed
	}
	var cjson *C.char
	cerr := C.lakestore_debug_sst_json(db.ptr, C.uint64_t(fileNumber), &cjson)
	if err := toErr(cerr); err != nil {
		return "", err
	}
	defer C.lakestore_free(unsafe.Pointer(cjson))
	return C.GoString(cjson), nil
}

// DebugWALJSON decodes the records of one WAL file (N.log) as JSON.
func (db *DB) DebugWALJSON(fileNumber uint64) (string, error) {
	if db.ptr == nil {
		return "", ErrClosed
	}
	var cjson *C.char
	cerr := C.lakestore_debug_wal_json(db.ptr, C.uint64_t(fileNumber), &cjson)
	if err := toErr(cerr); err != nil {
		return "", err
	}
	defer C.lakestore_free(unsafe.Pointer(cjson))
	return C.GoString(cjson), nil
}

// DebugManifestJSON decodes the VersionEdits of one MANIFEST-N file as JSON.
func (db *DB) DebugManifestJSON(fileNumber uint64) (string, error) {
	if db.ptr == nil {
		return "", ErrClosed
	}
	var cjson *C.char
	cerr := C.lakestore_debug_manifest_json(db.ptr, C.uint64_t(fileNumber), &cjson)
	if err := toErr(cerr); err != nil {
		return "", err
	}
	defer C.lakestore_free(unsafe.Pointer(cjson))
	return C.GoString(cjson), nil
}

// Key returns a copy of the current key.
func (it *Iterator) Key() []byte {
	var clen C.size_t
	cptr := C.lakestore_iter_key(it.ptr, &clen)
	return C.GoBytes(unsafe.Pointer(cptr), C.int(clen))
}

// Value returns a copy of the current value.
func (it *Iterator) Value() []byte {
	var clen C.size_t
	cptr := C.lakestore_iter_value(it.ptr, &clen)
	return C.GoBytes(unsafe.Pointer(cptr), C.int(clen))
}
