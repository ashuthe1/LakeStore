package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// Meta is the small, frequently-rewritten part of a node's durable state.
type Meta struct {
	Term          uint64
	VotedFor      uint64
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// Storage persists a node's Raft state. All mutating calls MUST be durable
// before returning: Raft's correctness depends on persisted state surviving
// a crash that happens right after an RPC reply.
//
// The log is stored separately from the metadata so the hot path — appending
// entries — is an O(append) fsync'd write instead of a full-state rewrite.
type Storage interface {
	// SaveMeta durably records term/vote/snapshot markers (small, atomic).
	SaveMeta(meta Meta) error
	// AppendLog durably appends entries extending the stored log.
	AppendLog(entries []Entry) error
	// RewriteLog atomically replaces the entire stored log. Rare: conflict
	// truncation and snapshot compaction.
	RewriteLog(entries []Entry) error

	SaveSnapshot(snapshot []byte) error
	ReadSnapshot() ([]byte, error)

	// Load returns the persisted meta and log at startup.
	Load() (Meta, []Entry, error)
}

// --- FileStorage ---

// FileStorage layout in dir:
//
//	meta.bin      gob(Meta), replaced atomically (tmp+rename)
//	log.bin       append-only records: [4B len][4B crc32c][gob(Entry)]
//	snapshot.bin  opaque snapshot bytes, replaced atomically
//
// Durability: files are fsync'd with plain fsync(2). On macOS that flushes
// to the drive, not through its power-loss cache barrier; set
// LAKESTORE_FULLFSYNC=1 to use F_FULLFSYNC (much slower, strongest
// guarantee) — the same trade-off SQLite and friends expose.
type FileStorage struct {
	mu      sync.Mutex
	dir     string
	logFile *os.File
}

var useFullFsync = os.Getenv("LAKESTORE_FULLFSYNC") == "1"

func fsyncFile(f *os.File) error {
	if useFullFsync {
		return f.Sync() // Go's Sync uses F_FULLFSYNC on darwin
	}
	return syscall.Fsync(int(f.Fd()))
}

func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("raft storage: %w", err)
	}
	logFile, err := os.OpenFile(filepath.Join(dir, "log.bin"),
		os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("raft storage: %w", err)
	}
	return &FileStorage{dir: dir, logFile: logFile}, nil
}

func (fs *FileStorage) metaPath() string     { return filepath.Join(fs.dir, "meta.bin") }
func (fs *FileStorage) logPath() string      { return filepath.Join(fs.dir, "log.bin") }
func (fs *FileStorage) snapshotPath() string { return filepath.Join(fs.dir, "snapshot.bin") }

func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := fsyncFile(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readIfExists(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

func (fs *FileStorage) SaveMeta(meta Meta) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&meta); err != nil {
		return err
	}
	return atomicWrite(fs.metaPath(), buf.Bytes())
}

func encodeLogRecord(buf *bytes.Buffer, e *Entry) error {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(e); err != nil {
		return err
	}
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(payload.Len()))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload.Bytes(), crcTable))
	buf.Write(hdr[:])
	buf.Write(payload.Bytes())
	return nil
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func (fs *FileStorage) AppendLog(entries []Entry) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var buf bytes.Buffer
	for i := range entries {
		if err := encodeLogRecord(&buf, &entries[i]); err != nil {
			return err
		}
	}
	if _, err := fs.logFile.Write(buf.Bytes()); err != nil {
		return err
	}
	return fsyncFile(fs.logFile)
}

func (fs *FileStorage) RewriteLog(entries []Entry) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var buf bytes.Buffer
	for i := range entries {
		if err := encodeLogRecord(&buf, &entries[i]); err != nil {
			return err
		}
	}
	if err := atomicWrite(fs.logPath(), buf.Bytes()); err != nil {
		return err
	}
	// Reopen the append handle on the new file.
	fs.logFile.Close()
	logFile, err := os.OpenFile(fs.logPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	fs.logFile = logFile
	return nil
}

func (fs *FileStorage) SaveSnapshot(snapshot []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return atomicWrite(fs.snapshotPath(), snapshot)
}

func (fs *FileStorage) ReadSnapshot() ([]byte, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return readIfExists(fs.snapshotPath())
}

func (fs *FileStorage) Load() (Meta, []Entry, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var meta Meta
	metaBytes, err := readIfExists(fs.metaPath())
	if err != nil {
		return meta, nil, err
	}
	if len(metaBytes) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(metaBytes)).Decode(&meta); err != nil {
			return meta, nil, fmt.Errorf("raft storage: decode meta: %w", err)
		}
	}

	logBytes, err := readIfExists(fs.logPath())
	if err != nil {
		return meta, nil, err
	}
	var entries []Entry
	r := bytes.NewReader(logBytes)
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // clean EOF or truncated tail — stop
		}
		length := binary.LittleEndian.Uint32(hdr[0:4])
		expectCRC := binary.LittleEndian.Uint32(hdr[4:8])
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			break // torn tail from a crash mid-append
		}
		if crc32.Checksum(payload, crcTable) != expectCRC {
			break // corrupt tail
		}
		var e Entry
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&e); err != nil {
			break
		}
		entries = append(entries, e)
	}
	return meta, entries, nil
}

// --- MemoryStorage (tests) ---

// MemoryStorage is an in-memory Storage for tests. It survives simulated
// restarts by handing the same instance to a fresh Raft.
type MemoryStorage struct {
	mu       sync.Mutex
	meta     Meta
	log      []Entry
	snapshot []byte
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (ms *MemoryStorage) SaveMeta(meta Meta) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.meta = meta
	return nil
}

func (ms *MemoryStorage) AppendLog(entries []Entry) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.log = append(ms.log, entries...)
	return nil
}

func (ms *MemoryStorage) RewriteLog(entries []Entry) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.log = append([]Entry(nil), entries...)
	return nil
}

func (ms *MemoryStorage) SaveSnapshot(snapshot []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.snapshot = append([]byte(nil), snapshot...)
	return nil
}

func (ms *MemoryStorage) ReadSnapshot() ([]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return append([]byte(nil), ms.snapshot...), nil
}

func (ms *MemoryStorage) Load() (Meta, []Entry, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.meta, append([]Entry(nil), ms.log...), nil
}
