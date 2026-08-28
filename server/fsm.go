package server

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"sync"

	"github.com/ashuthe1/LakeStore/engine"
)

// appliedKey is a reserved engine key holding the index of the last Raft
// entry applied to this engine. It is written in the SAME atomic batch as
// the entry's effects, so engine state and the marker can never diverge.
// The \x00 prefix keeps it out of user scans.
var appliedKey = []byte("\x00lakestore:applied_index")

// FSM is the replicated state machine: the storage engine plus apply
// bookkeeping. All mutations flow through Apply/Restore in Raft log order.
type FSM struct {
	mu          sync.Mutex
	dir         string
	opts        *engine.Options
	db          *engine.DB
	lastApplied uint64
}

func NewFSM(dir string, opts *engine.Options) (*FSM, error) {
	db, err := engine.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	f := &FSM{dir: dir, opts: opts, db: db}

	// Recover the applied-index high-water mark from the engine itself.
	val, found, err := db.Get(appliedKey)
	if err != nil {
		db.Close()
		return nil, err
	}
	if found && len(val) == 8 {
		f.lastApplied = binary.BigEndian.Uint64(val)
	}
	return f, nil
}

func (f *FSM) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.db.Close()
}

func (f *FSM) LastApplied() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastApplied
}

// Apply executes one committed command. Idempotent: entries at or below the
// applied high-water mark are skipped (Raft re-delivers entries after
// restarts, since its apply floor is the last snapshot).
func (f *FSM) Apply(index uint64, cmdBytes []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if index <= f.lastApplied {
		return nil // duplicate delivery
	}

	batch := engine.NewWriteBatch()
	defer batch.Destroy()

	// Zero-length commands are Raft leader no-ops: they only advance the
	// applied marker (linearizable reads wait on it). Non-empty payloads
	// hold one or more commands (concurrent proposals are batched into a
	// single entry) — all applied in one atomic engine batch.
	if len(cmdBytes) > 0 {
		cmds, err := DecodeCommands(cmdBytes)
		if err != nil {
			return fmt.Errorf("fsm: apply index %d: %w", index, err)
		}
		for _, cmd := range cmds {
			switch cmd.Op {
			case OpPut:
				batch.Put(cmd.Key, cmd.Value)
			case OpDelete:
				batch.Delete(cmd.Key)
			}
		}
	}
	var marker [8]byte
	binary.BigEndian.PutUint64(marker[:], index)
	batch.Put(appliedKey, marker[:])

	// No engine-level fsync: durability comes from the Raft log, which is
	// fsync'd before entries commit. The engine's own WAL replays on crash.
	if err := f.db.Write(batch, false); err != nil {
		return fmt.Errorf("fsm: apply index %d: %w", index, err)
	}
	f.lastApplied = index
	return nil
}

// Get reads directly from the local engine.
func (f *FSM) Get(key []byte) ([]byte, bool, error) {
	f.mu.Lock()
	db := f.db
	f.mu.Unlock()
	return db.Get(key)
}

// Scan returns up to limit user pairs starting at startKey. Reserved keys
// (\x00 prefix) are skipped.
func (f *FSM) Scan(startKey []byte, limit int) ([][2][]byte, error) {
	f.mu.Lock()
	db := f.db
	f.mu.Unlock()

	it, err := db.NewIterator()
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var out [][2][]byte
	if len(startKey) > 0 {
		it.Seek(startKey)
	} else {
		it.SeekToFirst()
	}
	for ; it.Valid() && len(out) < limit; it.Next() {
		k := it.Key()
		if len(k) > 0 && k[0] == 0 {
			continue // reserved namespace
		}
		out = append(out, [2][]byte{k, it.Value()})
	}
	return out, nil
}

// DebugEngineJSON returns the engine's internal-state JSON (inspector UI).
func (f *FSM) DebugEngineJSON() (string, error) {
	f.mu.Lock()
	db := f.db
	f.mu.Unlock()
	return db.DebugJSON()
}

// DebugSSTableJSON dumps one SSTable's entries + structure (inspector UI).
func (f *FSM) DebugSSTableJSON(fileNumber uint64) (string, error) {
	f.mu.Lock()
	db := f.db
	f.mu.Unlock()
	return db.DebugSSTableJSON(fileNumber)
}

// DebugWALJSON decodes one engine WAL file (inspector UI).
func (f *FSM) DebugWALJSON(fileNumber uint64) (string, error) {
	f.mu.Lock()
	db := f.db
	f.mu.Unlock()
	return db.DebugWALJSON(fileNumber)
}

// DebugManifestJSON decodes one MANIFEST file (inspector UI).
func (f *FSM) DebugManifestJSON(fileNumber uint64) (string, error) {
	f.mu.Lock()
	db := f.db
	f.mu.Unlock()
	return db.DebugManifestJSON(fileNumber)
}

// DecodeSnapshotForDebug summarizes a raft snapshot blob (inspector UI).
func DecodeSnapshotForDebug(data []byte, samplePairs int) (map[string]any, error) {
	var payload snapshotPayload
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&payload); err != nil {
		return nil, err
	}
	sample := make([]map[string]string, 0, samplePairs)
	for i, kv := range payload.Pairs {
		if i >= samplePairs {
			break
		}
		v := kv[1]
		if len(v) > 128 {
			v = v[:128]
		}
		sample = append(sample, map[string]string{"key": string(kv[0]), "value": string(v)})
	}
	return map[string]any{
		"applied_index": payload.AppliedIndex,
		"pair_count":    len(payload.Pairs),
		"sample_pairs":  sample,
	}, nil
}

// --- Snapshots ---
// Snapshot format (gob): SnapshotPayload with the applied index and every
// user + reserved pair. Fine for the scale this project targets; a
// production system would stream chunks instead.

type snapshotPayload struct {
	AppliedIndex uint64
	Pairs        [][2][]byte
}

// SnapshotBytes serializes the full state through the engine's stable
// snapshot iterator.
func (f *FSM) SnapshotBytes() ([]byte, uint64, error) {
	f.mu.Lock()
	db := f.db
	applied := f.lastApplied
	f.mu.Unlock()

	it, err := db.NewIterator()
	if err != nil {
		return nil, 0, err
	}
	defer it.Close()

	payload := snapshotPayload{AppliedIndex: applied}
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := it.Key()
		if bytes.Equal(k, appliedKey) {
			continue // re-written on restore with the snapshot's own index
		}
		payload.Pairs = append(payload.Pairs, [2][]byte{k, it.Value()})
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&payload); err != nil {
		return nil, 0, err
	}
	return buf.Bytes(), applied, nil
}

// Restore replaces the entire engine state with the snapshot: the engine
// directory is wiped and rebuilt, so keys absent from the snapshot vanish.
func (f *FSM) Restore(snapshotIndex uint64, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if snapshotIndex <= f.lastApplied {
		return nil // we already have newer state
	}

	var payload snapshotPayload
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&payload); err != nil {
		return fmt.Errorf("fsm: decode snapshot: %w", err)
	}

	f.db.Close()
	if err := os.RemoveAll(f.dir); err != nil {
		return fmt.Errorf("fsm: wipe for restore: %w", err)
	}
	db, err := engine.Open(f.dir, f.opts)
	if err != nil {
		return fmt.Errorf("fsm: reopen for restore: %w", err)
	}
	f.db = db

	batch := engine.NewWriteBatch()
	defer batch.Destroy()
	for _, kv := range payload.Pairs {
		batch.Put(kv[0], kv[1])
	}
	var marker [8]byte
	binary.BigEndian.PutUint64(marker[:], snapshotIndex)
	batch.Put(appliedKey, marker[:])

	if err := f.db.Write(batch, true); err != nil {
		return fmt.Errorf("fsm: restore write: %w", err)
	}
	f.lastApplied = snapshotIndex
	return nil
}
