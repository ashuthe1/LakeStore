package server

// The inspector: a read-only debug HTTP server embedded in each node. It
// exposes the node's internal state — Raft log with decoded commands, engine
// memtables/levels/SSTables, and the raw files on disk — plus a small
// embedded web UI to browse it all. Everything is fetched on demand and
// read-only: it uses the same snapshot pinning as normal reads and never
// touches the write path or the block cache.

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ashuthe1/LakeStore/raft"
)

//go:embed ui/index.html
var inspectorHTML []byte

const debugMaxLogEntries = 200
const debugValuePreview = 128

type debugOp struct {
	Op             string `json:"op"`
	Key            string `json:"key"`
	Value          string `json:"value,omitempty"`
	ValueLen       int    `json:"value_len,omitempty"`
	ValueTruncated bool   `json:"value_truncated,omitempty"`
}

type debugLogEntry struct {
	Index uint64    `json:"index"`
	Term  uint64    `json:"term"`
	Bytes int       `json:"bytes"`
	Noop  bool      `json:"noop"`
	Ops   []debugOp `json:"ops,omitempty"`
}

type debugFile struct {
	Dir  string `json:"dir"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type debugRaft struct {
	// embeds raft.DebugInfo fields via json marshalling of the struct,
	// with entries replaced by their decoded form.
	Info          any             `json:"info"`
	RecentEntries []debugLogEntry `json:"recent_entries"`
}

func previewString(b []byte) (string, bool) {
	if len(b) > debugValuePreview {
		return string(b[:debugValuePreview]), true
	}
	return string(b), false
}

func decodeEntryOps(data []byte) []debugOp {
	cmds, err := DecodeCommands(data)
	if err != nil {
		return []debugOp{{Op: "UNPARSEABLE", Key: fmt.Sprintf("(%v)", err)}}
	}
	ops := make([]debugOp, 0, len(cmds))
	for _, c := range cmds {
		op := debugOp{Key: string(c.Key)}
		switch c.Op {
		case OpPut:
			op.Op = "PUT"
			op.Value, op.ValueTruncated = previewString(c.Value)
			op.ValueLen = len(c.Value)
		case OpDelete:
			op.Op = "DEL"
		default:
			op.Op = fmt.Sprintf("OP(%d)", c.Op)
		}
		ops = append(ops, op)
	}
	return ops
}

func listDir(root, label string) []debugFile {
	var out []debugFile
	entries, err := os.ReadDir(root)
	if err != nil {
		return out
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, debugFile{Dir: label, Name: e.Name(), Size: info.Size()})
	}
	return out
}

// startDebugServer wires the handlers and serves cfg.DebugAddr.
func (n *Node) startDebugServer() error {
	mux := http.NewServeMux()

	cors := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			// The UI on one node fetches state from all nodes (localhost
			// debug ports), so allow cross-origin reads.
			w.Header().Set("Access-Control-Allow-Origin", "*")
			h(w, r)
		}
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(inspectorHTML)
	})

	mux.HandleFunc("/api/state", cors(func(w http.ResponseWriter, r *http.Request) {
		info := n.rf.Debug(debugMaxLogEntries)
		decoded := make([]debugLogEntry, 0, len(info.RecentEntries))
		for _, e := range info.RecentEntries {
			de := debugLogEntry{Index: e.Index, Term: e.Term, Bytes: len(e.Data)}
			if len(e.Data) == 0 {
				de.Noop = true
			} else {
				de.Ops = decodeEntryOps(e.Data)
			}
			decoded = append(decoded, de)
		}
		info.RecentEntries = nil // replaced by the decoded form

		engineJSON, err := n.fsm.DebugEngineJSON()
		if err != nil {
			engineJSON = fmt.Sprintf(`{"error":%q}`, err.Error())
		}

		// Hint the UI where the other nodes' inspectors probably live
		// (scripts use client port + 1000 by convention).
		debugPeers := make(map[uint64]string, len(n.cfg.Peers))
		for id, addr := range n.cfg.Peers {
			if host, port, err := net.SplitHostPort(addr); err == nil {
				if p, err := strconv.Atoi(port); err == nil {
					debugPeers[id] = net.JoinHostPort(host, strconv.Itoa(p+1000))
				}
			}
		}

		resp := map[string]any{
			"node_id":            n.cfg.ID,
			"listen_addr":        n.cfg.ListenAddr,
			"peers":              n.cfg.Peers,
			"debug_peers_hint":   debugPeers,
			"snapshot_threshold": n.cfg.SnapshotThreshold,
			"raft":               info,
			"raft_log_decoded":   decoded,
			"engine":             json.RawMessage(engineJSON),
			"disk": append(
				listDir(filepath.Join(n.cfg.DataDir, "engine"), "engine"),
				listDir(filepath.Join(n.cfg.DataDir, "raft"), "raft")...),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))

	mux.HandleFunc("/api/sst", cors(func(w http.ResponseWriter, r *http.Request) {
		fileNum, err := strconv.ParseUint(r.URL.Query().Get("file"), 10, 64)
		if err != nil {
			http.Error(w, `{"error":"bad ?file= parameter"}`, http.StatusBadRequest)
			return
		}
		out, err := n.fsm.DebugSSTableJSON(fileNum)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(out))
	}))

	// Typed viewer for ANY file in the node's data directory: engine WALs,
	// SSTables, MANIFEST, CURRENT, and the raft log/meta/snapshot files.
	mux.HandleFunc("/api/file", cors(n.handleFileDump))

	// Single operations and a bulk load generator. These run through the
	// node's NORMAL propose/apply pipeline — exactly what a gRPC client
	// triggers — so what you observe is real behavior. They exist only on
	// this optional localhost debug listener.
	mux.HandleFunc("/api/op", cors(n.handleDebugOp))
	mux.HandleFunc("/api/load", cors(n.handleDebugLoad))

	listener, err := net.Listen("tcp", n.cfg.DebugAddr)
	if err != nil {
		return fmt.Errorf("debug listen %s: %w", n.cfg.DebugAddr, err)
	}
	n.debugListener = listener
	n.debugSrv = &http.Server{Handler: mux}
	go func() {
		if err := n.debugSrv.Serve(listener); err != nil && err != http.ErrServerClosed {
			n.logger.Printf("node %d: debug server: %v", n.cfg.ID, err)
		}
	}()
	n.logger.Printf("node %d: inspector UI on http://%s", n.cfg.ID, listener.Addr())
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// handleFileDump decodes one on-disk file by type. Everything is opened
// read-only; SSTable/WAL decoding happens inside the engine's debug paths.
func (n *Node) handleFileDump(w http.ResponseWriter, r *http.Request) {
	dir := r.URL.Query().Get("dir")
	name := r.URL.Query().Get("name")
	if strings.ContainsAny(name, "/\\") || name == "" || (dir != "engine" && dir != "raft") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad dir/name"})
		return
	}

	fail := func(err error) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	}
	rawJSON := func(kind, payload string, err error) {
		if err != nil {
			fail(err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"kind":%q,"data":%s}`, kind, payload)
	}

	switch {
	case dir == "engine" && strings.HasSuffix(name, ".ldb"):
		num, err := strconv.ParseUint(strings.TrimSuffix(name, ".ldb"), 10, 64)
		if err != nil {
			fail(err)
			return
		}
		out, err := n.fsm.DebugSSTableJSON(num)
		rawJSON("sstable", out, err)

	case dir == "engine" && strings.HasSuffix(name, ".log"):
		num, err := strconv.ParseUint(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			fail(err)
			return
		}
		out, err := n.fsm.DebugWALJSON(num)
		rawJSON("wal", out, err)

	case dir == "engine" && strings.HasPrefix(name, "MANIFEST-"):
		num, err := strconv.ParseUint(strings.TrimPrefix(name, "MANIFEST-"), 10, 64)
		if err != nil {
			fail(err)
			return
		}
		out, err := n.fsm.DebugManifestJSON(num)
		rawJSON("manifest", out, err)

	case dir == "engine" && name == "CURRENT":
		content, err := os.ReadFile(filepath.Join(n.cfg.DataDir, "engine", "CURRENT"))
		if err != nil {
			fail(err)
			return
		}
		writeJSON(w, 200, map[string]any{"kind": "current",
			"data": map[string]string{"content": strings.TrimSpace(string(content))}})

	case dir == "raft" && name == "meta.bin":
		meta, err := raft.ReadMetaFile(filepath.Join(n.cfg.DataDir, "raft", "meta.bin"))
		if err != nil {
			fail(err)
			return
		}
		writeJSON(w, 200, map[string]any{"kind": "raft_meta", "data": map[string]uint64{
			"term": meta.Term, "voted_for": meta.VotedFor,
			"snapshot_index": meta.SnapshotIndex, "snapshot_term": meta.SnapshotTerm,
		}})

	case dir == "raft" && name == "log.bin":
		entries, err := raft.ReadLogFile(filepath.Join(n.cfg.DataDir, "raft", "log.bin"))
		if err != nil {
			fail(err)
			return
		}
		const cap = 2000
		start := 0
		if len(entries) > cap {
			start = len(entries) - cap
		}
		decoded := make([]debugLogEntry, 0, len(entries)-start)
		for _, e := range entries[start:] {
			de := debugLogEntry{Index: e.Index, Term: e.Term, Bytes: len(e.Data)}
			if len(e.Data) == 0 {
				de.Noop = true
			} else {
				de.Ops = decodeEntryOps(e.Data)
			}
			decoded = append(decoded, de)
		}
		writeJSON(w, 200, map[string]any{"kind": "raft_log", "data": map[string]any{
			"entry_count": len(entries), "entries_capped": len(entries) > cap,
			"entries": decoded,
		}})

	case dir == "raft" && name == "snapshot.bin":
		data, err := os.ReadFile(filepath.Join(n.cfg.DataDir, "raft", "snapshot.bin"))
		if err != nil {
			fail(err)
			return
		}
		summary, err := DecodeSnapshotForDebug(data, 200)
		if err != nil {
			fail(err)
			return
		}
		summary["total_bytes"] = len(data)
		writeJSON(w, 200, map[string]any{"kind": "raft_snapshot", "data": summary})

	default:
		writeJSON(w, http.StatusBadRequest,
			map[string]string{"error": "no viewer for this file type: " + name})
	}
}

// leaderDebugHint maps the current leader to its likely inspector address.
func (n *Node) leaderDebugHint() string {
	id, addr := n.leaderHint()
	if id == 0 || addr == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(p+1000))
}

func (n *Node) handleDebugOp(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		Op    string `json:"op"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "need {op, key, value?}"})
		return
	}

	notLeader := func() {
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "not the leader", "leader_debug": n.leaderDebugHint()})
	}

	switch req.Op {
	case "put":
		err := n.propose(Command{Op: OpPut, Key: []byte(req.Key), Value: []byte(req.Value)})
		if errors.Is(err, errNotLeader) {
			notLeader()
			return
		}
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	case "del":
		err := n.propose(Command{Op: OpDelete, Key: []byte(req.Key)})
		if errors.Is(err, errNotLeader) {
			notLeader()
			return
		}
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	case "get":
		if err := n.linearizableRead(); err != nil {
			if errors.Is(err, errNotLeader) {
				notLeader()
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		val, found, err := n.fsm.Get([]byte(req.Key))
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "found": found, "value": string(val)})
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "op must be put/get/del"})
	}
}

// handleDebugLoad generates N random-key PUTs through the normal proposal
// pipeline so flushes and compactions can be watched happening.
func (n *Node) handleDebugLoad(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		N         int `json:"n"`
		ValueSize int `json:"value_size"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "need {n, value_size?}"})
		return
	}
	if req.N <= 0 || req.N > 200000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "n must be 1..200000"})
		return
	}
	if req.ValueSize <= 0 {
		req.ValueSize = 100
	}
	if req.ValueSize > 4096 {
		req.ValueSize = 4096
	}
	if _, isLeader := n.rf.GetState(); !isLeader {
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "not the leader", "leader_debug": n.leaderDebugHint()})
		return
	}

	value := bytes.Repeat([]byte("x"), req.ValueSize)
	seed := mrand.Uint64()
	start := time.Now()

	var wg sync.WaitGroup
	var failed atomic.Int64
	work := make(chan int, req.N)
	for i := 0; i < req.N; i++ {
		work <- i
	}
	close(work)
	for wkr := 0; wkr < 16; wkr++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				key := fmt.Sprintf("load_%08x_%06d", seed&0xFFFFFFFF, i)
				if err := n.propose(Command{Op: OpPut, Key: []byte(key), Value: value}); err != nil {
					failed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	writeJSON(w, 200, map[string]any{
		"ok":          true,
		"requested":   req.N,
		"failed":      failed.Load(),
		"duration_ms": elapsed.Milliseconds(),
		"ops_per_sec": int(float64(req.N-int(failed.Load())) / elapsed.Seconds()),
	})
}

func (n *Node) stopDebugServer() {
	if n.debugSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		n.debugSrv.Shutdown(ctx)
	}
}

// DebugAddr returns the inspector's actual address ("" if disabled).
func (n *Node) DebugAddr() string {
	if n.debugListener == nil {
		return ""
	}
	return n.debugListener.Addr().String()
}
