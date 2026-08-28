package server

// The inspector: a read-only debug HTTP server embedded in each node. It
// exposes the node's internal state — Raft log with decoded commands, engine
// memtables/levels/SSTables, and the raw files on disk — plus a small
// embedded web UI to browse it all. Everything is fetched on demand and
// read-only: it uses the same snapshot pinning as normal reads and never
// touches the write path or the block cache.

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
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
