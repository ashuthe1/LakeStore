package server

// Integration tests: real 3-node clusters over gRPC on localhost, with the
// C++ engine underneath. Cover replication, leader failover, node restart,
// consistency after recovery, and raft log compaction.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ashuthe1/LakeStore/client"
	"github.com/ashuthe1/LakeStore/engine"
)

// freePorts grabs n distinct localhost ports.
func freePorts(t *testing.T, n int) []string {
	t.Helper()
	addrs := make([]string, n)
	listeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = l
		addrs[i] = l.Addr().String()
	}
	for _, l := range listeners {
		l.Close()
	}
	return addrs
}

type testCluster struct {
	t     *testing.T
	dir   string
	peers map[uint64]string
	nodes map[uint64]*Node
	snapshotThreshold uint64
}

func startCluster(t *testing.T, n int) *testCluster {
	return startClusterOpts(t, n, 8192)
}

func startClusterOpts(t *testing.T, n int, snapThreshold uint64) *testCluster {
	t.Helper()
	addrs := freePorts(t, n)
	peers := make(map[uint64]string)
	for i := 0; i < n; i++ {
		peers[uint64(i+1)] = addrs[i]
	}
	c := &testCluster{
		t:     t,
		dir:   t.TempDir(),
		peers: peers,
		nodes: make(map[uint64]*Node),
		snapshotThreshold: snapThreshold,
	}
	for id := range peers {
		c.startNode(id)
	}
	t.Cleanup(func() {
		for _, node := range c.nodes {
			node.Close()
		}
	})
	return c
}

func (c *testCluster) startNode(id uint64) {
	c.t.Helper()
	node, err := NewNode(Config{
		ID:                id,
		ListenAddr:        c.peers[id],
		Peers:             c.peers,
		DataDir:           filepath.Join(c.dir, fmt.Sprintf("node%d", id)),
		SnapshotThreshold: c.snapshotThreshold,
		EngineOptions:     &engine.Options{WriteBufferSize: 1 << 20},
		Logger:            log.New(os.Stderr, fmt.Sprintf("[node%d] ", id), 0),
	})
	if err != nil {
		c.t.Fatalf("start node %d: %v", id, err)
	}
	c.nodes[id] = node
}

func (c *testCluster) stopNode(id uint64) {
	c.t.Helper()
	c.nodes[id].Close()
	delete(c.nodes, id)
}

func (c *testCluster) addrs() []string {
	out := make([]string, 0, len(c.peers))
	for _, addr := range c.peers {
		out = append(out, addr)
	}
	return out
}

func (c *testCluster) waitForLeader() uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, node := range c.nodes {
			if node.IsLeader() {
				return id
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("no leader elected within 10s")
	return 0
}

func TestClusterPutGetDelete(t *testing.T) {
	c := startCluster(t, 3)
	c.waitForLeader()

	cl := client.New(c.addrs())
	defer cl.Close()
	ctx := context.Background()

	// Put + consistent Get
	if err := cl.Put(ctx, []byte("greeting"), []byte("hello lakestore")); err != nil {
		t.Fatal(err)
	}
	val, err := cl.Get(ctx, []byte("greeting"))
	if err != nil || string(val) != "hello lakestore" {
		t.Fatalf("get: %v %q", err, val)
	}

	// Delete
	if err := cl.Delete(ctx, []byte("greeting")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Get(ctx, []byte("greeting")); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}

	// Scan
	for i := 0; i < 20; i++ {
		if err := cl.Put(ctx, []byte(fmt.Sprintf("scan_%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	pairs, err := cl.Scan(ctx, []byte("scan_"), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 20 {
		t.Fatalf("scan returned %d pairs, want 20", len(pairs))
	}
	for i, kv := range pairs {
		want := fmt.Sprintf("scan_%02d", i)
		if string(kv[0]) != want {
			t.Fatalf("scan[%d] = %q, want %q", i, kv[0], want)
		}
	}
}

func TestClusterReplicationToAllNodes(t *testing.T) {
	c := startCluster(t, 3)
	c.waitForLeader()

	cl := client.New(c.addrs())
	defer cl.Close()
	ctx := context.Background()

	const n = 100
	for i := 0; i < n; i++ {
		if err := cl.Put(ctx, []byte(fmt.Sprintf("rep_%03d", i)), []byte(fmt.Sprintf("val_%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Every node's local FSM must converge to the same state.
	deadline := time.Now().Add(5 * time.Second)
	for id, node := range c.nodes {
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("rep_%03d", i))
			for {
				val, found, err := node.fsm.Get(key)
				if err != nil {
					t.Fatal(err)
				}
				if found && string(val) == fmt.Sprintf("val_%d", i) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("node %d never received %s", id, key)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
}

func TestClusterLeaderFailover(t *testing.T) {
	c := startCluster(t, 3)
	leader := c.waitForLeader()

	cl := client.New(c.addrs())
	defer cl.Close()
	ctx := context.Background()

	if err := cl.Put(ctx, []byte("before_failover"), []byte("v1")); err != nil {
		t.Fatal(err)
	}

	// Kill the leader.
	c.stopNode(leader)

	// The cluster must elect a new leader and keep serving.
	newLeader := c.waitForLeader()
	if newLeader == leader {
		t.Fatalf("dead node still leader")
	}

	// Data survives, writes continue (client retries through the election).
	val, err := cl.Get(ctx, []byte("before_failover"))
	if err != nil || string(val) != "v1" {
		t.Fatalf("data lost after failover: %v %q", err, val)
	}
	if err := cl.Put(ctx, []byte("after_failover"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	val, err = cl.Get(ctx, []byte("after_failover"))
	if err != nil || string(val) != "v2" {
		t.Fatalf("write after failover: %v %q", err, val)
	}
}

func TestClusterNodeRestartCatchesUp(t *testing.T) {
	c := startCluster(t, 3)
	leader := c.waitForLeader()

	cl := client.New(c.addrs())
	defer cl.Close()
	ctx := context.Background()

	// Pick a follower and stop it.
	var follower uint64
	for id := range c.nodes {
		if id != leader {
			follower = id
			break
		}
	}
	c.stopNode(follower)

	// Write while it is down.
	for i := 0; i < 50; i++ {
		if err := cl.Put(ctx, []byte(fmt.Sprintf("down_%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	// Restart: it must catch up from the leader's log.
	c.startNode(follower)
	node := c.nodes[follower]

	deadline := time.Now().Add(10 * time.Second)
	for {
		val, found, err := node.fsm.Get([]byte("down_49"))
		if err != nil {
			t.Fatal(err)
		}
		if found && string(val) == "v" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted follower never caught up")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestClusterFullRestartDurability(t *testing.T) {
	c := startCluster(t, 3)
	c.waitForLeader()

	cl := client.New(c.addrs())
	ctx := context.Background()

	for i := 0; i < 30; i++ {
		if err := cl.Put(ctx, []byte(fmt.Sprintf("durable_%02d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	cl.Close()

	// Stop the whole cluster, then bring it all back.
	ids := make([]uint64, 0)
	for id := range c.nodes {
		ids = append(ids, id)
	}
	for _, id := range ids {
		c.stopNode(id)
	}
	for _, id := range ids {
		c.startNode(id)
	}
	c.waitForLeader()

	cl = client.New(c.addrs())
	defer cl.Close()
	for i := 0; i < 30; i++ {
		val, err := cl.Get(ctx, []byte(fmt.Sprintf("durable_%02d", i)))
		if err != nil || string(val) != fmt.Sprintf("v%d", i) {
			t.Fatalf("durable_%02d: %v %q", i, err, val)
		}
	}
}

func TestClusterSnapshotCatchUp(t *testing.T) {
	// Tiny snapshot threshold: the leader compacts its log quickly, so a
	// long-dead follower can only recover via InstallSnapshot.
	c := startClusterOpts(t, 3, 64)
	leader := c.waitForLeader()

	cl := client.New(c.addrs())
	defer cl.Close()
	ctx := context.Background()

	var follower uint64
	for id := range c.nodes {
		if id != leader {
			follower = id
			break
		}
	}
	c.stopNode(follower)

	// Write far past the snapshot threshold while the follower is down.
	for i := 0; i < 300; i++ {
		if err := cl.Put(ctx, []byte(fmt.Sprintf("snap_%04d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	c.startNode(follower)
	node := c.nodes[follower]

	deadline := time.Now().Add(15 * time.Second)
	for {
		val, found, err := node.fsm.Get([]byte("snap_0299"))
		if err != nil {
			t.Fatal(err)
		}
		if found && string(val) == "v" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower never caught up via snapshot")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestInspectorEndpoints(t *testing.T) {
	// Single-node cluster (majority of 1) with the debug server enabled.
	addrs := freePorts(t, 2)
	peers := map[uint64]string{1: addrs[0]}
	node, err := NewNode(Config{
		ID:                1,
		ListenAddr:        addrs[0],
		DebugAddr:         addrs[1],
		Peers:             peers,
		DataDir:           t.TempDir(),
		SnapshotThreshold: 50, // force a raft snapshot so its viewer is testable
		EngineOptions:     &engine.Options{WriteBufferSize: 16 * 1024},
		Logger:            log.New(os.Stderr, "[inspect] ", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	// Wait for leadership, then write through the normal path.
	deadline := time.Now().Add(5 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("single node never became leader")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cl := client.New([]string{node.Addr()})
	defer cl.Close()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := cl.Put(ctx, []byte(fmt.Sprintf("ins_%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	// /api/state must return coherent JSON reflecting the writes.
	resp, err := http.Get("http://" + node.DebugAddr() + "/api/state")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/api/state returned %d", resp.StatusCode)
	}
	var state struct {
		NodeID uint64 `json:"node_id"`
		Raft   struct {
			State       string `json:"state"`
			CommitIndex uint64 `json:"commit_index"`
		} `json:"raft"`
		RaftLog []struct {
			Index uint64 `json:"index"`
			Noop  bool   `json:"noop"`
			Ops   []struct{ Op, Key string } `json:"ops"`
		} `json:"raft_log_decoded"`
		Engine struct {
			LastSequence uint64 `json:"last_sequence"`
			Memtable     struct {
				EntryCount int `json:"entry_count"`
			} `json:"memtable"`
		} `json:"engine"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		t.Fatalf("state is not valid JSON: %v", err)
	}
	if state.NodeID != 1 || state.Raft.State != "Leader" {
		t.Fatalf("unexpected state: %+v", state)
	}
	if state.Engine.Memtable.EntryCount < 10 {
		t.Fatalf("memtable should show the writes, got %d entries", state.Engine.Memtable.EntryCount)
	}
	foundPut := false
	for _, e := range state.RaftLog {
		for _, op := range e.Ops {
			if op.Op == "PUT" && op.Key == "ins_00" {
				foundPut = true
			}
		}
	}
	if !foundPut {
		t.Fatal("decoded raft log does not show the PUT commands")
	}

	// /api/sst with a bogus file must fail cleanly, not crash.
	resp2, err := http.Get("http://" + node.DebugAddr() + "/api/sst?file=999999")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode == 200 {
		t.Fatal("expected an error for a missing SSTable")
	}

	// The UI itself must be served.
	resp3, err := http.Get("http://" + node.DebugAddr() + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	if !bytes.Contains(body, []byte("LakeStore Inspector")) {
		t.Fatal("inspector HTML not served")
	}

	base := "http://" + node.DebugAddr()
	postJSON := func(path string, payload string) (int, map[string]any) {
		resp, err := http.Post(base+path, "application/json", bytes.NewBufferString(payload))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	getJSON := func(path string) (int, map[string]any) {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	// Single ops through the debug server hit the normal pipeline.
	if code, out := postJSON("/api/op", `{"op":"put","key":"dbg","value":"v1"}`); code != 200 || out["ok"] != true {
		t.Fatalf("op put failed: %d %v", code, out)
	}
	if code, out := postJSON("/api/op", `{"op":"get","key":"dbg"}`); code != 200 || out["found"] != true || out["value"] != "v1" {
		t.Fatalf("op get failed: %d %v", code, out)
	}
	if code, out := postJSON("/api/op", `{"op":"del","key":"dbg"}`); code != 200 || out["ok"] != true {
		t.Fatalf("op del failed: %d %v", code, out)
	}

	// Load generator: enough entries to cross the snapshot threshold and
	// (with the tiny write buffer) force at least one memtable flush.
	if code, out := postJSON("/api/load", `{"n":300,"value_size":200}`); code != 200 || out["ok"] != true {
		t.Fatalf("load failed: %d %v", code, out)
	}

	// File viewers: engine WAL, SSTable, MANIFEST, CURRENT, raft files.
	code, st2 := getJSON("/api/state")
	if code != 200 {
		t.Fatalf("state after load: %d", code)
	}
	stillOnDisk := func(dir, name string) bool {
		_, cur := getJSON("/api/state")
		for _, f := range cur["disk"].([]any) {
			file := f.(map[string]any)
			if file["dir"] == dir && file["name"] == name {
				return true
			}
		}
		return false
	}
	sawKinds := map[string]bool{}
	for _, f := range st2["disk"].([]any) {
		file := f.(map[string]any)
		dir, name := file["dir"].(string), file["name"].(string)
		code, out := getJSON("/api/file?dir=" + dir + "&name=" + name)
		if code != 200 {
			// Background compaction may delete a file between the listing
			// and the dump — that's fine; anything else is a bug.
			if code == 404 && !stillOnDisk(dir, name) {
				continue
			}
			t.Fatalf("file dump %s/%s: HTTP %d %v", dir, name, code, out)
		}
		kind, _ := out["kind"].(string)
		if kind == "" {
			t.Fatalf("file dump %s/%s: no kind in %v", dir, name, out)
		}
		sawKinds[kind] = true
	}
	for _, want := range []string{"wal", "sstable", "manifest", "current", "raft_log", "raft_meta", "raft_snapshot"} {
		if !sawKinds[want] {
			t.Fatalf("no file of kind %q was decodable (saw %v)", want, sawKinds)
		}
	}
}

func TestStaleReadsServedByFollowers(t *testing.T) {
	c := startCluster(t, 3)
	c.waitForLeader()

	cl := client.New(c.addrs())
	defer cl.Close()
	ctx := context.Background()

	if err := cl.Put(ctx, []byte("stale_key"), []byte("stale_val")); err != nil {
		t.Fatal(err)
	}

	// Every node must eventually serve the value with allow_stale.
	for id, node := range c.nodes {
		single := client.New([]string{node.Addr()})
		deadline := time.Now().Add(5 * time.Second)
		for {
			val, err := single.GetStale(ctx, []byte("stale_key"))
			if err == nil && string(val) == "stale_val" {
				break
			}
			if time.Now().After(deadline) {
				single.Close()
				t.Fatalf("node %d never served stale read: %v", id, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
		single.Close()
	}
}
