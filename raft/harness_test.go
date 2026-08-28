package raft

// Test harness: an in-process cluster whose transport supports partitions,
// message loss, and delays, plus node crash/restart with persistent state.
// Modeled after the MIT 6.824/6.5840 Raft test harness.

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type cluster struct {
	t  *testing.T
	mu sync.Mutex
	n  int

	nodes     map[uint64]*Raft
	storages  map[uint64]*MemoryStorage
	applyChs  map[uint64]chan ApplyMsg
	collectDone map[uint64]chan struct{}
	connected map[uint64]bool
	alive     map[uint64]bool

	unreliable bool
	longDelays bool

	// committed[node][index] = command; consistency-checked on every apply.
	committed map[uint64]map[uint64]string
	maxIndex  uint64
}

// --- transport ---

type testTransport struct {
	c    *cluster
	from uint64
}

func (t *testTransport) reachable(to uint64) bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	return t.c.connected[t.from] && t.c.connected[to] && t.c.alive[to]
}

func (t *testTransport) preflight(to uint64) bool {
	if !t.reachable(to) {
		return false
	}
	t.c.mu.Lock()
	unreliable := t.c.unreliable
	long := t.c.longDelays
	t.c.mu.Unlock()

	if unreliable {
		if rand.Intn(1000) < 100 {
			return false // drop ~10%
		}
		delay := time.Duration(rand.Intn(25)) * time.Millisecond
		if long && rand.Intn(1000) < 100 {
			delay = time.Duration(200+rand.Intn(200)) * time.Millisecond
		}
		time.Sleep(delay)
	}
	return true
}

func (t *testTransport) target(to uint64) *Raft {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	if !t.c.alive[to] {
		return nil
	}
	return t.c.nodes[to]
}

var errUnreachable = fmt.Errorf("unreachable")

func (t *testTransport) RequestVote(to uint64, args *RequestVoteArgs) (*RequestVoteReply, error) {
	if !t.preflight(to) {
		return nil, errUnreachable
	}
	node := t.target(to)
	if node == nil {
		return nil, errUnreachable
	}
	reply := node.HandleRequestVote(args)
	if !t.reachable(to) {
		return nil, errUnreachable // reply lost
	}
	return reply, nil
}

func (t *testTransport) AppendEntries(to uint64, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if !t.preflight(to) {
		return nil, errUnreachable
	}
	node := t.target(to)
	if node == nil {
		return nil, errUnreachable
	}
	reply := node.HandleAppendEntries(args)
	if !t.reachable(to) {
		return nil, errUnreachable
	}
	return reply, nil
}

func (t *testTransport) InstallSnapshot(to uint64, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	if !t.preflight(to) {
		return nil, errUnreachable
	}
	node := t.target(to)
	if node == nil {
		return nil, errUnreachable
	}
	reply := node.HandleInstallSnapshot(args)
	if !t.reachable(to) {
		return nil, errUnreachable
	}
	return reply, nil
}

// --- cluster lifecycle ---

func newCluster(t *testing.T, n int, unreliable bool) *cluster {
	c := &cluster{
		t:         t,
		n:         n,
		nodes:     make(map[uint64]*Raft),
		storages:  make(map[uint64]*MemoryStorage),
		applyChs:  make(map[uint64]chan ApplyMsg),
		collectDone: make(map[uint64]chan struct{}),
		connected: make(map[uint64]bool),
		alive:     make(map[uint64]bool),
		committed: make(map[uint64]map[uint64]string),
		unreliable: unreliable,
	}
	for i := 1; i <= n; i++ {
		id := uint64(i)
		c.storages[id] = NewMemoryStorage()
		c.committed[id] = make(map[uint64]string)
		c.connected[id] = true
		c.startNode(id)
	}
	return c
}

func (c *cluster) peerIDs() []uint64 {
	ids := make([]uint64, 0, c.n)
	for i := 1; i <= c.n; i++ {
		ids = append(ids, uint64(i))
	}
	return ids
}

// startNode boots (or reboots) node id from its persistent storage.
func (c *cluster) startNode(id uint64) {
	applyCh := make(chan ApplyMsg, 256)
	transport := &testTransport{c: c, from: id}

	c.mu.Lock()
	c.applyChs[id] = applyCh
	c.alive[id] = true
	done := make(chan struct{})
	c.collectDone[id] = done
	c.mu.Unlock()

	rf, err := New(id, c.peerIDs(), transport, c.storages[id], applyCh)
	if err != nil {
		c.t.Fatalf("start node %d: %v", id, err)
	}

	c.mu.Lock()
	c.nodes[id] = rf
	c.mu.Unlock()

	go c.collect(id, applyCh, done)
}

// collect drains a node's apply channel, recording and consistency-checking
// committed commands.
func (c *cluster) collect(id uint64, ch chan ApplyMsg, done chan struct{}) {
	for {
		select {
		case <-done:
			return
		case msg := <-ch:
			if msg.CommandValid {
				cmd := string(msg.Command)
				c.mu.Lock()
				for other, m := range c.committed {
					if v, ok := m[msg.CommandIndex]; ok && v != cmd {
						c.mu.Unlock()
						c.t.Errorf("commit disagreement at index %d: node %d has %q, node %d has %q",
							msg.CommandIndex, other, v, id, cmd)
						return
					}
				}
				c.committed[id][msg.CommandIndex] = cmd
				if msg.CommandIndex > c.maxIndex {
					c.maxIndex = msg.CommandIndex
				}
				c.mu.Unlock()
			}
			// Snapshot messages carry state the FSM would restore; the
			// harness keeps its committed map (snapshots never contradict it).
		}
	}
}

func (c *cluster) crash(id uint64) {
	c.mu.Lock()
	rf := c.nodes[id]
	c.alive[id] = false
	c.connected[id] = false
	close(c.collectDone[id])
	c.mu.Unlock()
	if rf != nil {
		rf.Kill()
	}
	// Drain any in-flight applies so the goroutine can exit.
	go func(ch chan ApplyMsg) {
		for range ch {
		}
	}(c.applyChs[id])
}

func (c *cluster) restart(id uint64) {
	c.startNode(id)
	c.mu.Lock()
	c.connected[id] = true
	c.mu.Unlock()
}

func (c *cluster) disconnect(id uint64) {
	c.mu.Lock()
	c.connected[id] = false
	c.mu.Unlock()
}

func (c *cluster) connect(id uint64) {
	c.mu.Lock()
	c.connected[id] = true
	c.mu.Unlock()
}

func (c *cluster) shutdown() {
	c.mu.Lock()
	nodes := make([]*Raft, 0, len(c.nodes))
	for id, rf := range c.nodes {
		if c.alive[id] {
			nodes = append(nodes, rf)
			close(c.collectDone[id])
		}
		c.alive[id] = false
	}
	chans := make([]chan ApplyMsg, 0)
	for _, ch := range c.applyChs {
		chans = append(chans, ch)
	}
	c.mu.Unlock()
	for _, rf := range nodes {
		rf.Kill()
	}
	for _, ch := range chans {
		go func(ch chan ApplyMsg) {
			for {
				select {
				case <-ch:
				case <-time.After(2 * time.Second):
					return
				}
			}
		}(ch)
	}
}

// --- assertions/helpers ---

// checkOneLeader waits for the cluster to elect exactly one leader among
// connected nodes and returns its id.
func (c *cluster) checkOneLeader() uint64 {
	for iters := 0; iters < 20; iters++ {
		time.Sleep(time.Duration(450+rand.Intn(100)) * time.Millisecond)

		leadersByTerm := make(map[uint64][]uint64)
		c.mu.Lock()
		for id, rf := range c.nodes {
			if c.connected[id] && c.alive[id] {
				if term, isLeader := rf.GetState(); isLeader {
					leadersByTerm[term] = append(leadersByTerm[term], id)
				}
			}
		}
		c.mu.Unlock()

		lastTerm := uint64(0)
		for term, leaders := range leadersByTerm {
			if len(leaders) > 1 {
				c.t.Fatalf("term %d has %d (>1) leaders", term, len(leaders))
			}
			if term > lastTerm {
				lastTerm = term
			}
		}
		if len(leadersByTerm) > 0 {
			return leadersByTerm[lastTerm][0]
		}
	}
	c.t.Fatalf("expected one leader, got none")
	return 0
}

func (c *cluster) checkNoLeader() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, rf := range c.nodes {
		if c.connected[id] && c.alive[id] {
			if _, isLeader := rf.GetState(); isLeader {
				c.t.Fatalf("expected no leader, but %d claims to be leader", id)
			}
		}
	}
}

func (c *cluster) checkTerms() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	term := uint64(0)
	for id, rf := range c.nodes {
		if c.connected[id] && c.alive[id] {
			t, _ := rf.GetState()
			if term == 0 {
				term = t
			} else if term != t {
				c.t.Fatalf("nodes disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// nCommitted counts how many nodes have committed `index` and returns the
// command.
func (c *cluster) nCommitted(index uint64) (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	cmd := ""
	for _, m := range c.committed {
		if v, ok := m[index]; ok {
			if count > 0 && v != cmd {
				c.t.Fatalf("committed values differ at index %d", index)
			}
			count++
			cmd = v
		}
	}
	return count, cmd
}

// one submits cmd through the current leader and waits until at least
// `expectedServers` nodes commit it. Retries on leadership changes.
func (c *cluster) one(cmd string, expectedServers int, retry bool) uint64 {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		// Find a node that will accept the command.
		var index uint64
		c.mu.Lock()
		ids := make([]uint64, 0)
		for id := range c.nodes {
			if c.connected[id] && c.alive[id] {
				ids = append(ids, id)
			}
		}
		c.mu.Unlock()

		for _, id := range ids {
			c.mu.Lock()
			rf := c.nodes[id]
			c.mu.Unlock()
			if rf == nil {
				continue
			}
			if idx, _, ok := rf.Start([]byte(cmd)); ok {
				index = idx
				break
			}
		}

		if index == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Wait for the agreement to reach expectedServers.
		waitUntil := time.Now().Add(2 * time.Second)
		for time.Now().Before(waitUntil) {
			n, committedCmd := c.nCommitted(index)
			if n >= expectedServers && committedCmd == cmd {
				return index
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !retry {
			c.t.Fatalf("one(%q) failed to reach agreement", cmd)
		}
	}
	c.t.Fatalf("one(%q) failed to reach agreement after retries", cmd)
	return 0
}
