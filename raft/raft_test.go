package raft

// Raft correctness tests: elections, replication, partitions, persistence,
// and snapshot-based log compaction. Scenarios follow the MIT 6.824 battery.

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, false)
	defer c.shutdown()

	c.checkOneLeader()

	// Terms must agree and stay stable without failures.
	term1 := c.checkTerms()
	time.Sleep(700 * time.Millisecond)
	term2 := c.checkTerms()
	if term1 != term2 {
		t.Logf("warning: term changed without failures (%d -> %d)", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, false)
	defer c.shutdown()

	leader1 := c.checkOneLeader()

	// Leader disconnects: a new one must be elected.
	c.disconnect(leader1)
	leader2 := c.checkOneLeader()
	if leader2 == leader1 {
		t.Fatalf("disconnected leader %d still leader", leader1)
	}

	// Old leader rejoins: exactly one leader remains.
	c.connect(leader1)
	c.checkOneLeader()

	// No quorum -> no leader.
	leader3 := c.checkOneLeader()
	c.disconnect(leader3)
	var other uint64
	for i := uint64(1); i <= 3; i++ {
		if i != leader3 {
			other = i
			break
		}
	}
	c.disconnect(other)
	time.Sleep(2 * electionTimeoutMax)
	c.checkNoLeader()

	// Quorum restored -> leader elected.
	c.connect(other)
	c.checkOneLeader()
}

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 3, false)
	defer c.shutdown()

	// Indices must be assigned sequentially (leader no-ops may occupy
	// election indices, so track relative order rather than absolutes).
	var prev uint64
	for i := 1; i <= 5; i++ {
		idx := c.one("cmd"+strconv.Itoa(i), 3, false)
		if prev != 0 && idx != prev+1 {
			t.Fatalf("non-sequential index %d after %d", idx, prev)
		}
		prev = idx
	}
}

func TestFailAgree(t *testing.T) {
	c := newCluster(t, 3, false)
	defer c.shutdown()

	c.one("101", 3, false)

	// One follower disconnects; agreement continues with 2/3.
	leader := c.checkOneLeader()
	var follower uint64
	for i := uint64(1); i <= 3; i++ {
		if i != leader {
			follower = i
			break
		}
	}
	c.disconnect(follower)

	c.one("102", 2, false)
	c.one("103", 2, false)
	time.Sleep(electionTimeoutMax)
	c.one("104", 2, false)

	// Follower rejoins and must catch up.
	c.connect(follower)
	c.one("105", 3, true)
	time.Sleep(electionTimeoutMax)
	c.one("106", 3, true)
}

func TestFailNoAgree(t *testing.T) {
	c := newCluster(t, 5, false)
	defer c.shutdown()

	c.one("10", 5, false)

	// 3 of 5 disconnect: no agreement possible.
	leader := c.checkOneLeader()
	var down []uint64
	for i := uint64(1); i <= 5 && len(down) < 3; i++ {
		if i != leader {
			down = append(down, i)
		}
	}
	for _, id := range down {
		c.disconnect(id)
	}

	c.mu.Lock()
	rf := c.nodes[leader]
	c.mu.Unlock()
	index, _, ok := rf.Start([]byte("20"))
	if !ok {
		t.Fatalf("leader rejected Start()")
	}

	time.Sleep(2 * time.Second)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("committed %d without a majority", index)
	}

	// Reconnect: agreement resumes.
	for _, id := range down {
		c.connect(id)
	}
	c.one("30", 5, true)
}

func TestConcurrentStarts(t *testing.T) {
	c := newCluster(t, 3, false)
	defer c.shutdown()

	leader := c.checkOneLeader()
	c.mu.Lock()
	rf := c.nodes[leader]
	c.mu.Unlock()

	done := make(chan uint64, 10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			idx, _, ok := rf.Start([]byte(fmt.Sprintf("concurrent_%d", i)))
			if ok {
				done <- idx
			} else {
				done <- 0
			}
		}(i)
	}

	indices := make(map[uint64]bool)
	accepted := 0
	for i := 0; i < 10; i++ {
		idx := <-done
		if idx != 0 {
			if indices[idx] {
				t.Fatalf("two commands assigned the same index %d", idx)
			}
			indices[idx] = true
			accepted++
		}
	}
	if accepted == 0 {
		t.Fatalf("leader accepted none of the concurrent commands")
	}

	// All accepted commands must eventually commit on all nodes.
	deadline := time.Now().Add(5 * time.Second)
	for idx := range indices {
		for {
			n, _ := c.nCommitted(idx)
			if n == 3 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("index %d never fully committed", idx)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func TestRejoinWithDivergentLog(t *testing.T) {
	c := newCluster(t, 3, false)
	defer c.shutdown()

	c.one("101", 3, false)

	// Leader gets partitioned and accepts entries that will never commit.
	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.mu.Lock()
	rf1 := c.nodes[leader1]
	c.mu.Unlock()
	rf1.Start([]byte("stale_102"))
	rf1.Start([]byte("stale_103"))
	rf1.Start([]byte("stale_104"))

	// Majority side commits different entries at those indices.
	c.one("real_102", 2, true)

	// New leader also gets partitioned; old leader rejoins.
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.connect(leader1)

	// Old leader's uncommitted entries must be overwritten.
	c.one("real_103", 2, true)

	c.connect(leader2)
	c.one("real_104", 3, true)

	// Verify no stale entry ever committed anywhere.
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, m := range c.committed {
		for idx, cmd := range m {
			if len(cmd) >= 5 && cmd[:5] == "stale" {
				t.Fatalf("node %d committed stale entry %q at %d", id, cmd, idx)
			}
		}
	}
}

func TestBackup(t *testing.T) {
	// Divergent logs of moderate length get repaired via backtracking.
	c := newCluster(t, 5, false)
	defer c.shutdown()

	c.one("base", 5, true)

	// Partition: leader + 1 follower accept many uncommittable entries.
	leader1 := c.checkOneLeader()
	kept := make([]uint64, 0)
	for i := uint64(1); i <= 5; i++ {
		if i != leader1 && len(kept) < 1 {
			kept = append(kept, i)
		}
	}
	minority := append([]uint64{leader1}, kept...)
	isMinority := func(id uint64) bool {
		for _, m := range minority {
			if m == id {
				return true
			}
		}
		return false
	}
	for i := uint64(1); i <= 5; i++ {
		if !isMinority(i) {
			c.disconnect(i)
		}
	}
	c.mu.Lock()
	rf := c.nodes[leader1]
	c.mu.Unlock()
	for i := 0; i < 50; i++ {
		rf.Start([]byte(fmt.Sprintf("lost_%d", i)))
	}
	time.Sleep(500 * time.Millisecond)

	// Flip the partition: majority commits its own long run of entries.
	for i := uint64(1); i <= 5; i++ {
		if isMinority(i) {
			c.disconnect(i)
		} else {
			c.connect(i)
		}
	}
	for i := 0; i < 50; i++ {
		c.one(fmt.Sprintf("won_%d", i), 3, true)
	}

	// Everyone reconnects; the divergent logs must be repaired.
	for i := uint64(1); i <= 5; i++ {
		c.connect(i)
	}
	c.one("final", 5, true)

	c.mu.Lock()
	defer c.mu.Unlock()
	for id, m := range c.committed {
		for idx, cmd := range m {
			if len(cmd) >= 4 && cmd[:4] == "lost" {
				t.Fatalf("node %d committed lost entry %q at %d", id, cmd, idx)
			}
		}
	}
}

func TestPersistence(t *testing.T) {
	c := newCluster(t, 3, false)
	defer c.shutdown()

	c.one("p1", 3, false)

	// Restart the whole cluster: committed entries must survive.
	for i := uint64(1); i <= 3; i++ {
		c.crash(i)
	}
	for i := uint64(1); i <= 3; i++ {
		c.restart(i)
	}
	c.one("p2", 3, true)

	// Restart just the leader.
	leader := c.checkOneLeader()
	c.crash(leader)
	c.restart(leader)
	c.one("p3", 3, true)

	// Crash a follower, commit without it, bring it back.
	leader2 := c.checkOneLeader()
	var follower uint64
	for i := uint64(1); i <= 3; i++ {
		if i != leader2 {
			follower = i
			break
		}
	}
	c.crash(follower)
	c.one("p4", 2, true)
	c.restart(follower)
	c.one("p5", 3, true)

	// All five commands must be committed on all three nodes (leader no-ops
	// shift the absolute indices, so search by command).
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 1; i <= 5; i++ {
		want := fmt.Sprintf("p%d", i)
		for id, m := range c.committed {
			found := false
			for _, cmd := range m {
				if cmd == want {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("node %d is missing committed command %q", id, want)
			}
		}
	}
}

func TestUnreliableAgree(t *testing.T) {
	c := newCluster(t, 5, true) // drops ~10% of messages
	defer c.shutdown()

	var indexes []uint64
	for i := 0; i < 30; i++ {
		idx := c.one(fmt.Sprintf("unreliable_%d", i), 3, true)
		indexes = append(indexes, idx)
	}

	// Let the cluster settle, then verify full replication of all entries.
	c.mu.Lock()
	c.unreliable = false
	c.mu.Unlock()
	deadline := time.Now().Add(10 * time.Second)
	for _, idx := range indexes {
		for {
			n, _ := c.nCommitted(idx)
			if n == 5 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("index %d only on %d/5 nodes", idx, n)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestSnapshotBasic(t *testing.T) {
	c := newCluster(t, 3, false)
	defer c.shutdown()

	leader := c.checkOneLeader()

	// Commit a batch of entries.
	for i := 1; i <= 20; i++ {
		c.one(fmt.Sprintf("snap_%d", i), 3, true)
	}

	// Every node compacts its log at index 15.
	c.mu.Lock()
	nodes := make([]*Raft, 0)
	for _, rf := range c.nodes {
		nodes = append(nodes, rf)
	}
	c.mu.Unlock()
	for _, rf := range nodes {
		if err := rf.Snapshot(15, []byte("state@15")); err != nil {
			t.Fatalf("snapshot: %v", err)
		}
	}

	// The cluster keeps working after compaction.
	c.one("after_snap", 3, true)
	_ = leader
}

func TestSnapshotInstallOnLaggingFollower(t *testing.T) {
	c := newCluster(t, 3, false)
	defer c.shutdown()

	leader := c.checkOneLeader()
	var follower uint64
	for i := uint64(1); i <= 3; i++ {
		if i != leader {
			follower = i
			break
		}
	}

	// Follower falls behind while the others commit and compact.
	c.disconnect(follower)
	for i := 1; i <= 30; i++ {
		c.one(fmt.Sprintf("live_%d", i), 2, true)
	}

	// Leader compacts away the entries the follower is missing.
	leader = c.checkOneLeader()
	c.mu.Lock()
	rfLeader := c.nodes[leader]
	c.mu.Unlock()
	commit := rfLeader.CommitIndex()
	if err := rfLeader.Snapshot(commit, []byte(fmt.Sprintf("state@%d", commit))); err != nil {
		t.Fatalf("leader snapshot: %v", err)
	}

	// Rejoin: follower can only catch up via InstallSnapshot.
	c.connect(follower)
	c.one("post_snapshot", 3, true)

	// Follower must be able to participate fully (e.g. become up to date).
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		rfF := c.nodes[follower]
		c.mu.Unlock()
		if rfF.CommitIndex() >= commit {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never caught up past snapshot (commit %d < %d)",
				rfF.CommitIndex(), commit)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestLeaderCompletenessAcrossRestart(t *testing.T) {
	// A committed entry must survive any sequence of crashes/elections.
	c := newCluster(t, 5, false)
	defer c.shutdown()

	idx := c.one("must_survive", 3, true)

	// Crash the leader immediately after commit.
	leader := c.checkOneLeader()
	c.crash(leader)

	// New leader must still expose the committed entry.
	c.one("after_crash", 3, true)
	c.restart(leader)
	c.one("after_restart", 4, true)

	n, cmd := c.nCommitted(idx)
	if n < 3 || cmd != "must_survive" {
		t.Fatalf("committed entry lost: n=%d cmd=%q", n, cmd)
	}
}
