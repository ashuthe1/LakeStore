// Package raft implements the Raft consensus algorithm from scratch:
// leader election, log replication, persistence, and snapshot-based log
// compaction, as described in "In Search of an Understandable Consensus
// Algorithm" (Ongaro & Ousterhout, 2014).
//
// The design follows the paper's Figure 2 closely. A Raft node is driven by
// three long-running goroutines: the ticker (election timeouts and heartbeat
// scheduling), the replicators (one per peer, condition-variable driven), and
// the applier (delivers committed entries to the state machine in order).
package raft

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// State of a Raft node.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

const (
	// Election timeouts are randomized in [electionTimeoutMin, electionTimeoutMax).
	electionTimeoutMin = 250 * time.Millisecond
	electionTimeoutMax = 500 * time.Millisecond
	heartbeatInterval  = 75 * time.Millisecond
	tickInterval       = 20 * time.Millisecond
)

// Entry is a single Raft log entry.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// ApplyMsg is delivered on the apply channel for every committed log entry,
// and for installed snapshots.
type ApplyMsg struct {
	// Command delivery
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	// Snapshot delivery (the state machine must reset itself to this state)
	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// --- RPC argument/reply types ---

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// Fast conflict backtracking (§5.3 optimization)
	ConflictTerm  uint64 // term of the conflicting entry, 0 if log too short
	ConflictIndex uint64 // first index with ConflictTerm, or len(log) if short
}

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          uint64
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}

// Transport sends RPCs to peers. Implementations: the in-memory test
// transport (with partitions and message loss) and the gRPC transport.
type Transport interface {
	RequestVote(peer uint64, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(peer uint64, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(peer uint64, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Raft is a single consensus node.
type Raft struct {
	mu        sync.Mutex
	id        uint64
	peers     []uint64 // all node IDs including self
	transport Transport
	storage   Storage

	// Persistent state (updated on stable storage before responding to RPCs)
	currentTerm uint64
	votedFor    uint64 // 0 = none (node IDs must be >= 1)
	log         []Entry

	// Snapshot bookkeeping: log entries <= lastSnapshotIndex are compacted.
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64

	// Volatile state
	state       State
	commitIndex uint64
	lastApplied uint64
	leaderID    uint64 // last known leader (for client redirection)

	// Leader volatile state
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// Coordination
	electionDeadline time.Time
	lastHeartbeat    time.Time
	replicateCond    map[uint64]*sync.Cond // per-peer replication signal
	applyCond        *sync.Cond
	applyCh          chan ApplyMsg
	killed           bool
}

// New creates a Raft node. IDs must be >= 1. The caller must consume applyCh
// promptly; delivery is in strict log order.
func New(id uint64, peers []uint64, transport Transport, storage Storage, applyCh chan ApplyMsg) (*Raft, error) {
	if id == 0 {
		return nil, fmt.Errorf("raft: node IDs must be >= 1")
	}
	rf := &Raft{
		id:         id,
		peers:      append([]uint64(nil), peers...),
		transport:  transport,
		storage:    storage,
		state:      Follower,
		nextIndex:  make(map[uint64]uint64),
		matchIndex: make(map[uint64]uint64),
		applyCh:    applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.replicateCond = make(map[uint64]*sync.Cond)
	for _, p := range rf.peers {
		if p != rf.id {
			rf.replicateCond[p] = sync.NewCond(&rf.mu)
		}
	}

	if err := rf.restore(); err != nil {
		return nil, err
	}
	rf.resetElectionTimer()

	go rf.ticker()
	go rf.applier()
	for _, p := range rf.peers {
		if p != rf.id {
			go rf.replicator(p)
		}
	}
	return rf, nil
}

// Kill shuts the node down. Outstanding goroutines exit promptly.
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.killed = true
	rf.applyCond.Broadcast()
	for _, c := range rf.replicateCond {
		c.Broadcast()
	}
	rf.mu.Unlock()
}

func (rf *Raft) isKilled() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.killed
}

// --- Log index helpers (all called with mu held) ---
// The in-memory log starts at lastSnapshotIndex+1. A sentinel is avoided by
// translating logical indices to slice positions.

func (rf *Raft) firstLogIndex() uint64 { return rf.lastSnapshotIndex + 1 }

func (rf *Raft) lastLogIndex() uint64 {
	return rf.lastSnapshotIndex + uint64(len(rf.log))
}

func (rf *Raft) lastLogTerm() uint64 {
	if len(rf.log) == 0 {
		return rf.lastSnapshotTerm
	}
	return rf.log[len(rf.log)-1].Term
}

// termAt returns the term of the entry at a logical index.
// ok is false if the index has been compacted away or is beyond the log.
func (rf *Raft) termAt(index uint64) (term uint64, ok bool) {
	if index == rf.lastSnapshotIndex {
		return rf.lastSnapshotTerm, true
	}
	if index < rf.firstLogIndex() || index > rf.lastLogIndex() {
		return 0, false
	}
	return rf.log[index-rf.firstLogIndex()].Term, true
}

func (rf *Raft) entriesFrom(index uint64) []Entry {
	if index > rf.lastLogIndex() {
		return nil
	}
	src := rf.log[index-rf.firstLogIndex():]
	out := make([]Entry, len(src))
	copy(out, src)
	return out
}

// --- Persistence ---
// The durable state is split for performance: small metadata (term, vote,
// snapshot markers) is rewritten atomically when it changes, while the log
// is an append-only file — appending an entry costs one fsync'd append, not
// a full-state rewrite. Every persist call happens with mu held, BEFORE the
// state change becomes visible via an RPC reply.

func (rf *Raft) persistMeta() {
	err := rf.storage.SaveMeta(Meta{
		Term:          rf.currentTerm,
		VotedFor:      rf.votedFor,
		SnapshotIndex: rf.lastSnapshotIndex,
		SnapshotTerm:  rf.lastSnapshotTerm,
	})
	if err != nil {
		panic("raft: persist meta: " + err.Error())
	}
}

func (rf *Raft) appendLogPersist(entries []Entry) {
	if err := rf.storage.AppendLog(entries); err != nil {
		panic("raft: append log: " + err.Error())
	}
}

func (rf *Raft) rewriteLogPersist() {
	if err := rf.storage.RewriteLog(rf.log); err != nil {
		panic("raft: rewrite log: " + err.Error())
	}
}

func (rf *Raft) restore() error {
	meta, entries, err := rf.storage.Load()
	if err != nil {
		return err
	}
	rf.currentTerm = meta.Term
	rf.votedFor = meta.VotedFor
	rf.lastSnapshotIndex = meta.SnapshotIndex
	rf.lastSnapshotTerm = meta.SnapshotTerm

	// Reconcile the log with the snapshot markers. A crash between the two
	// writes can leave entries already covered by the snapshot (drop them)
	// or a gap after it (drop the whole suffix; it re-replicates safely —
	// everything at or below the snapshot index is committed data).
	for len(entries) > 0 && entries[0].Index <= rf.lastSnapshotIndex {
		entries = entries[1:]
	}
	if len(entries) > 0 && entries[0].Index != rf.lastSnapshotIndex+1 {
		entries = nil
	}
	rf.log = entries

	// A snapshot is the floor of the state machine: nothing below it can be
	// re-applied, and the applier starts from there.
	rf.commitIndex = rf.lastSnapshotIndex
	rf.lastApplied = rf.lastSnapshotIndex
	return nil
}

// --- Public API ---

// Start proposes a command. Returns (index, term, true) if this node is the
// leader; the command is committed once a majority replicates it.
func (rf *Raft) Start(command []byte) (uint64, uint64, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.killed {
		return 0, rf.currentTerm, false
	}

	entry := Entry{
		Index: rf.lastLogIndex() + 1,
		Term:  rf.currentTerm,
		Data:  command,
	}
	rf.log = append(rf.log, entry)
	rf.appendLogPersist([]Entry{entry})
	rf.matchIndex[rf.id] = entry.Index
	// Our own match advanced; in a single-node cluster that alone is a
	// majority, so the commit check cannot wait for follower ACKs.
	rf.maybeAdvanceCommit()

	// Wake all replicators to push the new entry immediately.
	for _, c := range rf.replicateCond {
		c.Signal()
	}
	return entry.Index, entry.Term, true
}

// GetState returns the current term and whether this node believes it is the
// leader.
func (rf *Raft) GetState() (uint64, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// LeaderID returns the last known leader (0 if unknown).
func (rf *Raft) LeaderID() uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return rf.id
	}
	return rf.leaderID
}

// CommitIndex returns the current commit index.
func (rf *Raft) CommitIndex() uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

// DebugEntry is one log entry as reported by Debug.
type DebugEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"-"` // raw payload; the server decodes it for display
}

// DebugPeerProgress is the leader's replication bookkeeping for one peer.
type DebugPeerProgress struct {
	NextIndex  uint64 `json:"next_index"`
	MatchIndex uint64 `json:"match_index"`
}

// DebugInfo is a read-only snapshot of a node's Raft state for inspection.
type DebugInfo struct {
	ID            uint64 `json:"id"`
	State         string `json:"state"`
	Term          uint64 `json:"term"`
	VotedFor      uint64 `json:"voted_for"`
	LeaderID      uint64 `json:"leader_id"`
	CommitIndex   uint64 `json:"commit_index"`
	LastApplied   uint64 `json:"last_applied"`
	SnapshotIndex uint64 `json:"snapshot_index"`
	SnapshotTerm  uint64 `json:"snapshot_term"`
	FirstLogIndex uint64 `json:"first_log_index"`
	LastLogIndex  uint64 `json:"last_log_index"`
	LogEntryCount int    `json:"log_entry_count"`

	RecentEntries []DebugEntry                 `json:"recent_entries"`
	PeerProgress  map[uint64]DebugPeerProgress `json:"peer_progress,omitempty"` // leader only
}

// Debug returns a consistent snapshot of the node's state, including up to
// maxEntries most recent log entries. Read-only; for inspection UIs.
func (rf *Raft) Debug(maxEntries int) DebugInfo {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	info := DebugInfo{
		ID:            rf.id,
		State:         rf.state.String(),
		Term:          rf.currentTerm,
		VotedFor:      rf.votedFor,
		LeaderID:      rf.leaderID,
		CommitIndex:   rf.commitIndex,
		LastApplied:   rf.lastApplied,
		SnapshotIndex: rf.lastSnapshotIndex,
		SnapshotTerm:  rf.lastSnapshotTerm,
		FirstLogIndex: rf.firstLogIndex(),
		LastLogIndex:  rf.lastLogIndex(),
		LogEntryCount: len(rf.log),
	}
	if rf.state == Leader {
		info.LeaderID = rf.id
		info.PeerProgress = make(map[uint64]DebugPeerProgress, len(rf.peers))
		for _, p := range rf.peers {
			info.PeerProgress[p] = DebugPeerProgress{
				NextIndex:  rf.nextIndex[p],
				MatchIndex: rf.matchIndex[p],
			}
		}
	}

	start := 0
	if maxEntries > 0 && len(rf.log) > maxEntries {
		start = len(rf.log) - maxEntries
	}
	for _, e := range rf.log[start:] {
		info.RecentEntries = append(info.RecentEntries, DebugEntry{
			Index: e.Index,
			Term:  e.Term,
			Data:  append([]byte(nil), e.Data...),
		})
	}
	return info
}

// Snapshot tells Raft the application has serialized its state through
// `index`; the log can be compacted up to and including it.
func (rf *Raft) Snapshot(index uint64, snapshot []byte) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastSnapshotIndex {
		return nil // Already compacted past this point
	}
	if index > rf.lastApplied {
		return fmt.Errorf("raft: cannot snapshot unapplied index %d (applied %d)", index, rf.lastApplied)
	}

	term, ok := rf.termAt(index)
	if !ok {
		return fmt.Errorf("raft: snapshot index %d out of range", index)
	}

	rf.log = rf.entriesFrom(index + 1)
	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = term

	// Order: snapshot data first, then markers, then the trimmed log. A
	// crash in between leaves either extra (already-committed) entries or a
	// newer snapshot than the markers claim — both are reconciled on load.
	if err := rf.storage.SaveSnapshot(snapshot); err != nil {
		return err
	}
	rf.persistMeta()
	rf.rewriteLogPersist()
	return nil
}

// ReadSnapshot returns the most recently saved snapshot.
func (rf *Raft) ReadSnapshot() ([]byte, error) {
	return rf.storage.ReadSnapshot()
}

// --- Election ---

func (rf *Raft) resetElectionTimer() {
	timeout := electionTimeoutMin +
		time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
	rf.electionDeadline = time.Now().Add(timeout)
}

func (rf *Raft) ticker() {
	for !rf.isKilled() {
		time.Sleep(tickInterval)

		rf.mu.Lock()
		switch rf.state {
		case Leader:
			if time.Since(rf.lastHeartbeat) >= heartbeatInterval {
				rf.lastHeartbeat = time.Now()
				for _, c := range rf.replicateCond {
					c.Signal()
				}
			}
		default:
			if time.Now().After(rf.electionDeadline) {
				rf.startElection()
			}
		}
		rf.mu.Unlock()
	}
}

// startElection is called with mu held.
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.id
	rf.persistMeta()
	rf.resetElectionTimer()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.id,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	term := rf.currentTerm
	votes := 1 // self

	// A single-node cluster wins immediately — there are no reply handlers
	// to perform the majority check otherwise.
	if votes*2 > len(rf.peers) {
		rf.becomeLeader()
		return
	}

	for _, peer := range rf.peers {
		if peer == rf.id {
			continue
		}
		go func(peer uint64) {
			reply, err := rf.transport.RequestVote(peer, args)
			if err != nil || reply == nil {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term, 0)
				return
			}
			// Stale response for an old election
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes*2 > len(rf.peers) {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeFollower is called with mu held.
func (rf *Raft) becomeFollower(term uint64, leaderID uint64) {
	changed := term > rf.currentTerm
	if changed {
		rf.currentTerm = term
		rf.votedFor = 0
	}
	rf.state = Follower
	if leaderID != 0 {
		rf.leaderID = leaderID
	}
	if changed {
		rf.persistMeta()
	}
}

// becomeLeader is called with mu held.
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.leaderID = rf.id
	last := rf.lastLogIndex()
	for _, p := range rf.peers {
		rf.nextIndex[p] = last + 1
		rf.matchIndex[p] = 0
	}
	rf.lastHeartbeat = time.Now()

	// Append a no-op entry in our own term (§5.4.2): a leader may only
	// commit entries from its current term by counting replicas, so without
	// this a quiet cluster would leave prior-term entries committed on a
	// majority but never APPLIED after a leader change.
	noop := Entry{Index: last + 1, Term: rf.currentTerm, Data: nil}
	rf.log = append(rf.log, noop)
	rf.appendLogPersist([]Entry{noop})
	rf.matchIndex[rf.id] = noop.Index
	rf.maybeAdvanceCommit() // single-node clusters commit right here

	// Immediate replication round to assert leadership and push the no-op.
	for _, c := range rf.replicateCond {
		c.Signal()
	}
}

// --- RPC handlers (invoked by the transport layer) ---

func (rf *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &RequestVoteReply{Term: rf.currentTerm}

	if args.Term < rf.currentTerm {
		return reply
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term, 0)
		reply.Term = rf.currentTerm
	}

	// §5.4.1 election restriction: only vote for candidates whose log is at
	// least as up-to-date as ours.
	upToDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())

	if (rf.votedFor == 0 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persistMeta()
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}
	return reply
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &AppendEntriesReply{Term: rf.currentTerm}

	if args.Term < rf.currentTerm {
		return reply
	}
	rf.becomeFollower(args.Term, args.LeaderID)
	reply.Term = rf.currentTerm
	rf.resetElectionTimer()

	// Consistency check on the entry preceding the new ones.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		reply.ConflictTerm = 0
		return reply
	}
	if args.PrevLogIndex >= rf.firstLogIndex()-1 {
		prevTerm, ok := rf.termAt(args.PrevLogIndex)
		if !ok {
			// PrevLogIndex is inside our snapshot: everything there is
			// committed, so treat the check as passed at the snapshot edge.
			reply.ConflictIndex = rf.lastSnapshotIndex + 1
			reply.ConflictTerm = 0
			return reply
		}
		if prevTerm != args.PrevLogTerm {
			// Fast backtracking: report the first index of the conflicting term.
			reply.ConflictTerm = prevTerm
			ci := args.PrevLogIndex
			for ci > rf.firstLogIndex() {
				t, ok := rf.termAt(ci - 1)
				if !ok || t != prevTerm {
					break
				}
				ci--
			}
			reply.ConflictIndex = ci
			return reply
		}
	}

	// Append/overwrite entries. Only truncate on a REAL conflict (§5.3);
	// a stale duplicate AppendEntries must not undo newer entries.
	for i, entry := range args.Entries {
		if entry.Index <= rf.lastSnapshotIndex {
			continue // already compacted, therefore already committed
		}
		existingTerm, ok := rf.termAt(entry.Index)
		if ok && existingTerm == entry.Term {
			continue // already have it
		}
		// Conflict or past our end: truncate from here and append the rest.
		truncating := entry.Index <= rf.lastLogIndex()
		rf.log = rf.log[:entry.Index-rf.firstLogIndex()]
		rf.log = append(rf.log, args.Entries[i:]...)
		if truncating {
			rf.rewriteLogPersist()
		} else {
			rf.appendLogPersist(args.Entries[i:])
		}
		break
	}

	// Advance commit index.
	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if last := rf.lastLogIndex(); newCommit > last {
			newCommit = last
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Signal()
		}
	}

	reply.Success = true
	return reply
}

func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &InstallSnapshotReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply
	}
	rf.becomeFollower(args.Term, args.LeaderID)
	reply.Term = rf.currentTerm
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.lastSnapshotIndex ||
		args.LastIncludedIndex <= rf.lastApplied {
		return reply // stale snapshot
	}

	// Retain any log suffix that extends beyond the snapshot; drop the rest.
	if term, ok := rf.termAt(args.LastIncludedIndex); ok && term == args.LastIncludedTerm {
		rf.log = rf.entriesFrom(args.LastIncludedIndex + 1)
	} else {
		rf.log = nil
	}
	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm

	if err := rf.storage.SaveSnapshot(args.Data); err != nil {
		panic("raft: save snapshot: " + err.Error())
	}
	rf.persistMeta()
	rf.rewriteLogPersist()

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	rf.lastApplied = args.LastIncludedIndex

	// Hand the snapshot to the state machine outside the lock.
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	go func() {
		rf.applyCh <- msg
	}()
	return reply
}

// --- Replication (leader side) ---

// replicator runs per peer, pushing entries whenever signaled.
func (rf *Raft) replicator(peer uint64) {
	rf.mu.Lock()
	cond := rf.replicateCond[peer]
	for !rf.killed {
		// Wait until we're the leader with something to send (or a heartbeat).
		cond.Wait()
		if rf.killed {
			break
		}
		for rf.state == Leader && !rf.killed {
			var again bool
			if rf.nextIndex[peer] < rf.firstLogIndex() {
				again = rf.sendSnapshotTo(peer) // temporarily releases the lock
			} else {
				again = rf.sendEntriesTo(peer) // temporarily releases the lock
			}
			if !again {
				break // retry on the next heartbeat signal
			}
		}
	}
	rf.mu.Unlock()
}

// sendEntriesTo pushes one AppendEntries RPC. Called with mu held; releases
// it during the RPC. Returns true if another round is needed immediately.
func (rf *Raft) sendEntriesTo(peer uint64) bool {
	next := rf.nextIndex[peer]
	prevIndex := next - 1
	prevTerm, ok := rf.termAt(prevIndex)
	if !ok {
		return true // races with compaction: caller will send a snapshot
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      rf.entriesFrom(next),
		LeaderCommit: rf.commitIndex,
	}
	term := rf.currentTerm

	rf.mu.Unlock()
	reply, err := rf.transport.AppendEntries(peer, args)
	rf.mu.Lock()

	if err != nil || reply == nil {
		return false // unreachable; retry on next heartbeat
	}
	if rf.state != Leader || rf.currentTerm != term || rf.killed {
		return false // world changed while we were unlocked
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term, 0)
		return false
	}

	if reply.Success {
		match := args.PrevLogIndex + uint64(len(args.Entries))
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		if match+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = match + 1
		}
		rf.maybeAdvanceCommit()
		// More entries may have arrived while we were sending.
		return rf.nextIndex[peer] <= rf.lastLogIndex()
	}

	// Rejected: backtrack using the conflict hints.
	if reply.ConflictTerm != 0 {
		// If we have entries of ConflictTerm, resume after our last one.
		lastOfTerm := uint64(0)
		for i := len(rf.log) - 1; i >= 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				lastOfTerm = rf.log[i].Index
				break
			}
		}
		if lastOfTerm > 0 {
			rf.nextIndex[peer] = lastOfTerm + 1
		} else {
			rf.nextIndex[peer] = reply.ConflictIndex
		}
	} else if reply.ConflictIndex > 0 {
		rf.nextIndex[peer] = reply.ConflictIndex
	} else if rf.nextIndex[peer] > 1 {
		rf.nextIndex[peer]--
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
	return true // retry immediately with the adjusted index
}

// sendSnapshotTo installs the current snapshot on a lagging peer.
// Called with mu held; releases it during the RPC. Returns true if the
// replicator should immediately continue with log entries.
func (rf *Raft) sendSnapshotTo(peer uint64) bool {
	snapshot, err := rf.storage.ReadSnapshot()
	if err != nil || len(snapshot) == 0 {
		return false
	}
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.id,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              snapshot,
	}
	term := rf.currentTerm

	rf.mu.Unlock()
	reply, rpcErr := rf.transport.InstallSnapshot(peer, args)
	rf.mu.Lock()

	if rpcErr != nil || reply == nil {
		return false
	}
	if rf.state != Leader || rf.currentTerm != term || rf.killed {
		return false
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term, 0)
		return false
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	rf.maybeAdvanceCommit()
	return rf.nextIndex[peer] <= rf.lastLogIndex()
}

// maybeAdvanceCommit advances commitIndex to the highest index replicated on
// a majority, restricted to entries from the current term (§5.4.2).
// Called with mu held.
func (rf *Raft) maybeAdvanceCommit() {
	matches := make([]uint64, 0, len(rf.peers))
	for _, p := range rf.peers {
		matches = append(matches, rf.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	majorityMatch := matches[len(rf.peers)/2]

	if majorityMatch > rf.commitIndex {
		if term, ok := rf.termAt(majorityMatch); ok && term == rf.currentTerm {
			rf.commitIndex = majorityMatch
			rf.applyCond.Signal()
			// Followers learn the new commit index on the next round.
			for _, c := range rf.replicateCond {
				c.Signal()
			}
		}
	}
}

// --- Apply loop ---

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed {
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			continue
		}
		// Copy the committed-but-unapplied window, then deliver unlocked.
		start := rf.lastApplied + 1
		if start < rf.firstLogIndex() {
			// A snapshot moved the floor; the snapshot ApplyMsg covers it.
			rf.lastApplied = rf.lastSnapshotIndex
			continue
		}
		entries := make([]Entry, 0, rf.commitIndex-rf.lastApplied)
		for idx := start; idx <= rf.commitIndex; idx++ {
			entries = append(entries, rf.log[idx-rf.firstLogIndex()])
		}

		rf.mu.Unlock()
		for _, e := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      e.Data,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
			}
		}
		rf.mu.Lock()

		if start+uint64(len(entries))-1 > rf.lastApplied {
			rf.lastApplied = start + uint64(len(entries)) - 1
		}
	}
}
