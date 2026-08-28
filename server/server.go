// Package server wires the pieces of a LakeStore node together: the C++
// storage engine (via the FSM), the Raft consensus core, and the gRPC
// surface (client KV API + inter-node Raft transport).
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ashuthe1/LakeStore/api/pb"
	"github.com/ashuthe1/LakeStore/engine"
	"github.com/ashuthe1/LakeStore/raft"
)

const (
	proposalTimeout  = 5 * time.Second
	readIndexTimeout = 5 * time.Second
	maxScanLimit     = 10000
)

// Config describes one node of a LakeStore cluster.
type Config struct {
	ID         uint64
	ListenAddr string            // gRPC bind address of this node
	Peers      map[uint64]string // node ID -> address, including self
	DataDir    string

	// DebugAddr, when non-empty, serves the read-only inspector UI and its
	// JSON API (debug.go) on this address. Keep it on localhost.
	DebugAddr string

	// SnapshotThreshold is the number of applied entries between Raft log
	// compactions. 0 uses the default.
	SnapshotThreshold uint64

	EngineOptions *engine.Options
	Logger        *log.Logger
}

// Node is a running LakeStore server.
type Node struct {
	cfg       Config
	fsm       *FSM
	rf        *raft.Raft
	transport *grpcTransport
	grpc      *grpc.Server
	listener  net.Listener
	applyCh   chan raft.ApplyMsg
	logger    *log.Logger

	debugSrv      *http.Server
	debugListener net.Listener

	mu             sync.Mutex
	waiters        map[uint64]*waiterGroup
	lastSnapshotAt uint64
	closed         bool

	proposeCh chan proposal
	stopCh    chan struct{}
	applyDone chan struct{}
}

// waiterGroup tracks every client proposal batched into one log entry.
type waiterGroup struct {
	term  uint64
	resps []chan error
}

type proposal struct {
	cmd  Command
	resp chan error
}

// maxProposalBatch bounds how many client commands share one Raft entry.
const maxProposalBatch = 512

// NewNode boots a node: recovers the engine and Raft state, starts serving.
func NewNode(cfg Config) (*Node, error) {
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 8192
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}

	fsm, err := NewFSM(cfg.DataDir+"/engine", cfg.EngineOptions)
	if err != nil {
		return nil, fmt.Errorf("node %d: open engine: %w", cfg.ID, err)
	}

	storage, err := raft.NewFileStorage(cfg.DataDir + "/raft")
	if err != nil {
		fsm.Close()
		return nil, fmt.Errorf("node %d: raft storage: %w", cfg.ID, err)
	}

	peerIDs := make([]uint64, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		peerIDs = append(peerIDs, id)
	}
	transport := newGRPCTransport(cfg.Peers)

	applyCh := make(chan raft.ApplyMsg, 256)
	rf, err := raft.New(cfg.ID, peerIDs, transport, storage, applyCh)
	if err != nil {
		fsm.Close()
		transport.Close()
		return nil, fmt.Errorf("node %d: raft: %w", cfg.ID, err)
	}

	n := &Node{
		cfg:       cfg,
		fsm:       fsm,
		rf:        rf,
		transport: transport,
		applyCh:   applyCh,
		logger:    logger,
		waiters:   make(map[uint64]*waiterGroup),
		proposeCh: make(chan proposal, 4096),
		stopCh:    make(chan struct{}),
		applyDone: make(chan struct{}),
	}
	n.lastSnapshotAt = fsm.LastApplied()

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		n.shutdownInternals()
		return nil, fmt.Errorf("node %d: listen %s: %w", cfg.ID, cfg.ListenAddr, err)
	}
	n.listener = listener

	n.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20))
	pb.RegisterRaftTransportServer(n.grpc, &raftService{rf: rf})
	pb.RegisterKVServer(n.grpc, &kvService{node: n})

	go n.applyLoop()
	go n.proposer()
	go func() {
		if err := n.grpc.Serve(listener); err != nil {
			logger.Printf("node %d: grpc serve: %v", cfg.ID, err)
		}
	}()

	if cfg.DebugAddr != "" {
		if err := n.startDebugServer(); err != nil {
			n.Close()
			return nil, fmt.Errorf("node %d: %w", cfg.ID, err)
		}
	}

	logger.Printf("node %d: serving on %s", cfg.ID, cfg.ListenAddr)
	return n, nil
}

// Close stops serving and shuts down Raft and the engine.
func (n *Node) Close() {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	for idx, wg := range n.waiters {
		for _, resp := range wg.resps {
			resp <- errors.New("node shutting down")
		}
		delete(n.waiters, idx)
	}
	n.mu.Unlock()
	close(n.stopCh)

	n.stopDebugServer()
	n.grpc.Stop()
	n.rf.Kill()

	// Unblock the raft applier if it is mid-send, then wait for applyLoop.
	go func() {
		for {
			select {
			case <-n.applyCh:
			case <-time.After(time.Second):
				return
			}
		}
	}()
	select {
	case <-n.applyDone:
	case <-time.After(3 * time.Second):
	}

	n.transport.Close()
	n.fsm.Close()
}

func (n *Node) shutdownInternals() {
	n.rf.Kill()
	n.transport.Close()
	n.fsm.Close()
}

// Addr returns the address the node is actually listening on (useful when
// the config used port 0).
func (n *Node) Addr() string { return n.listener.Addr().String() }

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	_, isLeader := n.rf.GetState()
	return isLeader
}

// --- apply loop ---

func (n *Node) applyLoop() {
	defer close(n.applyDone)
	for msg := range n.applyCh {
		if msg.CommandValid {
			err := n.fsm.Apply(msg.CommandIndex, msg.Command)
			if err != nil {
				n.logger.Printf("node %d: FATAL apply %d: %v", n.cfg.ID, msg.CommandIndex, err)
			}
			n.notifyWaiters(msg.CommandIndex, msg.CommandTerm, err)
			n.maybeSnapshot(msg.CommandIndex)
		} else if msg.SnapshotValid {
			if err := n.fsm.Restore(msg.SnapshotIndex, msg.Snapshot); err != nil {
				n.logger.Printf("node %d: FATAL restore snapshot @%d: %v", n.cfg.ID, msg.SnapshotIndex, err)
			} else {
				n.mu.Lock()
				if msg.SnapshotIndex > n.lastSnapshotAt {
					n.lastSnapshotAt = msg.SnapshotIndex
				}
				n.mu.Unlock()
			}
		}

		n.mu.Lock()
		closed := n.closed
		n.mu.Unlock()
		if closed {
			return
		}
	}
}

// notifyWaiters resolves every proposal batched into the entry at `index`.
// If a different leader's entry landed there (term mismatch), the proposals
// were lost and callers must retry against the new leader.
func (n *Node) notifyWaiters(index uint64, entryTerm uint64, applyErr error) {
	n.mu.Lock()
	wg, ok := n.waiters[index]
	if ok {
		delete(n.waiters, index)
	}
	n.mu.Unlock()
	if !ok {
		return
	}
	err := applyErr
	if err == nil && entryTerm != wg.term {
		err = errNotLeader
	}
	for _, resp := range wg.resps {
		resp <- err
	}
}

func (n *Node) maybeSnapshot(appliedIndex uint64) {
	n.mu.Lock()
	due := appliedIndex >= n.lastSnapshotAt+n.cfg.SnapshotThreshold
	n.mu.Unlock()
	if !due {
		return
	}

	data, applied, err := n.fsm.SnapshotBytes()
	if err != nil {
		n.logger.Printf("node %d: snapshot serialize: %v", n.cfg.ID, err)
		return
	}
	if err := n.rf.Snapshot(applied, data); err != nil {
		n.logger.Printf("node %d: snapshot compaction: %v", n.cfg.ID, err)
		return
	}
	n.mu.Lock()
	if applied > n.lastSnapshotAt {
		n.lastSnapshotAt = applied
	}
	n.mu.Unlock()
	n.logger.Printf("node %d: compacted raft log through index %d (%d KB snapshot)",
		n.cfg.ID, applied, len(data)/1024)
}

// --- proposals ---

var errNotLeader = errors.New("not the leader")

// propose hands one command to the batching proposer and waits until it is
// applied locally (or fails).
func (n *Node) propose(cmd Command) error {
	resp := make(chan error, 1)
	select {
	case n.proposeCh <- proposal{cmd: cmd, resp: resp}:
	default:
		return errors.New("proposal queue full")
	}
	select {
	case err := <-resp:
		return err
	case <-time.After(proposalTimeout):
		return errors.New("proposal timed out (no quorum?)")
	case <-n.stopCh:
		return errors.New("node shutting down")
	}
}

// proposer coalesces concurrently submitted commands into single Raft
// entries: one log append, one fsync, and one replication round then cover
// the whole batch. This is where most of the write throughput comes from.
func (n *Node) proposer() {
	for {
		var first proposal
		select {
		case first = <-n.proposeCh:
		case <-n.stopCh:
			return
		}

		batch := []proposal{first}
	drain:
		for len(batch) < maxProposalBatch {
			select {
			case p := <-n.proposeCh:
				batch = append(batch, p)
			default:
				break drain
			}
		}

		cmds := make([]Command, len(batch))
		for i, p := range batch {
			cmds[i] = p.cmd
		}

		index, term, isLeader := n.rf.Start(EncodeCommands(cmds))
		if !isLeader {
			for _, p := range batch {
				p.resp <- errNotLeader
			}
			continue
		}

		resps := make([]chan error, len(batch))
		for i, p := range batch {
			resps[i] = p.resp
		}
		n.mu.Lock()
		n.waiters[index] = &waiterGroup{term: term, resps: resps}
		n.mu.Unlock()
	}
}

// linearizableRead performs the read barrier: capture the commit index at
// the start of the read on the leader, and wait until the FSM has applied
// through it. Combined with the leader check this gives read-your-writes and
// monotonic semantics (a deposed leader can serve a stale read only within
// one election timeout window).
func (n *Node) linearizableRead() error {
	if _, isLeader := n.rf.GetState(); !isLeader {
		return errNotLeader
	}
	readIndex := n.rf.CommitIndex()
	deadline := time.Now().Add(readIndexTimeout)
	for n.fsm.LastApplied() < readIndex {
		if time.Now().After(deadline) {
			return errors.New("read index wait timed out")
		}
		time.Sleep(time.Millisecond)
	}
	if _, isLeader := n.rf.GetState(); !isLeader {
		return errNotLeader
	}
	return nil
}

func (n *Node) leaderHint() (uint64, string) {
	id := n.rf.LeaderID()
	if id == 0 {
		return 0, ""
	}
	return id, n.cfg.Peers[id]
}

func (n *Node) notLeaderStatus() error {
	id, addr := n.leaderHint()
	return status.Errorf(codes.FailedPrecondition,
		"not the leader; leader_id=%d leader_addr=%s", id, addr)
}

// --- KV gRPC service ---

type kvService struct {
	pb.UnimplementedKVServer
	node *Node
}

func (s *kvService) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	if len(req.Key) > 0 && req.Key[0] == 0 {
		return nil, status.Error(codes.InvalidArgument, "keys must not start with a zero byte (reserved)")
	}
	err := s.node.propose(Command{Op: OpPut, Key: req.Key, Value: req.Value})
	if errors.Is(err, errNotLeader) {
		return nil, s.node.notLeaderStatus()
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &pb.PutResponse{}, nil
}

func (s *kvService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	err := s.node.propose(Command{Op: OpDelete, Key: req.Key})
	if errors.Is(err, errNotLeader) {
		return nil, s.node.notLeaderStatus()
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &pb.DeleteResponse{}, nil
}

func (s *kvService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	if !req.AllowStale {
		if err := s.node.linearizableRead(); err != nil {
			if errors.Is(err, errNotLeader) {
				return nil, s.node.notLeaderStatus()
			}
			return nil, status.Error(codes.Unavailable, err.Error())
		}
	}
	val, found, err := s.node.fsm.Get(req.Key)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.GetResponse{Found: found, Value: val}, nil
}

func (s *kvService) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	limit := int(req.Limit)
	if limit <= 0 || limit > maxScanLimit {
		limit = maxScanLimit
	}
	if err := s.node.linearizableRead(); err != nil {
		if errors.Is(err, errNotLeader) {
			return nil, s.node.notLeaderStatus()
		}
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	pairs, err := s.node.fsm.Scan(req.StartKey, limit)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &pb.ScanResponse{}
	for _, kv := range pairs {
		resp.Pairs = append(resp.Pairs, &pb.KeyValue{Key: kv[0], Value: kv[1]})
	}
	return resp, nil
}

func (s *kvService) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	term, isLeader := s.node.rf.GetState()
	state := "Follower"
	if isLeader {
		state = "Leader"
	}
	leaderID, leaderAddr := s.node.leaderHint()
	return &pb.StatusResponse{
		NodeId:      s.node.cfg.ID,
		State:       state,
		Term:        term,
		LeaderId:    leaderID,
		LeaderAddr:  leaderAddr,
		CommitIndex: s.node.rf.CommitIndex(),
		LastApplied: s.node.fsm.LastApplied(),
	}, nil
}
