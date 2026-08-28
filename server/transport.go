package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ashuthe1/LakeStore/api/pb"
	"github.com/ashuthe1/LakeStore/raft"
)

const rpcTimeout = 3 * time.Second

// grpcTransport implements raft.Transport over gRPC, with lazily dialed,
// cached connections per peer.
type grpcTransport struct {
	mu    sync.Mutex
	addrs map[uint64]string
	conns map[uint64]*grpc.ClientConn
}

func newGRPCTransport(addrs map[uint64]string) *grpcTransport {
	return &grpcTransport{
		addrs: addrs,
		conns: make(map[uint64]*grpc.ClientConn),
	}
}

func (t *grpcTransport) client(peer uint64) (pb.RaftTransportClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[peer]; ok {
		return pb.NewRaftTransportClient(conn), nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, fmt.Errorf("unknown peer %d", peer)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[peer] = conn
	return pb.NewRaftTransportClient(conn), nil
}

func (t *grpcTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		conn.Close()
	}
	t.conns = make(map[uint64]*grpc.ClientConn)
}

func (t *grpcTransport) RequestVote(peer uint64, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := c.RequestVote(ctx, &pb.RequestVoteArgs{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *grpcTransport) AppendEntries(peer uint64, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	entries := make([]*pb.Entry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &pb.Entry{Index: e.Index, Term: e.Term, Data: e.Data}
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := c.AppendEntries(ctx, &pb.AppendEntriesArgs{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictTerm:  resp.ConflictTerm,
		ConflictIndex: resp.ConflictIndex,
	}, nil
}

func (t *grpcTransport) InstallSnapshot(peer uint64, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	// Snapshots can be large; allow extra time.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := c.InstallSnapshot(ctx, &pb.InstallSnapshotArgs{
		Term:              args.Term,
		LeaderId:          args.LeaderID,
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	})
	if err != nil {
		return nil, err
	}
	return &raft.InstallSnapshotReply{Term: resp.Term}, nil
}

// raftService is the gRPC server side: it unwraps protobuf messages and
// dispatches to the local Raft node's handlers.
type raftService struct {
	pb.UnimplementedRaftTransportServer
	rf *raft.Raft
}

func (s *raftService) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	reply := s.rf.HandleRequestVote(&raft.RequestVoteArgs{
		Term:         args.Term,
		CandidateID:  args.CandidateId,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	return &pb.RequestVoteReply{Term: reply.Term, VoteGranted: reply.VoteGranted}, nil
}

func (s *raftService) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	entries := make([]raft.Entry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = raft.Entry{Index: e.Index, Term: e.Term, Data: e.Data}
	}
	reply := s.rf.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term:         args.Term,
		LeaderID:     args.LeaderId,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	})
	return &pb.AppendEntriesReply{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictTerm:  reply.ConflictTerm,
		ConflictIndex: reply.ConflictIndex,
	}, nil
}

func (s *raftService) InstallSnapshot(ctx context.Context, args *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	reply := s.rf.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term:              args.Term,
		LeaderID:          args.LeaderId,
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	})
	return &pb.InstallSnapshotReply{Term: reply.Term}, nil
}
