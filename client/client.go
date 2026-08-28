// Package client is a leader-following Go client for a LakeStore cluster.
// Writes and consistent reads are routed to the leader automatically; on
// leadership changes the client re-discovers the leader and retries.
package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/ashuthe1/LakeStore/api/pb"
)

const (
	requestTimeout = 5 * time.Second
	maxRetries     = 12
	retryBackoff   = 250 * time.Millisecond
)

// Client talks to a LakeStore cluster.
type Client struct {
	mu     sync.Mutex
	addrs  []string // seed addresses
	leader string   // last known leader address ("" = unknown)
	conns  map[string]*grpc.ClientConn
}

// New creates a client from a list of node addresses (any subset works;
// the leader is discovered automatically).
func New(addrs []string) *Client {
	return &Client{
		addrs: append([]string(nil), addrs...),
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
	c.conns = make(map[string]*grpc.ClientConn)
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

func (c *Client) kv(addr string) (pb.KVClient, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewKVClient(conn), nil
}

// target returns the current best address to try.
func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	return c.addrs[rand.Intn(len(c.addrs))]
}

func (c *Client) noteLeader(addr string) {
	c.mu.Lock()
	c.leader = addr
	c.mu.Unlock()
}

func (c *Client) forgetLeader() {
	c.mu.Lock()
	c.leader = ""
	c.mu.Unlock()
}

// parseLeaderHint extracts "leader_addr=..." from a not-leader error.
func parseLeaderHint(msg string) string {
	const marker = "leader_addr="
	i := strings.Index(msg, marker)
	if i < 0 {
		return ""
	}
	addr := msg[i+len(marker):]
	if j := strings.IndexAny(addr, " ,;"); j >= 0 {
		addr = addr[:j]
	}
	return strings.TrimSpace(addr)
}

// do runs op against the leader, following redirects and retrying through
// elections.
func (c *Client) do(ctx context.Context, op func(ctx context.Context, kv pb.KVClient) error) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		addr := c.target()
		kv, err := c.kv(addr)
		if err != nil {
			lastErr = err
			c.forgetLeader()
			time.Sleep(retryBackoff)
			continue
		}

		opCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		err = op(opCtx, kv)
		cancel()

		if err == nil {
			c.noteLeader(addr)
			return nil
		}
		lastErr = err

		st, ok := status.FromError(err)
		if ok && st.Code() == codes.FailedPrecondition {
			// Not the leader: follow the hint if present.
			if hint := parseLeaderHint(st.Message()); hint != "" {
				c.noteLeader(hint)
			} else {
				c.forgetLeader()
			}
			time.Sleep(retryBackoff / 5)
			continue
		}
		if ok && (st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded) {
			// Node down or election in progress: try another node.
			c.forgetLeader()
			time.Sleep(retryBackoff)
			continue
		}
		return err // real error (InvalidArgument etc.)
	}
	return fmt.Errorf("cluster unavailable after %d attempts: %w", maxRetries, lastErr)
}

// Put writes a key-value pair (replicated, durable on commit).
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.do(ctx, func(ctx context.Context, kv pb.KVClient) error {
		_, err := kv.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		return err
	})
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key []byte) error {
	return c.do(ctx, func(ctx context.Context, kv pb.KVClient) error {
		_, err := kv.Delete(ctx, &pb.DeleteRequest{Key: key})
		return err
	})
}

// ErrNotFound is returned by Get for missing keys.
var ErrNotFound = errors.New("key not found")

// Get reads a key from the leader (consistent read).
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	return c.get(ctx, key, false)
}

// GetStale reads from any reachable node without the leader barrier.
func (c *Client) GetStale(ctx context.Context, key []byte) ([]byte, error) {
	return c.get(ctx, key, true)
}

func (c *Client) get(ctx context.Context, key []byte, stale bool) ([]byte, error) {
	var out []byte
	var found bool
	err := c.do(ctx, func(ctx context.Context, kv pb.KVClient) error {
		resp, err := kv.Get(ctx, &pb.GetRequest{Key: key, AllowStale: stale})
		if err != nil {
			return err
		}
		found = resp.Found
		out = resp.Value
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}
	return out, nil
}

// Scan returns up to limit pairs starting at startKey, in key order.
func (c *Client) Scan(ctx context.Context, startKey []byte, limit int) ([][2][]byte, error) {
	var out [][2][]byte
	err := c.do(ctx, func(ctx context.Context, kv pb.KVClient) error {
		resp, err := kv.Scan(ctx, &pb.ScanRequest{StartKey: startKey, Limit: uint32(limit)})
		if err != nil {
			return err
		}
		out = out[:0]
		for _, p := range resp.Pairs {
			out = append(out, [2][]byte{p.Key, p.Value})
		}
		return nil
	})
	return out, err
}

// Status queries one specific node (not leader-routed).
func (c *Client) Status(ctx context.Context, addr string) (*pb.StatusResponse, error) {
	kv, err := c.kv(addr)
	if err != nil {
		return nil, err
	}
	opCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return kv.Status(opCtx, &pb.StatusRequest{})
}

// Addrs returns the seed addresses.
func (c *Client) Addrs() []string { return c.addrs }
