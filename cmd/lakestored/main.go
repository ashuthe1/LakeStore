// lakestored is the LakeStore server daemon: one Raft-replicated KV node.
//
// Example 3-node local cluster:
//
//	lakestored --id 1 --listen 127.0.0.1:7101 --data-dir ./data/node1 \
//	    --peers 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103
//	lakestored --id 2 --listen 127.0.0.1:7102 --data-dir ./data/node2 \
//	    --peers 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103
//	lakestored --id 3 --listen 127.0.0.1:7103 --data-dir ./data/node3 \
//	    --peers 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/ashuthe1/LakeStore/server"
)

func parsePeers(s string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad peer %q (want id=host:port)", part)
		}
		id, err := strconv.ParseUint(kv[0], 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("bad peer id %q (must be integer >= 1)", kv[0])
		}
		peers[id] = kv[1]
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("no peers given")
	}
	return peers, nil
}

func main() {
	var (
		id            = flag.Uint64("id", 0, "node ID (>= 1, unique in the cluster)")
		listen        = flag.String("listen", "", "gRPC listen address (host:port)")
		dataDir       = flag.String("data-dir", "", "directory for engine + raft state")
		peersFlag     = flag.String("peers", "", "cluster membership: id=addr,id=addr,...")
		snapThreshold = flag.Uint64("snapshot-threshold", 8192, "raft entries between log compactions")
	)
	flag.Parse()

	if *id == 0 || *listen == "" || *dataDir == "" || *peersFlag == "" {
		flag.Usage()
		os.Exit(2)
	}
	peers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("--peers: %v", err)
	}
	if _, ok := peers[*id]; !ok {
		log.Fatalf("--peers must include this node's own id=%d", *id)
	}

	logger := log.New(os.Stderr, fmt.Sprintf("[lakestored %d] ", *id), log.LstdFlags|log.Lmsgprefix)

	node, err := server.NewNode(server.Config{
		ID:                *id,
		ListenAddr:        *listen,
		Peers:             peers,
		DataDir:           *dataDir,
		SnapshotThreshold: *snapThreshold,
		Logger:            logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Printf("shutting down")
	node.Close()
}
