// Command lockd runs one node of the three-node Raft lease lock cluster.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/huangjie666777-ux/raft-lease-coordinator-149/internal/lease"
)

// peersFlag parses "id=raftAddr@httpAddr,id=raftAddr@httpAddr,...".
func parsePeers(spec string) ([]lease.Peer, error) {
	var peers []lease.Peer
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, rest, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("peer %q missing '='", part)
		}
		raftAddr, httpAddr, ok := strings.Cut(rest, "@")
		if !ok {
			return nil, fmt.Errorf("peer %q missing '@' between raft and http addr", part)
		}
		peers = append(peers, lease.Peer{ID: id, RaftAddr: raftAddr, HTTPAddr: httpAddr})
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("no peers configured")
	}
	return peers, nil
}

func main() {
	var (
		id       = flag.String("id", "", "node ID (must match one of -peers)")
		raftAddr = flag.String("raft-addr", "", "Raft TCP bind address, host:port")
		httpAddr = flag.String("http-addr", "", "HTTP API bind address, host:port")
		dataDir  = flag.String("data-dir", "", "node-private data directory")
		peers    = flag.String("peers", "", "static voters: id=raftAddr@httpAddr,id=raftAddr@httpAddr,...")
	)
	flag.Parse()

	if *id == "" || *raftAddr == "" || *httpAddr == "" || *dataDir == "" || *peers == "" {
		flag.Usage()
		os.Exit(2)
	}
	peerList, err := parsePeers(*peers)
	if err != nil {
		log.Fatalf("invalid -peers: %v", err)
	}
	found := false
	for _, p := range peerList {
		if p.ID == *id {
			found = true
			break
		}
	}
	if !found {
		log.Fatalf("node id %q not present in -peers", *id)
	}

	node, err := lease.Open(lease.NodeConfig{
		ID:       *id,
		RaftAddr: *raftAddr,
		HTTPAddr: *httpAddr,
		DataDir:  *dataDir,
		Peers:    peerList,
	})
	if err != nil {
		log.Fatalf("open node: %v", err)
	}
	srv, err := lease.ServeHTTP(node)
	if err != nil {
		node.Close()
		log.Fatalf("http server: %v", err)
	}
	log.Printf("node %s up: raft=%s http=%s data=%s", *id, *raftAddr, *httpAddr, *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("node %s shutting down", *id)
	if err := srv.Close(); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if err := node.Close(); err != nil {
		log.Printf("raft shutdown: %v", err)
	}
}
