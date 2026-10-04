package lease

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Peer describes one static voting member of the cluster.
type Peer struct {
	ID       string
	RaftAddr string
	HTTPAddr string
}

// NodeConfig configures a single node process.
type NodeConfig struct {
	ID       string
	RaftAddr string
	HTTPAddr string
	DataDir  string
	Peers    []Peer // exactly the three static voters, including self
}

// Node bundles the Raft instance, its storage and the lease FSM.
type Node struct {
	cfg       NodeConfig
	Raft      *raft.Raft
	FSM       *FSM
	store     *raftboltdb.BoltStore
	transport *raft.NetworkTransport
}

// ApplyTimeout bounds how long the leader waits for majority commit.
const ApplyTimeout = 5 * time.Second

// ErrNotLeader is returned when a mutation hits a non-leader node.
var ErrNotLeader = errors.New("not the leader")

// Open starts (or restarts) a node. Bootstrapping happens only when the
// data directory holds no existing Raft state; a node with data always
// recovers from its own logs/snapshots and never re-bootstraps.
func Open(cfg NodeConfig) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.ID)
	rc.SnapshotInterval = 120 * time.Second
	rc.SnapshotThreshold = 8192
	rc.TrailingLogs = 10240

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft transport: %w", err)
	}

	store, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("bolt store: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	existing, err := raft.HasExistingState(store, store, snaps)
	if err != nil {
		return nil, err
	}
	if !existing {
		servers := make([]raft.Server, 0, len(cfg.Peers))
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:       raft.ServerID(p.ID),
				Address:  raft.ServerAddress(p.RaftAddr),
				Suffrage: raft.Voter,
			})
		}
		cluster := raft.Configuration{Servers: servers}
		if err := raft.BootstrapCluster(rc, store, store, snaps, transport, cluster); err != nil {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}

	fsm := NewFSM()
	r, err := raft.NewRaft(rc, fsm, store, store, snaps, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}
	return &Node{cfg: cfg, Raft: r, FSM: fsm, store: store, transport: transport}, nil
}

// LeaderHTTPAddr returns the HTTP address of the known leader, or "".
func (n *Node) LeaderHTTPAddr() string {
	raftAddr := string(n.Raft.Leader())
	if raftAddr == "" {
		return ""
	}
	for _, p := range n.cfg.Peers {
		if p.RaftAddr == raftAddr {
			return p.HTTPAddr
		}
	}
	return ""
}

// Propose validates, stamps and replicates a mutation. It must only be
// called on the leader; the decision instant is taken here, on the leader.
func (n *Node) Propose(op, resource, holder string, token uint64, ttl time.Duration) (*Result, error) {
	if n.Raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}
	cmd := Command{
		Op:       op,
		Resource: resource,
		Holder:   holder,
		Token:    token,
		TTLNanos: int64(ttl),
		NowNanos: time.Now().UnixNano(),
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	future := n.Raft.Apply(data, ApplyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return nil, ErrNotLeader
		}
		// e.g. commit timeout: the entry may or may not be applied later.
		return nil, fmt.Errorf("result unconfirmed: %w", err)
	}
	res, ok := future.Response().(*Result)
	if !ok {
		return nil, fmt.Errorf("unexpected FSM response %T", future.Response())
	}
	return res, nil
}

// Close shuts down networking and storage in order.
func (n *Node) Close() error {
	if err := n.Raft.Shutdown().Error(); err != nil {
		return err
	}
	n.transport.Close()
	return n.store.Close()
}
