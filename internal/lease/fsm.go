// Package lease implements a Raft-backed lease lock state machine.
// All lease decisions are made by the leader and replicated through the
// Raft log; the decision timestamp is written into the command by the
// leader so followers never read their own clocks during replay.
package lease

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// Operation kinds carried by Command.
const (
	OpAcquire = "acquire"
	OpRenew   = "renew"
	OpRelease = "release"
)

const (
	MinTTL = time.Second
	MaxTTL = 300 * time.Second
)

// Command is a replicated lease operation. Now is the decision instant
// stamped by the leader before proposing; replicas must use it verbatim.
type Command struct {
	Op       string `json:"op"`
	Resource string `json:"resource"`
	Holder   string `json:"holder"`
	Token    uint64 `json:"token"`
	TTLNanos int64  `json:"ttl_nanos"`
	NowNanos int64  `json:"now_nanos"`
}

// Result is the FSM response for an applied command.
type Result struct {
	OK             bool   `json:"ok"`
	Err            string `json:"err,omitempty"`
	Holder         string `json:"holder,omitempty"`
	Token          uint64 `json:"token,omitempty"`
	ExpiresAtNanos int64  `json:"expires_at_nanos,omitempty"`
}

func fail(format string, args ...any) *Result {
	return &Result{Err: fmt.Sprintf(format, args...)}
}

// Lease is the live lease record for one resource.
type Lease struct {
	Holder         string `json:"holder"`
	Token          uint64 `json:"token"`
	ExpiresAtNanos int64  `json:"expires_at_nanos"`
}

// FSM is the lease state machine applied on every replica.
type FSM struct {
	mu     sync.RWMutex
	leases map[string]*Lease
	tokens map[string]uint64 // per-resource fencing token high-water mark
}

func NewFSM() *FSM {
	return &FSM{
		leases: make(map[string]*Lease),
		tokens: make(map[string]uint64),
	}
}

// Apply is invoked by Raft in committed log order on every replica.
func (f *FSM) Apply(log *raft.Log) any {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fail("decode command: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Unix(0, cmd.NowNanos)
	switch cmd.Op {
	case OpAcquire:
		return f.acquire(cmd, now)
	case OpRenew:
		return f.renew(cmd, now)
	case OpRelease:
		return f.release(cmd, now)
	default:
		return fail("unknown op %q", cmd.Op)
	}
}

func (f *FSM) acquire(cmd Command, now time.Time) *Result {
	if l, ok := f.leases[cmd.Resource]; ok && now.Before(time.Unix(0, l.ExpiresAtNanos)) {
		return fail("resource %q is held by %q until %d", cmd.Resource, l.Holder, l.ExpiresAtNanos)
	}
	token := f.tokens[cmd.Resource] + 1
	f.tokens[cmd.Resource] = token
	expires := now.Add(time.Duration(cmd.TTLNanos))
	f.leases[cmd.Resource] = &Lease{
		Holder:         cmd.Holder,
		Token:          token,
		ExpiresAtNanos: expires.UnixNano(),
	}
	return &Result{OK: true, Holder: cmd.Holder, Token: token, ExpiresAtNanos: expires.UnixNano()}
}

func (f *FSM) renew(cmd Command, now time.Time) *Result {
	l, ok := f.leases[cmd.Resource]
	if !ok {
		return fail("no lease on %q", cmd.Resource)
	}
	if !now.Before(time.Unix(0, l.ExpiresAtNanos)) {
		return fail("lease on %q already expired", cmd.Resource)
	}
	if l.Holder != cmd.Holder || l.Token != cmd.Token {
		return fail("holder/token mismatch on %q", cmd.Resource)
	}
	expires := now.Add(time.Duration(cmd.TTLNanos))
	l.ExpiresAtNanos = expires.UnixNano()
	return &Result{OK: true, Holder: l.Holder, Token: l.Token, ExpiresAtNanos: expires.UnixNano()}
}

func (f *FSM) release(cmd Command, now time.Time) *Result {
	l, ok := f.leases[cmd.Resource]
	if !ok {
		return fail("no lease on %q", cmd.Resource)
	}
	if !now.Before(time.Unix(0, l.ExpiresAtNanos)) {
		return fail("lease on %q already expired", cmd.Resource)
	}
	if l.Holder != cmd.Holder || l.Token != cmd.Token {
		return fail("holder/token mismatch on %q", cmd.Resource)
	}
	// The fencing token high-water mark in f.tokens is intentionally kept.
	delete(f.leases, cmd.Resource)
	return &Result{OK: true}
}

// Lookup returns the live lease for a resource as of the caller's clock,
// or nil if absent or expired. Used for local read-only queries.
func (f *FSM) Lookup(resource string, now time.Time) *Lease {
	f.mu.RLock()
	defer f.mu.RUnlock()
	l, ok := f.leases[resource]
	if !ok || !now.Before(time.Unix(0, l.ExpiresAtNanos)) {
		return nil
	}
	cp := *l
	return &cp
}

// TokenBound returns the fencing token high-water mark for a resource.
func (f *FSM) TokenBound(resource string) uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.tokens[resource]
}

type snapshotState struct {
	Leases map[string]*Lease `json:"leases"`
	Tokens map[string]uint64 `json:"tokens"`
}

// Snapshot persists live leases plus per-resource token high-water marks.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	state := snapshotState{
		Leases: make(map[string]*Lease, len(f.leases)),
		Tokens: make(map[string]uint64, len(f.tokens)),
	}
	for k, v := range f.leases {
		cp := *v
		state.Leases[k] = &cp
	}
	for k, v := range f.tokens {
		state.Tokens[k] = v
	}
	return &fsmSnapshot{state: state}, nil
}

// Restore replaces the state machine with the snapshot contents.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var state snapshotState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases = state.Leases
	if f.leases == nil {
		f.leases = make(map[string]*Lease)
	}
	f.tokens = state.Tokens
	if f.tokens == nil {
		f.tokens = make(map[string]uint64)
	}
	return nil
}

type fsmSnapshot struct {
	state snapshotState
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.state)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
