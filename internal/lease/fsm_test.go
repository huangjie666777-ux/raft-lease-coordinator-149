package lease

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

var base = time.Unix(1_700_000_000, 0)

func applyCmd(t *testing.T, f *FSM, cmd Command) *Result {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	res, ok := f.Apply(&raft.Log{Data: data}).(*Result)
	if !ok {
		t.Fatalf("unexpected response type")
	}
	return res
}

func acquireAt(f *FSM, t *testing.T, res, holder string, ttl time.Duration, at time.Time) *Result {
	return applyCmd(t, f, Command{Op: OpAcquire, Resource: res, Holder: holder, TTLNanos: int64(ttl), NowNanos: at.UnixNano()})
}

func TestAcquireContentionAndFencing(t *testing.T) {
	f := NewFSM()
	r1 := acquireAt(f, t, "db", "alice", 10*time.Second, base)
	if !r1.OK || r1.Token != 1 {
		t.Fatalf("first acquire failed: %+v", r1)
	}
	// Concurrent loser: same decision instant, valid lease already held.
	r2 := acquireAt(f, t, "db", "bob", 10*time.Second, base)
	if r2.OK {
		t.Fatalf("second acquire should fail while held")
	}
	// After expiry, re-acquire must mint a strictly larger token.
	r3 := acquireAt(f, t, "db", "bob", 10*time.Second, base.Add(11*time.Second))
	if !r3.OK || r3.Token != 2 {
		t.Fatalf("post-expiry acquire: %+v", r3)
	}
	if r3.ExpiresAtNanos != base.Add(21*time.Second).UnixNano() {
		t.Fatalf("deadline not recomputed from decision instant: %+v", r3)
	}
}

func TestRenewRequiresHolderAndToken(t *testing.T) {
	f := NewFSM()
	acquireAt(f, t, "q", "alice", 5*time.Second, base)
	bad := applyCmd(t, f, Command{Op: OpRenew, Resource: "q", Holder: "alice", Token: 99, TTLNanos: int64(5 * time.Second), NowNanos: base.UnixNano()})
	if bad.OK {
		t.Fatalf("renew with wrong token must fail")
	}
	good := applyCmd(t, f, Command{Op: OpRenew, Resource: "q", Holder: "alice", Token: 1, TTLNanos: int64(5 * time.Second), NowNanos: base.Add(2 * time.Second).UnixNano()})
	if !good.OK || good.ExpiresAtNanos != base.Add(7*time.Second).UnixNano() {
		t.Fatalf("renew must extend from decision instant: %+v", good)
	}
}

func TestReleaseAndTokenBoundSurvives(t *testing.T) {
	f := NewFSM()
	acquireAt(f, t, "r", "alice", 5*time.Second, base)
	rel := applyCmd(t, f, Command{Op: OpRelease, Resource: "r", Holder: "alice", Token: 1, NowNanos: base.Add(time.Second).UnixNano()})
	if !rel.OK {
		t.Fatalf("release failed: %+v", rel)
	}
	if got := f.TokenBound("r"); got != 1 {
		t.Fatalf("token bound lost after release: %d", got)
	}
	again := acquireAt(f, t, "r", "carol", 5*time.Second, base.Add(2*time.Second))
	if !again.OK || again.Token != 2 {
		t.Fatalf("re-acquire after release must bump token: %+v", again)
	}
}

func TestStaleHolderCannotTouchNewLease(t *testing.T) {
	f := NewFSM()
	acquireAt(f, t, "s", "alice", 3*time.Second, base)
	acquireAt(f, t, "s", "bob", 3*time.Second, base.Add(4*time.Second)) // alice's lease expired
	stale := applyCmd(t, f, Command{Op: OpRelease, Resource: "s", Holder: "alice", Token: 1, NowNanos: base.Add(5 * time.Second).UnixNano()})
	if stale.OK {
		t.Fatalf("stale holder release must be rejected")
	}
	if l := f.Lookup("s", base.Add(5*time.Second)); l == nil || l.Holder != "bob" {
		t.Fatalf("bob's lease must be unaffected: %+v", l)
	}
}

func TestLookupExpiredReturnsNil(t *testing.T) {
	f := NewFSM()
	acquireAt(f, t, "x", "alice", 2*time.Second, base)
	if f.Lookup("x", base.Add(time.Second)) == nil {
		t.Fatalf("live lease must be visible")
	}
	if f.Lookup("x", base.Add(3*time.Second)) != nil {
		t.Fatalf("expired lease must read as empty")
	}
}

func TestSnapshotRestoreKeepsLeasesAndTokenBounds(t *testing.T) {
	f := NewFSM()
	acquireAt(f, t, "a", "alice", 30*time.Second, base)
	acquireAt(f, t, "b", "bob", 30*time.Second, base)
	applyCmd(t, f, Command{Op: OpRelease, Resource: "b", Holder: "bob", Token: 1, NowNanos: base.UnixNano()})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	sink := &memSink{buf: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	g := NewFSM()
	if err := g.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if l := g.Lookup("a", base.Add(time.Second)); l == nil || l.Holder != "alice" || l.Token != 1 {
		t.Fatalf("lease lost across snapshot: %+v", l)
	}
	if got := g.TokenBound("b"); got != 1 {
		t.Fatalf("token bound for released resource lost: %d", got)
	}
	next := acquireAt(g, t, "b", "carol", 30*time.Second, base.Add(time.Second))
	if !next.OK || next.Token != 2 {
		t.Fatalf("token must continue from restored bound: %+v", next)
	}
}

type memSink struct {
	buf *bytes.Buffer
}

func (m *memSink) Write(p []byte) (int, error) { return m.buf.Write(p) }
func (m *memSink) Close() error                { return nil }
func (m *memSink) ID() string                  { return "mem" }
func (m *memSink) Cancel() error               { return nil }
