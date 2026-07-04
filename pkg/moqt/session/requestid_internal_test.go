package session

import (
	"errors"
	"testing"
)

// TestCheckPeerRequestID pins the §10.1 receiver rules: parity by role,
// duplicates are fatal, and cross-stream delivery reordering is tolerated —
// an ID below the high-water mark is claimable exactly once.
func TestCheckPeerRequestID(t *testing.T) {
	t.Parallel()

	// We are the server, so the peer (client) sends even IDs.
	newServer := func() *Session { return &Session{role: roleServer} }

	t.Run("parity violation", func(t *testing.T) {
		t.Parallel()
		s := newServer()
		var parity *ErrRequestIDParityViolation
		if err := s.CheckPeerRequestID(3); !errors.As(err, &parity) {
			t.Fatalf("odd ID from a client: got %v, want parity violation", err)
		}
	})

	t.Run("monotonic sequence", func(t *testing.T) {
		t.Parallel()
		s := newServer()
		for _, rid := range []uint64{0, 2, 4} {
			if err := s.CheckPeerRequestID(rid); err != nil {
				t.Fatalf("CheckPeerRequestID(%d): %v", rid, err)
			}
		}
	})

	t.Run("exact duplicate is fatal", func(t *testing.T) {
		t.Parallel()
		s := newServer()
		if err := s.CheckPeerRequestID(0); err != nil {
			t.Fatalf("first ID: %v", err)
		}
		var dup *ErrDuplicateRequestID
		if err := s.CheckPeerRequestID(0); !errors.As(err, &dup) {
			t.Fatalf("duplicate: got %v, want ErrDuplicateRequestID", err)
		}
	})

	t.Run("reordered delivery is tolerated once", func(t *testing.T) {
		t.Parallel()
		s := newServer()
		// Requests 0,2,4 in flight; stream carrying 4 is delivered first,
		// then 0, then 2 — a conforming peer under QUIC reordering.
		for _, rid := range []uint64{4, 0, 2} {
			if err := s.CheckPeerRequestID(rid); err != nil {
				t.Fatalf("CheckPeerRequestID(%d): %v", rid, err)
			}
		}
		// Each gap is claimable exactly once: a second 2 is a duplicate.
		var dup *ErrDuplicateRequestID
		if err := s.CheckPeerRequestID(2); !errors.As(err, &dup) {
			t.Fatalf("refilled gap: got %v, want ErrDuplicateRequestID", err)
		}
		// And an ID that was never in a gap window is a duplicate too...
		if err := s.CheckPeerRequestID(4); !errors.As(err, &dup) {
			t.Fatalf("high-water reuse: got %v, want ErrDuplicateRequestID", err)
		}
		// ...while fresh higher IDs keep working.
		if err := s.CheckPeerRequestID(6); err != nil {
			t.Fatalf("CheckPeerRequestID(6): %v", err)
		}
	})

	t.Run("gap tracking is bounded, newest kept", func(t *testing.T) {
		t.Parallel()
		s := newServer()
		if err := s.CheckPeerRequestID(0); err != nil {
			t.Fatalf("first ID: %v", err)
		}
		// Jump far beyond the cap: only the newest maxTrackedRequestIDGaps
		// gaps stay claimable.
		jump := uint64(2 * (maxTrackedRequestIDGaps + 10))
		if err := s.CheckPeerRequestID(jump); err != nil {
			t.Fatalf("CheckPeerRequestID(%d): %v", jump, err)
		}
		if err := s.CheckPeerRequestID(jump - 2); err != nil {
			t.Fatalf("newest gap must be claimable: %v", err)
		}
		var dup *ErrDuplicateRequestID
		if err := s.CheckPeerRequestID(2); !errors.As(err, &dup) {
			t.Fatalf("evicted oldest gap: got %v, want ErrDuplicateRequestID", err)
		}
	})
}

// TestCheckPeerRequestID_EvictionKeepsNewestAcrossJumps pins the cap policy
// over MULTIPLE jumps: new gaps are always newer than every existing entry,
// so when the cap forces a choice the OLDEST entries are evicted — a fresh
// one-step reorder right after a large jump must stay tolerated, while an
// arrival for an evicted ancient gap reads as a duplicate.
func TestCheckPeerRequestID_EvictionKeepsNewestAcrossJumps(t *testing.T) {
	t.Parallel()
	s := &Session{role: roleServer}

	// First jump fills the cap exactly: gaps 0..2*(cap-1).
	first := uint64(2 * maxTrackedRequestIDGaps)
	if err := s.CheckPeerRequestID(first); err != nil {
		t.Fatalf("CheckPeerRequestID(%d): %v", first, err)
	}
	// Second, small jump: skips exactly one ID (first+2). The cap is full,
	// so the oldest entry (0) must be evicted to keep this newest gap.
	if err := s.CheckPeerRequestID(first + 4); err != nil {
		t.Fatalf("CheckPeerRequestID(%d): %v", first+4, err)
	}
	if err := s.CheckPeerRequestID(first + 2); err != nil {
		t.Fatalf("fresh one-step reorder after a full cap must stay tolerated: %v", err)
	}
	var dup *ErrDuplicateRequestID
	if err := s.CheckPeerRequestID(0); !errors.As(err, &dup) {
		t.Fatalf("evicted oldest gap: got %v, want ErrDuplicateRequestID", err)
	}
	// A still-tracked (non-evicted) old gap remains claimable.
	if err := s.CheckPeerRequestID(2); err != nil {
		t.Fatalf("tracked gap must stay claimable: %v", err)
	}
}
