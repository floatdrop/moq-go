package sessiontest

import (
	"errors"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/internal/conntest"
)

// TestOpenStream_UnlimitedByDefault verifies the default NewConnPair grants
// unlimited bidi-stream credit: repeated non-blocking OpenStream calls all
// succeed. The peer accepts each stream so the in-process delivery channel
// (a bounded buffer) never backs up — this isolates the credit semantics from
// the pipe's channel capacity.
func TestOpenStream_UnlimitedByDefault(t *testing.T) {
	t.Parallel()
	a, b := NewConnPair()
	for i := range 8 {
		if _, err := a.OpenStream(); err != nil {
			t.Fatalf("OpenStream #%d: %v", i, err)
		}
		if _, err := b.AcceptStream(t.Context()); err != nil {
			t.Fatalf("AcceptStream #%d: %v", i, err)
		}
	}
}

// TestOpenStream_ExhaustsCreditThenErrNoStreamCredit pins the PUBLISH_SKIPPED
// trigger: once the per-endpoint bidi cap is used up, OpenStream returns
// session.ErrNoStreamCredit rather than succeeding or blocking.
func TestOpenStream_ExhaustsCreditThenErrNoStreamCredit(t *testing.T) {
	t.Parallel()
	a, _ := NewConnPairWithLimits(2 /*a*/, -1 /*b*/)

	if _, err := a.OpenStream(); err != nil {
		t.Fatalf("OpenStream #0: %v", err)
	}
	if _, err := a.OpenStream(); err != nil {
		t.Fatalf("OpenStream #1: %v", err)
	}
	_, err := a.OpenStream()
	if !errors.Is(err, session.ErrNoStreamCredit) {
		t.Fatalf("OpenStream #2 err = %v, want ErrNoStreamCredit", err)
	}
}

// TestOpenStream_CreditIsPerEndpoint verifies a's cap does not constrain b.
func TestOpenStream_CreditIsPerEndpoint(t *testing.T) {
	t.Parallel()
	a, b := NewConnPairWithLimits(1, -1)

	if _, err := a.OpenStream(); err != nil {
		t.Fatalf("a.OpenStream #0: %v", err)
	}
	if _, err := a.OpenStream(); !errors.Is(err, session.ErrNoStreamCredit) {
		t.Fatalf("a.OpenStream #1 err = %v, want ErrNoStreamCredit", err)
	}
	// b is unlimited.
	for i := range 3 {
		if _, err := b.OpenStream(); err != nil {
			t.Fatalf("b.OpenStream #%d: %v", i, err)
		}
	}
}

// TestConnConformance holds the in-process adapter to the same [session.Conn]
// contract as the two real transports. It is the one the hermetic suite drives
// everywhere else, so a divergence here means every other test in the tree is
// asserting against semantics the real transports do not have.
func TestConnConformance(t *testing.T) {
	conntest.RunSuite(t, conntest.Suite{
		SupportsBidiLimit: true,
		NewPair: func(_ *testing.T, bidiLimit int64) (session.Conn, session.Conn) {
			// The suite's 0 means "no cap"; NewConnPairWithLimits spells that
			// as a negative limit and would read 0 as "no streams at all".
			limit := int(bidiLimit)
			if limit <= 0 {
				limit = -1
			}
			return NewConnPairWithLimits(limit, -1)
		},
	})
}
