package sessiontest

import (
	"errors"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
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
