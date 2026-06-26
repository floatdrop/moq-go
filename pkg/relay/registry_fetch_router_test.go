package relay

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// The router uses the session pointer only as a map key, so a bare allocation
// is a fine stand-in; the response stream is likewise only moved through the
// rendezvous, so a nil stream exercises the plumbing without a live transport.
func TestFetchRouter_RegisterThenDeliver(t *testing.T) {
	r := newFetchRouter()
	sess := new(session.Session)

	ch, cleanup := r.register(sess, 7)
	defer cleanup()

	if !r.deliver(sess, 7, nil) {
		t.Fatal("deliver to a registered key must succeed")
	}
	select {
	case <-ch:
	default:
		t.Fatal("reader did not receive the delivered stream")
	}
}

func TestFetchRouter_DeliverThenRegister(t *testing.T) {
	r := newFetchRouter()
	sess := new(session.Session)

	// Response races ahead of registration: deliver parks the stream.
	if !r.deliver(sess, 9, nil) {
		t.Fatal("deliver before register must succeed (parked)")
	}

	ch, cleanup := r.register(sess, 9)
	defer cleanup()
	select {
	case <-ch:
	default:
		t.Fatal("reader did not pick up the parked stream")
	}
}

func TestFetchRouter_DuplicateDeliverRejected(t *testing.T) {
	r := newFetchRouter()
	sess := new(session.Session)

	if !r.deliver(sess, 1, nil) {
		t.Fatal("first deliver must succeed")
	}
	if r.deliver(sess, 1, nil) {
		t.Fatal("second deliver for the same key must be rejected")
	}
}

// Distinct keys (different reqID) are independent.
func TestFetchRouter_KeysAreIndependent(t *testing.T) {
	r := newFetchRouter()
	sess := new(session.Session)

	chA, cleanupA := r.register(sess, 1)
	defer cleanupA()
	chB, cleanupB := r.register(sess, 2)
	defer cleanupB()

	r.deliver(sess, 2, nil)
	select {
	case <-chA:
		t.Fatal("stream for reqID 2 leaked to reqID 1's reader")
	default:
	}
	select {
	case <-chB:
	default:
		t.Fatal("reqID 2 reader did not receive its stream")
	}
}
