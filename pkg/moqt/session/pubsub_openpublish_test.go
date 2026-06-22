package session_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// openPairWithLimits performs the SETUP handshake over a credit-capped conn
// pair. aBidiLimit caps the client's outbound bidi-stream credit (the SETUP
// control stream is unidirectional, so it is unaffected by the cap). A
// negative limit means unlimited.
func openPairWithLimits(t *testing.T, aBidiLimit int) (*session.Session, *session.Session) {
	t.Helper()
	ctx := t.Context()
	aConn, bConn := sessiontest.NewConnPairWithLimits(aBidiLimit, -1)

	var (
		wg           sync.WaitGroup
		aSess, bSess *session.Session
		aErr, bErr   error
	)
	wg.Go(func() { aSess, aErr = session.Client(ctx, aConn) })
	wg.Go(func() { bSess, bErr = session.Server(ctx, bConn) })
	wg.Wait()
	if aErr != nil {
		t.Fatalf("client Open: %v", aErr)
	}
	if bErr != nil {
		t.Fatalf("server Open: %v", bErr)
	}
	t.Cleanup(func() {
		_ = aSess.Close(moqt.SessionNoError, "test cleanup")
		_ = bSess.Close(moqt.SessionNoError, "test cleanup")
	})
	return aSess, bSess
}

// TestOpenPublish_SuccessDeliversPublish verifies the happy path: OpenPublish
// assigns a Request ID, writes PUBLISH as the stream's first message, and the
// peer accepts a bidi request carrying that exact PUBLISH.
func TestOpenPublish_SuccessDeliversPublish(t *testing.T) {
	t.Parallel()
	client, server := openPairWithLimits(t, -1)

	var (
		wg  sync.WaitGroup
		req *session.Request
		err error
	)
	wg.Go(func() { req, err = server.AcceptRequest(t.Context()) })

	m := &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: 7,
	}
	stream, openErr := client.OpenPublish(m)
	if openErr != nil {
		t.Fatalf("OpenPublish: %v", openErr)
	}
	defer stream.Close()
	// Client Request IDs are even (§10.1); the first allocation is 0.
	if m.RequestID%2 != 0 {
		t.Fatalf("client Request ID %d is not even", m.RequestID)
	}

	wg.Wait()
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	pub, ok := req.First.(*message.Publish)
	if !ok {
		t.Fatalf("server got %T, want *message.Publish", req.First)
	}
	if string(pub.Name) != "cam1" || pub.TrackAlias != 7 {
		t.Fatalf("server got Publish{Name:%q, Alias:%d}, want {cam1, 7}", pub.Name, pub.TrackAlias)
	}
}

// TestOpenPublish_ExhaustedCreditReturnsErrNoStreamCredit pins the
// PUBLISH_BLOCKED trigger: with the client's bidi credit used up, OpenPublish
// returns session.ErrNoStreamCredit rather than blocking.
func TestOpenPublish_ExhaustedCreditReturnsErrNoStreamCredit(t *testing.T) {
	t.Parallel()
	client, server := openPairWithLimits(t, 1)

	// Drain accepts so the first (successful) open's delivery never backs up.
	go func() {
		for {
			if _, err := server.AcceptRequest(t.Context()); err != nil {
				return
			}
		}
	}()

	// First publish consumes the single unit of credit.
	first := &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	}
	s1, err := client.OpenPublish(first)
	if err != nil {
		t.Fatalf("OpenPublish #0: %v", err)
	}
	defer s1.Close()
	firstID := first.RequestID

	// Second publish: credit exhausted → ErrNoStreamCredit, no ID consumed.
	second := &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam2"),
	}
	_, err = client.OpenPublish(second)
	if !errors.Is(err, session.ErrNoStreamCredit) {
		t.Fatalf("OpenPublish #1 err = %v, want ErrNoStreamCredit", err)
	}

	// §6.1: a blocked attempt MUST NOT consume a Request ID. The next
	// successful allocation (via a plain AllocRequestID) must be exactly
	// firstID+2, proving the blocked OpenPublish left the sequence untouched.
	if got := client.AllocRequestID(); got != firstID+2 {
		t.Fatalf("Request ID after blocked OpenPublish = %d, want %d (firstID %d + 2)",
			got, firstID+2, firstID)
	}
}
