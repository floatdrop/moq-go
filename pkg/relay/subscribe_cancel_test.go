package relay_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// A subscriber that cancels its SUBSCRIBE gets the subscription's open
// streams reset: §5.1.1 "It MUST reset any open streams associated with the
// SUBSCRIBE", and §5.1.3.1 "When the subscription is cancelled, the publisher
// MUST reset any open fill fetch streams".

// readEnd reads next until it fails, returning that error, or fails the test
// after 2s.
func readEnd(t *testing.T, next func() error) error {
	t.Helper()
	end := make(chan error, 1)
	go func() {
		for {
			if err := next(); err != nil {
				end <- err
				return
			}
		}
	}()
	select {
	case err := <-end:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("the stream stayed open after the subscription was cancelled")
		return nil
	}
}

// subClosedMetrics signals each SubscriptionClosed, which the relay reports
// after it has acted on a subscription's end.
type subClosedMetrics struct {
	relay.NopMetrics

	closed chan struct{}
}

func (m *subClosedMetrics) SubscriptionClosed(relay.TrackRef) { m.closed <- struct{}{} }

// newCancelTestRelay is [newCam1Publisher] on a relay whose closed channel
// receives once for each subscription it has finished.
func newCancelTestRelay(t *testing.T) (pubSess *session.Session, alias uint64, closed <-chan struct{}) {
	t.Helper()
	m := &subClosedMetrics{closed: make(chan struct{}, 8)}
	pubSess, teardown := connectRelay(t, relay.Config{Metrics: m})
	t.Cleanup(teardown)
	alias = 7
	publishVideoTrackProps(t, pubSess, "cam1", alias, nil)
	return pubSess, alias, m.closed
}

// cancelAndAwait cancels the subscription and waits for the relay to finish
// it, so what the test does next cannot overtake the cancellation.
func cancelAndAwait(t *testing.T, cancel func(), closed <-chan struct{}) {
	t.Helper()
	cancel()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("the relay did not finish the cancelled subscription")
	}
}

// subscribeVia subscribes sess to video/cam1 with params, directly or as the
// subscription a SUBSCRIBE_TRACKS on video gets through a forwarded PUBLISH,
// and returns what cancels it.
type subscribeVia func(t *testing.T, sess *session.Session, params ...message.Parameter) (cancel func())

var subscribeVias = []struct {
	name string
	sub  subscribeVia
}{
	{"SUBSCRIBE", func(t *testing.T, sess *session.Session, params ...message.Parameter) func() {
		sub := subscribeCam1(t, sess, params...)
		return func() { _ = sub.Close() }
	}},
	{"SUBSCRIBE_TRACKS", func(t *testing.T, sess *session.Session, params ...message.Parameter) func() {
		reqs := forwardedPublishes(t, sess)
		subscribeTracks(t, sess, ns("video"), params...)
		in := acceptForwarded(t, awaitForwarded(t, reqs))
		return func() { _ = in.Close() }
	}},
}

// TestSubscribe_CancelResetsOpenSubgroup: the subgroup stream the relay has
// open for a cancelled subscription is reset, whether the publisher goes on
// writing to it or not.
func TestSubscribe_CancelResetsOpenSubgroup(t *testing.T) {
	t.Parallel()
	for _, via := range subscribeVias {
		for _, continues := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/publisher continues=%v", via.name, continues), func(t *testing.T) {
				t.Parallel()
				pubSess, alias, closed := newCancelTestRelay(t)
				subSess := dialAnotherClient(t, pubSess)
				cancel := via.sub(t, subSess)

				sg, err := openSubgroupWaiting(t, pubSess, subgroupHeader(alias, 0))
				if err != nil {
					t.Fatalf("OpenSubgroup: %v", err)
				}
				wrote := make(chan struct{})
				go func() {
					defer close(wrote)
					_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
				}()
				ds, err := subSess.AcceptDataStream(t.Context())
				if err != nil {
					t.Fatalf("AcceptDataStream: %v", err)
				}
				in := ds.(*session.IncomingSubgroupStream)
				if _, err := in.ReadDecoded(); err != nil {
					t.Fatalf("ReadDecoded: %v", err)
				}

				<-wrote
				cancelAndAwait(t, cancel, closed)
				if continues {
					go func() {
						for range 3 {
							if sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}) != nil {
								return
							}
						}
						_ = sg.Close()
					}()
				}
				if err := readEnd(t, func() error { _, err := in.ReadDecoded(); return err }); errors.Is(err, io.EOF) {
					t.Fatal("the subgroup stream was FINed after the subscription was cancelled, want a reset")
				}
			})
		}
	}
}

// TestSubscribe_CancelResetsFill: a fill fetch stream still being written
// when its subscription is cancelled is reset, not completed.
func TestSubscribe_CancelResetsFill(t *testing.T) {
	t.Parallel()
	for _, via := range subscribeVias {
		t.Run(via.name, func(t *testing.T) {
			t.Parallel()
			pubSess, alias, closed := newCancelTestRelay(t)
			sg, err := openSubgroupWaiting(t, pubSess, subgroupHeader(alias, 0))
			if err != nil {
				t.Fatalf("OpenSubgroup: %v", err)
			}
			// Enough bytes that the fill cannot be written ahead of the reader.
			payload := bytes.Repeat([]byte("x"), 4096)
			for range 32 {
				if err := sg.WriteObject(&message.SubgroupObject{Payload: payload}); err != nil {
					t.Fatalf("WriteObject: %v", err)
				}
			}
			if err := sg.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			time.Sleep(50 * time.Millisecond)

			subSess := dialAnotherClient(t, pubSess)
			cancel := via.sub(t, subSess,
				message.NextObjectFilter(),
				message.FillParametersParam(message.Parameters{message.UnfilteredFilter()}),
			)
			ds, err := subSess.AcceptDataStream(t.Context())
			if err != nil {
				t.Fatalf("AcceptDataStream: %v", err)
			}
			fs, ok := ds.(*session.IncomingFetchStream)
			if !ok {
				t.Fatalf("got %T, want the fill fetch stream", ds)
			}
			if _, err := fs.ReadDecoded(); err != nil {
				t.Fatalf("ReadDecoded: %v", err)
			}

			cancelAndAwait(t, cancel, closed)
			// The fill is reset from a goroutine the cancellation starts; not
			// reading meanwhile holds the writer on its next Object, which the
			// synchronous test transport would otherwise let it race past.
			time.Sleep(100 * time.Millisecond)
			if err := readEnd(t, func() error { _, err := fs.ReadDecoded(); return err }); errors.Is(err, io.EOF) {
				t.Fatal("the fill fetch stream was completed after the subscription was cancelled, want a reset")
			}
		})
	}
}
