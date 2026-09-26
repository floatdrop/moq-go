package relay_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// Shared helpers for the relay_test package: they drive clients of a relay
// started by connectRelay (harness_test.go). FETCH helpers are in
// helpers_fetch_test.go.

// ns builds a Track Namespace from its fields.
func ns(fields ...string) wire.TrackNamespace {
	out := make(wire.TrackNamespace, len(fields))
	for i, f := range fields {
		out[i] = []byte(f)
	}
	return out
}

// trackProp is one integer Track Property, for PUBLISH or SUBSCRIBE_OK.
func trackProp(typ, v uint64) []wire.KVPair {
	return []wire.KVPair{{Type: typ, IntVal: v}}
}

// dynamicGroupsProperties is Track Properties carrying DYNAMIC_GROUPS = value.
func dynamicGroupsProperties(value uint64) []byte {
	return message.AppendTrackProperties(trackProp(message.PropertyDynamicGroups, value))
}

// Publishing.

// publishVideoTrack PUBLISHes video/<name> on alias with the given Message
// Parameters; the publication closes at cleanup.
func publishVideoTrack(
	t *testing.T,
	sess *session.Session,
	name string,
	alias uint64,
	params ...message.Parameter,
) *session.Publication {
	t.Helper()
	return publish(t, sess, &message.Publish{
		Namespace: ns("video"), Name: []byte(name), TrackAlias: alias, Parameters: params,
	})
}

// publishVideoTrackProps is [publishVideoTrack] with Track Properties.
func publishVideoTrackProps(
	t *testing.T,
	sess *session.Session,
	name string,
	alias uint64,
	props []wire.KVPair,
) *session.Publication {
	t.Helper()
	return publish(t, sess, &message.Publish{
		Namespace: ns("video"), Name: []byte(name), TrackAlias: alias,
		TrackProperties: message.AppendTrackProperties(props),
	})
}

// publish sends m from sess, failing the test on error; the publication closes
// at cleanup.
func publish(t *testing.T, sess *session.Session, m *message.Publish) *session.Publication {
	t.Helper()
	p, err := sess.Publish(t.Context(), m)
	if err != nil {
		t.Fatalf("Publish %s: %v", m.Name, err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// newCam1Publisher starts a relay and PUBLISHes video/cam1 on it with the given
// Track Properties, returning the publisher session and its Track Alias.
func newCam1Publisher(t *testing.T, props []wire.KVPair) (*session.Session, uint64) {
	t.Helper()
	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	const alias = uint64(7)
	publishVideoTrackProps(t, pubSess, "cam1", alias, props)
	return pubSess, alias
}

// publishAndCache is [newCam1Publisher] plus a drained live subscriber, so the
// relay forwards and caches what is published. It returns the publisher, the
// subscriber and the publisher's Track Alias.
func publishAndCache(t *testing.T) (*session.Session, *session.Session, uint64) {
	t.Helper()
	pubSess, alias := newCam1Publisher(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess)
	go drainAll(t.Context(), subSess)
	return pubSess, subSess, alias
}

// Objects.

// writeSubgroup writes n Objects (payloads "A", "B", ...) on a new subgroup
// stream with hdr, then FINs it.
func writeSubgroup(sess *session.Session, hdr message.SubgroupHeader, n int) error {
	sg, err := sess.OpenSubgroup(hdr)
	if err != nil {
		return fmt.Errorf("OpenSubgroup g=%d: %w", hdr.GroupID, err)
	}
	for i := range n {
		if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte{byte('A' + i)}}); err != nil {
			return fmt.Errorf("WriteObject g=%d #%d: %w", hdr.GroupID, i, err)
		}
	}
	if err := sg.Close(); err != nil {
		return fmt.Errorf("Close g=%d: %w", hdr.GroupID, err)
	}
	return nil
}

// subgroupHeader is the header of subgroup 0 of group on alias, inheriting the
// publisher priority.
func subgroupHeader(alias, group uint64) message.SubgroupHeader {
	return message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias, GroupID: group}
}

// publishObjects writes Objects 0..n-1 of group on alias as one subgroup,
// failing the test on error.
func publishObjects(t *testing.T, sess *session.Session, alias, group uint64, n int) {
	t.Helper()
	if err := writeSubgroup(sess, subgroupHeader(alias, group), n); err != nil {
		t.Fatal(err)
	}
}

// sendObjects is [publishObjects] for use off the test goroutine, where the
// synchronous in-process transport blocks until the relay reads; errors are
// dropped.
func sendObjects(sess *session.Session, alias, group uint64, n int) {
	_ = writeSubgroup(sess, subgroupHeader(alias, group), n)
}

// publishSubgroupWith writes objects Objects to group of pub's track on one
// subgroup stream from a goroutine, calling between(i) before Object i, then
// FINs it.
func publishSubgroupWith(t *testing.T, pub *session.Publication, group uint64, objects int, between func(i int)) {
	t.Helper()
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: group})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	go func() {
		for i := range objects {
			if between != nil {
				between(i)
			}
			if sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}) != nil {
				return
			}
		}
		_ = sg.Close()
	}()
}

// openSubgroupWaiting opens a subgroup stream, waiting up to 5s out
// [session.ErrNoStreamCredit]: sessiontest's bounded stream queue reports a
// full queue that way, and the relay drains it on its own.
func openSubgroupWaiting(
	t *testing.T,
	sess *session.Session,
	hdr message.SubgroupHeader,
) (*session.OutgoingSubgroupStream, error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		sg, err := sess.OpenSubgroup(hdr)
		if !errors.Is(err, session.ErrNoStreamCredit) {
			return sg, err
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Subscribing.

// subscribeCam1 SUBSCRIBEs sess to video/cam1; the subscription closes at
// cleanup.
func subscribeCam1(t *testing.T, sess *session.Session, params ...message.Parameter) *session.Subscription {
	t.Helper()
	sub, err := sess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"), Name: []byte("cam1"), Parameters: params,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

// newCam1Subscriber dials a new client into via's relay and SUBSCRIBEs it to
// video/cam1.
func newCam1Subscriber(t *testing.T, via *session.Session, params ...message.Parameter) *session.Session {
	t.Helper()
	sess := dialAnotherClient(t, via)
	subscribeCam1(t, sess, params...)
	return sess
}

// subscribeTracks sends SUBSCRIBE_TRACKS for prefix and returns its request
// stream; it closes at cleanup.
func subscribeTracks(
	t *testing.T,
	sess *session.Session,
	prefix wire.TrackNamespace,
	params ...message.Parameter,
) session.Stream {
	t.Helper()
	ts, err := sess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: prefix, Parameters: params,
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	return ts.Stream
}

// forwardedPublishes delivers the PUBLISHes the relay forwards to sess.
func forwardedPublishes(t *testing.T, sess *session.Session) <-chan *session.Request {
	t.Helper()
	out := make(chan *session.Request, 4)
	go func() {
		for {
			r, err := sess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			if _, ok := r.First.(*message.Publish); ok {
				out <- r
			}
		}
	}()
	return out
}

// awaitForwarded returns the next forwarded PUBLISH, failing after 2s.
func awaitForwarded(t *testing.T, reqs <-chan *session.Request) *session.Request {
	t.Helper()
	select {
	case r := <-reqs:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("no PUBLISH forwarded to the SUBSCRIBE_TRACKS holder")
	}
	return nil
}

// requireNoForward fails if a PUBLISH is forwarded within 300ms.
func requireNoForward(t *testing.T, reqs <-chan *session.Request, what string) {
	t.Helper()
	select {
	case r := <-reqs:
		p := r.First.(*message.Publish)
		t.Fatalf("%s: forwarded PUBLISH for %s", what, p.Name)
	case <-time.After(300 * time.Millisecond):
	}
}

// acceptForwarded replies PUBLISH_OK to a forwarded PUBLISH, failing rather
// than hanging if the relay never reads the reply.
func acceptForwarded(t *testing.T, r *session.Request) *session.IncomingPublication {
	t.Helper()
	type result struct {
		in  *session.IncomingPublication
		err error
	}
	done := make(chan result, 1)
	go func() {
		in, err := r.AcceptPublish()
		done <- result{in, err}
	}()
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("AcceptPublish: %v", res.err)
		}
		return res.in
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never read the PUBLISH_OK")
	}
	return nil
}

// Receiving.

// awaitSubgroupObject reports whether the next data stream sess accepts within
// the deadline is a subgroup stream carrying an Object.
func awaitSubgroupObject(t *testing.T, sess *session.Session, within time.Duration) bool {
	t.Helper()
	got := make(chan bool, 1)
	go func() {
		ds, err := sess.AcceptDataStream(t.Context())
		if err != nil {
			got <- false
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			got <- false
			return
		}
		_, err = sg.ReadObject()
		got <- err == nil
	}()
	select {
	case ok := <-got:
		return ok
	case <-time.After(within):
		return false
	}
}

// awaitObjectOn reads the first Object of a subgroup stream on alias, skipping
// streams for other aliases and failing after 2s.
func awaitObjectOn(t *testing.T, sess *session.Session, alias uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for {
		ds, err := sess.AcceptDataStream(ctx)
		if err != nil {
			t.Fatalf("no Object delivered on alias %d: %v", alias, err)
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok || sg.Header.TrackAlias != alias {
			continue
		}
		if _, err := sg.ReadObject(); err != nil {
			t.Fatalf("ReadObject: %v", err)
		}
		return
	}
}

// tryAcceptDataStream waits up to d for a data stream, reporting whether one
// arrived.
func tryAcceptDataStream(t *testing.T, sess *session.Session, d time.Duration) (session.DataStream, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	ds, err := sess.AcceptDataStream(ctx)
	if err != nil {
		return nil, false
	}
	return ds, true
}

// drainAll reads and discards every data stream on sess until ctx ends, so the
// relay never blocks on an unread subscriber.
func drainAll(ctx context.Context, sess *session.Session) {
	for {
		ds, err := sess.AcceptDataStream(ctx)
		if err != nil {
			return
		}
		switch s := ds.(type) {
		case *session.IncomingSubgroupStream:
			for {
				if _, err := s.ReadObject(); err != nil {
					break
				}
			}
		case *session.IncomingFetchStream:
			for {
				if _, err := s.ReadObject(); err != nil {
					break
				}
			}
		}
	}
}

// streamMessages delivers the control messages read from stream until it
// ends, then closes the channel.
func streamMessages(t *testing.T, stream session.Stream) <-chan message.Message {
	t.Helper()
	out := make(chan message.Message, 16)
	go func() {
		defer close(out)
		for {
			m, err := message.Parse(stream)
			if err != nil {
				return
			}
			out <- m
		}
	}()
	return out
}

// nextMessage returns the next message from msgs, failing after 2s or if the
// stream ended.
func nextMessage(t *testing.T, msgs <-chan message.Message) message.Message {
	t.Helper()
	select {
	case m, ok := <-msgs:
		if !ok {
			t.Fatal("stream ended")
		}
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for a message")
	}
	return nil
}

// isNamespace reports whether m is a NAMESPACE.
func isNamespace(m message.Message) bool {
	_, ok := m.(*message.Namespace)
	return ok
}

// isNamespaceDone reports whether m is a NAMESPACE_DONE.
func isNamespaceDone(m message.Message) bool {
	_, ok := m.(*message.NamespaceDone)
	return ok
}

// awaitPublishDone reads the next message on a subscription's request stream
// and requires it to be PUBLISH_DONE within 2s.
func awaitPublishDone(t *testing.T, sub *session.Subscription) *message.PublishDone {
	t.Helper()
	got := make(chan message.Message, 1)
	go func() {
		msg, _ := message.Parse(sub)
		got <- msg
	}()
	select {
	case msg := <-got:
		pd, ok := msg.(*message.PublishDone)
		if !ok {
			t.Fatalf("got %T on the subscription stream, want *message.PublishDone", msg)
		}
		return pd
	case <-time.After(2 * time.Second):
		t.Fatal("no PUBLISH_DONE on the subscription stream")
		return nil
	}
}

// watchUpstreamNewGroup answers each REQUEST_UPDATE on a publisher's request
// stream with REQUEST_OK and delivers the NEW_GROUP_REQUEST values it carries.
func watchUpstreamNewGroup(t *testing.T, pubStream session.Stream) <-chan uint64 {
	t.Helper()
	got := make(chan uint64, 1)
	go func() {
		for {
			m, err := message.Parse(pubStream)
			if err != nil {
				return
			}
			upd, ok := m.(*message.RequestUpdate)
			if !ok {
				continue
			}
			_ = message.Marshal(pubStream, &message.RequestOK{})
			if v, ok := newGroupReqValue(upd.Parameters); ok {
				select {
				case got <- v:
				default:
				}
			}
		}
	}()
	return got
}

// Assertions.

// requireRejectedWithCode asserts that err is a REQUEST_ERROR with code want.
func requireRejectedWithCode(t *testing.T, err error, want moqt.RequestErrorCode) {
	t.Helper()
	rejected, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok {
		t.Fatalf("want *RequestRejectedError, got %T: %v", err, err)
	}
	if rejected.Code != want {
		t.Fatalf("rejected code = %#x, want %#x; reason=%q", uint64(rejected.Code), uint64(want), rejected.Reason)
	}
}

// requireSessionClosed fails unless sess closes within 2s.
func requireSessionClosed(t *testing.T, sess *session.Session, what string) {
	t.Helper()
	select {
	case <-sess.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("relay left the session open after %s", what)
	}
}

// waitFor polls cond every 10ms for up to d, failing with msg if it never
// holds.
func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatal(msg)
	}
}
