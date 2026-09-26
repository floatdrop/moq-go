package relay_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// Shared helpers for the relay_test package. The relay itself is started by
// connectRelay (harness_test.go); these drive clients of it.

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

// waitRelayLargest polls TRACK_STATUS until the relay reports Largest Object
// {wantGroup, wantObject} (§10.2.17), failing after 10s.
func waitRelayLargest(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	wantGroup, wantObject uint64,
) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last string
	for {
		req, err := sess.TrackStatus(t.Context(), &message.TrackStatus{
			Namespace: ns,
			Name:      name,
		})
		if err == nil {
			p, ok := req.OK.Parameters.Find(message.ParamLargestObject)
			_ = req.Close()
			if ok && p.Group == wantGroup && p.Object == wantObject {
				return
			}
			last = fmt.Sprintf("largest={%d,%d} present=%t", p.Group, p.Object, ok)
		} else {
			last = err.Error()
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay never reported largest {%d,%d}: %s", wantGroup, wantObject, last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// FETCH.

// fetchRangeFilter is the LOCATION_FILTER (§5.1.2) for the inclusive FETCH
// range [start, endIncl]; an end Object of MaxUint64 means the whole end group.
func fetchRangeFilter(start, endIncl message.Location) message.Parameter {
	if endIncl.Object == math.MaxUint64 {
		return message.AbsoluteRangeFilter(start, endIncl.Group-start.Group)
	}
	return message.AbsoluteRangeObjectFilter(start, endIncl.Group-start.Group, endIncl.Object)
}

// fetchRequestRange returns the inclusive range a FETCH's LOCATION_FILTER asks
// for; ok is false when the filter is absent or open-ended.
func fetchRequestRange(m *message.Fetch) (start, end message.Location, ok bool) {
	f, err := message.LocationFilterFromParam(m.Parameters)
	if err != nil || f == nil {
		return start, end, false
	}
	end, hasEnd := f.End()
	if !hasEnd {
		return start, end, false
	}
	return message.Location{Group: f.StartGroup, Object: f.StartObject}, end, true
}

// fetchOKEnd is the end of [fetchRequestRange], for fake upstreams that echo it
// in FETCH_OK.
func fetchOKEnd(m *message.Fetch) message.Location {
	_, end, _ := fetchRequestRange(m)
	return end
}

// decodedFetchObject is one Object of a FETCH response with absolute IDs.
type decodedFetchObject struct {
	group, object uint64
	payload       []byte
}

// fetchAndDrain FETCHes [start, endIncl] in order and reads the response to
// FIN, returning the FETCH_OK and the Objects decoded by [decodeFetchStream].
func fetchAndDrain(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	start, endIncl message.Location,
	order message.GroupOrder,
	extra ...message.Parameter,
) (*message.FetchOK, []decodedFetchObject) {
	t.Helper()
	params := message.Parameters{message.GroupOrderParam(order), fetchRangeFilter(start, endIncl)}
	params = append(params, extra...)
	reqStream, err := sess.Fetch(t.Context(), &message.Fetch{
		Namespace:  ns,
		Name:       name,
		Parameters: params,
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	t.Cleanup(func() { reqStream.Close() })

	ds, err := sess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, isFetch := ds.(*session.IncomingFetchStream)
	if !isFetch {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}
	return reqStream.OK, decodeFetchStream(t, fs, order)
}

// decodeFetchStream reads fs to EOF, reversing the §11.4.4.1 delta encoding
// itself rather than through the session decoder.
func decodeFetchStream(t *testing.T, fs *session.IncomingFetchStream, order message.GroupOrder) []decodedFetchObject {
	t.Helper()
	var (
		out        []decodedFetchObject
		prevGroup  uint64
		prevObject uint64
		havePrev   bool
		descending = order == message.GroupOrderDescending
	)
	for {
		fo, err := fs.ReadObject()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out
			}
			t.Fatalf("ReadObject: %v", err)
		}

		var g, o uint64
		switch {
		case !havePrev:
			// The first Object's deltas are absolute.
			g = fo.GroupIDDelta
			o = fo.ObjectIDDelta
		case fo.SerializationFlags&message.FetchFlagGroupIDDelta != 0:
			if descending {
				g = prevGroup - fo.GroupIDDelta - 1
			} else {
				g = prevGroup + fo.GroupIDDelta + 1
			}
			o = fo.ObjectIDDelta
		default:
			g = prevGroup
			if fo.SerializationFlags&message.FetchFlagObjectIDDelta != 0 {
				o = prevObject + fo.ObjectIDDelta // no +1 here
			} else {
				o = prevObject + 1
			}
		}

		out = append(out, decodedFetchObject{group: g, object: o, payload: fo.ObjectPayload})
		prevGroup = g
		prevObject = o
		havePrev = true
	}
}

// fetchElem is one element of a FETCH response: an Object or a §11.4.4.2
// End of Range marker.
type fetchElem struct {
	Group, Object uint64
	Unknown       bool // End of Unknown Range
	Marker        bool // any End of Range marker
}

// tryFetchElems FETCHes [{0,0}, {lastGroup,0}] with params and returns the
// response elements, or nil when the FETCH is refused.
func tryFetchElems(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	lastGroup uint64,
	params message.Parameters,
) []fetchElem {
	t.Helper()
	fetchReq, err := sess.Fetch(t.Context(), &message.Fetch{
		Namespace: ns,
		Name:      name,
		Parameters: append(message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: lastGroup, Object: 0}),
		}, params...),
	})
	if err != nil {
		return nil // not yet serviceable: the caller retries
	}
	defer fetchReq.Close()
	order := message.GroupOrderAscending
	if p, ok := params.Find(message.ParamGroupOrder); ok {
		order = message.GroupOrder(p.Byte)
	}
	return collectFetchElems(t, sess, order, 3*time.Second)
}

// collectFetchElems reads the next FETCH response on sess to FIN within
// timeout and returns its elements in arrival order.
func collectFetchElems(
	t *testing.T,
	sess *session.Session,
	order message.GroupOrder,
	timeout time.Duration,
) []fetchElem {
	t.Helper()
	var elems []fetchElem
	for _, obj := range readFetchResponse(t, sess, order, timeout) {
		elems = append(elems, fetchElem{
			Group:   obj.GroupID,
			Object:  obj.ObjectID,
			Unknown: obj.EndOfUnknownRange,
			Marker:  obj.IsEndOfRange(),
		})
	}
	return elems
}

// readFetchResponse accepts the next data stream on sess, requires a FETCH
// response, and decodes it to FIN within timeout, markers included.
func readFetchResponse(
	t *testing.T,
	sess *session.Session,
	order message.GroupOrder,
	timeout time.Duration,
) []*session.DecodedFetchObject {
	t.Helper()
	type result struct {
		objs []*session.DecodedFetchObject
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		ds, err := sess.AcceptDataStream(t.Context())
		if err != nil {
			ch <- result{err: err}
			return
		}
		fs, ok := ds.(*session.IncomingFetchStream)
		if !ok {
			ch <- result{err: errors.New("not a fetch stream")}
			return
		}
		fs.GroupOrder = order
		var r result
		for {
			obj, err := fs.ReadDecoded()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					r.err = err
				}
				ch <- r
				return
			}
			r.objs = append(r.objs, obj)
		}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("reading FETCH response: %v", r.err)
		}
		return r.objs
	case <-time.After(timeout):
		t.Fatal("FETCH response did not arrive within deadline")
		return nil
	}
}

// realGroups returns the groups of the elements that are not End of Unknown
// Range markers.
func realGroups(elems []fetchElem) []uint64 {
	var out []uint64
	for _, e := range elems {
		if !e.Unknown {
			out = append(out, e.Group)
		}
	}
	return out
}

// objectGroups returns the groups of the elements that are Objects.
func objectGroups(elems []fetchElem) []uint64 {
	var out []uint64
	for _, e := range elems {
		if !e.Marker {
			out = append(out, e.Group)
		}
	}
	return out
}

// groupsEqual reports whether got is exactly wantLo..wantHi in order.
func groupsEqual(got []uint64, wantLo, wantHi uint64) bool {
	if uint64(len(got)) != wantHi-wantLo+1 {
		return false
	}
	for i, g := range got {
		if g != wantLo+uint64(i) {
			return false
		}
	}
	return true
}

// fetchRange FETCHes [start, end] and returns the response's Objects; served
// is false when the FETCH was refused or no stream arrived within 2s.
func fetchRange(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	start, end message.Location,
) (objs []*session.DecodedFetchObject, served bool) {
	t.Helper()
	fr, err := sess.Fetch(t.Context(), &message.Fetch{
		Namespace: ns, Name: name,
		Parameters: message.Parameters{fetchRangeFilter(start, end)},
	})
	if err != nil {
		return nil, false
	}
	defer fr.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ds, err := sess.AcceptDataStream(ctx)
	if err != nil {
		return nil, false
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		return nil, false
	}
	for {
		o, err := fs.ReadDecoded()
		if err != nil {
			return objs, true
		}
		if !o.IsEndOfRange() {
			objs = append(objs, o)
		}
	}
}

// writeFetchGroupRange writes one Object per group startG..endG to a FETCH
// response in ascending delta encoding (§11.4.4).
func writeFetchGroupRange(out *session.OutgoingFetchStream, startG, endG uint64) {
	first := true
	for g := startG; g <= endG; g++ {
		fo := &message.FetchObject{}
		fo.SerializationFlags |= message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta
		if first {
			fo.GroupIDDelta = g // absolute
			fo.SerializationFlags |= message.FetchFlagPriority
			first = false
		} else {
			fo.GroupIDDelta = 0 // the next group
		}
		fo.ObjectIDDelta = 0
		fo.ObjectPayload = []byte{byte('a' + g)}
		if err := out.WriteObject(fo); err != nil {
			return
		}
	}
}
