package relay_test

import (
	"math"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// A FETCH served from the relay's cache says an Object does not exist only
// when the relay knows it (§2.1: "A gap in the observed Object IDs does not by
// itself convey any information about the skipped Objects"). Any other
// uncached Location is asked of an upstream (§10.13: "the relay MUST pause
// subsequent delivery until it has confirmed the object's status upstream"),
// or, with none to ask, marked End of Unknown Range (§11.4.4.2).

// cam1Object is one Object of video/cam1 to publish: its Location, and its
// Object Properties.
type cam1Object struct {
	group, object uint64
	props         []byte
}

// publishCam1Group writes objs, all of one Group and ascending, on one
// subgroup stream of alias, with END_OF_GROUP when ended, and FINs it.
func publishCam1Group(t *testing.T, sess *session.Session, alias uint64, ended bool, objs ...cam1Object) {
	t.Helper()
	hdr := subgroupHeader(alias, objs[0].group)
	hdr.EndOfGroup = ended
	hdr.Properties = slices.ContainsFunc(objs, func(o cam1Object) bool { return o.props != nil })
	sg, err := openSubgroupWaiting(t, sess, hdr)
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	for i, o := range objs {
		delta := o.object
		if i > 0 {
			delta = o.object - objs[i-1].object - 1
		}
		if err := sg.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: delta, Properties: o.props, Payload: []byte("x"),
		}); err != nil {
			t.Fatalf("WriteObject: %v", err)
		}
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// fetchCam1Range FETCHes [start, end] of video/cam1 in order and returns the
// response elements.
func fetchCam1Range(
	t *testing.T,
	sess *session.Session,
	start, end message.Location,
	order message.GroupOrder,
) []fetchElem {
	t.Helper()
	params := message.Parameters{fetchRangeFilter(start, end)}
	if order == message.GroupOrderDescending {
		params = append(params, message.GroupOrderParam(order))
	}
	fr, err := sess.Fetch(t.Context(), &message.Fetch{Namespace: ns("video"), Name: []byte("cam1"), Parameters: params})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer fr.Close()
	return collectFetchElems(t, sess, order, 3*time.Second)
}

// obj and unknownAt are the fetchElems of an Object and of an End of Unknown
// Range marker.
func obj(g, o uint64) fetchElem { return fetchElem{Group: g, Object: o} }
func unknownAt(g, o uint64) fetchElem {
	return fetchElem{Group: g, Object: o, Unknown: true, Marker: true}
}

// timedOutAt is the fetchElem of an End of Timed-Out Range marker.
func timedOutAt(g, o uint64) fetchElem { return fetchElem{Group: g, Object: o, Marker: true} }

// TestFetch_CacheHolesWithoutUpstream: with no upstream to ask, an uncached
// Location the relay knows nothing of is marked unknown, in the order the
// response carries it (Groups descending, Objects ascending within one); a
// known one is a plain gap.
func TestFetch_CacheHolesWithoutUpstream(t *testing.T) {
	t.Parallel()
	const maxID = math.MaxUint64
	for _, tc := range []struct {
		name    string
		publish func(t *testing.T, sess *session.Session, alias uint64)
		end     message.Location
		order   message.GroupOrder
		want    []fetchElem
	}{
		{
			name: "hole inside a Group",
			publish: func(t *testing.T, sess *session.Session, alias uint64) {
				publishCam1Group(t, sess, alias, true, cam1Object{0, 0, nil}, cam1Object{0, 1, nil}, cam1Object{0, 3, nil})
			},
			end:  message.Location{Group: 0, Object: 3},
			want: []fetchElem{obj(0, 0), obj(0, 1), unknownAt(0, 2), obj(0, 3)},
		},
		{
			name: "Group tail with no end",
			publish: func(t *testing.T, sess *session.Session, alias uint64) {
				publishCam1Group(t, sess, alias, false, cam1Object{0, 0, nil}, cam1Object{0, 1, nil})
				publishCam1Group(t, sess, alias, false, cam1Object{1, 0, nil})
			},
			end:  message.Location{Group: 1, Object: 0},
			want: []fetchElem{obj(0, 0), obj(0, 1), unknownAt(0, maxID), obj(1, 0)},
		},
		{
			name: "Group tail with no end, Descending",
			publish: func(t *testing.T, sess *session.Session, alias uint64) {
				publishCam1Group(t, sess, alias, false, cam1Object{0, 0, nil}, cam1Object{0, 1, nil})
				publishCam1Group(t, sess, alias, false, cam1Object{1, 0, nil})
			},
			end:   message.Location{Group: 1, Object: 0},
			order: message.GroupOrderDescending,
			want:  []fetchElem{obj(1, 0), obj(0, 0), obj(0, 1), unknownAt(0, maxID)},
		},
		{
			name: "Groups ended by END_OF_GROUP",
			publish: func(t *testing.T, sess *session.Session, alias uint64) {
				publishCam1Group(t, sess, alias, true, cam1Object{0, 0, nil}, cam1Object{0, 1, nil})
				publishCam1Group(t, sess, alias, true, cam1Object{1, 0, nil})
			},
			end:  message.Location{Group: 1, Object: 0},
			want: []fetchElem{obj(0, 0), obj(0, 1), obj(1, 0)},
		},
		{
			name: "hole a Prior Object ID Gap announces",
			publish: func(t *testing.T, sess *session.Session, alias uint64) {
				publishCam1Group(t, sess, alias, true, cam1Object{0, 0, nil}, cam1Object{0, 1, nil},
					cam1Object{0, 3, priorGap(message.PropertyPriorObjectIDGap, 1)})
			},
			end:  message.Location{Group: 0, Object: 3},
			want: []fetchElem{obj(0, 0), obj(0, 1), obj(0, 3)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubSess, alias := newCam1Publisher(t, nil)
			tc.publish(t, pubSess, alias)
			time.Sleep(50 * time.Millisecond) // the relay caches them

			order := tc.order
			if order == 0 {
				order = message.GroupOrderAscending
			}
			got := fetchCam1Range(t, dialAnotherClient(t, pubSess), message.Location{}, tc.end, order)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("FETCH elements %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFetch_CacheHoleAskedUpstream: with a fetch-capable upstream, the relay
// FETCHes an uncached Location of unknown status from it, and its answer
// decides: the Object it sends is served, and a gap under its FIN is one.
func TestFetch_CacheHoleAskedUpstream(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		serve bool // whether the upstream has Object {0, 2}
		want  []fetchElem
	}{
		{"upstream has it", true, []fetchElem{obj(0, 0), obj(0, 1), obj(0, 2), obj(0, 3)}},
		{"upstream says it does not exist", false, []fetchElem{obj(0, 0), obj(0, 1), obj(0, 3)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			upSess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			if _, err := upSess.PublishNamespace(
				t.Context(),
				&message.PublishNamespace{Namespace: ns("video")},
			); err != nil {
				t.Fatalf("PublishNamespace: %v", err)
			}
			var asked atomic.Int32
			go func() {
				for {
					req, err := upSess.AcceptRequest(t.Context())
					if err != nil {
						return
					}
					switch m := req.First.(type) {
					case *message.Subscribe:
						if req.Reply(&message.SubscribeOK{TrackAlias: 42}) != nil {
							return
						}
						// The live stream misses Object 2.
						publishCam1Group(t, upSess, 42, true,
							cam1Object{0, 0, nil}, cam1Object{0, 1, nil}, cam1Object{0, 3, nil})
					case *message.Fetch:
						asked.Add(1)
						if req.Reply(&message.FetchOK{EndLocation: message.Location{Group: 0, Object: 3}}) != nil {
							return
						}
						out, err := upSess.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
						if err != nil {
							return
						}
						if tc.serve {
							_ = out.WriteObject(&message.FetchObject{
								SerializationFlags: message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta |
									message.FetchFlagPriority | uint64(message.FetchSubgroupIDExplicit),
								GroupIDDelta: 0, ObjectIDDelta: 2, ObjectPayload: []byte("x"),
							})
						}
						_ = out.Close()
					}
				}
			}()
			live := dialAnotherClient(t, upSess)
			subscribeCam1(t, live)
			go drainAll(t.Context(), live)
			fc := dialAnotherClient(t, upSess)
			waitRelayLargest(t, fc, ns("video"), []byte("cam1"), 0, 3)

			got := fetchCam1Range(t, fc, message.Location{}, message.Location{Group: 0, Object: 3},
				message.GroupOrderAscending)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("FETCH elements %v, want %v", got, tc.want)
			}
			if asked.Load() != 1 {
				t.Fatalf("the relay asked the upstream %d times, want once", asked.Load())
			}
		})
	}
}

// TestFetch_UpstreamUnknownKeepsWhatTheRelayKnows: an upstream that marks the
// whole span unknown, or timed out, does not unsay what the relay knows: its
// cached Objects are served, and a Group tail an END_OF_GROUP ended stays a
// gap. Only the holes are marked, with the upstream's kind, in either order.
func TestFetch_UpstreamUnknownKeepsWhatTheRelayKnows(t *testing.T) {
	t.Parallel()
	const maxID = math.MaxUint64
	for _, tc := range []struct {
		name   string
		order  message.GroupOrder
		marker message.Location // the upstream's, at the span's end in stream order
		flags  uint64
		want   []fetchElem
	}{
		{
			"Ascending", message.GroupOrderAscending, message.Location{Group: 1, Object: 1},
			message.FetchEndOfUnknownRange,
			[]fetchElem{obj(0, 0), unknownAt(0, 1), obj(0, 2), obj(1, 0), unknownAt(1, 1), obj(1, 2)},
		},
		{
			"Descending", message.GroupOrderDescending, message.Location{Group: 0, Object: maxID},
			message.FetchEndOfUnknownRange,
			[]fetchElem{obj(1, 0), unknownAt(1, 1), obj(1, 2), obj(0, 0), unknownAt(0, 1), obj(0, 2)},
		},
		{
			"Timed-Out", message.GroupOrderAscending, message.Location{Group: 1, Object: 1},
			message.FetchEndOfTimedOutRange,
			[]fetchElem{obj(0, 0), timedOutAt(0, 1), obj(0, 2), obj(1, 0), timedOutAt(1, 1), obj(1, 2)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			upSess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			if _, err := upSess.PublishNamespace(
				t.Context(),
				&message.PublishNamespace{Namespace: ns("video")},
			); err != nil {
				t.Fatalf("PublishNamespace: %v", err)
			}
			var asked atomic.Int32
			go func() {
				for {
					req, err := upSess.AcceptRequest(t.Context())
					if err != nil {
						return
					}
					switch m := req.First.(type) {
					case *message.Subscribe:
						if req.Reply(&message.SubscribeOK{TrackAlias: 42}) != nil {
							return
						}
						// Each Group misses Object 1 and ends after Object 2.
						for g := range uint64(2) {
							publishCam1Group(t, upSess, 42, true, cam1Object{g, 0, nil}, cam1Object{g, 2, nil})
						}
					case *message.Fetch:
						asked.Add(1)
						_, end, _ := fetchRequestRange(m)
						if req.Reply(&message.FetchOK{EndLocation: end}) != nil {
							return
						}
						out, err := upSess.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
						if err != nil {
							return
						}
						_ = out.WriteObject(&message.FetchObject{
							SerializationFlags: tc.flags,
							GroupIDDelta:       tc.marker.Group, ObjectIDDelta: tc.marker.Object,
						})
						_ = out.Close()
					}
				}
			}()
			live := dialAnotherClient(t, upSess)
			subscribeCam1(t, live)
			go drainAll(t.Context(), live)
			fc := dialAnotherClient(t, upSess)
			waitRelayLargest(t, fc, ns("video"), []byte("cam1"), 1, 2)
			time.Sleep(50 * time.Millisecond) // the relay caches Group 0 too

			got := fetchCam1Range(t, fc, message.Location{}, message.Location{Group: 1, Object: 2}, tc.order)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("FETCH elements %v, want %v", got, tc.want)
			}
			if asked.Load() != 1 {
				t.Fatalf("the relay asked the upstream %d times, want once", asked.Load())
			}
		})
	}
}
