package session_test

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// openPairWithOpts creates a client/server session pair where each side can
// receive custom session.Option values. This is needed to test
// WithKnownMandatoryTrackProperties.
func openPairWithOpts(t *testing.T, clientOpts, serverOpts []session.Option) (*session.Session, *session.Session) {
	t.Helper()
	ctx := t.Context()
	aConn, bConn := sessiontest.NewConnPair()

	var (
		wg           sync.WaitGroup
		aSess, bSess *session.Session
		aErr, bErr   error
	)
	wg.Go(func() {
		aSess, aErr = session.Client(ctx, aConn, clientOpts...)
	})
	wg.Go(func() {
		bSess, bErr = session.Server(ctx, bConn, serverOpts...)
	})
	wg.Wait()
	if aErr != nil {
		t.Fatalf("client Open: %v", aErr)
	}
	if bErr != nil {
		t.Fatalf("server Open: %v", bErr)
	}
	t.Cleanup(func() {
		aSess.Close(moqt.SessionNoError, "test cleanup")
		bSess.Close(moqt.SessionNoError, "test cleanup")
	})
	return aSess, bSess
}

// mandatoryTrackProps builds raw Track Properties bytes containing a single
// mandatory track property with the given type and varint value.
func mandatoryTrackProps(propType message.PropertyType, val uint64) []byte {
	return message.AppendTrackProperties([]wire.KVPair{
		{Type: propType, IntVal: val},
	})
}

// ---------------------------------------------------------------------------
// ValidateTrackProperties (exported helper)
// ---------------------------------------------------------------------------

func TestValidateTrackProperties_NoMandatory(t *testing.T) {
	// Track properties with only well-known non-mandatory types should pass.
	raw := message.AppendTrackProperties([]wire.KVPair{
		{Type: message.PropertyMaxCacheDuration, IntVal: 5000},
	})
	pairs, err := session.ValidateTrackProperties(raw, nil, "TEST")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("got %d pairs, want 1", len(pairs))
	}
}

func TestValidateTrackProperties_UnknownMandatory(t *testing.T) {
	raw := mandatoryTrackProps(0x5000, 1)
	_, err := session.ValidateTrackProperties(raw, nil, "TEST")
	var unsupported *session.ErrUnsupportedMandatoryTrackProperty
	if !errors.As(err, &unsupported) {
		t.Fatalf("error = %v (%T), want *ErrUnsupportedMandatoryTrackProperty", err, err)
	}
	if unsupported.PropertyType != 0x5000 {
		t.Errorf("PropertyType = 0x%X, want 0x5000", unsupported.PropertyType)
	}
	if unsupported.Context != "TEST" {
		t.Errorf("Context = %q, want %q", unsupported.Context, "TEST")
	}
}

func TestValidateTrackProperties_KnownMandatory(t *testing.T) {
	raw := mandatoryTrackProps(0x5000, 1)
	known := map[message.PropertyType]struct{}{0x5000: {}}
	pairs, err := session.ValidateTrackProperties(raw, known, "TEST")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("got %d pairs, want 1", len(pairs))
	}
}

func TestValidateTrackProperties_Empty(t *testing.T) {
	pairs, err := session.ValidateTrackProperties(nil, nil, "TEST")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pairs) != 0 {
		t.Fatalf("got %d pairs, want 0", len(pairs))
	}
}

// TestValidateTrackProperties_MalformedBytes covers the parse-failure branch.
// A truncated pair must surface as a parse error rather than as
// *ErrUnsupportedMandatoryTrackProperty: §2.5.1 attaches specific remedies to
// the latter — REQUEST_ERROR / UNSUPPORTED_EXTENSION when it arrives on
// PUBLISH, cancelling the subscription or fetch (§3.3.3) when it arrives on
// SUBSCRIBE_OK or FETCH_OK — none of which fit Track Properties that simply
// would not decode.
//
// 0x02 is a one-byte Delta Type varint (§1.4.3). It resolves to Type 2 because
// this is the first pair and the running type total starts at zero, and an even
// Type carries a varint value — truncated off the end here.
func TestValidateTrackProperties_MalformedBytes(t *testing.T) {
	pairs, err := session.ValidateTrackProperties([]byte{0x02}, nil, "SUBSCRIBE_OK")
	if err == nil {
		t.Fatal("ValidateTrackProperties(truncated pair) = nil error, want a parse error")
	}
	if pairs != nil {
		t.Errorf("pairs = %v, want nil on a parse failure", pairs)
	}
	if _, ok := errors.AsType[*session.ErrUnsupportedMandatoryTrackProperty](err); ok {
		t.Errorf("error = %v, typed as *ErrUnsupportedMandatoryTrackProperty; "+
			"a malformed pair is not an unsupported mandatory property", err)
	}
	if !strings.Contains(err.Error(), "SUBSCRIBE_OK") {
		t.Errorf("error = %q, want it to name the caller's context", err)
	}
}

// TestErrUnsupportedMandatoryTrackPropertyError pins the rendered message of
// the exported error, which nothing else formats — the other tests all match it
// by type, so the string itself went unchecked despite being public and
// log-facing.
//
// It asserts the parts a reader depends on rather than the whole line: the
// package prefix, the offending type in hex, the caller's context, and the
// §2.5.1 reference. Full-string equality was the first cut and is worse — it
// breaks on any behaviour-neutral rewording, and it cannot catch the stale
// section number it appears to guard, since the expected text is a copy of the
// format string and a renumbering would edit both together.
func TestErrUnsupportedMandatoryTrackPropertyError(t *testing.T) {
	err := &session.ErrUnsupportedMandatoryTrackProperty{
		PropertyType: 0x5000,
		Context:      "SUBSCRIBE_OK",
	}
	got := err.Error()
	// "0x5000" covers the hex rendering: a %d or a dropped 0x prefix both miss.
	for _, want := range []string{"moqt/session:", "0x5000", "SUBSCRIBE_OK", "§2.5.1"} {
		if !strings.Contains(got, want) {
			t.Errorf("Error() = %q, want it to contain %q", got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Subscribe: mandatory track property enforcement
// ---------------------------------------------------------------------------

// TestSubscribeMandatoryTrackPropertyRejected verifies that Subscribe()
// returns *ErrUnsupportedMandatoryTrackProperty when the server sends
// SUBSCRIBE_OK with an unknown mandatory track property and the client has
// opted in to enforcement via WithKnownMandatoryTrackProperties.
func TestSubscribeMandatoryTrackPropertyRejected(t *testing.T) {
	// Client opts in with empty known set → all mandatory are unknown → reject.
	cli, srv := openPairWithOpts(t,
		[]session.Option{session.WithKnownMandatoryTrackProperties(map[message.PropertyType]struct{}{})},
		nil,
	)
	ctx := t.Context()

	var wg sync.WaitGroup

	// Server: accept SUBSCRIBE, reply SUBSCRIBE_OK with a mandatory track property.
	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			return
		}
		_ = r.Reply(&message.SubscribeOK{
			TrackAlias:      7,
			TrackProperties: mandatoryTrackProps(0x5000, 42),
		})
	})

	// Client: Subscribe should fail with ErrUnsupportedMandatoryTrackProperty.
	wg.Go(func() {
		_, err := cli.Subscribe(ctx, &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("ns")},
			Name:      []byte("track"),
		})
		var unsupported *session.ErrUnsupportedMandatoryTrackProperty
		if !errors.As(err, &unsupported) {
			t.Errorf("Subscribe error = %v (%T), want *ErrUnsupportedMandatoryTrackProperty", err, err)
			return
		}
		if unsupported.PropertyType != 0x5000 {
			t.Errorf("PropertyType = 0x%X, want 0x5000", unsupported.PropertyType)
		}
		if unsupported.Context != "SUBSCRIBE_OK" {
			t.Errorf("Context = %q, want %q", unsupported.Context, "SUBSCRIBE_OK")
		}
	})

	wg.Wait()
}

// TestSubscribeMandatoryTrackPropertyAccepted verifies that Subscribe()
// succeeds when the server sends SUBSCRIBE_OK with a mandatory track property
// that the client has registered as known.
func TestSubscribeMandatoryTrackPropertyAccepted(t *testing.T) {
	known := map[message.PropertyType]struct{}{0x5000: {}}
	cli, srv := openPairWithOpts(t,
		[]session.Option{session.WithKnownMandatoryTrackProperties(known)},
		nil,
	)
	ctx := t.Context()

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotOK     *message.SubscribeOK
	)

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		serverErr = r.Reply(&message.SubscribeOK{
			TrackAlias:      7,
			TrackProperties: mandatoryTrackProps(0x5000, 42),
		})
	})

	wg.Go(func() {
		stream, err := cli.Subscribe(ctx, &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("ns")},
			Name:      []byte("track"),
		})
		if err != nil {
			clientErr = err
			return
		}
		defer stream.Close()
		gotOK = stream.OK
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client Subscribe: %v", clientErr)
	}
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
	if gotOK.TrackAlias != 7 {
		t.Errorf("TrackAlias = %d, want 7", gotOK.TrackAlias)
	}
}

// TestSubscribeNoMandatoryTrackPropertyPasses verifies that Subscribe()
// succeeds when SUBSCRIBE_OK has no mandatory track properties, even when
// the client has no known mandatory set configured.
func TestSubscribeNoMandatoryTrackPropertyPasses(t *testing.T) {
	cli, srv := openPairWithOpts(t, nil, nil)
	ctx := t.Context()

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
	)

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		// Reply with non-mandatory track properties only.
		serverErr = r.Reply(&message.SubscribeOK{
			TrackAlias: 3,
			TrackProperties: message.AppendTrackProperties([]wire.KVPair{
				{Type: message.PropertyMaxCacheDuration, IntVal: 5000},
			}),
		})
	})

	wg.Go(func() {
		stream, err := cli.Subscribe(ctx, &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("ns")},
			Name:      []byte("track"),
		})
		if err != nil {
			clientErr = err
			return
		}
		_ = stream.Close()
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client Subscribe: %v", clientErr)
	}
}

// ---------------------------------------------------------------------------
// Fetch: mandatory track property enforcement
// ---------------------------------------------------------------------------

// TestFetchMandatoryTrackPropertyRejected verifies that Fetch() returns
// *ErrUnsupportedMandatoryTrackProperty when the server sends FETCH_OK with
// an unknown mandatory track property.
func TestFetchMandatoryTrackPropertyRejected(t *testing.T) {
	// Client opts in with empty known set → all mandatory are unknown → reject.
	cli, srv := openPairWithOpts(t,
		[]session.Option{session.WithKnownMandatoryTrackProperties(map[message.PropertyType]struct{}{})},
		nil,
	)
	ctx := t.Context()

	var wg sync.WaitGroup

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			return
		}
		_ = r.Reply(&message.FetchOK{
			EndOfTrack:      false,
			EndLocation:     message.Location{Group: 1, Object: 5},
			TrackProperties: mandatoryTrackProps(0x6000, 99),
		})
	})

	wg.Go(func() {
		_, err := cli.Fetch(ctx, &message.Fetch{
			FetchType: message.FetchTypeStandalone,
			Standalone: &message.StandaloneFetch{
				Namespace: wire.TrackNamespace{[]byte("ns")},
				Name:      []byte("track"),
			},
		})
		var unsupported *session.ErrUnsupportedMandatoryTrackProperty
		if !errors.As(err, &unsupported) {
			t.Errorf("Fetch error = %v (%T), want *ErrUnsupportedMandatoryTrackProperty", err, err)
			return
		}
		if unsupported.PropertyType != 0x6000 {
			t.Errorf("PropertyType = 0x%X, want 0x6000", unsupported.PropertyType)
		}
		if unsupported.Context != "FETCH_OK" {
			t.Errorf("Context = %q, want %q", unsupported.Context, "FETCH_OK")
		}
	})

	wg.Wait()
}

// TestFetchMandatoryTrackPropertyAccepted verifies that Fetch() succeeds
// when the server sends FETCH_OK with a mandatory track property that the
// client has registered as known.
func TestFetchMandatoryTrackPropertyAccepted(t *testing.T) {
	known := map[message.PropertyType]struct{}{0x6000: {}}
	cli, srv := openPairWithOpts(t,
		[]session.Option{session.WithKnownMandatoryTrackProperties(known)},
		nil,
	)
	ctx := t.Context()

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotOK     *message.FetchOK
	)

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		serverErr = r.Reply(&message.FetchOK{
			EndOfTrack:      true,
			EndLocation:     message.Location{Group: 1, Object: 5},
			TrackProperties: mandatoryTrackProps(0x6000, 99),
		})
	})

	wg.Go(func() {
		stream, err := cli.Fetch(ctx, &message.Fetch{
			FetchType: message.FetchTypeStandalone,
			Standalone: &message.StandaloneFetch{
				Namespace: wire.TrackNamespace{[]byte("ns")},
				Name:      []byte("track"),
			},
		})
		if err != nil {
			clientErr = err
			return
		}
		defer stream.Close()
		gotOK = stream.OK
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client Fetch: %v", clientErr)
	}
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
	if !gotOK.EndOfTrack {
		t.Error("FetchOK.EndOfTrack = false, want true")
	}
}

// ---------------------------------------------------------------------------
// TrackStatus: mandatory track property enforcement
// ---------------------------------------------------------------------------

// TestTrackStatusMandatoryTrackPropertyRejected verifies that TrackStatus()
// returns *ErrUnsupportedMandatoryTrackProperty when the server sends
// TRACK_STATUS_OK with an unknown mandatory track property.
func TestTrackStatusMandatoryTrackPropertyRejected(t *testing.T) {
	// Client opts in with empty known set → all mandatory are unknown → reject.
	cli, srv := openPairWithOpts(t,
		[]session.Option{session.WithKnownMandatoryTrackProperties(map[message.PropertyType]struct{}{})},
		nil,
	)
	ctx := t.Context()

	var wg sync.WaitGroup

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			return
		}
		_ = r.Reply(&message.RequestOK{
			TrackProperties: mandatoryTrackProps(0x7000, 1),
		})
	})

	wg.Go(func() {
		_, err := cli.TrackStatus(ctx, &message.TrackStatus{
			Namespace: wire.TrackNamespace{[]byte("ns")},
			Name:      []byte("track"),
		})
		var unsupported *session.ErrUnsupportedMandatoryTrackProperty
		if !errors.As(err, &unsupported) {
			t.Errorf("TrackStatus error = %v (%T), want *ErrUnsupportedMandatoryTrackProperty", err, err)
			return
		}
		if unsupported.PropertyType != 0x7000 {
			t.Errorf("PropertyType = 0x%X, want 0x7000", unsupported.PropertyType)
		}
		if unsupported.Context != "TRACK_STATUS_OK" {
			t.Errorf("Context = %q, want %q", unsupported.Context, "TRACK_STATUS_OK")
		}
	})

	wg.Wait()
}

// TestTrackStatusMandatoryTrackPropertyAccepted verifies that TrackStatus()
// succeeds when the server sends TRACK_STATUS_OK with a mandatory track
// property that the client has registered as known.
func TestTrackStatusMandatoryTrackPropertyAccepted(t *testing.T) {
	known := map[message.PropertyType]struct{}{0x7000: {}}
	cli, srv := openPairWithOpts(t,
		[]session.Option{session.WithKnownMandatoryTrackProperties(known)},
		nil,
	)
	ctx := t.Context()

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotOK     *message.TrackStatusOK
	)

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		serverErr = r.Reply(&message.RequestOK{
			TrackProperties: mandatoryTrackProps(0x7000, 1),
		})
	})

	wg.Go(func() {
		ts, err := cli.TrackStatus(ctx, &message.TrackStatus{
			Namespace: wire.TrackNamespace{[]byte("ns")},
			Name:      []byte("track"),
		})
		if err != nil {
			clientErr = err
			return
		}
		defer ts.Close()
		gotOK = ts.OK
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client TrackStatus: %v", clientErr)
	}
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
}

// ---------------------------------------------------------------------------
// Inbound PUBLISH: mandatory track property validation via exported helper
// ---------------------------------------------------------------------------

// TestInboundPublishMandatoryTrackPropertyValidation verifies that the
// exported ValidateTrackProperties helper correctly detects unknown mandatory
// track properties in an inbound PUBLISH message's TrackProperties field.
// This simulates what a relay or subscriber would do after AcceptRequest.
func TestInboundPublishMandatoryTrackPropertyValidation(t *testing.T) {
	cli, srv := openPairWithOpts(t, nil, nil)
	ctx := t.Context()

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
	)

	// Client: send PUBLISH with a mandatory track property.
	wg.Go(func() {
		_, err := cli.Publish(ctx, &message.Publish{
			Namespace:       wire.TrackNamespace{[]byte("ns")},
			Name:            []byte("track"),
			TrackAlias:      1,
			TrackProperties: mandatoryTrackProps(0x4000, 1),
		})
		// The server will reject with UNSUPPORTED_EXTENSION.
		var rejected *session.RequestRejectedError
		if !errors.As(err, &rejected) {
			clientErr = err
			return
		}
		if rejected.Code != moqt.RequestUnsupportedExtension {
			clientErr = errors.New("expected UNSUPPORTED_EXTENSION error code")
		}
	})

	// Server: accept PUBLISH, validate track properties, reject if unknown mandatory.
	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		pub, ok := r.First.(*message.Publish)
		if !ok {
			serverErr = errors.New("expected *message.Publish")
			return
		}
		// Use the exported ValidateTrackProperties helper.
		_, err = session.ValidateTrackProperties(pub.TrackProperties, nil, "PUBLISH")
		if _, ok := errors.AsType[*session.ErrUnsupportedMandatoryTrackProperty](err); !ok {
			serverErr = errors.New("expected *ErrUnsupportedMandatoryTrackProperty from ValidateTrackProperties")
			return
		}
		// Reject with UNSUPPORTED_EXTENSION per §2.5.1.
		serverErr = r.RejectError(moqt.RequestUnsupportedExtension, "unsupported mandatory track property")
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client: %v", clientErr)
	}
}

// ---------------------------------------------------------------------------
// Default (no option): enforcement disabled — mandatory properties pass through
// ---------------------------------------------------------------------------

// TestSubscribeDefaultNoEnforcement verifies that when
// WithKnownMandatoryTrackProperties is NOT called, mandatory track properties
// in SUBSCRIBE_OK are silently accepted. This is the correct default for
// relays and forwarding endpoints.
func TestSubscribeDefaultNoEnforcement(t *testing.T) {
	// No WithKnownMandatoryTrackProperties → enforcement disabled.
	cli, srv := openPairWithOpts(t, nil, nil)
	ctx := t.Context()

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotOK     *message.SubscribeOK
	)

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		serverErr = r.Reply(&message.SubscribeOK{
			TrackAlias:      7,
			TrackProperties: mandatoryTrackProps(0x5000, 42),
		})
	})

	wg.Go(func() {
		stream, err := cli.Subscribe(ctx, &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("ns")},
			Name:      []byte("track"),
		})
		if err != nil {
			clientErr = err
			return
		}
		defer stream.Close()
		gotOK = stream.OK
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client Subscribe: %v", clientErr)
	}
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
	if gotOK.TrackAlias != 7 {
		t.Errorf("TrackAlias = %d, want 7", gotOK.TrackAlias)
	}
}

// TestFetchDefaultNoEnforcement verifies that when
// WithKnownMandatoryTrackProperties is NOT called, mandatory track properties
// in FETCH_OK are silently accepted.
func TestFetchDefaultNoEnforcement(t *testing.T) {
	cli, srv := openPairWithOpts(t, nil, nil)
	ctx := t.Context()

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotOK     *message.FetchOK
	)

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		serverErr = r.Reply(&message.FetchOK{
			EndOfTrack:      true,
			EndLocation:     message.Location{Group: 1, Object: 5},
			TrackProperties: mandatoryTrackProps(0x6000, 99),
		})
	})

	wg.Go(func() {
		stream, err := cli.Fetch(ctx, &message.Fetch{
			FetchType: message.FetchTypeStandalone,
			Standalone: &message.StandaloneFetch{
				Namespace: wire.TrackNamespace{[]byte("ns")},
				Name:      []byte("track"),
			},
		})
		if err != nil {
			clientErr = err
			return
		}
		defer stream.Close()
		gotOK = stream.OK
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client Fetch: %v", clientErr)
	}
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
}

// TestTrackStatusDefaultNoEnforcement verifies that when
// WithKnownMandatoryTrackProperties is NOT called, mandatory track properties
// in TRACK_STATUS_OK are silently accepted.
func TestTrackStatusDefaultNoEnforcement(t *testing.T) {
	cli, srv := openPairWithOpts(t, nil, nil)
	ctx := t.Context()

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotOK     *message.TrackStatusOK
	)

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		serverErr = r.Reply(&message.RequestOK{
			TrackProperties: mandatoryTrackProps(0x7000, 1),
		})
	})

	wg.Go(func() {
		ts, err := cli.TrackStatus(ctx, &message.TrackStatus{
			Namespace: wire.TrackNamespace{[]byte("ns")},
			Name:      []byte("track"),
		})
		if err != nil {
			clientErr = err
			return
		}
		defer ts.Close()
		gotOK = ts.OK
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client TrackStatus: %v", clientErr)
	}
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
}

// ---------------------------------------------------------------------------
// Boundary: mandatory range boundaries
// ---------------------------------------------------------------------------

// TestMandatoryTrackPropertyBoundaries verifies that properties at the exact
// boundaries of the mandatory range (0x4000 and 0x7FFF) are correctly
// detected, while properties just outside (0x3FFF and 0x8000) are not.
func TestMandatoryTrackPropertyBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		propType message.PropertyType
		wantErr  bool
	}{
		{"below range 0x3FFF", 0x3FFF, false},
		{"lower bound 0x4000", 0x4000, true},
		{"mid range 0x5ABC", 0x5ABC, true},
		{"upper bound 0x7FFF", 0x7FFF, true},
		{"above range 0x8000", 0x8000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := message.AppendTrackProperties([]wire.KVPair{
				{Type: tt.propType, IntVal: 1},
			})
			_, err := session.ValidateTrackProperties(raw, nil, "TEST")
			var unsupported *session.ErrUnsupportedMandatoryTrackProperty
			gotErr := errors.As(err, &unsupported)
			if gotErr != tt.wantErr {
				t.Errorf("ValidateTrackProperties(0x%X): gotErr=%v, wantErr=%v (err=%v)",
					tt.propType, gotErr, tt.wantErr, err)
			}
		})
	}
}
