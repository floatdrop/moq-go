package message

import (
	"bytes"
	"errors"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestFetchValidate(t *testing.T) {
	mk := func(start, end Location) *Fetch {
		return &Fetch{
			RequestID: 1,
			FetchType: FetchTypeStandalone,
			Standalone: &StandaloneFetch{
				Namespace:     wire.TrackNamespace{[]byte("ns")},
				Name:          []byte("track"),
				StartLocation: start,
				EndLocation:   end,
			},
		}
	}

	// End strictly before Start is rejected (§10.12).
	if err := mk(Location{Group: 10}, Location{Group: 5}).Validate(); err == nil {
		t.Fatal("expected error when End < Start")
	}
	if err := mk(Location{Group: 5, Object: 9}, Location{Group: 5, Object: 8}).Validate(); err == nil {
		t.Fatal("expected error when End.Object < Start.Object in same group")
	}
	// Equal Start/End is a valid single-object range.
	if err := mk(Location{Group: 5, Object: 8}, Location{Group: 5, Object: 8}).Validate(); err != nil {
		t.Fatalf("equal Start/End must be valid: %v", err)
	}
	// End after Start is valid.
	if err := mk(Location{Group: 5}, Location{Group: 6}).Validate(); err != nil {
		t.Fatalf("End > Start must be valid: %v", err)
	}

	// Sub-message presence guards.
	if err := (&Fetch{FetchType: FetchTypeStandalone}).Validate(); err == nil {
		t.Fatal("expected error for standalone FETCH with no range")
	}
	if err := (&Fetch{FetchType: FetchTypeRelativeJoining}).Validate(); err == nil {
		t.Fatal("expected error for joining FETCH with no joining fields")
	}
}

// A FETCH frame whose End Location precedes its Start Location must be rejected
// by the parser, not only by an explicit Validate call.
func TestFetchParseRejectsInvertedRange(t *testing.T) {
	bad := &Fetch{
		RequestID: 1,
		FetchType: FetchTypeStandalone,
		Standalone: &StandaloneFetch{
			Namespace:     wire.TrackNamespace{[]byte("ns")},
			Name:          []byte("track"),
			StartLocation: Location{Group: 10, Object: 0},
			EndLocation:   Location{Group: 5, Object: 0},
		},
	}
	var buf bytes.Buffer
	if err := Marshal(&buf, bad); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := Parse(&buf); err == nil {
		t.Fatal("Parse must reject a FETCH with End < Start")
	}
}

func TestFetchStandaloneRoundTrip(t *testing.T) {
	fetch := &Fetch{
		RequestID: 42,
		FetchType: FetchTypeStandalone,
		Standalone: &StandaloneFetch{
			Namespace:     wire.TrackNamespace{[]byte("example.com"), []byte("live")},
			Name:          []byte("video"),
			StartLocation: Location{Group: 100, Object: 0},
			EndLocation:   Location{Group: 200, Object: 999},
		},
		Parameters: Parameters{},
	}

	w := wire.NewWriter(nil)
	fetch.Append(w)
	got := &Fetch{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.RequestID != fetch.RequestID {
		t.Errorf("RequestID: got %d, want %d", got.RequestID, fetch.RequestID)
	}
	if got.FetchType != fetch.FetchType {
		t.Errorf("FetchType: got %d, want %d", got.FetchType, fetch.FetchType)
	}
	if got.Standalone == nil {
		t.Fatal("Standalone is nil")
	}
	if !bytes.Equal(got.Standalone.Name, fetch.Standalone.Name) {
		t.Errorf("Name: got %q, want %q", got.Standalone.Name, fetch.Standalone.Name)
	}
	if got.Standalone.StartLocation != fetch.Standalone.StartLocation {
		t.Errorf("StartLocation: got %+v, want %+v", got.Standalone.StartLocation, fetch.Standalone.StartLocation)
	}
	if got.Standalone.EndLocation != fetch.Standalone.EndLocation {
		t.Errorf("EndLocation: got %+v, want %+v", got.Standalone.EndLocation, fetch.Standalone.EndLocation)
	}
}

func TestFetchRelativeJoiningRoundTrip(t *testing.T) {
	fetch := &Fetch{
		RequestID: 123,
		FetchType: FetchTypeRelativeJoining,
		Joining: &JoiningFetch{
			JoiningRequestID: 42,
			JoiningStart:     50,
		},
		Parameters: Parameters{},
	}

	w := wire.NewWriter(nil)
	fetch.Append(w)
	got := &Fetch{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.RequestID != fetch.RequestID {
		t.Errorf("RequestID: got %d, want %d", got.RequestID, fetch.RequestID)
	}
	if got.FetchType != fetch.FetchType {
		t.Errorf("FetchType: got %d, want %d", got.FetchType, fetch.FetchType)
	}
	if got.Joining == nil {
		t.Fatal("Joining is nil")
	}
	if got.Joining.JoiningRequestID != fetch.Joining.JoiningRequestID {
		t.Errorf("JoiningRequestID: got %d, want %d", got.Joining.JoiningRequestID, fetch.Joining.JoiningRequestID)
	}
	if got.Joining.JoiningStart != fetch.Joining.JoiningStart {
		t.Errorf("JoiningStart: got %d, want %d", got.Joining.JoiningStart, fetch.Joining.JoiningStart)
	}
}

func TestFetchAbsoluteJoiningRoundTrip(t *testing.T) {
	fetch := &Fetch{
		RequestID: 456,
		FetchType: FetchTypeAbsoluteJoining,
		Joining: &JoiningFetch{
			JoiningRequestID: 789,
			JoiningStart:     1000,
		},
		Parameters: Parameters{},
	}

	w := wire.NewWriter(nil)
	fetch.Append(w)
	got := &Fetch{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.RequestID != fetch.RequestID {
		t.Errorf("RequestID: got %d, want %d", got.RequestID, fetch.RequestID)
	}
	if got.FetchType != fetch.FetchType {
		t.Errorf("FetchType: got %d, want %d", got.FetchType, fetch.FetchType)
	}
	if got.Joining == nil {
		t.Fatal("Joining is nil")
	}
	if got.Joining.JoiningRequestID != fetch.Joining.JoiningRequestID {
		t.Errorf("JoiningRequestID: got %d, want %d", got.Joining.JoiningRequestID, fetch.Joining.JoiningRequestID)
	}
	if got.Joining.JoiningStart != fetch.Joining.JoiningStart {
		t.Errorf("JoiningStart: got %d, want %d", got.Joining.JoiningStart, fetch.Joining.JoiningStart)
	}
}

func TestFetchUnknownType(t *testing.T) {
	w := wire.NewWriter(nil)
	w.Varint(999) // RequestID
	w.Varint(99)  // Invalid FetchType

	fetch := &Fetch{}
	err := fetch.Parse(wire.NewReader(w.Bytes()))
	if err == nil {
		t.Fatal("expected error for unknown fetch type")
	}
	if !errors.Is(err, ErrUnknownFetchType) {
		t.Errorf("expected ErrUnknownFetchType, got %T", err)
	}
}

func TestFetchOKRoundTrip(t *testing.T) {
	fetchOK := &FetchOK{
		EndOfTrack:      true,
		EndLocation:     Location{Group: 500, Object: 1000},
		Parameters:      Parameters{},
		TrackProperties: []byte("track-props"),
	}

	w := wire.NewWriter(nil)
	fetchOK.Append(w)
	got := &FetchOK{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.EndOfTrack != fetchOK.EndOfTrack {
		t.Errorf("EndOfTrack: got %v, want %v", got.EndOfTrack, fetchOK.EndOfTrack)
	}
	if got.EndLocation != fetchOK.EndLocation {
		t.Errorf("EndLocation: got %+v, want %+v", got.EndLocation, fetchOK.EndLocation)
	}
	if !bytes.Equal(got.TrackProperties, fetchOK.TrackProperties) {
		t.Errorf("TrackProperties: got %q, want %q", got.TrackProperties, fetchOK.TrackProperties)
	}
}

func TestFetchOKWithEndOfTrackFalse(t *testing.T) {
	fetchOK := &FetchOK{
		EndOfTrack:      false,
		EndLocation:     Location{Group: 100, Object: 200},
		Parameters:      Parameters{},
		TrackProperties: []byte{},
	}

	w := wire.NewWriter(nil)
	fetchOK.Append(w)
	got := &FetchOK{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.EndOfTrack {
		t.Error("EndOfTrack: got true, want false")
	}
}

func TestFetchObjectMinimal(t *testing.T) {
	obj := &FetchObject{ObjectPayload: []byte("test payload")}

	w := wire.NewWriter(nil)
	obj.Append(w)
	got := &FetchObject{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.SerializationFlags != 0 {
		t.Errorf("SerializationFlags: got %d, want 0", got.SerializationFlags)
	}
	if !bytes.Equal(got.ObjectPayload, []byte("test payload")) {
		t.Errorf("ObjectPayload: got %q, want %q", got.ObjectPayload, []byte("test payload"))
	}
}

func TestFetchObjectWithAllFields(t *testing.T) {
	obj := &FetchObject{
		SerializationFlags: FetchFlagGroupIDDelta | FetchFlagObjectIDDelta | FetchFlagPriority |
			FetchFlagProperties | uint64(FetchSubgroupIDExplicit),
		GroupIDDelta:      10,
		SubgroupID:        5,
		ObjectIDDelta:     3,
		PublisherPriority: 127,
		Properties:        []byte("props"),
		ObjectPayload:     []byte("data"),
	}

	w := wire.NewWriter(nil)
	obj.Append(w)
	got := &FetchObject{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.GroupIDDelta != 10 {
		t.Errorf("GroupIDDelta: got %d, want 10", got.GroupIDDelta)
	}
	if got.SubgroupID != 5 {
		t.Errorf("SubgroupID: got %d, want 5", got.SubgroupID)
	}
	if got.ObjectIDDelta != 3 {
		t.Errorf("ObjectIDDelta: got %d, want 3", got.ObjectIDDelta)
	}
	if got.PublisherPriority != 127 {
		t.Errorf("PublisherPriority: got %d, want 127", got.PublisherPriority)
	}
	if !bytes.Equal(got.Properties, []byte("props")) {
		t.Errorf("Properties: got %q, want %q", got.Properties, []byte("props"))
	}
	if !bytes.Equal(got.ObjectPayload, []byte("data")) {
		t.Errorf("ObjectPayload: got %q, want %q", got.ObjectPayload, []byte("data"))
	}
}

// TestFetchObjectDatagramBit exercises §11.4.4.1 Table 9: bit 0x40 marks a
// Datagram-preference object — the payload is still present (FETCH objects
// never carry an Object Status field) and the two subgroup-mode LSBs are
// ignored, so no Subgroup ID field is written or read even when they spell
// the "explicit" mode.
func TestFetchObjectDatagramBit(t *testing.T) {
	obj := &FetchObject{
		// Datagram bit + explicit-subgroup LSBs: the LSBs must be ignored.
		SerializationFlags: FetchFlagDatagram | uint64(FetchSubgroupIDExplicit) |
			FetchFlagGroupIDDelta | FetchFlagObjectIDDelta,
		GroupIDDelta:  5,
		ObjectIDDelta: 2,
		SubgroupID:    9, // must NOT reach the wire
		ObjectPayload: []byte("payload"),
	}

	w := wire.NewWriter(nil)
	obj.Append(w)
	got := &FetchObject{}
	r := wire.NewReader(w.Bytes())
	if err := got.Parse(r); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !r.Empty() {
		t.Errorf("trailing bytes after parse: %d left", r.Remaining())
	}

	if !got.IsDatagram() {
		t.Error("IsDatagram: got false, want true")
	}
	if got.SubgroupID != 0 {
		t.Errorf("SubgroupID: got %d, want 0 (field must be absent)", got.SubgroupID)
	}
	if got.GroupIDDelta != 5 || got.ObjectIDDelta != 2 {
		t.Errorf("deltas: got (%d,%d), want (5,2)", got.GroupIDDelta, got.ObjectIDDelta)
	}
	if !bytes.Equal(got.ObjectPayload, []byte("payload")) {
		t.Errorf("ObjectPayload: got %q, want %q", got.ObjectPayload, "payload")
	}
}

func TestFetchObjectEndOfRangeObject(t *testing.T) {
	// Per §11.4.4.2: end-of-range markers carry Group ID and Object ID.
	obj := &FetchObject{
		SerializationFlags: FetchEndOfRangeObject,
		GroupIDDelta:       42,  // absolute Group ID for end-of-range
		ObjectIDDelta:      100, // absolute Object ID for end-of-range
	}

	w := wire.NewWriter(nil)
	obj.Append(w)
	got := &FetchObject{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if !got.IsEndOfRangeObject() {
		t.Error("IsEndOfRangeObject: got false, want true")
	}
	if got.GroupIDDelta != 42 {
		t.Errorf("GroupIDDelta (end-of-range Group ID): got %d, want 42", got.GroupIDDelta)
	}
	if got.ObjectIDDelta != 100 {
		t.Errorf("ObjectIDDelta (end-of-range Object ID): got %d, want 100", got.ObjectIDDelta)
	}
}

func TestFetchObjectEndOfRangeGroup(t *testing.T) {
	// Per §11.4.4.2: end-of-range markers carry Group ID and Object ID.
	obj := &FetchObject{
		SerializationFlags: FetchEndOfRangeGroup,
		GroupIDDelta:       7,
		ObjectIDDelta:      3,
	}

	w := wire.NewWriter(nil)
	obj.Append(w)
	got := &FetchObject{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if !got.IsEndOfRangeGroup() {
		t.Error("IsEndOfRangeGroup: got false, want true")
	}
	if got.GroupIDDelta != 7 {
		t.Errorf("GroupIDDelta (end-of-range Group ID): got %d, want 7", got.GroupIDDelta)
	}
	if got.ObjectIDDelta != 3 {
		t.Errorf("ObjectIDDelta (end-of-range Object ID): got %d, want 3", got.ObjectIDDelta)
	}
}

func TestFetchObjectValidateInvalidFlags(t *testing.T) {
	// Values >= 128 that are not end-of-range markers are PROTOCOL_VIOLATION.
	obj := &FetchObject{
		SerializationFlags: 0x80, // >= 128, not an end-of-range marker
	}
	if err := obj.Validate(); err == nil {
		t.Fatal("expected error for invalid serialization flags >= 128")
	}
}

func TestFetchObjectSubgroupModes(t *testing.T) {
	// Verify all four subgroup modes round-trip correctly.
	modes := []struct {
		name  string
		flags uint64
	}{
		{"zero", uint64(FetchSubgroupIDZero)},
		{"prior", uint64(FetchSubgroupIDPrior)},
		{"prior+1", uint64(FetchSubgroupIDPriorPlusOne)},
	}
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			obj := &FetchObject{
				SerializationFlags: m.flags,
				ObjectPayload:      []byte("x"),
			}
			w := wire.NewWriter(nil)
			obj.Append(w)
			got := &FetchObject{}
			if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got.SubgroupMode() != FetchSubgroupIDMode(m.flags&FetchFlagSubgroupIDMode) {
				t.Errorf("SubgroupMode: got %d, want %d", got.SubgroupMode(), m.flags&FetchFlagSubgroupIDMode)
			}
		})
	}
}

func TestFetchHeaderRoundTrip(t *testing.T) {
	header := FetchHeader{RequestID: 42}

	var buf bytes.Buffer
	if err := WriteFetchHeader(&buf, header); err != nil {
		t.Fatalf("WriteFetchHeader: %v", err)
	}

	// ReadFetchHeader expects the stream type to have already been read
	// So we need to read the type first, then call ReadFetchHeader
	typ, err := ReadDataStreamType(&buf)
	if err != nil {
		t.Fatalf("ReadDataStreamType: %v", err)
	}
	if typ != 0x05 {
		t.Fatalf("Expected type 0x05, got %d", typ)
	}

	got, err := ReadFetchHeader(&buf)
	if err != nil {
		t.Fatalf("ReadFetchHeader: %v", err)
	}

	if got.RequestID != header.RequestID {
		t.Errorf("RequestID: got %d, want %d", got.RequestID, header.RequestID)
	}
	if got.RawType() != 0x05 {
		t.Errorf("RawType: got %d, want 0x05", got.RawType())
	}
}

func TestIsFetchHeaderType(t *testing.T) {
	if !IsFetchHeaderType(0x05) {
		t.Error("IsFetchHeaderType(0x05): got false, want true")
	}
	if IsFetchHeaderType(0x04) {
		t.Error("IsFetchHeaderType(0x04): got true, want false")
	}
}

func TestFetchMessageTypes(t *testing.T) {
	fetch := &Fetch{RequestID: 1, FetchType: FetchTypeStandalone}
	if fetch.Type() != TypeFetch {
		t.Errorf("Fetch.Type(): got %d, want %d", fetch.Type(), TypeFetch)
	}

	fetchOK := &FetchOK{EndOfTrack: true}
	if fetchOK.Type() != TypeFetchOK {
		t.Errorf("FetchOK.Type(): got %d, want %d", fetchOK.Type(), TypeFetchOK)
	}
}

func TestFetchWithParameters(t *testing.T) {
	params := Parameters{
		VarintParam(ParamObjectDeliveryTimeout, 100),
		ByteParam(ParamSubscriberPriority, 50),
	}

	fetch := &Fetch{
		RequestID: 777,
		FetchType: FetchTypeStandalone,
		Standalone: &StandaloneFetch{
			Namespace:     wire.TrackNamespace{[]byte("test")},
			Name:          []byte("track"),
			StartLocation: Location{Group: 0, Object: 0},
			EndLocation:   Location{Group: 10, Object: 100},
		},
		Parameters: params,
	}

	w := wire.NewWriter(nil)
	fetch.Append(w)
	got := &Fetch{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(got.Parameters) != len(params) {
		t.Fatalf("Parameters length: got %d, want %d", len(got.Parameters), len(params))
	}
	for i, p := range params {
		if got.Parameters[i].Type != p.Type {
			t.Errorf("Parameter %d Type: got %d, want %d", i, got.Parameters[i].Type, p.Type)
		}
	}
}

func TestLocationCompare(t *testing.T) {
	cases := []struct {
		name     string
		a, b     Location
		want     int
		wantLess bool
	}{
		{"equal", Location{1, 1}, Location{1, 1}, 0, false},
		{"less object", Location{1, 1}, Location{1, 2}, -1, true},
		{"greater object", Location{1, 2}, Location{1, 1}, 1, false},
		{"less group", Location{1, 99}, Location{2, 0}, -1, true},
		{"greater group", Location{2, 0}, Location{1, 99}, 1, false},
		{"zero vs nonzero", Location{}, Location{0, 1}, -1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.a.Compare(c.b); got != c.want {
				t.Errorf("Compare = %d, want %d", got, c.want)
			}
			if got := c.a.Less(c.b); got != c.wantLess {
				t.Errorf("Less = %v, want %v", got, c.wantLess)
			}
		})
	}
}
