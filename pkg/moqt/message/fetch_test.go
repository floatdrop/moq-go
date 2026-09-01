package message

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestFetchValidate(t *testing.T) {
	// draft-20 stripped FETCH down to a track name plus parameters, so the only
	// invariant left for Validate is §2.4.1's 4,096-byte full-track-name cap —
	// the namespace half is already enforced by wire.Reader.TrackNamespace, and
	// the range moved to LOCATION_FILTER.
	ok := &Fetch{
		RequestID: 1,
		Namespace: wire.TrackNamespace{[]byte("ns")},
		Name:      []byte("track"),
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("well-formed FETCH must validate: %v", err)
	}

	ns := wire.TrackNamespace{bytes.Repeat([]byte("n"), 4000)}
	tooLong := &Fetch{
		RequestID: 1,
		Namespace: ns,
		Name:      bytes.Repeat([]byte("t"), wire.MaxFullTrackNameBytes-ns.ByteLen()+1),
	}
	if err := tooLong.Validate(); err == nil {
		t.Fatal("expected error for full track name over the §2.4.1 cap")
	}
}

// The range moved out of FETCH into LOCATION_FILTER (§5.1.2), which the
// message layer no longer range-checks: a filter naming an end below its own
// start is well-formed on the wire, and it is the publisher that answers
// INVALID_RANGE (§10.13). Pin that division of labour so a future "helpful"
// parse-time rejection doesn't turn a request-scoped error into a session kill.
func TestFetchParseAcceptsInvertedRangeFilter(t *testing.T) {
	inverted := &Fetch{
		RequestID:  1,
		Namespace:  wire.TrackNamespace{[]byte("ns")},
		Name:       []byte("track"),
		Parameters: Parameters{AbsoluteRangeObjectFilter(Location{Group: 5, Object: 10}, 0, 3)},
	}
	var buf bytes.Buffer
	if err := Marshal(&buf, inverted); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Parse(&buf)
	if err != nil {
		t.Fatalf("Parse must accept an inverted range filter: %v", err)
	}
	f, err := LocationFilterFromParam(got.(*Fetch).Parameters)
	if err != nil {
		t.Fatalf("LocationFilterFromParam: %v", err)
	}
	end, hasEnd := f.End()
	if !hasEnd || end != (Location{Group: 5, Object: 3}) {
		t.Fatalf("End() = %+v, %v; want {5 3}, true", end, hasEnd)
	}
}

func TestFetchRoundTrip(t *testing.T) {
	fetch := &Fetch{
		RequestID:  42,
		Namespace:  wire.TrackNamespace{[]byte("example.com"), []byte("live")},
		Name:       []byte("video"),
		Parameters: Parameters{AbsoluteRangeFilter(Location{Group: 100}, 100)},
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
	if !bytes.Equal(got.Name, fetch.Name) {
		t.Errorf("Name: got %q, want %q", got.Name, fetch.Name)
	}
	if len(got.Namespace) != 2 || !bytes.Equal(got.Namespace[0], []byte("example.com")) {
		t.Errorf("Namespace: got %q, want %q", got.Namespace, fetch.Namespace)
	}
	f, err := LocationFilterFromParam(got.Parameters)
	if err != nil || f == nil {
		t.Fatalf("LocationFilterFromParam: %v (filter %v)", err, f)
	}
	if f.Start(Location{}, false) != (Location{Group: 100}) {
		t.Errorf("Start: got %+v, want {100 0}", f.Start(Location{}, false))
	}
	end, _ := f.End()
	if end.Group != 200 {
		t.Errorf("end group: got %d, want 200", end.Group)
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
		SerializationFlags: FetchEndOfNonExistentRange,
		GroupIDDelta:       42,  // absolute Group ID for end-of-range
		ObjectIDDelta:      100, // absolute Object ID for end-of-range
	}

	w := wire.NewWriter(nil)
	obj.Append(w)
	got := &FetchObject{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if !got.IsEndOfNonExistentRange() {
		t.Error("IsEndOfNonExistentRange: got false, want true")
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
		SerializationFlags: FetchEndOfUnknownRange,
		GroupIDDelta:       7,
		ObjectIDDelta:      3,
	}

	w := wire.NewWriter(nil)
	obj.Append(w)
	got := &FetchObject{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if !got.IsEndOfUnknownRange() {
		t.Error("IsEndOfUnknownRange: got false, want true")
	}
	if got.GroupIDDelta != 7 {
		t.Errorf("GroupIDDelta (end-of-range Group ID): got %d, want 7", got.GroupIDDelta)
	}
	if got.ObjectIDDelta != 3 {
		t.Errorf("ObjectIDDelta (end-of-range Object ID): got %d, want 3", got.ObjectIDDelta)
	}
}

// draft-20 added End of Timed-Out Range (0x20C) alongside 0x8C and 0x10C, so a
// subscriber can tell Objects abandoned when FILL_TIMEOUT expired (§10.2.5)
// from Objects whose status is genuinely unknown. All three share a wire shape.
func TestFetchObjectEndOfTimedOutRange(t *testing.T) {
	obj := &FetchObject{
		SerializationFlags: FetchEndOfTimedOutRange,
		GroupIDDelta:       11,
		ObjectIDDelta:      4,
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

	if !got.IsEndOfTimedOutRange() {
		t.Error("IsEndOfTimedOutRange: got false, want true")
	}
	if got.IsEndOfNonExistentRange() || got.IsEndOfUnknownRange() {
		t.Error("0x20C must not be mistaken for the 0x8C or 0x10C markers")
	}
	if !got.IsEndOfRange() {
		t.Error("IsEndOfRange: got false, want true")
	}
	if got.GroupIDDelta != 11 || got.ObjectIDDelta != 4 {
		t.Errorf("range boundary: got {%d %d}, want {11 4}", got.GroupIDDelta, got.ObjectIDDelta)
	}
	// Subgroup ID, Priority and Properties are absent on every end-of-range
	// marker (§11.4.4.2).
	if len(got.Properties) != 0 || got.SubgroupID != 0 || got.PublisherPriority != 0 {
		t.Errorf("marker carried object fields: %+v", got)
	}
}

// The other two markers must keep answering IsEndOfRange too — the relay's
// serve path uses it to decide what is a marker rather than an object.
func TestFetchObjectIsEndOfRangeCoversAllThree(t *testing.T) {
	for _, flags := range []uint64{FetchEndOfNonExistentRange, FetchEndOfUnknownRange, FetchEndOfTimedOutRange} {
		if !(&FetchObject{SerializationFlags: flags}).IsEndOfRange() {
			t.Errorf("flags %#x: IsEndOfRange = false, want true", flags)
		}
	}
	if (&FetchObject{ObjectPayload: []byte("x")}).IsEndOfRange() {
		t.Error("a normal object must not report IsEndOfRange")
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
	fetch := &Fetch{RequestID: 1}
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
		RequestID:  777,
		Namespace:  wire.TrackNamespace{[]byte("test")},
		Name:       []byte("track"),
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

// TestFetchObjectParseTruncated pins the EOF classification Parse gives a
// FETCH response stream that ends mid-object: only an EOF on the very first
// byte (the flags varint) is a clean end-of-stream; an EOF anywhere after it
// means a truncated object and must surface as io.ErrUnexpectedEOF. The
// relay's upstream stitcher relies on this to tell "the sender FIN'd between
// objects, vouching for the rest of the range (§11.4.4)" from "the response
// broke off mid-object".
func TestFetchObjectParseTruncated(t *testing.T) {
	full := &FetchObject{
		SerializationFlags: FetchFlagGroupIDDelta | FetchFlagObjectIDDelta |
			FetchFlagPriority | uint64(FetchSubgroupIDExplicit),
		GroupIDDelta:      3,
		SubgroupID:        1,
		ObjectIDDelta:     7,
		PublisherPriority: 5,
		ObjectPayload:     []byte("payload"),
	}
	w := wire.NewWriter(nil)
	full.Append(w)
	encoded := w.Bytes()

	t.Run("empty stream is clean EOF", func(t *testing.T) {
		err := new(FetchObject).Parse(wire.NewStreamReader(bytes.NewReader(nil)))
		if !errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("Parse(empty) = %v, want io.EOF", err)
		}
	})

	// Every proper prefix that includes at least the flags byte is a
	// truncated object.
	for cut := 1; cut < len(encoded); cut++ {
		err := new(FetchObject).Parse(wire.NewStreamReader(bytes.NewReader(encoded[:cut])))
		if err == nil {
			t.Fatalf("Parse(%d/%d bytes) succeeded, want truncation error", cut, len(encoded))
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("Parse(%d/%d bytes) = %v, want io.ErrUnexpectedEOF", cut, len(encoded), err)
		}
	}

	// Same for an End of Unknown Range marker (§11.4.4.2).
	marker := &FetchObject{
		SerializationFlags: FetchEndOfUnknownRange,
		GroupIDDelta:       2,
		ObjectIDDelta:      9,
	}
	mw := wire.NewWriter(nil)
	marker.Append(mw)
	mEncoded := mw.Bytes()
	for cut := 2; cut < len(mEncoded); cut++ { // 0x10C flags varint is 2 bytes
		err := new(FetchObject).Parse(wire.NewStreamReader(bytes.NewReader(mEncoded[:cut])))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("marker Parse(%d/%d bytes) = %v, want io.ErrUnexpectedEOF", cut, len(mEncoded), err)
		}
	}
}
