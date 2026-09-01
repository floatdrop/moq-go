package message

import (
	"strings"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestFillParametersRoundTrip(t *testing.T) {
	inner := Parameters{
		RelativeStartFilter(1),
		ByteParam(ParamSubscriberPriority, 200),
		VarintParam(ParamFillTimeout, 500),
	}
	ps := Parameters{FillParametersParam(inner)}

	got, ok, err := FillParametersFromParam(ps)
	if err != nil {
		t.Fatalf("FillParametersFromParam: %v", err)
	}
	if !ok {
		t.Fatal("FILL_PARAMETERS reported absent")
	}
	if len(got) != len(inner) {
		t.Fatalf("inner parameter count = %d, want %d", len(got), len(inner))
	}
	f, err := LocationFilterFromParam(got)
	if err != nil || f == nil {
		t.Fatalf("inner LOCATION_FILTER: %v (filter %v)", err, f)
	}
	if !f.RelativeStart() || f.StartGroup != 1 {
		t.Errorf("inner filter = %+v, want the 1-field StartGroup=1 form", *f)
	}
	if d, ok := FillTimeoutFromParamOK(got); !ok || d.Milliseconds() != 500 {
		t.Errorf("inner FILL_TIMEOUT = %v, %v; want 500ms, true", d, ok)
	}
}

// §10.2.15: "Its presence is what requests a fill fetch stream." An empty inner
// list still requests one, so present-but-empty has to stay distinguishable
// from absent. It does NOT mean "fill the whole track": with no inner
// LOCATION_FILTER the fill range falls back to the subscription's own filter
// (§5.1.3) — see TestSubscribe_FillInheritsSubscriptionFilter in pkg/relay.
func TestFillParametersPresentButEmpty(t *testing.T) {
	got, ok, err := FillParametersFromParam(Parameters{FillParametersParam(nil)})
	if err != nil {
		t.Fatalf("FillParametersFromParam: %v", err)
	}
	if !ok {
		t.Fatal("an empty FILL_PARAMETERS must still report present")
	}
	if len(got) != 0 {
		t.Errorf("inner parameters = %v, want empty", got)
	}
}

func TestFillParametersAbsent(t *testing.T) {
	got, ok, err := FillParametersFromParam(Parameters{ByteParam(ParamForward, 1)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("absent FILL_PARAMETERS reported present")
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

// §10.2.15 Table 6 is a closed list — a parameter inside FILL_PARAMETERS that
// is not listed there MUST close the session with PROTOCOL_VIOLATION.
func TestFillParametersRejectsUnlistedParameter(t *testing.T) {
	// TRACK_PROPERTY_FILTER is deliberately absent from Table 6 — a fill is
	// scoped to Objects, so only the Object-scoped filters carry over.
	inner := Parameters{BytesParam(ParamTrackPropertyFilter, []byte{0x00})}
	_, ok, err := FillParametersFromParam(Parameters{FillParametersParam(inner)})
	if !ok {
		t.Fatal("the parameter was present, so ok must be true even on error")
	}
	if err == nil {
		t.Fatal("expected error for TRACK_PROPERTY_FILTER inside FILL_PARAMETERS")
	}
	if !strings.Contains(err.Error(), "PROTOCOL_VIOLATION") {
		t.Errorf("error should name the violation, got: %v", err)
	}

	// FORWARD is likewise not in Table 6.
	inner = Parameters{ByteParam(ParamForward, 1)}
	if _, _, err := FillParametersFromParam(Parameters{FillParametersParam(inner)}); err == nil {
		t.Fatal("expected error for FORWARD inside FILL_PARAMETERS")
	}
}

func TestFillParametersAcceptsEveryTableSixEntry(t *testing.T) {
	inner := Parameters{
		VarintParam(ParamFillTimeout, 1),
		ByteParam(ParamSubscriberPriority, 1),
		LocationFilterParam(&LocationFilter{Fields: 1}),
		ByteParam(ParamGroupOrder, uint8(GroupOrderAscending)),
		BytesParam(ParamSubgroupFilter, []byte{0x00, 0x01}),
		BytesParam(ParamObjectIDFilter, []byte{0x00, 0x01}),
		BytesParam(ParamPriorityFilter, []byte{0x00, 0x01}),
		BytesParam(ParamObjectPropertyFilter, []byte{0x00, 0x06, 0x01}),
	}
	if _, _, err := FillParametersFromParam(Parameters{FillParametersParam(inner)}); err != nil {
		t.Fatalf("every Table 6 parameter must be accepted: %v", err)
	}
}

// §10.2.15: "The value of FILL_PARAMETERS is a separate parameter scope...
// so a Parameter Type MAY appear both in the message and inside
// FILL_PARAMETERS." The two must not bleed into each other.
func TestFillParametersIsASeparateScope(t *testing.T) {
	ps := Parameters{
		AbsoluteStartFilter(Location{Group: 50}), // the subscription's own filter
		FillParametersParam(Parameters{RelativeStartFilter(2)}),
	}
	outer, err := LocationFilterFromParam(ps)
	if err != nil {
		t.Fatalf("outer LOCATION_FILTER: %v", err)
	}
	if outer.RelativeStart() || outer.StartGroup != 50 {
		t.Errorf("outer filter = %+v, want the absolute {50 0} start", *outer)
	}
	inner, _, err := FillParametersFromParam(ps)
	if err != nil {
		t.Fatalf("FillParametersFromParam: %v", err)
	}
	innerFilter, err := LocationFilterFromParam(inner)
	if err != nil {
		t.Fatalf("inner LOCATION_FILTER: %v", err)
	}
	if !innerFilter.RelativeStart() || innerFilter.StartGroup != 2 {
		t.Errorf("inner filter = %+v, want the 1-field StartGroup=2 form", *innerFilter)
	}
}

func TestIncludeProperties(t *testing.T) {
	// §10.2.21: "the default is 1".
	if got, err := IncludePropertiesFromParam(Parameters{}); err != nil || !got {
		t.Errorf("absent INCLUDE_PROPERTIES = %v, %v; want true, nil", got, err)
	}
	if got, err := IncludePropertiesFromParam(Parameters{IncludePropertiesParam(false)}); err != nil || got {
		t.Errorf("INCLUDE_PROPERTIES(false) = %v, %v; want false, nil", got, err)
	}
	if got, err := IncludePropertiesFromParam(Parameters{IncludePropertiesParam(true)}); err != nil || !got {
		t.Errorf("INCLUDE_PROPERTIES(true) = %v, %v; want true, nil", got, err)
	}
	// "If an endpoint receives a value outside this range, it MUST close the
	// session with PROTOCOL_VIOLATION."
	if _, err := IncludePropertiesFromParam(Parameters{ByteParam(ParamIncludeProperties, 2)}); err == nil {
		t.Fatal("expected error for INCLUDE_PROPERTIES = 2")
	}
}

func TestPublishStateNotifyRoundTrip(t *testing.T) {
	msg := &PublishStateNotify{
		Parameters: Parameters{
			LargestObjectParam(12, 34),
			ByteParam(ParamForward, 0),
		},
	}
	if msg.Type() != TypePublishStateNotify {
		t.Errorf("Type = %#x, want %#x", uint64(msg.Type()), uint64(TypePublishStateNotify))
	}
	if TypePublishStateNotify != 0x22 {
		t.Errorf("PUBLISH_STATE_NOTIFY type = %#x, want 0x22", uint64(TypePublishStateNotify))
	}

	w := wire.NewWriter(nil)
	msg.Append(w)
	got := &PublishStateNotify{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(got.Parameters) != 2 {
		t.Fatalf("parameter count = %d, want 2", len(got.Parameters))
	}
	p, ok := got.Parameters.Find(ParamLargestObject)
	if !ok || p.Group != 12 || p.Object != 34 {
		t.Errorf("LARGEST_OBJECT = %+v, %v; want {12 34}", p, ok)
	}
}

// It carries no Request ID (§10.10) — the stream it arrives on names the
// subscription. Pin that it does NOT satisfy WithRequestID, since the session's
// §10.1 parity check keys off that interface.
func TestPublishStateNotifyHasNoRequestID(t *testing.T) {
	var m Message = &PublishStateNotify{}
	if _, ok := m.(WithRequestID); ok {
		t.Fatal("PUBLISH_STATE_NOTIFY must not implement WithRequestID")
	}
}
