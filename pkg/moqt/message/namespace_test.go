package message

import (
	"bytes"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestPublishNamespaceRoundTrip(t *testing.T) {
	publishNamespace := &PublishNamespace{
		RequestID:  42,
		Namespace:  wire.TrackNamespace{[]byte("example.com"), []byte("live")},
		Parameters: Parameters{},
	}

	w := wire.NewWriter(nil)
	publishNamespace.Append(w)
	got := &PublishNamespace{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.RequestID != publishNamespace.RequestID {
		t.Errorf("RequestID: got %d, want %d", got.RequestID, publishNamespace.RequestID)
	}
	if len(got.Namespace) != len(publishNamespace.Namespace) {
		t.Errorf("Namespace length: got %d, want %d", len(got.Namespace), len(publishNamespace.Namespace))
	}
	for i, field := range publishNamespace.Namespace {
		if !bytes.Equal(got.Namespace[i], field) {
			t.Errorf("Namespace field %d: got %q, want %q", i, got.Namespace[i], field)
		}
	}
}

func TestNamespaceRoundTrip(t *testing.T) {
	namespace := &Namespace{
		TrackNamespaceSuffix: wire.TrackNamespace{[]byte("example"), []byte("com")},
	}

	w := wire.NewWriter(nil)
	namespace.Append(w)
	got := &Namespace{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(got.TrackNamespaceSuffix) != len(namespace.TrackNamespaceSuffix) {
		t.Errorf(
			"TrackNamespaceSuffix length: got %d, want %d",
			len(got.TrackNamespaceSuffix),
			len(namespace.TrackNamespaceSuffix),
		)
	}
	for i, field := range namespace.TrackNamespaceSuffix {
		if !bytes.Equal(got.TrackNamespaceSuffix[i], field) {
			t.Errorf("TrackNamespaceSuffix field %d: got %q, want %q", i, got.TrackNamespaceSuffix[i], field)
		}
	}
}

func TestNamespaceDoneRoundTrip(t *testing.T) {
	namespaceDone := &NamespaceDone{
		TrackNamespaceSuffix: wire.TrackNamespace{[]byte("test"), []byte("ns")},
	}

	w := wire.NewWriter(nil)
	namespaceDone.Append(w)
	got := &NamespaceDone{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(got.TrackNamespaceSuffix) != len(namespaceDone.TrackNamespaceSuffix) {
		t.Errorf(
			"TrackNamespaceSuffix length: got %d, want %d",
			len(got.TrackNamespaceSuffix),
			len(namespaceDone.TrackNamespaceSuffix),
		)
	}
	for i, field := range namespaceDone.TrackNamespaceSuffix {
		if !bytes.Equal(got.TrackNamespaceSuffix[i], field) {
			t.Errorf("TrackNamespaceSuffix field %d: got %q, want %q", i, got.TrackNamespaceSuffix[i], field)
		}
	}
}

func TestSubscribeNamespaceRoundTrip(t *testing.T) {
	subscribeNamespace := &SubscribeNamespace{
		RequestID:            123,
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("example.com"), []byte("live")},
		Parameters:           Parameters{},
	}

	w := wire.NewWriter(nil)
	subscribeNamespace.Append(w)
	got := &SubscribeNamespace{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.RequestID != subscribeNamespace.RequestID {
		t.Errorf("RequestID: got %d, want %d", got.RequestID, subscribeNamespace.RequestID)
	}
	if len(got.TrackNamespacePrefix) != len(subscribeNamespace.TrackNamespacePrefix) {
		t.Errorf(
			"TrackNamespacePrefix length: got %d, want %d",
			len(got.TrackNamespacePrefix),
			len(subscribeNamespace.TrackNamespacePrefix),
		)
	}
	for i, field := range subscribeNamespace.TrackNamespacePrefix {
		if !bytes.Equal(got.TrackNamespacePrefix[i], field) {
			t.Errorf("TrackNamespacePrefix field %d: got %q, want %q", i, got.TrackNamespacePrefix[i], field)
		}
	}
}

func TestSubscribeTracksRoundTrip(t *testing.T) {
	subscribeTracks := &SubscribeTracks{
		RequestID:            456,
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("test")},
		Parameters:           Parameters{},
	}

	w := wire.NewWriter(nil)
	subscribeTracks.Append(w)
	got := &SubscribeTracks{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.RequestID != subscribeTracks.RequestID {
		t.Errorf("RequestID: got %d, want %d", got.RequestID, subscribeTracks.RequestID)
	}
	if len(got.TrackNamespacePrefix) != len(subscribeTracks.TrackNamespacePrefix) {
		t.Errorf(
			"TrackNamespacePrefix length: got %d, want %d",
			len(got.TrackNamespacePrefix),
			len(subscribeTracks.TrackNamespacePrefix),
		)
	}
	for i, field := range subscribeTracks.TrackNamespacePrefix {
		if !bytes.Equal(got.TrackNamespacePrefix[i], field) {
			t.Errorf("TrackNamespacePrefix field %d: got %q, want %q", i, got.TrackNamespacePrefix[i], field)
		}
	}
}

func TestPublishSkippedRoundTrip(t *testing.T) {
	publishBlocked := &PublishSkipped{
		TrackNamespaceSuffix: wire.TrackNamespace{[]byte("example.com")},
		TrackName:            []byte("blocked-track"),
	}

	w := wire.NewWriter(nil)
	publishBlocked.Append(w)
	got := &PublishSkipped{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(got.TrackNamespaceSuffix) != len(publishBlocked.TrackNamespaceSuffix) {
		t.Errorf(
			"TrackNamespaceSuffix length: got %d, want %d",
			len(got.TrackNamespaceSuffix),
			len(publishBlocked.TrackNamespaceSuffix),
		)
	}
	for i, field := range publishBlocked.TrackNamespaceSuffix {
		if !bytes.Equal(got.TrackNamespaceSuffix[i], field) {
			t.Errorf("TrackNamespaceSuffix field %d: got %q, want %q", i, got.TrackNamespaceSuffix[i], field)
		}
	}
	if !bytes.Equal(got.TrackName, publishBlocked.TrackName) {
		t.Errorf("TrackName: got %q, want %q", got.TrackName, publishBlocked.TrackName)
	}
}

func TestPublishNamespaceWithParameters(t *testing.T) {
	params := Parameters{
		VarintParam(ParamObjectDeliveryTimeout, 5000),
	}

	publishNamespace := &PublishNamespace{
		RequestID:  789,
		Namespace:  wire.TrackNamespace{[]byte("test")},
		Parameters: params,
	}

	w := wire.NewWriter(nil)
	publishNamespace.Append(w)
	got := &PublishNamespace{}
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

func TestSubscribeNamespaceWithParameters(t *testing.T) {
	params := Parameters{
		ByteParam(ParamSubscriberPriority, 100),
	}

	subscribeNamespace := &SubscribeNamespace{
		RequestID:            999,
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("example")},
		Parameters:           params,
	}

	w := wire.NewWriter(nil)
	subscribeNamespace.Append(w)
	got := &SubscribeNamespace{}
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

func TestNamespaceDiscoveryMessageTypes(t *testing.T) {
	publishNamespace := &PublishNamespace{RequestID: 1}
	if publishNamespace.Type() != TypePublishNamespace {
		t.Errorf("PublishNamespace.Type(): got %d, want %d", publishNamespace.Type(), TypePublishNamespace)
	}

	namespace := &Namespace{}
	if namespace.Type() != TypeNamespace {
		t.Errorf("Namespace.Type(): got %d, want %d", namespace.Type(), TypeNamespace)
	}

	namespaceDone := &NamespaceDone{}
	if namespaceDone.Type() != TypeNamespaceDone {
		t.Errorf("NamespaceDone.Type(): got %d, want %d", namespaceDone.Type(), TypeNamespaceDone)
	}

	subscribeNamespace := &SubscribeNamespace{RequestID: 1}
	if subscribeNamespace.Type() != TypeSubscribeNamespace {
		t.Errorf("SubscribeNamespace.Type(): got %d, want %d", subscribeNamespace.Type(), TypeSubscribeNamespace)
	}

	subscribeTracks := &SubscribeTracks{RequestID: 1}
	if subscribeTracks.Type() != TypeSubscribeTracks {
		t.Errorf("SubscribeTracks.Type(): got %d, want %d", subscribeTracks.Type(), TypeSubscribeTracks)
	}

	publishBlocked := &PublishSkipped{}
	if publishBlocked.Type() != TypePublishSkipped {
		t.Errorf("PublishSkipped.Type(): got %d, want %d", publishBlocked.Type(), TypePublishSkipped)
	}
}

func TestComplexNamespaceRoundTrip(t *testing.T) {
	// Test with complex multi-field namespaces
	complexNamespace := wire.TrackNamespace{
		[]byte("example"),
		[]byte("com"),
		[]byte("live"),
		[]byte("stream"),
		[]byte("4k"),
	}

	publishNamespace := &PublishNamespace{
		RequestID:  888,
		Namespace:  complexNamespace,
		Parameters: Parameters{},
	}

	w := wire.NewWriter(nil)
	publishNamespace.Append(w)
	got := &PublishNamespace{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.RequestID != publishNamespace.RequestID {
		t.Errorf("RequestID: got %d, want %d", got.RequestID, publishNamespace.RequestID)
	}
	if len(got.Namespace) != len(complexNamespace) {
		t.Errorf("Namespace length: got %d, want %d", len(got.Namespace), len(complexNamespace))
	}
	for i, field := range complexNamespace {
		if !bytes.Equal(got.Namespace[i], field) {
			t.Errorf("Namespace field %d: got %q, want %q", i, got.Namespace[i], field)
		}
	}
}

func TestEmptyNamespaceRoundTrip(t *testing.T) {
	// Test with empty namespace (edge case)
	namespace := &Namespace{
		TrackNamespaceSuffix: wire.TrackNamespace{},
	}

	w := wire.NewWriter(nil)
	namespace.Append(w)
	got := &Namespace{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(got.TrackNamespaceSuffix) != 0 {
		t.Errorf("TrackNamespaceSuffix: got %d fields, want 0", len(got.TrackNamespaceSuffix))
	}
}

func TestPublishSkippedEmptyTrackName(t *testing.T) {
	// Test with empty track name (edge case)
	publishBlocked := &PublishSkipped{
		TrackNamespaceSuffix: wire.TrackNamespace{[]byte("test")},
		TrackName:            []byte{},
	}

	w := wire.NewWriter(nil)
	publishBlocked.Append(w)
	got := &PublishSkipped{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(got.TrackName) != 0 {
		t.Errorf("TrackName: got %d bytes, want 0", len(got.TrackName))
	}
}
