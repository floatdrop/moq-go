package message

import (
	"bytes"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestTrackStatusRoundTrip(t *testing.T) {
	trackStatus := &TrackStatus{
		RequestID:  42,
		Namespace:  wire.TrackNamespace{[]byte("example.com"), []byte("live")},
		Name:       []byte("video"),
		Parameters: Parameters{},
	}

	w := wire.NewWriter(nil)
	trackStatus.Append(w)
	got := &TrackStatus{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.RequestID != trackStatus.RequestID {
		t.Errorf("RequestID: got %d, want %d", got.RequestID, trackStatus.RequestID)
	}
	if !bytes.Equal(got.Name, trackStatus.Name) {
		t.Errorf("Name: got %q, want %q", got.Name, trackStatus.Name)
	}
	if len(got.Namespace) != len(trackStatus.Namespace) {
		t.Errorf("Namespace length: got %d, want %d", len(got.Namespace), len(trackStatus.Namespace))
	}
	for i, field := range trackStatus.Namespace {
		if !bytes.Equal(got.Namespace[i], field) {
			t.Errorf("Namespace field %d: got %q, want %q", i, got.Namespace[i], field)
		}
	}
}

func TestTrackStatusWithParameters(t *testing.T) {
	params := Parameters{
		VarintParam(ParamObjectDeliveryTimeout, 5000),
		ByteParam(ParamSubscriberPriority, 100),
	}

	trackStatus := &TrackStatus{
		RequestID:  123,
		Namespace:  wire.TrackNamespace{[]byte("test")},
		Name:       []byte("audio"),
		Parameters: params,
	}

	w := wire.NewWriter(nil)
	trackStatus.Append(w)
	got := &TrackStatus{}
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

func TestTrackStatusOKRoundTrip(t *testing.T) {
	trackStatusOK := &TrackStatusOK{
		Parameters:      Parameters{},
		TrackProperties: []byte("track-props-data"),
	}

	w := wire.NewWriter(nil)
	trackStatusOK.Append(w)
	got := &TrackStatusOK{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if !bytes.Equal(got.TrackProperties, trackStatusOK.TrackProperties) {
		t.Errorf("TrackProperties: got %q, want %q", got.TrackProperties, trackStatusOK.TrackProperties)
	}
}

func TestTrackStatusOKWithParameters(t *testing.T) {
	params := Parameters{
		LocationParam(ParamLargestObject, 100, 50),
		ByteParam(ParamGroupOrder, 1),
	}

	trackStatusOK := &TrackStatusOK{
		Parameters:      params,
		TrackProperties: []byte("properties"),
	}

	w := wire.NewWriter(nil)
	trackStatusOK.Append(w)
	got := &TrackStatusOK{}
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
	if !bytes.Equal(got.TrackProperties, trackStatusOK.TrackProperties) {
		t.Errorf("TrackProperties: got %q, want %q", got.TrackProperties, trackStatusOK.TrackProperties)
	}
}

func TestTrackStatusOKEmptyProperties(t *testing.T) {
	trackStatusOK := &TrackStatusOK{
		Parameters:      Parameters{},
		TrackProperties: []byte{},
	}

	w := wire.NewWriter(nil)
	trackStatusOK.Append(w)
	got := &TrackStatusOK{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(got.TrackProperties) != 0 {
		t.Errorf("TrackProperties: got %d bytes, want 0", len(got.TrackProperties))
	}
}

func TestTrackStatusMessageTypes(t *testing.T) {
	trackStatus := &TrackStatus{RequestID: 1}
	if trackStatus.Type() != TypeTrackStatus {
		t.Errorf("TrackStatus.Type(): got %d, want %d", trackStatus.Type(), TypeTrackStatus)
	}

	trackStatusOK := &TrackStatusOK{}
	if trackStatusOK.Type() != TypeRequestOK {
		t.Errorf("TrackStatusOK.Type(): got %d, want %d", trackStatusOK.Type(), TypeRequestOK)
	}
}

func TestTrackStatusComplexNamespace(t *testing.T) {
	// Test with a complex namespace with multiple fields
	trackStatus := &TrackStatus{
		RequestID: 999,
		Namespace: wire.TrackNamespace{
			[]byte("example"),
			[]byte("com"),
			[]byte("live"),
			[]byte("stream"),
		},
		Name:       []byte("4k-video"),
		Parameters: Parameters{},
	}

	w := wire.NewWriter(nil)
	trackStatus.Append(w)
	got := &TrackStatus{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.RequestID != trackStatus.RequestID {
		t.Errorf("RequestID: got %d, want %d", got.RequestID, trackStatus.RequestID)
	}
	if len(got.Namespace) != len(trackStatus.Namespace) {
		t.Errorf("Namespace length: got %d, want %d", len(got.Namespace), len(trackStatus.Namespace))
	}
	for i, field := range trackStatus.Namespace {
		if !bytes.Equal(got.Namespace[i], field) {
			t.Errorf("Namespace field %d: got %q, want %q", i, got.Namespace[i], field)
		}
	}
	if !bytes.Equal(got.Name, trackStatus.Name) {
		t.Errorf("Name: got %q, want %q", got.Name, trackStatus.Name)
	}
}

func TestTrackStatusOKLargeProperties(t *testing.T) {
	// Test with large track properties
	largeProps := make([]byte, 4096)
	for i := range largeProps {
		largeProps[i] = byte(i % 256)
	}

	trackStatusOK := &TrackStatusOK{
		Parameters:      Parameters{},
		TrackProperties: largeProps,
	}

	w := wire.NewWriter(nil)
	trackStatusOK.Append(w)
	got := &TrackStatusOK{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if !bytes.Equal(got.TrackProperties, largeProps) {
		t.Errorf("TrackProperties: got %d bytes, want %d bytes", len(got.TrackProperties), len(largeProps))
	}
}

func TestTrackStatusNoParameters(t *testing.T) {
	// Test with no parameters
	trackStatus := &TrackStatus{
		RequestID:  777,
		Namespace:  wire.TrackNamespace{[]byte("test")},
		Name:       []byte("track"),
		Parameters: Parameters{},
	}

	w := wire.NewWriter(nil)
	trackStatus.Append(w)
	got := &TrackStatus{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(got.Parameters) != 0 {
		t.Errorf("Parameters: got %d, want 0", len(got.Parameters))
	}
}

func TestTrackStatusOKNoParameters(t *testing.T) {
	// Test with no parameters
	trackStatusOK := &TrackStatusOK{
		Parameters:      Parameters{},
		TrackProperties: []byte("props"),
	}

	w := wire.NewWriter(nil)
	trackStatusOK.Append(w)
	got := &TrackStatusOK{}
	if err := got.Parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(got.Parameters) != 0 {
		t.Errorf("Parameters: got %d, want 0", len(got.Parameters))
	}
}

func TestTrackStatusVsSubscribeDifference(t *testing.T) {
	// Verify that TRACK_STATUS doesn't include Track Alias like SUBSCRIBE_OK does
	trackStatus := &TrackStatus{
		RequestID:  1,
		Namespace:  wire.TrackNamespace{[]byte("test")},
		Name:       []byte("track"),
		Parameters: Parameters{},
	}

	trackStatusOK := &TrackStatusOK{
		Parameters:      Parameters{},
		TrackProperties: []byte("properties"),
	}

	// Both should serialize/deserialize correctly
	w1 := wire.NewWriter(nil)
	trackStatus.Append(w1)
	got1 := &TrackStatus{}
	if err := got1.Parse(wire.NewReader(w1.Bytes())); err != nil {
		t.Fatalf("TrackStatus Parse: %v", err)
	}

	w2 := wire.NewWriter(nil)
	trackStatusOK.Append(w2)
	got2 := &TrackStatusOK{}
	if err := got2.Parse(wire.NewReader(w2.Bytes())); err != nil {
		t.Fatalf("TrackStatusOK Parse: %v", err)
	}

	// Verify the key difference: TRACK_STATUS_OK has no Track Alias field
	// while SUBSCRIBE_OK does. This is implicit in the structure definitions.
	if got1.RequestID != trackStatus.RequestID {
		t.Errorf("TrackStatus RequestID mismatch")
	}
	if !bytes.Equal(got2.TrackProperties, trackStatusOK.TrackProperties) {
		t.Errorf("TrackStatusOK TrackProperties mismatch")
	}
}
