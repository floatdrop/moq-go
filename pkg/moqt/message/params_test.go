package message

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestTrackNamespacePrefixParam(t *testing.T) {
	tests := []struct {
		name   string
		prefix wire.TrackNamespace
	}{
		{
			name:   "empty namespace",
			prefix: wire.TrackNamespace{},
		},
		{
			name:   "single field",
			prefix: wire.TrackNamespace{[]byte("video")},
		},
		{
			name:   "multiple fields",
			prefix: wire.TrackNamespace{[]byte("live"), []byte("sports"), []byte("soccer")},
		},
		{
			name:   "binary fields",
			prefix: wire.TrackNamespace{[]byte{0x01, 0x02, 0x03}, []byte{0xFF, 0xFE}},
		},
		{
			name:   "mixed content",
			prefix: wire.TrackNamespace{[]byte("stream"), []byte{0x00, 0x01}, []byte("4k")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create parameter
			param := TrackNamespacePrefixParam(tt.prefix)

			// Verify parameter type
			if param.Type != ParamTrackNamespacePrefix {
				t.Errorf("expected type %v, got %v", ParamTrackNamespacePrefix, param.Type)
			}

			// Verify parameter has bytes
			if len(param.Bytes) == 0 && len(tt.prefix) > 0 {
				t.Errorf("expected non-empty bytes for non-empty prefix")
			}

			// Verify round-trip serialization
			var buf []byte
			w := wire.NewWriter(buf)
			w.Varint(1) // parameter count
			w.Varint(uint64(ParamTrackNamespacePrefix))
			w.VarintBytes(param.Bytes)

			// Parse back
			r := wire.NewReader(w.Bytes())
			count, err := r.Varint()
			if err != nil {
				t.Fatalf("failed to read parameter count: %v", err)
			}
			if count != 1 {
				t.Fatalf("expected 1 parameter, got %d", count)
			}

			delta, err := r.Varint()
			if err != nil {
				t.Fatalf("failed to read parameter type delta: %v", err)
			}
			if delta != uint64(ParamTrackNamespacePrefix) {
				t.Fatalf("expected type delta %d, got %d", ParamTrackNamespacePrefix, delta)
			}

			parsedBytes, err := r.VarintBytes()
			if err != nil {
				t.Fatalf("failed to read parameter bytes: %v", err)
			}

			// Parse the namespace from the bytes
			nsReader := wire.NewReader(parsedBytes)
			parsedPrefix, err := nsReader.TrackNamespace()
			if err != nil {
				t.Fatalf("failed to parse track namespace: %v", err)
			}

			// Verify the namespace matches
			if len(parsedPrefix) != len(tt.prefix) {
				t.Errorf("expected %d fields, got %d", len(tt.prefix), len(parsedPrefix))
			}

			for i, field := range tt.prefix {
				if i >= len(parsedPrefix) {
					break
				}
				if string(parsedPrefix[i]) != string(field) {
					t.Errorf("field %d: expected %q, got %q", i, field, parsedPrefix[i])
				}
			}
		})
	}
}

func TestTrackNamespacePrefixParamInParameters(t *testing.T) {
	tests := []struct {
		name       string
		parameters Parameters
	}{
		{
			name: "single namespace prefix parameter",
			parameters: Parameters{
				TrackNamespacePrefixParam(wire.TrackNamespace{[]byte("live"), []byte("sports")}),
			},
		},
		{
			name: "namespace prefix with other parameters",
			parameters: Parameters{
				SubscriberPriorityParam(100),
				TrackNamespacePrefixParam(wire.TrackNamespace{[]byte("video"), []byte("4k")}),
				ForwardParam(true),
			},
			// Note: Parameters are sorted by type during serialization
			// Expected order after sorting: FORWARD (0x10), SUBSCRIBER_PRIORITY (0x20), TRACK_NAMESPACE_PREFIX (0x33)
		},
		{
			name: "multiple namespace prefixes (should be sorted)",
			parameters: Parameters{
				TrackNamespacePrefixParam(wire.TrackNamespace{[]byte("audio")}),
				TrackNamespacePrefixParam(wire.TrackNamespace{[]byte("video")}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize parameters
			var buf []byte
			w := wire.NewWriter(buf)
			tt.parameters.append(w)

			// Parse back
			r := wire.NewReader(w.Bytes())
			var parsed Parameters
			if err := parsed.parse(r); err != nil {
				t.Fatalf("failed to parse parameters: %v", err)
			}

			// Verify parameter count
			if len(parsed) != len(tt.parameters) {
				t.Errorf("expected %d parameters, got %d", len(tt.parameters), len(parsed))
			}

			// Verify each parameter (accounting for sorting by type)
			// Create a map of expected parameters by type
			expectedByType := make(map[ParamID]Parameter)
			for _, p := range tt.parameters {
				expectedByType[p.Type] = p
			}

			// Verify each parsed parameter exists in expected
			for _, actualParam := range parsed {
				expectedParam, ok := expectedByType[actualParam.Type]
				if !ok {
					t.Errorf("unexpected parameter type %v", actualParam.Type)
					continue
				}
				if actualParam.Type != expectedParam.Type {
					t.Errorf("parameter type mismatch: expected %v, got %v", expectedParam.Type, actualParam.Type)
				}
			}
		})
	}
}

func TestTrackNamespacePrefixParamFind(t *testing.T) {
	prefix := wire.TrackNamespace{[]byte("live"), []byte("sports")}
	params := Parameters{
		SubscriberPriorityParam(100),
		TrackNamespacePrefixParam(prefix),
		ForwardParam(true),
	}

	// Find the namespace prefix parameter
	found, ok := params.Find(ParamTrackNamespacePrefix)
	if !ok {
		t.Fatal("TRACK_NAMESPACE_PREFIX parameter not found")
	}

	if found.Type != ParamTrackNamespacePrefix {
		t.Errorf("expected type %v, got %v", ParamTrackNamespacePrefix, found.Type)
	}

	// Verify we can parse the namespace back
	r := wire.NewReader(found.Bytes)
	parsedPrefix, err := r.TrackNamespace()
	if err != nil {
		t.Fatalf("failed to parse track namespace: %v", err)
	}

	if len(parsedPrefix) != len(prefix) {
		t.Errorf("expected %d fields, got %d", len(prefix), len(parsedPrefix))
	}

	for i, field := range prefix {
		if i >= len(parsedPrefix) {
			break
		}
		if string(parsedPrefix[i]) != string(field) {
			t.Errorf("field %d: expected %q, got %q", i, field, parsedPrefix[i])
		}
	}
}

func TestTrackNamespacePrefixParamString(t *testing.T) {
	param := TrackNamespacePrefixParam(wire.TrackNamespace{[]byte("test")})

	expected := "TRACK_NAMESPACE_PREFIX"
	if param.Type.String() != expected {
		t.Errorf("expected %q, got %q", expected, param.Type.String())
	}
}

func TestTrackNamespacePrefixParamKind(t *testing.T) {
	kind, err := kindOf(ParamTrackNamespacePrefix)
	if err != nil {
		t.Fatalf("failed to get parameter kind: %v", err)
	}

	if kind != KindBytes {
		t.Errorf("expected kind %v, got %v", KindBytes, kind)
	}
}

func TestTrackNamespacePrefixParamLargeNamespace(t *testing.T) {
	// Test with a namespace close to the maximum allowed fields
	var fields wire.TrackNamespace
	for i := range 10 {
		fields = append(fields, []byte{byte(i)})
	}

	param := TrackNamespacePrefixParam(fields)

	// Verify the parameter was created successfully
	if param.Type != ParamTrackNamespacePrefix {
		t.Errorf("expected type %v, got %v", ParamTrackNamespacePrefix, param.Type)
	}

	// Verify round-trip
	r := wire.NewReader(param.Bytes)
	parsed, err := r.TrackNamespace()
	if err != nil {
		t.Fatalf("failed to parse namespace: %v", err)
	}

	if len(parsed) != len(fields) {
		t.Errorf("expected %d fields, got %d", len(fields), len(parsed))
	}
}

func TestParametersSortedOnEncode(t *testing.T) {
	in := Parameters{
		ForwardParam(true),
		ExpiresParam(100 * time.Millisecond),
		AuthorizationTokenParam(Token{AliasType: AliasTypeUseValue, TokenType: 0, TokenValue: []byte("tok")}),
	}
	w := wire.NewWriter(nil)
	in.append(w)
	out := Parameters{}
	if err := out.parse(wire.NewReader(w.Bytes())); err != nil {
		t.Fatalf("parse: %v", err)
	}
	for i := 1; i < len(out); i++ {
		if out[i].Type <= out[i-1].Type {
			t.Fatalf("parameters not in ascending order: %v", out)
		}
	}
}

// TestBareParameterLiteralPanicsOnEncode locks in the §10.2 "loud" behaviour:
// a Parameter built as a bare struct literal carries no value kind, so encoding
// it panics rather than silently emitting varint(0). The constructors set the
// kind, so a constructor-built parameter with the same value encodes cleanly.
func TestBareParameterLiteralPanicsOnEncode(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("encoding a bare-literal Parameter did not panic")
		}
	}()
	// No constructor → kind is kindUnset.
	bare := Parameters{{Type: ParamObjectDeliveryTimeout, Varint: 5}}
	bare.append(wire.NewWriter(nil))
}

func TestConstructedParameterEncodes(_ *testing.T) {
	ps := Parameters{VarintParam(ParamObjectDeliveryTimeout, 5)}
	ps.append(wire.NewWriter(nil)) // must not panic
}
