package message

import (
	"fmt"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestIsValidDatagramType(t *testing.T) {
	tests := []struct {
		name     string
		typ      uint64
		expected bool
	}{
		// Low range: all 0x00..0x0F are valid.
		{"Valid type 0x00", DatagramTypeMin, true},
		{"Valid type 0x0F", DatagramTypeMax, true},
		{"Valid type with PROPERTIES bit", DatagramPropertiesBit, true},
		{"Valid type with END_OF_GROUP bit", DatagramEndOfGroupBit, true},
		{"Valid type with ZERO_OBJECT_ID bit", DatagramZeroObjectIDBit, true},
		{"Valid type with DEFAULT_PRIORITY bit", DatagramDefaultPriorityBit, true},
		// STATUS range: only STATUS without PROPERTIES or END_OF_GROUP is valid.
		{"Valid type 0x20 (STATUS only)", DatagramStatusBit, true},
		{"Valid type 0x24 (STATUS|ZERO_OBJECT_ID)", DatagramStatusBit | DatagramZeroObjectIDBit, true},
		{"Valid type 0x28 (STATUS|DEFAULT_PRIORITY)", DatagramStatusBit | DatagramDefaultPriorityBit, true},
		{
			"Valid type 0x2C (STATUS|ZERO_OBJECT_ID|DEFAULT_PRIORITY)",
			DatagramStatusBit | DatagramZeroObjectIDBit | DatagramDefaultPriorityBit,
			true,
		},
		// Invalid: STATUS+END_OF_GROUP
		{"Invalid type 0x22 (STATUS|END_OF_GROUP)", DatagramStatusBit | DatagramEndOfGroupBit, false},
		{"Invalid type 0x23", 0x23, false},
		{"Invalid type 0x26", 0x26, false},
		{"Invalid type 0x27", 0x27, false},
		{"Invalid type 0x2A", 0x2A, false},
		{"Invalid type 0x2B", 0x2B, false},
		{"Invalid type 0x2E", 0x2E, false},
		{"Invalid type 0x2F", 0x2F, false},
		// Invalid: STATUS+PROPERTIES
		{"Invalid type 0x21 (STATUS|PROPERTIES)", DatagramStatusBit | DatagramPropertiesBit, false},
		{"Invalid type 0x25", 0x25, false},
		{"Invalid type 0x29", 0x29, false},
		{"Invalid type 0x2D", 0x2D, false},
		// Out of range entirely
		{"Invalid type 0x10", 0x10, false},
		{"Invalid type 0x1F", 0x1F, false},
		{"Invalid type 0x30", 0x30, false},
		{"Invalid type 0xFF", 0xFF, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsValidDatagramType(tt.typ)
			if result != tt.expected {
				t.Errorf("IsValidDatagramType(0x%02X) = %v, want %v", tt.typ, result, tt.expected)
			}
		})
	}
}

func TestObjectDatagramBitMethods(t *testing.T) {
	tests := []struct {
		name               string
		typ                uint64
		hasProperties      bool
		hasEndOfGroup      bool
		hasZeroObjectID    bool
		hasDefaultPriority bool
		hasStatus          bool
	}{
		{"No bits set", 0x00, false, false, false, false, false},
		{"PROPERTIES bit", DatagramPropertiesBit, true, false, false, false, false},
		{"END_OF_GROUP bit", DatagramEndOfGroupBit, false, true, false, false, false},
		{"ZERO_OBJECT_ID bit", DatagramZeroObjectIDBit, false, false, true, false, false},
		{"DEFAULT_PRIORITY bit", DatagramDefaultPriorityBit, false, false, false, true, false},
		{"STATUS bit", DatagramStatusBit, false, false, false, false, true},
		{
			"Multiple bits",
			DatagramPropertiesBit | DatagramEndOfGroupBit | DatagramZeroObjectIDBit | DatagramDefaultPriorityBit,
			true,
			true,
			true,
			true,
			false,
		},
		{"STATUS with other bits", DatagramStatusBit | DatagramPropertiesBit, true, false, false, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &ObjectDatagram{Type: tt.typ}

			if d.HasProperties() != tt.hasProperties {
				t.Errorf("HasProperties() = %v, want %v", d.HasProperties(), tt.hasProperties)
			}
			if d.HasEndOfGroup() != tt.hasEndOfGroup {
				t.Errorf("HasEndOfGroup() = %v, want %v", d.HasEndOfGroup(), tt.hasEndOfGroup)
			}
			if d.HasZeroObjectID() != tt.hasZeroObjectID {
				t.Errorf("HasZeroObjectID() = %v, want %v", d.HasZeroObjectID(), tt.hasZeroObjectID)
			}
			if d.HasDefaultPriority() != tt.hasDefaultPriority {
				t.Errorf("HasDefaultPriority() = %v, want %v", d.HasDefaultPriority(), tt.hasDefaultPriority)
			}
			if d.HasStatus() != tt.hasStatus {
				t.Errorf("HasStatus() = %v, want %v", d.HasStatus(), tt.hasStatus)
			}
		})
	}
}

func TestObjectDatagramValidate(t *testing.T) {
	tests := []struct {
		name        string
		datagram    *ObjectDatagram
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid basic datagram",
			datagram: &ObjectDatagram{
				Type:          0x00,
				TrackAlias:    1,
				GroupID:       100,
				ObjectID:      50,
				ObjectPayload: []byte("test payload"),
			},
			expectError: false,
		},
		{
			name: "Valid datagram with properties",
			datagram: &ObjectDatagram{
				Type:          DatagramPropertiesBit,
				TrackAlias:    1,
				GroupID:       100,
				ObjectID:      50,
				Properties:    []byte("properties"),
				ObjectPayload: []byte("test payload"),
			},
			expectError: false,
		},
		{
			name: "Valid datagram with zero object ID",
			datagram: &ObjectDatagram{
				Type:          DatagramZeroObjectIDBit,
				TrackAlias:    1,
				GroupID:       100,
				ObjectPayload: []byte("test payload"),
			},
			expectError: false,
		},
		{
			name: "Valid datagram with default priority",
			datagram: &ObjectDatagram{
				Type:          DatagramDefaultPriorityBit,
				TrackAlias:    1,
				GroupID:       100,
				ObjectID:      50,
				ObjectPayload: []byte("test payload"),
			},
			expectError: false,
		},
		{
			name: "Valid datagram with status",
			datagram: &ObjectDatagram{
				Type:         DatagramStatusBit,
				TrackAlias:   1,
				GroupID:      100,
				ObjectID:     50,
				ObjectStatus: 1, // Normal status
			},
			expectError: false,
		},
		{
			name: "Invalid type 0x10",
			datagram: &ObjectDatagram{
				Type:          0x10,
				TrackAlias:    1,
				GroupID:       100,
				ObjectPayload: []byte("test payload"),
			},
			expectError: true,
			errorMsg:    "invalid datagram type: 0x10",
		},
		{
			// Per §11.3.1: STATUS+END_OF_GROUP is an invalid type value itself.
			name: "Invalid: STATUS and END_OF_GROUP both set (type 0x22)",
			datagram: &ObjectDatagram{
				Type:         DatagramStatusBit | DatagramEndOfGroupBit, // 0x22
				TrackAlias:   1,
				GroupID:      100,
				ObjectStatus: 1,
			},
			expectError: true,
			errorMsg:    "invalid datagram type: 0x22",
		},
		{
			// Per §11.3.1: STATUS+PROPERTIES is an invalid type value itself.
			name: "Invalid: PROPERTIES with STATUS bit (type 0x21)",
			datagram: &ObjectDatagram{
				Type:         DatagramStatusBit | DatagramPropertiesBit, // 0x21
				TrackAlias:   1,
				GroupID:      100,
				Properties:   []byte("properties"),
				ObjectStatus: 1,
			},
			expectError: true,
			errorMsg:    "invalid datagram type: 0x21",
		},
		{
			// Per §11.2.1.1: ObjectStatus 0x0 = Normal, which is valid
			name: "Valid: STATUS bit set with ObjectStatus zero (Normal)",
			datagram: &ObjectDatagram{
				Type:         DatagramStatusBit,
				TrackAlias:   1,
				GroupID:      100,
				ObjectStatus: 0, // Normal status = valid
			},
			expectError: false,
		},
		{
			// Per spec: zero-length payload is valid for Normal objects.
			name: "Valid: no STATUS bit with empty ObjectPayload",
			datagram: &ObjectDatagram{
				Type:          0x00,
				TrackAlias:    1,
				GroupID:       100,
				ObjectID:      50,
				ObjectPayload: []byte{},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.datagram.Validate()
			if tt.expectError {
				if err == nil {
					t.Errorf("Validate() expected error but got nil")
				} else if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("Validate() error = %v, want %v", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("Validate() unexpected error: %v", err)
				}
			}
		})
	}
}

func TestObjectDatagramRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		datagram *ObjectDatagram
	}{
		{
			name: "Basic datagram",
			datagram: &ObjectDatagram{
				Type:          0x00,
				TrackAlias:    1,
				GroupID:       100,
				ObjectID:      50,
				ObjectPayload: []byte("test payload"),
			},
		},
		{
			name: "Datagram with all optional fields",
			datagram: &ObjectDatagram{
				Type:              DatagramPropertiesBit,
				TrackAlias:        2,
				GroupID:           200,
				ObjectID:          75,
				PublisherPriority: 128,
				Properties:        []byte("properties data"),
				ObjectPayload:     []byte("payload data"),
			},
		},
		{
			name: "Datagram with zero object ID",
			datagram: &ObjectDatagram{
				Type:          DatagramZeroObjectIDBit,
				TrackAlias:    3,
				GroupID:       300,
				ObjectPayload: []byte("zero object id payload"),
			},
		},
		{
			name: "Datagram with default priority",
			datagram: &ObjectDatagram{
				Type:          DatagramDefaultPriorityBit,
				TrackAlias:    4,
				GroupID:       400,
				ObjectID:      100,
				ObjectPayload: []byte("default priority payload"),
			},
		},
		{
			name: "Datagram with status",
			datagram: &ObjectDatagram{
				Type:         DatagramStatusBit,
				TrackAlias:   5,
				GroupID:      500,
				ObjectID:     125,
				ObjectStatus: 1,
			},
		},
		{
			name: "Complex datagram with multiple bits",
			datagram: &ObjectDatagram{
				Type:              DatagramPropertiesBit | DatagramZeroObjectIDBit | DatagramDefaultPriorityBit,
				TrackAlias:        6,
				GroupID:           600,
				ObjectID:          0, // ZERO_OBJECT_ID bit set
				PublisherPriority: 0, // DEFAULT_PRIORITY bit set
				Properties:        []byte("complex properties"),
				ObjectPayload:     []byte("complex payload"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Validate original datagram
			if err := tt.datagram.Validate(); err != nil {
				t.Fatalf("Original datagram validation failed: %v", err)
			}

			// Serialize
			var w wire.Writer
			tt.datagram.Append(&w)
			serialized := w.Bytes()

			// Deserialize
			r := wire.NewReader(serialized)
			parsed := &ObjectDatagram{}
			if err := parsed.Parse(r); err != nil {
				t.Fatalf("ObjectDatagram.Parse() failed: %v", err)
			}

			// Validate parsed datagram
			if err := parsed.Validate(); err != nil {
				t.Fatalf("Parsed datagram validation failed: %v", err)
			}

			// Compare fields
			if parsed.Type != tt.datagram.Type {
				t.Errorf("Type = %v, want %v", parsed.Type, tt.datagram.Type)
			}
			if parsed.TrackAlias != tt.datagram.TrackAlias {
				t.Errorf("TrackAlias = %v, want %v", parsed.TrackAlias, tt.datagram.TrackAlias)
			}
			if parsed.GroupID != tt.datagram.GroupID {
				t.Errorf("GroupID = %v, want %v", parsed.GroupID, tt.datagram.GroupID)
			}
			if parsed.ObjectID != tt.datagram.ObjectID {
				t.Errorf("ObjectID = %v, want %v", parsed.ObjectID, tt.datagram.ObjectID)
			}
			if parsed.PublisherPriority != tt.datagram.PublisherPriority {
				t.Errorf("PublisherPriority = %v, want %v", parsed.PublisherPriority, tt.datagram.PublisherPriority)
			}
			if string(parsed.Properties) != string(tt.datagram.Properties) {
				t.Errorf("Properties = %v, want %v", parsed.Properties, tt.datagram.Properties)
			}
			if parsed.ObjectStatus != tt.datagram.ObjectStatus {
				t.Errorf("ObjectStatus = %v, want %v", parsed.ObjectStatus, tt.datagram.ObjectStatus)
			}
			if string(parsed.ObjectPayload) != string(tt.datagram.ObjectPayload) {
				t.Errorf("ObjectPayload = %v, want %v", parsed.ObjectPayload, tt.datagram.ObjectPayload)
			}
		})
	}
}

func TestParseObjectDatagramErrors(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expectError string
	}{
		{
			name:        "Empty data",
			data:        []byte{},
			expectError: "failed to read datagram type: moqt/wire: short buffer",
		},
		{
			name:        "Invalid type 0x10",
			data:        []byte{0x10, 0x01}, // Type 0x10, TrackAlias 1
			expectError: "invalid datagram type: 0x10",
		},
		{
			// Per §11.3.1: STATUS+END_OF_GROUP (0x22) is an invalid type value.
			name:        "STATUS and END_OF_GROUP both set (type 0x22)",
			data:        []byte{byte(DatagramStatusBit | DatagramEndOfGroupBit), 0x01, 0x64},
			expectError: "invalid datagram type: 0x22",
		},
		{
			// Per §11.3.1: STATUS+PROPERTIES (0x21) is an invalid type value.
			name:        "PROPERTIES with STATUS bit (type 0x21)",
			data:        []byte{0x21, 0x01, 0x64},
			expectError: "invalid datagram type: 0x21",
		},
		{
			// Per §11.3.1: PROPERTIES bit set with Properties Length 0 is a
			// PROTOCOL_VIOLATION. Type 0x01 = PROPERTIES only; fields are
			// TrackAlias, GroupID, ObjectID, Priority, then Properties (len 0).
			name:        "PROPERTIES bit with zero-length Properties (type 0x01)",
			data:        []byte{0x01, 0x01, 0x01, 0x01, 0x00, 0x00},
			expectError: "invalid datagram: PROPERTIES bit set with zero-length Properties",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := wire.NewReader(tt.data)
			err := (&ObjectDatagram{}).Parse(r)
			if err == nil {
				t.Errorf("ObjectDatagram.Parse() expected error but got nil")
			} else if tt.expectError != "" {
				if err.Error() != tt.expectError {
					t.Errorf("ObjectDatagram.Parse() error = %v, want %v", err.Error(), tt.expectError)
				}
			}
		})
	}
}

func TestObjectDatagramValidateInvalidType(t *testing.T) {
	// Valid basic datagram — struct literal, no constructor needed.
	d := &ObjectDatagram{
		Type:              0x00,
		TrackAlias:        1,
		GroupID:           100,
		ObjectID:          50,
		PublisherPriority: 128,
		ObjectPayload:     []byte("test"),
	}
	if err := d.Validate(); err != nil {
		t.Errorf("Validate() unexpected error: %v", err)
	}

	// Invalid type — Validate must reject it.
	bad := &ObjectDatagram{
		Type:          0x10,
		TrackAlias:    1,
		GroupID:       100,
		ObjectID:      50,
		ObjectPayload: []byte("test"),
	}
	if err := bad.Validate(); err == nil {
		t.Errorf("Validate() expected error for type 0x10, got nil")
	}
}

func TestObjectDatagramValidateZeroLengthProperties(t *testing.T) {
	// §11.3.1: PROPERTIES bit set with empty Properties is a PROTOCOL_VIOLATION.
	d := &ObjectDatagram{
		Type:          DatagramPropertiesBit,
		TrackAlias:    1,
		GroupID:       100,
		ObjectID:      50,
		Properties:    nil,
		ObjectPayload: []byte("test"),
	}
	if err := d.Validate(); err == nil {
		t.Errorf("Validate() expected error for PROPERTIES bit with empty Properties, got nil")
	}

	// Non-empty Properties with the bit set is fine.
	d.Properties = []byte{0x01}
	if err := d.Validate(); err != nil {
		t.Errorf("Validate() unexpected error for non-empty Properties: %v", err)
	}
}

func TestObjectDatagramWithStatusValidation(t *testing.T) {
	// Valid status datagram.
	d := &ObjectDatagram{
		Type:              DatagramStatusBit,
		TrackAlias:        1,
		GroupID:           100,
		ObjectID:          50,
		PublisherPriority: 128,
		ObjectStatus:      1,
	}
	if err := d.Validate(); err != nil {
		t.Errorf("Validate() unexpected error: %v", err)
	}

	// Invalid type with status.
	bad := &ObjectDatagram{
		Type:         0x10,
		TrackAlias:   1,
		GroupID:      100,
		ObjectID:     50,
		ObjectStatus: 1,
	}
	if err := bad.Validate(); err == nil {
		t.Errorf("Validate() expected error for type 0x10, got nil")
	}
}

func TestObjectDatagramEndOfGroup(t *testing.T) {
	// Test END_OF_GROUP bit functionality
	d := &ObjectDatagram{
		Type:          DatagramEndOfGroupBit,
		TrackAlias:    1,
		GroupID:       100,
		ObjectPayload: []byte("end of group marker"),
	}

	if !d.HasEndOfGroup() {
		t.Errorf("HasEndOfGroup() = false, want true")
	}

	if err := d.Validate(); err != nil {
		t.Errorf("Validate() failed: %v", err)
	}

	// Test round-trip
	var w wire.Writer
	d.Append(&w)
	serialized := w.Bytes()

	r := wire.NewReader(serialized)
	parsed := &ObjectDatagram{}
	if err := parsed.Parse(r); err != nil {
		t.Fatalf("ObjectDatagram.Parse() failed: %v", err)
	}

	if !parsed.HasEndOfGroup() {
		t.Errorf("Parsed datagram HasEndOfGroup() = false, want true")
	}
}

func TestObjectDatagramComplexCombinations(t *testing.T) {
	// Test various valid combinations of bits
	// Note: Types with both PROPERTIES and STATUS bits are invalid
	// Note: Types with both STATUS and END_OF_GROUP bits are invalid
	validTypes := []uint64{
		// Basic types (no optional fields)
		0x00,

		// Single bit types
		DatagramPropertiesBit,
		DatagramEndOfGroupBit,
		DatagramZeroObjectIDBit,
		DatagramDefaultPriorityBit,

		// Two bit combinations (no STATUS bit)
		DatagramPropertiesBit | DatagramEndOfGroupBit,
		DatagramPropertiesBit | DatagramZeroObjectIDBit,
		DatagramPropertiesBit | DatagramDefaultPriorityBit,
		DatagramEndOfGroupBit | DatagramZeroObjectIDBit,
		DatagramEndOfGroupBit | DatagramDefaultPriorityBit,
		DatagramZeroObjectIDBit | DatagramDefaultPriorityBit,

		// Three bit combinations (no STATUS bit)
		DatagramPropertiesBit | DatagramEndOfGroupBit | DatagramZeroObjectIDBit,
		DatagramPropertiesBit | DatagramEndOfGroupBit | DatagramDefaultPriorityBit,
		DatagramPropertiesBit | DatagramZeroObjectIDBit | DatagramDefaultPriorityBit,
		DatagramEndOfGroupBit | DatagramZeroObjectIDBit | DatagramDefaultPriorityBit,

		// All four bits (no STATUS bit)
		DatagramPropertiesBit | DatagramEndOfGroupBit | DatagramZeroObjectIDBit | DatagramDefaultPriorityBit,

		// STATUS bit types (no PROPERTIES or END_OF_GROUP bits allowed)
		DatagramStatusBit,
		DatagramStatusBit | DatagramZeroObjectIDBit,
		DatagramStatusBit | DatagramDefaultPriorityBit,
		DatagramStatusBit | DatagramZeroObjectIDBit | DatagramDefaultPriorityBit,
	}

	for _, typ := range validTypes {
		t.Run(fmt.Sprintf("Type_0x%02X", typ), func(t *testing.T) {
			if !IsValidDatagramType(typ) {
				t.Errorf("Type 0x%02X should be valid", typ)
			}

			d := &ObjectDatagram{
				Type:       typ,
				TrackAlias: 1,
				GroupID:    100,
			}

			// Set appropriate fields based on type
			if !d.HasZeroObjectID() {
				d.ObjectID = 50
			}
			if !d.HasDefaultPriority() {
				d.PublisherPriority = 128
			}
			// Properties only allowed without STATUS bit
			if d.HasProperties() && !d.HasStatus() {
				d.Properties = []byte("properties")
			}
			if d.HasStatus() {
				d.ObjectStatus = 1
			} else {
				d.ObjectPayload = []byte("payload")
			}

			if err := d.Validate(); err != nil {
				t.Errorf("Validate() failed for type 0x%02X: %v", typ, err)
			}

			// Test round-trip
			var w wire.Writer
			d.Append(&w)
			serialized := w.Bytes()

			r := wire.NewReader(serialized)
			parsed := &ObjectDatagram{}
			if err := parsed.Parse(r); err != nil {
				t.Fatalf("ObjectDatagram.Parse() failed for type 0x%02X: %v", typ, err)
			}

			if parsed.Type != typ {
				t.Errorf("Parsed Type = 0x%02X, want 0x%02X", parsed.Type, typ)
			}
		})
	}
}
