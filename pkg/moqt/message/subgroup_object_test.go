package message

import (
	"bytes"
	"strings"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestSubgroupObject_AppendAndParse(t *testing.T) {
	tests := []struct {
		name          string
		hasProperties bool
		object        *SubgroupObject
		wantErr       bool
	}{
		{
			name:          "normal object without properties",
			hasProperties: false,
			object: &SubgroupObject{
				ObjectIDDelta: 42,
				Payload:       []byte("test payload"),
			},
		},
		{
			name:          "normal object with properties",
			hasProperties: true,
			object: &SubgroupObject{
				ObjectIDDelta: 1,
				Properties:    []byte("properties data"),
				Payload:       []byte("test payload"),
			},
		},
		{
			name:          "end of group status",
			hasProperties: false,
			object: &SubgroupObject{
				ObjectIDDelta: 100,
				ObjectStatus:  0x3, // EndOfGroup
			},
		},
		{
			name:          "end of track status",
			hasProperties: true,
			object: &SubgroupObject{
				ObjectIDDelta: 200,
				Properties:    []byte("props"),
				ObjectStatus:  0x4, // EndOfTrack
			},
		},
		{
			name:          "zero object ID delta",
			hasProperties: false,
			object: &SubgroupObject{
				ObjectIDDelta: 0,
				Payload:       []byte("data"),
			},
		},
		{
			name:          "empty properties when required",
			hasProperties: true,
			object: &SubgroupObject{
				ObjectIDDelta: 10,
				Properties:    []byte{}, // empty but present
				Payload:       []byte("payload"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			w := wire.NewWriter(nil)
			tt.object.Append(w, tt.hasProperties)

			// Parse back
			r := wire.NewReader(w.Bytes())
			parsed := NewSubgroupObject()
			err := parsed.Parse(r, tt.hasProperties)

			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil {
				// Compare fields
				if parsed.ObjectIDDelta != tt.object.ObjectIDDelta {
					t.Errorf("ObjectIDDelta = %v, want %v", parsed.ObjectIDDelta, tt.object.ObjectIDDelta)
				}

				if !bytes.Equal(parsed.Properties, tt.object.Properties) {
					t.Errorf("Properties = %v, want %v", parsed.Properties, tt.object.Properties)
				}

				if !bytes.Equal(parsed.Payload, tt.object.Payload) {
					t.Errorf("Payload = %v, want %v", parsed.Payload, tt.object.Payload)
				}

				if parsed.ObjectStatus != tt.object.ObjectStatus {
					t.Errorf("ObjectStatus = %v, want %v", parsed.ObjectStatus, tt.object.ObjectStatus)
				}
			}
		})
	}
}

func TestSubgroupObject_Validate(t *testing.T) {
	tests := []struct {
		name    string
		object  *SubgroupObject
		wantErr bool
	}{
		{
			name: "valid normal object",
			object: &SubgroupObject{
				ObjectIDDelta: 1,
				Payload:       []byte("data"),
			},
			wantErr: false,
		},
		{
			name: "valid end of group",
			object: &SubgroupObject{
				ObjectIDDelta: 1,
				ObjectStatus:  0x3,
			},
			wantErr: false,
		},
		{
			name: "valid end of track",
			object: &SubgroupObject{
				ObjectIDDelta: 1,
				ObjectStatus:  0x4,
			},
			wantErr: false,
		},
		{
			name: "invalid status value",
			object: &SubgroupObject{
				ObjectIDDelta: 1,
				ObjectStatus:  0xFF, // invalid status
			},
			wantErr: true,
		},
		{
			name: "normal object with status ignored",
			object: &SubgroupObject{
				ObjectIDDelta: 1,
				Payload:       []byte("data"),
				ObjectStatus:  0xFF, // status ignored when payload present
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.object.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSubgroupObject_HelperMethods(t *testing.T) {
	tests := []struct {
		name           string
		object         *SubgroupObject
		wantEndOfGroup bool
		wantEndOfTrack bool
		wantNormal     bool
	}{
		{
			name: "normal object",
			object: &SubgroupObject{
				ObjectIDDelta: 1,
				Payload:       []byte("data"),
			},
			wantEndOfGroup: false,
			wantEndOfTrack: false,
			wantNormal:     true,
		},
		{
			name: "end of group",
			object: &SubgroupObject{
				ObjectIDDelta: 1,
				ObjectStatus:  0x3,
			},
			wantEndOfGroup: true,
			wantEndOfTrack: false,
			wantNormal:     false,
		},
		{
			name: "end of track",
			object: &SubgroupObject{
				ObjectIDDelta: 1,
				ObjectStatus:  0x4,
			},
			wantEndOfGroup: false,
			wantEndOfTrack: true,
			wantNormal:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.object.IsEndOfGroup(); got != tt.wantEndOfGroup {
				t.Errorf("IsEndOfGroup() = %v, want %v", got, tt.wantEndOfGroup)
			}
			if got := tt.object.IsEndOfTrack(); got != tt.wantEndOfTrack {
				t.Errorf("IsEndOfTrack() = %v, want %v", got, tt.wantEndOfTrack)
			}
			if got := tt.object.IsNormalObject(); got != tt.wantNormal {
				t.Errorf("IsNormalObject() = %v, want %v", got, tt.wantNormal)
			}
		})
	}
}

func TestSubgroupObject_BuilderMethods(t *testing.T) {
	obj := NewSubgroupObject().
		WithObjectIDDelta(42).
		WithProperties([]byte("props")).
		WithPayload([]byte("payload")).
		WithStatus(0x3)

	if obj.ObjectIDDelta != 42 {
		t.Errorf("ObjectIDDelta = %v, want 42", obj.ObjectIDDelta)
	}
	if !bytes.Equal(obj.Properties, []byte("props")) {
		t.Errorf("Properties = %v, want 'props'", obj.Properties)
	}
	if !bytes.Equal(obj.Payload, []byte("payload")) {
		t.Errorf("Payload = %v, want 'payload'", obj.Payload)
	}
	if obj.ObjectStatus != 0x3 {
		t.Errorf("ObjectStatus = %v, want 0x3", obj.ObjectStatus)
	}
}

func TestSubgroupObject_ParseErrors(t *testing.T) {
	tests := []struct {
		name          string
		data          []byte
		hasProperties bool
		wantErr       string
	}{
		{
			name:          "truncated object ID delta",
			data:          []byte{0x80}, // incomplete varint
			hasProperties: false,
			wantErr:       "object ID delta",
		},
		{
			name:          "truncated properties",
			data:          []byte{0x01, 0x80}, // object ID = 1, incomplete properties length
			hasProperties: true,
			wantErr:       "properties",
		},
		{
			name:          "truncated payload length",
			data:          []byte{0x01}, // object ID = 1, missing payload length
			hasProperties: false,
			wantErr:       "payload length",
		},
		{
			name:          "truncated status",
			data:          []byte{0x01, 0x00}, // object ID = 1, payload length = 0, missing status
			hasProperties: false,
			wantErr:       "object status",
		},
		{
			name:          "truncated payload",
			data:          []byte{0x01, 0x05}, // object ID = 1, payload length = 5, no payload bytes
			hasProperties: false,
			wantErr:       "payload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := wire.NewReader(tt.data)
			obj := NewSubgroupObject()
			err := obj.Parse(r, tt.hasProperties)

			if err == nil {
				t.Fatal("Parse() expected error, got nil")
			}

			if tt.wantErr != "" && !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Parse() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}
