package message

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestCheckObjectProperties pins the per-Object malformed-track conditions of
// §12.7–§12.9 and §2.5.1. Conditions that need state across Objects (a gap
// covering an Object already received, differing gaps within a Group) are
// not checked here.
func TestCheckObjectProperties(t *testing.T) {
	kv := func(typ PropertyType, v uint64) wire.KVPair { return wire.KVPair{Type: typ, IntVal: v} }
	imm := func(pairs ...wire.KVPair) wire.KVPair {
		return wire.KVPair{Type: PropertyImmutableProperties, ByteVal: AppendTrackProperties(pairs)}
	}
	const group, object = 10, 5
	for _, tc := range []struct {
		name  string
		raw   []byte
		valid bool
	}{
		{"empty", nil, true},
		{"ordinary properties", AppendTrackProperties([]wire.KVPair{kv(0x40, 1), kv(0x42, 2)}), true},
		{"gaps within range", AppendTrackProperties([]wire.KVPair{
			kv(PropertyPriorGroupIDGap, group), kv(PropertyPriorObjectIDGap, object),
		}), true},
		{"gap inside Immutable", AppendTrackProperties([]wire.KVPair{imm(kv(PropertyPriorObjectIDGap, 2))}), true},
		{"same type in both lists", AppendTrackProperties([]wire.KVPair{kv(0x40, 1), imm(kv(0x40, 2))}), true},

		// "A Key-Value-Pair cannot be parsed" (§12.7)
		{"unparseable", []byte{0x02}, false},
		{"unparseable inside Immutable", AppendTrackProperties([]wire.KVPair{
			{Type: PropertyImmutableProperties, ByteVal: []byte{0x02}},
		}), false},
		// §12.7: nested Immutable, and "An Object MUST NOT contain more than
		// one instance of this property"
		{"Immutable inside Immutable", AppendTrackProperties([]wire.KVPair{imm(imm(kv(0x40, 1)))}), false},
		{"two Immutable", AppendTrackProperties([]wire.KVPair{imm(kv(0x40, 1)), imm(kv(0x42, 1))}), false},
		// §12.8 / §12.9: more than one instance, or larger than the ID
		{"two Prior Group ID Gaps", AppendTrackProperties([]wire.KVPair{
			kv(PropertyPriorGroupIDGap, 1), kv(PropertyPriorGroupIDGap, 2),
		}), false},
		{"Prior Group ID Gap in both lists", AppendTrackProperties([]wire.KVPair{
			kv(PropertyPriorGroupIDGap, 1), imm(kv(PropertyPriorGroupIDGap, 1)),
		}), false},
		{"Prior Group ID Gap > Group ID", AppendTrackProperties([]wire.KVPair{
			kv(PropertyPriorGroupIDGap, group+1),
		}), false},
		{"two Prior Object ID Gaps", AppendTrackProperties([]wire.KVPair{
			kv(PropertyPriorObjectIDGap, 1), kv(PropertyPriorObjectIDGap, 1),
		}), false},
		{"Prior Object ID Gap > Object ID", AppendTrackProperties([]wire.KVPair{
			imm(kv(PropertyPriorObjectIDGap, object+1)),
		}), false},
		// §2.5.1: "An Object received with a Mandatory Track Property as an
		// Object Property is malformed"
		{"Mandatory Track Property", AppendTrackProperties([]wire.KVPair{kv(0x4000, 1)}), false},
		{"Mandatory inside Immutable", AppendTrackProperties([]wire.KVPair{imm(kv(0x7FFE, 1))}), false},
	} {
		err := CheckObjectProperties(tc.raw, group, object)
		if (err == nil) != tc.valid {
			t.Errorf("%s: CheckObjectProperties = %v, want valid=%v", tc.name, err, tc.valid)
		}
	}
}

func BenchmarkCheckObjectProperties(b *testing.B) {
	raw := AppendTrackProperties([]wire.KVPair{
		{Type: 0x40, IntVal: 1}, {Type: PropertyPriorObjectIDGap, IntVal: 1},
		{Type: PropertyImmutableProperties, ByteVal: AppendTrackProperties([]wire.KVPair{{Type: 0x42, IntVal: 7}})},
	})
	b.ReportAllocs()
	for b.Loop() {
		if err := CheckObjectProperties(raw, 10, 5); err != nil {
			b.Fatal(err)
		}
	}
}
