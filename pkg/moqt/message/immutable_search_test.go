package message

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// §12.7: "When looking for the value of a property, processors MUST search
// both the mutable properties and the contents of Immutable Properties."

// immutable wraps pairs in an Immutable Properties property.
func immutable(pairs ...wire.KVPair) wire.KVPair {
	return wire.KVPair{Type: PropertyImmutableProperties, ByteVal: AppendTrackProperties(pairs)}
}

func TestApplyObjectPropertiesSearchesImmutable(t *testing.T) {
	track := DeliveryTimeouts{Object: 5 * time.Second, Subgroup: 5 * time.Second}
	raw := AppendTrackProperties([]wire.KVPair{immutable(
		wire.KVPair{Type: PropertyObjectDeliveryTimeout, IntVal: 2000},
		wire.KVPair{Type: PropertySubgroupDeliveryTimeout, IntVal: 3000},
	)})
	want := DeliveryTimeouts{Object: 2 * time.Second, Subgroup: 3 * time.Second}
	if got := track.ApplyObjectProperties(raw); got != want {
		t.Errorf("got %+v, want %+v from inside Immutable Properties", got, want)
	}

	// Present in both: the mutable value wins, as for
	// TrackDefaultPublisherPriority.
	raw = AppendTrackProperties([]wire.KVPair{
		{Type: PropertyObjectDeliveryTimeout, IntVal: 1000},
		immutable(wire.KVPair{Type: PropertyObjectDeliveryTimeout, IntVal: 2000}),
	})
	if got := track.ApplyObjectProperties(raw); got.Object != time.Second {
		t.Errorf("Object = %v, want the mutable 1s over the immutable 2s", got.Object)
	}
}

func TestRangeFiltersSearchImmutable(t *testing.T) {
	inImmutable := func(typ PropertyType, val uint64) []byte {
		return AppendTrackProperties([]wire.KVPair{immutable(wire.KVPair{Type: typ, IntVal: val})})
	}
	track := mustSet(t,
		RangeFilter{Type: ParamTrackPropertyFilter, PropertyType: 0x40, Ranges: []Range{{Start: 1, End: 5}}})
	if !track.MatchesTrack(inImmutable(0x40, 3)) {
		t.Error("MatchesTrack: 0x40=3 inside Immutable Properties should match [1,5]")
	}
	if pass := track.TrackPassPerGroup(inImmutable(0x40, 3)); len(pass) != 1 || !pass[0] {
		t.Errorf("TrackPassPerGroup = %v, want [true]", pass)
	}
	obj := mustSet(t,
		RangeFilter{Type: ParamObjectPropertyFilter, PropertyType: 0x3C, Ranges: []Range{{Start: 40, End: 50}}})
	if !obj.MatchesObject(0, 0, 0, inImmutable(0x3C, 42)) {
		t.Error("MatchesObject: 0x3C=42 inside Immutable Properties should match [40,50]")
	}
}
