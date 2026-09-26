package registry_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// dynamicGroupsProps encodes Track Properties carrying DYNAMIC_GROUPS = value.
func dynamicGroupsProps(t *testing.T, value uint64) []byte {
	t.Helper()
	return message.AppendTrackProperties([]wire.KVPair{
		{Type: message.PropertyDynamicGroups, IntVal: value},
	})
}

// TestTrackEntry_DynamicGroups pins the §12.6 DYNAMIC_GROUPS decode that
// SetProperties performs once and caches: an absent property and value 0 are
// false, and value 1 is true. The session closes on a value above 1 (§12.6)
// before Properties reach the registry.
func TestTrackEntry_DynamicGroups(t *testing.T) {
	t.Parallel()

	setProps := func(props []byte) *registry.TrackEntry {
		e := &registry.TrackEntry{}
		e.SetProperties(props)
		return e
	}

	t.Run("absent is false", func(t *testing.T) {
		t.Parallel()
		got, err := setProps(nil).DynamicGroups()
		if err != nil || got {
			t.Fatalf("got (%v, %v), want (false, nil)", got, err)
		}
	})
	t.Run("value 0 is false", func(t *testing.T) {
		t.Parallel()
		got, err := setProps(dynamicGroupsProps(t, 0)).DynamicGroups()
		if err != nil || got {
			t.Fatalf("got (%v, %v), want (false, nil)", got, err)
		}
	})
	t.Run("value 1 is true", func(t *testing.T) {
		t.Parallel()
		got, err := setProps(dynamicGroupsProps(t, 1)).DynamicGroups()
		if err != nil || !got {
			t.Fatalf("got (%v, %v), want (true, nil)", got, err)
		}
	})
	t.Run("malformed block is an error", func(t *testing.T) {
		t.Parallel()
		// 0x40 is the first byte of a 2-byte varint with no second byte, so
		// the Properties block fails to parse structurally.
		if _, err := setProps([]byte{0x40}).DynamicGroups(); err == nil {
			t.Fatal("got nil error, want a structural parse error")
		}
	})
}

// TestTrackEntry_DefaultGroupOrder pins the §12.5 DEFAULT_PUBLISHER_GROUP_ORDER
// decode: Ascending when omitted, the value when allowed, and Ascending for a
// value outside {1, 2} or a malformed block, including one that would
// truncate to an allowed byte.
func TestTrackEntry_DefaultGroupOrder(t *testing.T) {
	t.Parallel()
	order := func(v uint64) []byte {
		return message.AppendTrackProperties([]wire.KVPair{
			{Type: message.PropertyDefaultPublisherGroupOrder, IntVal: v},
		})
	}
	for _, tc := range []struct {
		name  string
		props []byte
		want  message.GroupOrder
	}{
		{"absent", nil, message.GroupOrderAscending},
		{"Ascending", order(1), message.GroupOrderAscending},
		{"Descending", order(2), message.GroupOrderDescending},
		{"out of range", order(3), message.GroupOrderAscending},
		{"truncates to Descending", order(0x102), message.GroupOrderAscending},
		{"malformed block", []byte{0x40}, message.GroupOrderAscending},
		{"inside Immutable Properties", message.AppendTrackProperties([]wire.KVPair{
			{Type: message.PropertyImmutableProperties, ByteVal: order(2)},
		}), message.GroupOrderDescending},
		{"mutable Ascending over immutable Descending", message.AppendTrackProperties([]wire.KVPair{
			{Type: message.PropertyDefaultPublisherGroupOrder, IntVal: 1},
			{Type: message.PropertyImmutableProperties, ByteVal: order(2)},
		}), message.GroupOrderAscending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := &registry.TrackEntry{}
			e.SetProperties(tc.props)
			if got := e.DefaultGroupOrder(); got != tc.want {
				t.Fatalf("DefaultGroupOrder() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestConsiderNewGroupRequest pins the §10.2.19 relay decision and its
// outstanding-request bookkeeping.
func TestConsiderNewGroupRequest(t *testing.T) {
	t.Parallel()

	t.Run("not forwarded when track lacks dynamic groups", func(t *testing.T) {
		t.Parallel()
		e := &registry.TrackEntry{}
		if e.ConsiderNewGroupRequest(5, false) {
			t.Fatal("forwarded despite dynamicGroups=false")
		}
	})

	t.Run("non-zero value at or below largest is not forwarded", func(t *testing.T) {
		t.Parallel()
		e := &registry.TrackEntry{HasLargestObject: true, LargestObject: message.Location{Group: 5}}
		if e.ConsiderNewGroupRequest(5, true) {
			t.Fatal("forwarded value == largest group")
		}
		if e.ConsiderNewGroupRequest(3, true) {
			t.Fatal("forwarded value < largest group")
		}
	})

	t.Run("value larger than largest is forwarded once until covered", func(t *testing.T) {
		t.Parallel()
		e := &registry.TrackEntry{HasLargestObject: true, LargestObject: message.Location{Group: 5}}
		if !e.ConsiderNewGroupRequest(6, true) {
			t.Fatal("first request not forwarded")
		}
		// A second equal request is already covered by the outstanding one.
		if e.ConsiderNewGroupRequest(6, true) {
			t.Fatal("duplicate request forwarded while outstanding")
		}
		// A smaller-or-equal outstanding value also covers a lower request.
		if e.ConsiderNewGroupRequest(6, true) {
			t.Fatal("covered request forwarded")
		}
	})

	t.Run("zero value always triggers but is covered while outstanding", func(t *testing.T) {
		t.Parallel()
		e := &registry.TrackEntry{} // no objects: largest 0
		if !e.ConsiderNewGroupRequest(0, true) {
			t.Fatal("value 0 not forwarded")
		}
		if e.ConsiderNewGroupRequest(0, true) {
			t.Fatal("value 0 forwarded again while outstanding")
		}
	})

	t.Run("outstanding clears once largest group advances", func(t *testing.T) {
		t.Parallel()
		e := &registry.TrackEntry{} // largest 0
		if !e.ConsiderNewGroupRequest(5, true) {
			t.Fatal("first request not forwarded")
		}
		// Publisher started a new group: largest advances past where we asked.
		e.LargestObject = message.Location{Group: 1}
		e.HasLargestObject = true
		if !e.ConsiderNewGroupRequest(5, true) {
			t.Fatal("request not re-forwarded after largest group advanced")
		}
	})

	t.Run("larger outstanding value covers a smaller later request", func(t *testing.T) {
		t.Parallel()
		e := &registry.TrackEntry{}
		if !e.ConsiderNewGroupRequest(10, true) {
			t.Fatal("first request not forwarded")
		}
		if e.ConsiderNewGroupRequest(5, true) {
			t.Fatal("smaller request forwarded despite larger outstanding value")
		}
	})
}

// TestTrackEntry_PropertiesInsideImmutable: every Track Property the relay
// decodes is also found inside Immutable Properties (§12.7).
func TestTrackEntry_PropertiesInsideImmutable(t *testing.T) {
	t.Parallel()
	nested := message.AppendTrackProperties([]wire.KVPair{
		{Type: message.PropertyDynamicGroups, IntVal: 1},
		{Type: message.PropertyObjectDeliveryTimeout, IntVal: 2000},
		{Type: message.PropertySubgroupDeliveryTimeout, IntVal: 3000},
		{Type: message.PropertyMaxCacheDuration, IntVal: 4000},
	})
	e := &registry.TrackEntry{}
	e.SetProperties(message.AppendTrackProperties([]wire.KVPair{
		{Type: message.PropertyImmutableProperties, ByteVal: nested},
	}))
	if got, err := e.DynamicGroups(); err != nil || !got {
		t.Errorf("DynamicGroups() = (%v, %v), want (true, nil)", got, err)
	}
	if got := e.DeliveryTimeouts(); got.Object != 2*time.Second || got.Subgroup != 3*time.Second {
		t.Errorf("DeliveryTimeouts() = %+v, want Object 2s, Subgroup 3s", got)
	}
	if got, ok := message.TrackMaxCacheDuration(e.GetProperties()); !ok || got != 4*time.Second {
		t.Errorf("TrackMaxCacheDuration = (%v, %v), want (4s, true)", got, ok)
	}
}
