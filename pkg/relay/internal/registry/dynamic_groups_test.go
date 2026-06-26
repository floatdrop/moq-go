package registry_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

func dynamicGroupsProps(t *testing.T, value uint64) []byte {
	t.Helper()
	return message.AppendTrackProperties([]wire.KVPair{
		{Type: message.PropertyDynamicGroups, IntVal: value},
	})
}

// TestTrackEntry_DynamicGroups pins the §12.6 DYNAMIC_GROUPS decode that
// SetProperties performs once and caches: an absent property and value 0 are
// false, value 1 is true, and value > 1 is a PROTOCOL_VIOLATION surfaced as an
// error.
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
	t.Run("value > 1 is an error (§12.6)", func(t *testing.T) {
		t.Parallel()
		if _, err := setProps(dynamicGroupsProps(t, 2)).DynamicGroups(); err == nil {
			t.Fatal("got nil error, want §12.6 protocol-violation error")
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

// TestConsiderNewGroupRequest pins the §10.2.13 relay decision and its
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
