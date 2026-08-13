package msf

import (
	"strings"
	"testing"
)

func deltaOf(ops ...DeltaOp) Catalog { return Catalog{DeltaUpdate: ops} }

func baseWith(tracks ...Track) Catalog { return Catalog{Tracks: tracks} }

// TestApplyRejectsMalformedOps covers the guards on each §5.1.6 / §5.3 delta
// operation. They are the paths a buggy or hostile publisher reaches, and a
// delta that is half-applied is worse than one refused: Apply works on a clone
// of the base, so an error must leave the caller's catalog untouched rather
// than partially patched.
func TestApplyRejectsMalformedOps(t *testing.T) {
	base := baseWith(
		Track{Name: "video", Namespace: "room1"},
		Track{Name: "audio", Namespace: "room1"},
	)

	tests := []struct {
		name    string
		delta   Catalog
		wantMsg string
	}{
		{
			"add without a name",
			deltaOf(DeltaOp{Op: "add", Tracks: []Track{{Namespace: "room1"}}}),
			"missing name",
		},
		{
			"remove without a name",
			deltaOf(DeltaOp{Op: "remove", Tracks: []Track{{Namespace: "room1"}}}),
			"missing name",
		},
		{
			"remove naming a namespace no track has",
			deltaOf(DeltaOp{Op: "remove", Tracks: []Track{{Name: "video", Namespace: "other"}}}),
			"no such track",
		},
		{
			"clone without a parentName",
			deltaOf(DeltaOp{Op: "clone", Tracks: []Track{{Name: "video-sd"}}}),
			"parentName required",
		},
		{
			"clone without a name",
			deltaOf(DeltaOp{Op: "clone", Tracks: []Track{{ParentName: "video"}}}),
			"name required",
		},
		{
			"clone whose parent is in another namespace",
			deltaOf(DeltaOp{Op: "clone", Tracks: []Track{
				{Name: "video-sd", ParentName: "video", ParentNamespace: "other"},
			}}),
			"not found",
		},
		{
			// §5.3: the clone MUST have a different Track Name.
			"clone that collides with its own parent",
			deltaOf(DeltaOp{Op: "clone", Tracks: []Track{
				{Name: "video", ParentName: "video", Namespace: "room1"},
			}}),
			"matches parent",
		},
		{
			"clone that collides with an existing track",
			deltaOf(DeltaOp{Op: "clone", Tracks: []Track{
				{Name: "audio", ParentName: "video", Namespace: "room1"},
			}}),
			"already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(base.Tracks)
			_, err := Apply(base, tt.delta)
			if err == nil {
				t.Fatalf("accepted a malformed delta")
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error %q does not mention %q", err, tt.wantMsg)
			}
			if len(base.Tracks) != before {
				t.Errorf("the base catalog was mutated by a failed Apply: %d -> %d tracks",
					before, len(base.Tracks))
			}
		})
	}
}

// TestApplyRemoveIsNamespaceScoped covers the §5.1.6 matching rule, where
// Namespace is optional and changes what the op means: given the same track
// name in two namespaces, an unqualified remove and a qualified one must not
// remove the same entry. Getting this backwards would silently drop another
// room's track.
func TestApplyRemoveIsNamespaceScoped(t *testing.T) {
	base := baseWith(
		Track{Name: "video", Namespace: "room1"},
		Track{Name: "video", Namespace: "room2"},
	)

	t.Run("qualified removes only the matching namespace", func(t *testing.T) {
		got, err := Apply(base, deltaOf(DeltaOp{
			Op:     "remove",
			Tracks: []Track{{Name: "video", Namespace: "room2"}},
		}))
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if len(got.Tracks) != 1 || got.Tracks[0].Namespace != "room1" {
			t.Errorf("remaining tracks = %+v, want only room1", got.Tracks)
		}
	})

	t.Run("unqualified removes the first match", func(t *testing.T) {
		got, err := Apply(base, deltaOf(DeltaOp{
			Op:     "remove",
			Tracks: []Track{{Name: "video"}},
		}))
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if len(got.Tracks) != 1 || got.Tracks[0].Namespace != "room2" {
			t.Errorf("remaining tracks = %+v, want only room2", got.Tracks)
		}
	})
}

// TestApplyCloneDeepCopiesInheritedSlices is the aliasing rule applyClone's own
// comment calls out: a clone that omits a slice field inherits the parent's,
// and the two tracks must not share its backing array. If they did, editing one
// track's Depends afterwards would silently edit the other's.
func TestApplyCloneDeepCopiesInheritedSlices(t *testing.T) {
	base := baseWith(Track{
		Name:      "video",
		Namespace: "room1",
		Depends:   []string{"base-layer"},
		Extras:    map[string]any{"k": "v"},
	})

	got, err := Apply(base, deltaOf(DeltaOp{
		Op:     "clone",
		Tracks: []Track{{Name: "video-sd", ParentName: "video", Namespace: "room1"}},
	}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(got.Tracks) != 2 {
		t.Fatalf("got %d tracks, want 2", len(got.Tracks))
	}

	parent, clone := got.Tracks[0], got.Tracks[1]
	if clone.Depends[0] != "base-layer" {
		t.Fatalf("clone did not inherit Depends: %+v", clone.Depends)
	}

	clone.Depends[0] = "mutated"
	clone.Extras["k"] = "mutated"
	if parent.Depends[0] == "mutated" {
		t.Error("clone shares the parent's Depends backing array")
	}
	if parent.Extras["k"] == "mutated" {
		t.Error("clone shares the parent's Extras map")
	}

	// The clone consumed its lineage fields rather than carrying them (§5.3).
	if clone.ParentName != "" || clone.ParentNamespace != "" {
		t.Errorf("clone kept lineage fields: parentName=%q parentNamespace=%q",
			clone.ParentName, clone.ParentNamespace)
	}
}
