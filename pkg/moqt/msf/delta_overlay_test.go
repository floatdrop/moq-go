package msf

import (
	"fmt"
	"reflect"
	"testing"
)

// Fields overlayTrack deliberately never copies: they identify the track rather
// than describe it. Name is the key a delta op matches on, and the Parent* pair
// is lineage — a patch that could rewrite either would change which track it is
// patching.
var overlayExemptFields = map[string]string{
	"Name":            "the key a delta op matches on",
	"ParentName":      "lineage, not a property",
	"ParentNamespace": "lineage, not a property",
}

// fillValue sets v to a distinctive non-zero value, recursing through pointers,
// slices, maps and structs. seed keeps the values distinguishable so a
// mis-assigned field shows up as a wrong value rather than a coincidental match
// between two fields that happen to share a type.
func fillValue(t *testing.T, v reflect.Value, seed int) {
	t.Helper()
	// exhaustive: reflect.Kind is a stdlib enum of 27 values, most of which
	// cannot appear in a Track. Enumerating Chan, Func, UnsafePointer and the
	// rest would add noise and still not survive a future Go adding a kind; the
	// default below fails the test loudly if one ever shows up, which is the
	// behaviour listing them would be for.
	//nolint:exhaustive // see above
	switch v.Kind() {
	case reflect.String:
		v.SetString(fmt.Sprintf("s%d", seed))
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(int64(seed) + 1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(uint64(seed) + 1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(float64(seed) + 1.5)
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fillValue(t, v.Elem(), seed)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fillValue(t, v.Index(0), seed)
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		key := reflect.New(v.Type().Key()).Elem()
		fillValue(t, key, seed)
		val := reflect.New(v.Type().Elem()).Elem()
		fillValue(t, val, seed)
		v.SetMapIndex(key, val)
	case reflect.Interface:
		v.Set(reflect.ValueOf(fmt.Sprintf("any%d", seed)))
	case reflect.Struct:
		for i := range v.NumField() {
			fillValue(t, v.Field(i), seed+i)
		}
	default:
		t.Fatalf("fillValue: unhandled kind %s — teach the filler about it", v.Kind())
	}
}

// TestOverlayTrackCopiesEveryField is the test this shape of code needs.
//
// overlayTrack is three long chains of `if src.X != zero { dst.X = src.X }`
// across 43 fields. A wrong assignment in one of them — `dst.Mimetype =
// src.Codec` — is invisible to the compiler, invisible to review, and produces
// a catalog delta that silently corrupts one property of one track. Coverage
// alone would not find it either: the old tests exercised roughly half these
// branches, and a branch being *run* says nothing about it assigning the right
// field.
//
// So this populates every field with a distinct value, overlays onto an empty
// Track, and checks each field individually. Distinct values matter: two string
// fields crossed over would still compare equal if both were "x".
//
// It is written by reflection rather than as a literal so that it keeps
// working. Add a field to Track and forget to overlay it, and this fails naming
// the field — which is the failure mode the chains are prone to, and the reason
// a hand-written fixture would have rotted the first time someone extended the
// struct.
func TestOverlayTrackCopiesEveryField(t *testing.T) {
	var src Track
	rv := reflect.ValueOf(&src).Elem()
	rt := rv.Type()
	for i := range rv.NumField() {
		fillValue(t, rv.Field(i), i)
	}

	// Guard the fixture itself: if a field were left zero, the overlay's
	// `if src.X != zero` would skip it and this test would pass vacuously.
	for i := range rv.NumField() {
		if rv.Field(i).IsZero() {
			t.Fatalf("fixture field %s is zero, so the overlay would skip it "+
				"and this test would prove nothing", rt.Field(i).Name)
		}
	}

	var dst Track
	overlayTrack(&dst, src)

	dv := reflect.ValueOf(&dst).Elem()
	for i := range rt.NumField() {
		name := rt.Field(i).Name
		got, want := dv.Field(i).Interface(), rv.Field(i).Interface()

		if why, exempt := overlayExemptFields[name]; exempt {
			if !dv.Field(i).IsZero() {
				t.Errorf("%s was copied but must not be (%s): got %v", name, why, got)
			}
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s = %v, want %v — check the assignment in overlayTrack*", name, got, want)
		}
	}
}

// TestOverlayTrackKeepsExistingWhenSourceIsZero pins the other half of the
// contract: a delta carries only the properties it means to change, so a zero
// field in the patch must leave the base value alone rather than clearing it.
func TestOverlayTrackKeepsExistingWhenSourceIsZero(t *testing.T) {
	var base Track
	bv := reflect.ValueOf(&base).Elem()
	for i := range bv.NumField() {
		fillValue(t, bv.Field(i), i)
	}
	before := base

	overlayTrack(&base, Track{}) // an empty patch changes nothing

	if !reflect.DeepEqual(base, before) {
		bt := bv.Type()
		for i := range bt.NumField() {
			got := reflect.ValueOf(base).Field(i).Interface()
			want := reflect.ValueOf(before).Field(i).Interface()
			if !reflect.DeepEqual(got, want) {
				t.Errorf("an empty patch changed %s: %v -> %v", bt.Field(i).Name, want, got)
			}
		}
	}
}

// TestOverlayTrackDoesNotAliasSource covers the clone helpers (cloneExtras,
// cloneURLRef and the slices.Clone calls): the overlaid Track must own its
// reference-typed fields, or mutating a delta after applying it would reach
// back into the catalog it patched.
func TestOverlayTrackDoesNotAliasSource(t *testing.T) {
	var src Track
	rv := reflect.ValueOf(&src).Elem()
	for i := range rv.NumField() {
		fillValue(t, rv.Field(i), i)
	}

	var dst Track
	overlayTrack(&dst, src)

	// Mutate every reference-typed field of the source in place.
	src.Depends[0] = "mutated"
	src.Accessibility[0].Scheme = "mutated"
	src.ContentProtectionRefIDs[0] = "mutated"
	src.Extras["s40"] = "mutated"
	src.AuthInfo["s39"] = "mutated"
	*src.IsLive = false
	*src.TargetLatency = 999

	if dst.Depends[0] == "mutated" {
		t.Error("Depends aliases the source slice")
	}
	if dst.Accessibility[0].Scheme == "mutated" {
		t.Error("Accessibility aliases the source slice")
	}
	if dst.ContentProtectionRefIDs[0] == "mutated" {
		t.Error("ContentProtectionRefIDs aliases the source slice")
	}
	if dst.Extras["s40"] == "mutated" {
		t.Error("Extras aliases the source map")
	}
	if dst.AuthInfo["s39"] == "mutated" {
		t.Error("AuthInfo aliases the source map")
	}
	if !*dst.IsLive {
		t.Error("IsLive aliases the source pointer")
	}
	if *dst.TargetLatency == 999 {
		t.Error("TargetLatency aliases the source pointer")
	}
}
