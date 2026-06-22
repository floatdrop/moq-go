package message

import "cmp"

// Location represents a track location per §10.12.1.
type Location struct {
	Group  uint64
	Object uint64
}

// Compare returns -1, 0, or +1 according to whether l sorts before, equal
// to, or after other in the (Group, Object) lexicographic order. This is
// the total order MoQT uses for §10.2.11 (LARGEST_OBJECT monotonicity),
// §11.2 (intra-track Object ordering), and Fetch/Cache range scans
// (§10.12.1).
//
// The signature matches [cmp.Compare] so callers can pass
// Location.Compare directly to [slices.SortFunc] and
// [slices.BinarySearchFunc].
func (l Location) Compare(other Location) int {
	return cmp.Or(
		cmp.Compare(l.Group, other.Group),
		cmp.Compare(l.Object, other.Object),
	)
}

// Less reports whether l comes strictly before other in the (Group, Object)
// order described on [Location.Compare].
func (l Location) Less(other Location) bool { return l.Compare(other) < 0 }
