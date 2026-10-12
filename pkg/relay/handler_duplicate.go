package relay

import (
	"bytes"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// recordRedundant handles dup, a copy [registry.TrackEntry.ClaimDelivered]
// found redundant, described to the ledger by info: it is checked against the
// first copy ([checkDuplicate]) and then recorded
// ([registry.TrackEntry.RecordDuplicate]). An error wraps
// [session.ErrMalformedTrack].
func recordRedundant(entry *registry.TrackEntry, info registry.ObjectInfo, dup *cache.CachedObject) error {
	if err := checkDuplicate(entry.Cache, dup); err != nil {
		return err
	}
	return entry.RecordDuplicate(info)
}

// checkDuplicate compares dup, a copy that lost the §7.3 dedup claim, with the
// first copy in c. A different Delivery Mode, Subgroup ID, Priority or
// Payload (§7.1), or different Immutable Properties (§12.1, §10.7; one copy
// having them and the other not counts), makes the track malformed: the error
// wraps [session.ErrMalformedTrack]. Mutable Properties may differ (§7.1).
//
// Normal becoming End of Group or End of Track is the existing-to-not-existing
// change §7.1 allows, and the reverse a late Object (§2.1.2). Only a Normal
// Object has a Payload or Properties (§11.1.1, §11.1.2), so those are
// compared only when both copies are Normal.
//
// A copy an announced gap says does not exist is compared too, when the
// first copy is cached: §2.1 excuses its arrival, not a different content.
//
// On a difference the first copy is removed from the cache: it triggered the
// Malformed Track status too, and such Objects MUST NOT be cached (§12.1).
//
// Limitation: nothing is compared when the first copy is not in the cache:
// evicted, expired (§10.3), or not yet put there by a concurrent contributor.
func checkDuplicate(c *cache.ObjectCache, dup *cache.CachedObject) error {
	first, ok := c.Get(dup.GroupID, dup.ObjectID)
	if !ok || first.IsRangeMarker() {
		return nil
	}
	var field string
	switch {
	case first.ForwardingPref != dup.ForwardingPref:
		field = "Delivery Mode"
	case first.ForwardingPref == cache.ForwardingSubgroup && first.SubgroupID != dup.SubgroupID:
		field = "Subgroup ID"
	case first.PublisherPriority != dup.PublisherPriority:
		field = "Priority"
	case first.Status != message.ObjectStatusNormal || dup.Status != message.ObjectStatusNormal:
		return nil
	case !bytes.Equal(first.Payload, dup.Payload):
		field = "Payload"
	case !sameImmutableProperties(first.Properties, dup.Properties):
		field = "Immutable Properties"
	default:
		return nil
	}
	c.Delete(dup.GroupID, dup.ObjectID)
	return fmt.Errorf("%w: a duplicate of Object %d in Group %d has a different %s (§7.1, §12.1)",
		session.ErrMalformedTrack, dup.ObjectID, dup.GroupID, field)
}

// sameImmutableProperties reports whether a and b, raw Object Properties,
// carry the same Immutable Properties, byte for byte, or neither has any.
func sameImmutableProperties(a, b []byte) bool {
	av, aok := message.ImmutableProperties(a)
	bv, bok := message.ImmutableProperties(b)
	return aok == bok && bytes.Equal(av, bv)
}
