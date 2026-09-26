package relay

import (
	"bytes"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
)

// checkDuplicate compares dup, a copy that lost the §9.3 dedup claim, with the
// first copy in c. A different Forwarding Preference, Subgroup ID, Priority or
// Payload (§9.1), or different Immutable Properties (§2.4.2, §12.7; one copy
// having them and the other not counts), makes the track malformed: the error
// wraps [session.ErrMalformedTrack]. Mutable Properties may differ (§9.1).
//
// Normal becoming End of Group or End of Track is the existing-to-not-existing
// change §9.1 allows, and the reverse a late Object (§2.1). Only a Normal
// Object has a Payload or Properties (§11.2.1.1, §11.2.1.2), so those are
// compared only when both copies are Normal.
//
// A copy an announced gap says does not exist is compared too, when the
// first copy is cached: §2.1 excuses its arrival, not a different content.
//
// On a difference the first copy is removed from the cache: it triggered the
// Malformed Track status too, and such Objects MUST NOT be cached (§2.4.2).
//
// Limitation: nothing is compared when the first copy is not in the cache:
// evicted, expired (§12.3), or not yet put there by a concurrent contributor.
func checkDuplicate(c *cache.ObjectCache, dup *cache.CachedObject) error {
	first, ok := c.Get(dup.GroupID, dup.ObjectID)
	if !ok || first.IsRangeMarker() {
		return nil
	}
	var field string
	switch {
	case first.ForwardingPref != dup.ForwardingPref:
		field = "Forwarding Preference"
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
	return fmt.Errorf("%w: a duplicate of Object %d in Group %d has a different %s (§9.1, §2.4.2)",
		session.ErrMalformedTrack, dup.ObjectID, dup.GroupID, field)
}

// sameImmutableProperties reports whether a and b, raw Object Properties,
// carry the same Immutable Properties, byte for byte, or neither has any.
func sameImmutableProperties(a, b []byte) bool {
	av, aok := message.ImmutableProperties(a)
	bv, bok := message.ImmutableProperties(b)
	return aok == bok && bytes.Equal(av, bv)
}
