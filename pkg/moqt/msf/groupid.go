package msf

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// GroupSequencer issues monotonically increasing MOQT Group IDs for a
// single track per §6.1. The initial value is the current Unix
// millisecond, which makes Group IDs across application restarts
// non-decreasing and avoids collisions as long as a publisher emits
// fewer than 1000 groups per second.
//
// GroupSequencer is safe for concurrent use.
type GroupSequencer struct {
	next atomic.Uint64
}

// NewGroupSequencer returns a sequencer seeded with the current
// wallclock as Unix milliseconds. The first call to [Next] returns
// that seed and increments internal state.
func NewGroupSequencer() *GroupSequencer {
	s := &GroupSequencer{}
	s.next.Store(uint64(time.Now().UnixMilli()))
	return s
}

// NewGroupSequencerAt returns a sequencer seeded at the given start ID.
// Useful for tests and for callers that maintain their own time source.
func NewGroupSequencerAt(start uint64) *GroupSequencer {
	s := &GroupSequencer{}
	s.next.Store(start)
	return s
}

// Next returns the next Group ID and advances the sequencer.
func (s *GroupSequencer) Next() uint64 {
	// atomic.Uint64.Add returns the new value, so to mirror the
	// "return current, then increment" semantics we subtract 1.
	return s.next.Add(1) - 1
}

// Peek returns the value that the next call to Next would produce
// without advancing the sequencer.
func (s *GroupSequencer) Peek() uint64 {
	return s.next.Load()
}

// PriorGapHeader returns the KV pair a publisher attaches to its first
// Object after a republish so subscribers can distinguish an
// intentional Group ID gap (e.g. encoder restart) from missing data.
// See §6.1 of the MSF draft and PRIOR_GROUP_ID_GAP in §12.8 of
// MoQ Transport.
//
// prev is the last Group ID the publisher emitted before the gap;
// curr is the first Group ID after the gap. Returns an error if
// curr <= prev (no gap) or if prev+1 == curr (no gap, just the next
// sequential ID).
func PriorGapHeader(prev, curr uint64) (wire.KVPair, error) {
	if curr <= prev {
		return wire.KVPair{}, fmt.Errorf(
			"moqt/msf: PriorGapHeader: curr (%d) must be > prev (%d)", curr, prev)
	}
	gap := curr - prev - 1
	if gap == 0 {
		return wire.KVPair{}, errors.New("moqt/msf: PriorGapHeader: curr is the immediate successor of prev (no gap)")
	}
	return wire.KVPair{
		Type:   message.PropertyPriorGroupIDGap,
		IntVal: gap,
	}, nil
}
