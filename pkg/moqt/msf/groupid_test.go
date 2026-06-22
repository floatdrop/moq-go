package msf

import (
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

func TestGroupSequencerMonotonic(t *testing.T) {
	s := NewGroupSequencerAt(100)
	for i := range uint64(10) {
		got := s.Next()
		if got != 100+i {
			t.Errorf("Next() #%d: got %d, want %d", i, got, 100+i)
		}
	}
}

func TestGroupSequencerPeek(t *testing.T) {
	s := NewGroupSequencerAt(42)
	if got := s.Peek(); got != 42 {
		t.Errorf("Peek before Next: got %d, want 42", got)
	}
	s.Next()
	if got := s.Peek(); got != 43 {
		t.Errorf("Peek after Next: got %d, want 43", got)
	}
}

func TestNewGroupSequencerUsesUnixMilli(t *testing.T) {
	before := uint64(time.Now().UnixMilli())
	s := NewGroupSequencer()
	got := s.Peek()
	after := uint64(time.Now().UnixMilli())
	if got < before || got > after {
		t.Errorf("seed %d not within [%d, %d]", got, before, after)
	}
}

func TestGroupSequencerConcurrent(t *testing.T) {
	s := NewGroupSequencerAt(0)
	const goroutines = 8
	const perGoroutine = 1000
	var wg sync.WaitGroup
	seen := make([][]uint64, goroutines)
	for g := range goroutines {
		seen[g] = make([]uint64, perGoroutine)
		wg.Go(func() {
			for i := range perGoroutine {
				seen[g][i] = s.Next()
			}
		})
	}
	wg.Wait()

	// Every ID in [0, goroutines*perGoroutine) should appear exactly once.
	total := goroutines * perGoroutine
	histogram := make([]int, total)
	for _, batch := range seen {
		for _, id := range batch {
			if id >= uint64(total) {
				t.Fatalf("id %d out of range", id)
			}
			histogram[id]++
		}
	}
	for id, count := range histogram {
		if count != 1 {
			t.Errorf("id %d appeared %d times", id, count)
		}
	}
}

func TestPriorGapHeader(t *testing.T) {
	kv, err := PriorGapHeader(100, 105)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kv.Type != message.PropertyPriorGroupIDGap {
		t.Errorf("Type: got 0x%X, want 0x%X", kv.Type, message.PropertyPriorGroupIDGap)
	}
	if kv.IntVal != 4 { // 105 - 100 - 1
		t.Errorf("IntVal: got %d, want 4", kv.IntVal)
	}
}

func TestPriorGapHeaderRejectsNoGap(t *testing.T) {
	if _, err := PriorGapHeader(100, 101); err == nil {
		t.Error("expected error for immediate successor")
	}
	if _, err := PriorGapHeader(100, 100); err == nil {
		t.Error("expected error for equal IDs")
	}
	if _, err := PriorGapHeader(100, 50); err == nil {
		t.Error("expected error for backwards IDs")
	}
}
