package session

import (
	"slices"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// cancelRecordingStream is a Stream that accepts every write and records
// each CancelRead's code. The tests using it are synchronous.
type cancelRecordingStream struct {
	brokerStubStream

	readCodes []uint64
}

func (s *cancelRecordingStream) CancelRead(code uint64) { s.readCodes = append(s.readCodes, code) }

// TestRejectStopsSendingWithCancelled: once its REQUEST_ERROR is written, a
// rejected request stops reading with CANCELLED, the relevant code (§3.3.4:
// "The stream was cancelled by either endpoint"), not INTERNAL_ERROR.
func TestRejectStopsSendingWithCancelled(t *testing.T) {
	t.Parallel()
	stream := &cancelRecordingStream{}
	r := &Request{Stream: stream, First: &message.Subscribe{}}
	if err := r.RejectError(moqt.RequestDoesNotExist, "no"); err != nil {
		t.Fatalf("RejectError: %v", err)
	}
	if want := []uint64{uint64(moqt.StreamResetCancelled)}; !slices.Equal(stream.readCodes, want) {
		t.Fatalf("STOP_SENDING codes = %v, want %v (CANCELLED)", stream.readCodes, want)
	}
}
