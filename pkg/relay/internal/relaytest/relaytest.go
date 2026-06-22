// Package relaytest holds helpers shared across the relay tests
// (package relay_test). Keeping them here avoids duplicating the same
// helper across the test files that cannot otherwise share unexported
// code.
package relaytest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// ReadNextMessage parses one full MoQT control message off stream, failing
// the test if reading takes longer than the deadline allows. A context
// cancellation while blocked in Parse is treated as a clean unblock and
// returns the (possibly nil) partial message rather than failing.
func ReadNextMessage(t *testing.T, stream session.Stream, deadline <-chan time.Time) message.Message {
	t.Helper()
	done := make(chan struct{})
	var (
		msg message.Message
		err error
	)
	go func() {
		defer close(done)
		msg, err = message.Parse(stream)
	}()
	select {
	case <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("message.Parse: %v", err)
		}
		return msg
	case <-deadline:
		t.Fatal("timeout waiting for next message")
		return nil
	}
}
