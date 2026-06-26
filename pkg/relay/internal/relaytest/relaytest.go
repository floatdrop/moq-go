// Package relaytest holds helpers shared across the relay tests (the
// relay_test and registry_test packages). Keeping them here avoids
// duplicating the same helper across test files that cannot otherwise share
// unexported code.
package relaytest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// FormatNamespace renders a Track Namespace as a readable slash-joined string
// for test failure messages. Shared by the relay_test and registry_test
// packages.
func FormatNamespace(ns wire.TrackNamespace) string {
	if len(ns) == 0 {
		return "<root>"
	}
	var out strings.Builder
	for i, f := range ns {
		if i > 0 {
			out.WriteString("/")
		}
		out.Write(f)
	}
	return out.String()
}

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
