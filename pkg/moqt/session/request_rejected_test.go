package session_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// TestRequestRejectedErrorCarriesRetryInterval: §10.6.2 "Retry Interval: The
// minimum time (in milliseconds) before the request SHOULD be sent again,
// plus one. If the value is 0, the request SHOULD NOT be retried." The
// requester can only honor it if the rejection reports it.
func TestRequestRejectedErrorCarriesRetryInterval(t *testing.T) {
	for _, tc := range []struct {
		interval  uint64
		wantAfter time.Duration
		wantRetry bool
	}{
		{0, 0, false},
		{1, 0, true},
		{501, 500 * time.Millisecond, true},
		{math.MaxUint64, time.Duration(math.MaxInt64), true}, // largest varint (§1.4.1): clamped
	} {
		client, server := openTokenPair(t)
		go func() {
			r, err := server.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			_ = message.Marshal(r.Stream, &message.RequestError{
				ErrorCode:     moqt.RequestExcessiveLoad,
				RetryInterval: tc.interval,
				ErrorReason:   "busy",
			})
			_ = r.Stream.Close()
		}()
		_, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
		rej, ok := errors.AsType[*session.RequestRejectedError](err)
		if !ok {
			t.Fatalf("interval %d: Subscribe = %v, want *RequestRejectedError", tc.interval, err)
		}
		if rej.RetryInterval != tc.interval {
			t.Errorf("RetryInterval = %d, want %d", rej.RetryInterval, tc.interval)
		}
		after, retry := rej.RetryAfter()
		if after != tc.wantAfter || retry != tc.wantRetry {
			t.Errorf("interval %d: RetryAfter() = (%v, %v), want (%v, %v)",
				tc.interval, after, retry, tc.wantAfter, tc.wantRetry)
		}
	}
}

// TestUpdateHandlerRejectionKeepsRetryInterval: a *RequestRejectedError an
// UpdateHandler returns goes out as REQUEST_ERROR with its Retry Interval
// intact; dropping it would turn "retry later" into "SHOULD NOT be retried"
// (§10.6.2).
func TestUpdateHandlerRejectionKeepsRetryInterval(t *testing.T) {
	client, server := openTokenPair(t)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		pub, err := r.AcceptSubscribe(nil)
		if err != nil {
			return
		}
		b := pub.Broker()
		b.HandleUpdates(func(*message.RequestUpdate) (*message.RequestOK, error) {
			return nil, &session.RequestRejectedError{
				Code:          moqt.RequestExcessiveLoad,
				Reason:        "busy",
				RetryInterval: 5001,
			}
		})
		_ = b.Serve(t.Context(), nil)
	}()
	sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_, err = sub.Update(t.Context(), message.Parameters{message.ForwardParam(false)})
	rej, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok || rej.RetryInterval != 5001 {
		t.Fatalf("Update = %v, want REQUEST_ERROR with Retry Interval 5001", err)
	}
}
