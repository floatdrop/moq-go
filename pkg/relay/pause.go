package relay

import (
	"context"
	"time"
)

// pauseUpdateTimeout bounds the REQUEST_UPDATE round trip a pause makes to
// the publisher: the pause runs on a relay-scoped goroutine, not under any
// session's context.
const pauseUpdateTimeout = 5 * time.Second

// unwatchedPauser pauses the publishers of tracks the relay accepted by
// PUBLISH once no downstream has forwarded them for its delay
// ([Config.PauseUnwatchedPublishes]). Shared by every session handler; nil,
// or a zero delay, pauses nothing.
type unwatchedPauser struct {
	delay time.Duration
	stop  <-chan struct{} // closed by Stop: a pending pause is dropped
	run   func(fn func()) // relay-scoped, joined by Stop
}

// schedule runs pause after the delay, on a relay-scoped goroutine, unless
// the relay stops first. pause decides afresh whether anything is to be
// paused: a downstream may have come meanwhile.
func (p *unwatchedPauser) schedule(pause func(ctx context.Context)) {
	if p == nil || p.delay <= 0 {
		return
	}
	p.run(func() {
		t := time.NewTimer(p.delay)
		defer t.Stop()
		select {
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), pauseUpdateTimeout)
			defer cancel()
			pause(ctx)
		case <-p.stop:
		}
	})
}
