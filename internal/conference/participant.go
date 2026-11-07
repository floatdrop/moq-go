package conference

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

type mediaKind int

const (
	kindVideo mediaKind = iota
	kindAudio
)

// subscribed is what an inbound Track Alias carries.
type subscribed struct {
	speaker int
	kind    mediaKind
	sub     *session.Subscription
}

// participant is one client: it subscribes to every speaker but itself, and
// a speaker additionally publishes (see [speaker]).
type participant struct {
	run   *run
	index int
	sess  *session.Session

	// Written by join before any receive loop starts, read-only after.
	tracks   map[uint64]subscribed
	joinErr  error
	joinTook time.Duration

	mu sync.Mutex
	// Delivery inside the window, per media kind.
	received [2]int64
	latency  [2]histogram
	bytes    int64
	// videoResets counts subgroup streams that ended other than by FIN.
	videoResets int
	// ended counts subscriptions the relay ended (PUBLISH_DONE or FIN)
	// before the run did.
	ended int
	// malformed counts tracks found malformed (§12.1), whose subscriptions
	// this side cancelled; cancelled lists their aliases.
	malformed int
	cancelled map[uint64]bool
	// unstamped counts Objects delivered without the send time they were
	// published with.
	unstamped int
}

func (p *participant) connect(ctx context.Context) error {
	sess, err := p.run.cfg.Dial(ctx)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	p.sess = sess
	return nil
}

// join connects (unless a speaker already has), subscribes to every other
// speaker's tracks and starts the receive loops, which run until ctx is done
// or the session closes. A failure is recorded in joinErr.
func (p *participant) join(ctx context.Context) {
	began := time.Now()
	jctx, cancel := context.WithTimeout(ctx, p.run.cfg.JoinTimeout)
	defer cancel()
	if p.sess == nil {
		if p.joinErr = p.connect(jctx); p.joinErr != nil {
			return
		}
	}
	p.tracks = make(map[uint64]subscribed, 2*len(p.run.speakers))
	var subs []*session.Subscription
	for _, s := range p.run.speakers {
		if s.index == p.index {
			continue
		}
		for _, t := range []struct {
			name string
			kind mediaKind
		}{{VideoTrack, kindVideo}, {AudioTrack, kindAudio}} {
			sub, err := p.sess.Subscribe(jctx, &message.Subscribe{
				Namespace: p.run.namespace(s.index),
				Name:      []byte(t.name),
				// From the next Group: where a decoder can start video.
				Parameters: message.Parameters{message.RelativeStartFilter(0)},
			})
			if err != nil {
				p.joinErr = fmt.Errorf("SUBSCRIBE %s of speaker %d: %w", t.name, s.index, err)
				return
			}
			subs = append(subs, sub)
			p.tracks[sub.TrackAlias()] = subscribed{speaker: s.index, kind: t.kind, sub: sub}
		}
	}
	p.joinTook = time.Since(began)

	for _, sub := range subs {
		p.run.wg.Go(func() { p.serve(ctx, sub) })
	}
	p.run.wg.Go(func() { p.receiveDatagrams(ctx) })
	p.run.wg.Go(func() { p.acceptStreams(ctx) })
}

// serve reads a subscription's follow-ups; its end before the run's, unless
// this side cancelled it, is the relay ending the subscription.
func (p *participant) serve(ctx context.Context, sub *session.Subscription) {
	_ = sub.Broker().Serve(ctx, func(m message.Message) bool {
		_, done := m.(*message.PublishDone)
		return !done
	})
	if !p.run.stopping.Load() {
		p.mu.Lock()
		if !p.cancelled[sub.TrackAlias()] {
			p.ended++
		}
		p.mu.Unlock()
	}
}

// isCancelled reports whether this side cancelled alias's subscription.
func (p *participant) isCancelled(alias uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cancelled[alias]
}

// malformedTrack cancels the subscription whose Track Alias is alias, once:
// §12.1, "When a subscriber detects a Malformed Track, it MUST cancel any
// corresponding subscription".
func (p *participant) malformedTrack(alias uint64) {
	t, ok := p.tracks[alias]
	if !ok {
		return
	}
	p.mu.Lock()
	first := !p.cancelled[alias]
	if first {
		if p.cancelled == nil {
			p.cancelled = map[uint64]bool{}
		}
		p.cancelled[alias] = true
		p.malformed++
	}
	p.mu.Unlock()
	if first {
		_ = t.sub.Close()
	}
}

func (p *participant) receiveDatagrams(ctx context.Context) {
	for {
		d, err := p.sess.ReceiveDatagram(ctx)
		switch {
		case errors.Is(err, session.ErrMalformedTrack):
			p.malformedTrack(d.TrackAlias) // returned with the error to name the track
			continue
		case err != nil:
			return
		}
		if t, ok := p.tracks[d.TrackAlias]; ok && t.kind == kindAudio {
			p.record(d.TrackAlias, kindAudio, d.ObjectPayload)
		}
	}
}

func (p *participant) acceptStreams(ctx context.Context) {
	for {
		ds, err := p.sess.AcceptDataStream(ctx)
		switch {
		case errors.Is(err, session.ErrPaddingStream):
			continue
		case err != nil:
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			ds.Cancel(moqt.StreamResetCancelled) // no FETCH was sent
			continue
		}
		if t, ok := p.tracks[sg.Header.TrackAlias]; !ok || t.kind != kindVideo {
			sg.Cancel(moqt.StreamResetCancelled)
			continue
		}
		p.run.wg.Go(func() { p.readVideo(sg) })
	}
}

func (p *participant) readVideo(sg *session.IncomingSubgroupStream) {
	for {
		obj, err := sg.ReadObject()
		if errors.Is(err, session.ErrMalformedTrack) {
			p.malformedTrack(sg.Header.TrackAlias)
			sg.Cancel(moqt.StreamResetCancelled) // nothing more on it is wanted (§11.3.2)
			return
		}
		if err != nil {
			// A reset that follows this side's own cancel is not the relay's.
			if !errors.Is(err, io.EOF) && !p.run.stopping.Load() && !p.isCancelled(sg.Header.TrackAlias) {
				p.mu.Lock()
				p.videoResets++
				p.mu.Unlock()
			}
			return
		}
		if !p.record(sg.Header.TrackAlias, kindVideo, obj.Payload) {
			// Cancelled while this stream was open: nothing more on it is wanted.
			sg.Cancel(moqt.StreamResetCancelled)
			return
		}
	}
}

// record counts an Object of alias's track whose send time falls in the
// window. It reports false, counting nothing, once this side cancelled the
// track: §3.1.3, Objects can still arrive, and are discarded.
func (p *participant) record(alias uint64, kind mediaKind, payload []byte) bool {
	sent, ok := stampOf(payload)
	latency := p.run.now() - sent
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.cancelled[alias]:
		return false
	case !ok:
		p.unstamped++
	case p.run.win.Load().contains(sent):
		p.received[kind]++
		p.latency[kind].record(latency)
		p.bytes += int64(len(payload))
	}
	return true
}
