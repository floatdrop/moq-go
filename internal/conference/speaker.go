package conference

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// Track names every speaker publishes under its namespace.
const (
	VideoTrack = "video"
	AudioTrack = "audio"
)

// stampLen is the prefix of every Object payload: its send time on the run's
// clock, as big-endian nanoseconds.
const stampLen = 8

func stamp(payload []byte, sent time.Duration) {
	binary.BigEndian.PutUint64(payload, uint64(sent)) //nolint:gosec // G115: the run's clock is never negative.
}

func stampOf(payload []byte) (time.Duration, bool) {
	if len(payload) < stampLen {
		return 0, false
	}
	return time.Duration(binary.BigEndian.Uint64(payload)), true //nolint:gosec // G115: stamp wrote it from a Duration.
}

// speaker publishes one participant's video and audio tracks.
type speaker struct {
	run          *run
	index        int
	video, audio *session.Publication

	// Objects published with a send time inside the window, counted once
	// written: what every subscriber of the track should receive.
	videoSent, audioSent atomic.Int64
	// writeErrors counts Objects that failed to publish at all.
	writeErrors atomic.Int64

	// pacing is how late each Object left against its schedule; written by
	// the media goroutines, read after they exit.
	videoPacing, audioPacing histogram
}

func newSpeaker(ctx context.Context, r *run, index int, sess *session.Session) (*speaker, error) {
	s := &speaker{run: r, index: index}
	var err error
	if s.video, err = s.publish(ctx, sess, VideoTrack); err != nil {
		return nil, err
	}
	if s.audio, err = s.publish(ctx, sess, AudioTrack); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *speaker) publish(ctx context.Context, sess *session.Session, name string) (*session.Publication, error) {
	pub, err := sess.Publish(ctx, &message.Publish{Namespace: s.run.namespace(s.index), Name: []byte(name)})
	if err != nil {
		return nil, fmt.Errorf("PUBLISH %s: %w", name, err)
	}
	// Serve the request stream, so the relay's follow-ups are read.
	s.run.wg.Go(func() { _ = pub.Broker().Serve(ctx, nil) })
	return pub, nil
}

// start launches the paced media loops. Each speaker's schedule is offset
// by its share of a GOP (and of an audio frame), so speakers' keyframes do
// not all land at the same instant, as in a real call they would not.
func (s *speaker) start(ctx context.Context) {
	p := s.run.cfg.Profile
	now := s.run.now()
	share := func(d time.Duration) time.Duration { return d * time.Duration(s.index) / time.Duration(p.Speakers) }
	s.run.wg.Go(func() { s.sendVideo(ctx, now+share(p.GOP)) })
	s.run.wg.Go(func() { s.sendAudio(ctx, now+share(p.AudioFrame)) })
}

// pace waits for the i-th tick of interval after begin and, inside the
// window, records how late it was taken; false once ctx is done.
func (s *speaker) pace(ctx context.Context, t *time.Timer, begin, interval time.Duration, i int, h *histogram) bool {
	due := begin + time.Duration(i)*interval
	if wait := due - s.run.now(); wait > 0 {
		t.Reset(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			return false
		}
	}
	if s.run.win.Load().contains(due) {
		h.record(s.run.now() - due)
	}
	return ctx.Err() == nil
}

// sendVideo writes one frame per 1/FPS: a Group per GOP on its own subgroup
// stream, its first Object a keyframe, FIN'd when the GOP ends.
func (s *speaker) sendVideo(ctx context.Context, begin time.Duration) {
	p := s.run.cfg.Profile
	perGroup := p.framesPerGroup()
	keySize, deltaSize := p.frameSizes()
	interval := time.Second / time.Duration(p.FPS)
	buf := make([]byte, keySize)
	obj := &message.SubgroupObject{}
	t := time.NewTimer(time.Hour)
	defer t.Stop()

	var sg *session.OutgoingSubgroupStream
	defer func() {
		if sg != nil {
			sg.Cancel(moqt.StreamResetCancelled)
		}
	}()
	for i := 0; s.pace(ctx, t, begin, interval, i, &s.videoPacing); i++ {
		objectID := i % perGroup
		if objectID == 0 {
			if sg != nil {
				_ = sg.Close()
			}
			var err error
			sg, err = s.video.OpenSubgroup(message.SubgroupHeader{
				SubgroupIDMode: message.SubgroupIDImplicitZero,
				GroupID:        uint64(i / perGroup), //nolint:gosec // G115: i counts up from 0.
				// One subgroup per Group, so its FIN ends the Group.
				EndOfGroup: true,
			})
			if err != nil {
				sg = nil
			}
		}
		if sg == nil {
			s.writeErrors.Add(1)
			continue
		}
		size := deltaSize
		if objectID == 0 {
			size = keySize
		}
		sent := s.run.now()
		stamp(buf, sent)
		obj.Payload = buf[:size]
		if err := sg.WriteObjectAt(uint64(objectID), obj); err != nil {
			s.writeErrors.Add(1)
			if !errors.Is(err, session.ErrForwardPaused) {
				sg.Cancel(moqt.StreamResetCancelled)
			}
			sg = nil // the rest of this Group is lost; the next one reopens
			continue
		}
		if s.run.win.Load().contains(sent) {
			s.videoSent.Add(1)
		}
	}
}

// sendAudio sends one Object Datagram per AudioFrame, each frame its own Group.
func (s *speaker) sendAudio(ctx context.Context, begin time.Duration) {
	p := s.run.cfg.Profile
	buf := make([]byte, p.audioSize())
	d := &message.ObjectDatagram{}
	t := time.NewTimer(time.Hour)
	defer t.Stop()
	for i := 0; s.pace(ctx, t, begin, p.AudioFrame, i, &s.audioPacing); i++ {
		sent := s.run.now()
		stamp(buf, sent)
		// Publisher Priority 0, the highest (§5.1.1): conferencing favours audio
		// over video, which keeps the track's default.
		*d = message.ObjectDatagram{GroupID: uint64(i), PublisherPriority: 0, ObjectPayload: buf}
		if err := s.audio.SendDatagram(d); err != nil {
			s.writeErrors.Add(1)
			continue
		}
		if s.run.win.Load().contains(sent) {
			s.audioSent.Add(1)
		}
	}
}
