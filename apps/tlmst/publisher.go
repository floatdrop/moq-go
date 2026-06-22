package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/loc"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/msf"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Track names within a room namespace.
const (
	videoTrackName = "video"
	audioTrackName = "audio"
)

// WebCodecs timestamps are microseconds, so LOC objects use a 1µs timescale.
const mediaTimescale = 1_000_000

// audioGroupObjects bounds how many Opus frames share one MoQ group. Opus
// frames are independently decodable, so the only goal is to keep groups from
// growing unbounded; ~50 frames is roughly one second at 20ms/frame.
const audioGroupObjects = 50

// VideoConfig describes the H.264 video track. The frontend fills it once its
// WebCodecs VideoEncoder is configured.
type VideoConfig struct {
	Codec     string  `json:"codec"` // e.g. "avc1.42E01F"
	Width     uint32  `json:"width"`
	Height    uint32  `json:"height"`
	Framerate float64 `json:"framerate"`
	Bitrate   uint64  `json:"bitrate"`
}

// AudioConfig describes the Opus audio track.
type AudioConfig struct {
	Codec         string `json:"codec"`         // "opus"
	Samplerate    uint32 `json:"samplerate"`    // 48000
	ChannelConfig string `json:"channelConfig"` // "1" or "2"
	Bitrate       uint64 `json:"bitrate"`
}

// newUserID returns a short random hex identifier for a participant.
func newUserID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// publisher owns the outbound side of a session: it announces the room
// namespace and publishes the catalog, video, and audio tracks. Encoded media
// arrives from the frontend via PublishVideoChunk / PublishAudioChunk.
type publisher struct {
	ctx    context.Context
	sess   *session.Session
	log    *slog.Logger
	userID string
	ns     wire.TrackNamespace

	nsStream session.Stream

	closeOnce sync.Once

	mu      sync.Mutex
	started bool

	// Catalog state. The catalog object is emitted exactly once at start;
	// late-joining subscribers retrieve it via Joining FETCH from the
	// relay's Object Cache, which is configured (via the operator's
	// CacheTTLPolicy on cmd/relay) to retain catalog-named tracks for
	// the lifetime of the publisher session. No periodic republishing
	// is necessary.
	catAlias   uint64
	catalogSeq *msf.GroupSequencer

	catStream   *session.Publication
	videoStream *session.Publication
	audioStream *session.Publication

	videoAlias uint64
	audioAlias uint64

	videoSeq *msf.GroupSequencer
	audioSeq *msf.GroupSequencer

	// Current video GOP subgroup. A new group opens on each keyframe.
	videoSG       *session.OutgoingSubgroupStream
	haveVideo     bool
	videoObjCount int // object index within the current video group

	// Current audio group subgroup, rotated every audioGroupObjects frames.
	audioSG       *session.OutgoingSubgroupStream
	audioObjCount int
}

// newPublisher picks a user id and prepares publisher state. The namespace is
// announced later, by start, only after the media tracks have been published —
// so that any peer which discovers us can immediately subscribe to our tracks
// (announcing earlier created a race where a fast subscriber would find no
// upstream yet).
func newPublisher(ctx context.Context, sess *session.Session, log *slog.Logger) *publisher {
	id := newUserID()
	return &publisher{
		ctx:        ctx,
		sess:       sess,
		log:        log,
		userID:     id,
		ns:         wire.Namespace("room", id),
		videoSeq:   msf.NewGroupSequencer(),
		audioSeq:   msf.NewGroupSequencer(),
		catalogSeq: msf.NewGroupSequencer(),
	}
}

// start publishes the catalog, video, and audio tracks for the room. The
// catalog describes both media tracks; the video/audio publish streams stay
// open for the lifetime of the session so chunks can be pushed onto them.
func (p *publisher) start(video VideoConfig, audio AudioConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return nil
	}

	nsString := "room/" + p.userID
	live := true
	cat := msf.BeginBroadcast([]msf.Track{
		{
			Name:      videoTrackName,
			Namespace: nsString,
			Packaging: msf.PackagingLOC,
			IsLive:    &live,
			Role:      msf.RoleVideo,
			Codec:     video.Codec,
			Width:     video.Width,
			Height:    video.Height,
			Framerate: video.Framerate,
			Bitrate:   video.Bitrate,
			Timescale: mediaTimescale,
		},
		{
			Name:          audioTrackName,
			Namespace:     nsString,
			Packaging:     msf.PackagingLOC,
			IsLive:        &live,
			Role:          msf.RoleAudio,
			Codec:         audio.Codec,
			Samplerate:    audio.Samplerate,
			ChannelConfig: audio.ChannelConfig,
			Bitrate:       audio.Bitrate,
			Timescale:     mediaTimescale,
		},
	}, time.Time{})
	if err := cat.Validate(); err != nil {
		return fmt.Errorf("catalog validate: %w", err)
	}
	catalogBytes, err := json.Marshal(cat)
	if err != nil {
		return fmt.Errorf("marshal catalog: %w", err)
	}

	// Ordering: emit the catalog object BEFORE PublishNamespace so
	// the relay's per-track cache already holds the catalog when a
	// discovering peer reacts to our namespace announce. The
	// alternative (announce first, then emit) loses both the live
	// SUBSCRIBE and the Joining FETCH for late joiners — the live
	// filter is future-only (FilterLargestObject) and the FETCH's
	// joining location is snapshotted at SUBSCRIBE_OK time, so a
	// hasLargest=false snapshot makes the FETCH permanently return
	// INVALID_RANGE regardless of subsequent emits. Catalog-first
	// closes both windows.

	// Catalog track.
	catAlias := p.sess.AllocOutboundTrackAlias()
	catStream, err := p.sess.Publish(p.ctx, &message.Publish{
		Namespace:  p.ns,
		Name:       []byte(msf.CatalogTrackName),
		TrackAlias: catAlias,
	})
	if err != nil {
		return fmt.Errorf("PUBLISH catalog: %w", err)
	}
	p.catStream = catStream
	p.catAlias = catAlias
	go drainRequestStream(p.log, "catalog", catStream)
	// Emit the catalog exactly once. The relay caches it for the
	// lifetime of this publisher's session (the operator's
	// CacheTTLPolicy gives the "catalog" name infinite retention),
	// so late-joining subscribers' Joining FETCH backfills the
	// catalog without us republishing on a timer.
	if err := p.emitObject(catAlias, p.catalogSeq.Next(), nil, catalogBytes); err != nil {
		return fmt.Errorf("emit catalog object: %w", err)
	}

	// Video track.
	p.videoAlias = p.sess.AllocOutboundTrackAlias()
	p.videoStream, err = p.sess.Publish(p.ctx, &message.Publish{
		Namespace:  p.ns,
		Name:       []byte(videoTrackName),
		TrackAlias: p.videoAlias,
	})
	if err != nil {
		return fmt.Errorf("PUBLISH video: %w", err)
	}
	go drainRequestStream(p.log, "video", p.videoStream)

	// Audio track.
	p.audioAlias = p.sess.AllocOutboundTrackAlias()
	p.audioStream, err = p.sess.Publish(p.ctx, &message.Publish{
		Namespace:  p.ns,
		Name:       []byte(audioTrackName),
		TrackAlias: p.audioAlias,
	})
	if err != nil {
		return fmt.Errorf("PUBLISH audio: %w", err)
	}
	go drainRequestStream(p.log, "audio", p.audioStream)

	// Announce the namespace now that all tracks are published and
	// the catalog object is queued at the relay. A peer that reacts
	// to this announce will issue SUBSCRIBE+FETCH; by the time the
	// relay processes those, the catalog is already in cache (it
	// travelled to the relay before the namespace announce did).
	nsStream, err := p.sess.PublishNamespace(p.ctx, &message.PublishNamespace{Namespace: p.ns})
	if err != nil {
		return fmt.Errorf("PUBLISH_NAMESPACE /room/%s: %w", p.userID, err)
	}
	p.nsStream = nsStream
	go drainRequestStream(p.log, "namespace", nsStream)

	p.started = true
	p.log.Info("publishing started",
		"namespace", "/room/"+p.userID,
		"video", video.Codec, "audio", audio.Codec)

	// The catalog is emitted exactly once above. Late-joining
	// subscribers recover it via Joining FETCH against the relay's
	// per-track Object Cache (retained for the publisher session's
	// lifetime by the operator's CacheTTLPolicy). No periodic
	// re-emit is necessary: the subscriber parks the inbound FETCH
	// stream until its route is registered, so the one-shot catalog
	// object is never dropped on a registration race.
	return nil
}

// publishVideo writes one encoded H.264 frame to the video track. Each
// keyframe starts a new MoQ group (a GOP); delta frames append to the current
// group as consecutive objects.
func (p *publisher) publishVideo(data []byte, timestampMicros uint64, keyframe bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started {
		return fmt.Errorf("publisher not started")
	}

	if keyframe || !p.haveVideo {
		if p.videoSG != nil {
			_ = p.videoSG.Close()
		}
		groupID := p.videoSeq.Next()
		sg, err := p.sess.OpenSubgroup(message.SubgroupHeader{
			Properties:     true,
			SubgroupIDMode: message.SubgroupIDImplicitZero,
			TrackAlias:     p.videoAlias,
			GroupID:        groupID,
		})
		if err != nil {
			return fmt.Errorf("open video subgroup: %w", err)
		}
		p.videoSG = sg
		p.haveVideo = true
		p.videoObjCount = 0
	}

	if err := writeLOCObject(p.videoSG, uint64(p.videoObjCount), data, timestampMicros); err != nil {
		return err
	}
	p.videoObjCount++
	return nil
}

// publishAudio writes one encoded Opus frame to the audio track, rotating the
// group every audioGroupObjects frames.
func (p *publisher) publishAudio(data []byte, timestampMicros uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started {
		return fmt.Errorf("publisher not started")
	}

	if p.audioSG == nil || p.audioObjCount >= audioGroupObjects {
		if p.audioSG != nil {
			_ = p.audioSG.Close()
		}
		groupID := p.audioSeq.Next()
		sg, err := p.sess.OpenSubgroup(message.SubgroupHeader{
			Properties:     true,
			SubgroupIDMode: message.SubgroupIDImplicitZero,
			TrackAlias:     p.audioAlias,
			GroupID:        groupID,
		})
		if err != nil {
			return fmt.Errorf("open audio subgroup: %w", err)
		}
		p.audioSG = sg
		p.audioObjCount = 0
	}

	if err := writeLOCObject(p.audioSG, uint64(p.audioObjCount), data, timestampMicros); err != nil {
		return err
	}
	p.audioObjCount++
	return nil
}

// close tears down all open publish streams and withdraws the namespace.
// Idempotent: a second call after the first returns is a no-op.
func (p *publisher) close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.videoSG != nil {
			_ = p.videoSG.Close()
			p.videoSG = nil
		}
		if p.audioSG != nil {
			_ = p.audioSG.Close()
			p.audioSG = nil
		}
		for _, st := range []*session.Publication{p.videoStream, p.audioStream, p.catStream} {
			if st != nil {
				_ = st.Done(moqt.PublishDoneTrackEnded, "")
			}
		}
		if p.nsStream != nil {
			_ = p.nsStream.Close()
			p.nsStream = nil
		}
		p.started = false
	})
}

// emitObject opens a single-object subgroup (used for the catalog).
func (p *publisher) emitObject(alias, groupID uint64, props, payload []byte) error {
	sg, err := p.sess.OpenSubgroup(message.SubgroupHeader{
		Properties:     len(props) > 0,
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		TrackAlias:     alias,
		GroupID:        groupID,
	})
	if err != nil {
		return fmt.Errorf("open subgroup: %w", err)
	}
	if err := sg.WriteObjectAt(0, &message.SubgroupObject{
		Properties: props,
		Payload:    payload,
	}); err != nil {
		sg.Cancel(moqt.StreamResetInternalError)
		return fmt.Errorf("write object: %w", err)
	}
	return sg.Close()
}

// writeLOCObject appends one LOC-packaged media chunk to an open subgroup at
// the given absolute Object ID. WriteObjectAt derives the §11.4.2 delta from
// the stream's running state, so the caller passes the object's index within
// the group (0, 1, 2, … reset each new group) rather than computing deltas.
func writeLOCObject(sg *session.OutgoingSubgroupStream, objectID uint64, data []byte, timestampMicros uint64) error {
	obj := loc.Object{
		Properties: loc.Properties{
			Timestamp:    timestampMicros,
			HasTimestamp: true,
			Timescale:    mediaTimescale,
			HasTimescale: true,
		},
		Payload: data,
	}
	props, payload := obj.Encode()
	if err := sg.WriteObjectAt(objectID, &message.SubgroupObject{
		Properties: props,
		Payload:    payload,
	}); err != nil {
		sg.Cancel(moqt.StreamResetInternalError)
		return fmt.Errorf("write object: %w", err)
	}
	return nil
}

// drainRequestStream consumes control messages on a publish/announce request
// stream until it closes, logging anything that arrives.
func drainRequestStream(log *slog.Logger, label string, stream io.Reader) {
	for {
		msg, err := message.Parse(stream)
		if err != nil {
			log.Debug("request stream closed", "stream", label, "err", err)
			return
		}
		log.Debug("request stream message", "stream", label, "type", fmt.Sprintf("%T", msg))
	}
}
