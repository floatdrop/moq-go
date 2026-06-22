package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/wailsapp/wails/v3/pkg/application"

	"github.com/floatdrop/moq-go/pkg/moqt/loc"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/msf"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Wails events emitted toward the frontend for remote participants.
const (
	participantJoinedEvent = "moq:participant-joined"
	participantLeftEvent   = "moq:participant-left"
	mediaChunkEvent        = "moq:media-chunk"
)

// RemoteParticipant announces a discovered peer and its track configuration.
type RemoteParticipant struct {
	ID    string       `json:"id"`
	Video *VideoConfig `json:"video,omitempty"`
	Audio *AudioConfig `json:"audio,omitempty"`
}

// RemoteLeft signals a peer withdrew its namespace.
type RemoteLeft struct {
	ID string `json:"id"`
}

// MediaChunk carries one encoded frame from a remote peer to the frontend,
// where WebCodecs decodes and renders it.
//
// GroupID/ObjectID are the absolute MoQ coordinates of the object (§11.4.2).
// The frontend needs them to restore decode order: each video GOP is its own
// subgroup = its own QUIC stream, and the subscriber reads streams
// concurrently (one goroutine per subgroup), so chunks from adjacent GOPs can
// reach the frontend out of order over a lossy link. The player feeds frames
// to the decoder in (GroupID, ObjectID) order and jumps to the newest keyframe
// rather than decoding stale deltas against a reset reference — which is what
// produced the severe smearing seen against a remote relay.
type MediaChunk struct {
	ParticipantID   string `json:"participantId"`
	Kind            string `json:"kind"` // "video" | "audio"
	Data            string `json:"data"` // base64-encoded codec bytes
	TimestampMicros uint64 `json:"timestampMicros"`
	Keyframe        bool   `json:"keyframe"`
	GroupID         uint64 `json:"groupId"`
	ObjectID        uint64 `json:"objectId"`
}

// route identifies what an inbound data stream carries.
type route struct {
	participantID string
	kind          string // "catalog" | "video" | "audio"
}

type remoteParticipant struct {
	id         string
	ns         wire.TrackNamespace
	gotCatalog bool
}

// subscriber discovers peers via SUBSCRIBE_NAMESPACE, subscribes to each
// peer's catalog/video/audio tracks, and forwards decoded LOC payloads to the
// frontend as Wails events.
type subscriber struct {
	ctx        context.Context
	cancel     context.CancelFunc
	sess       *session.Session
	log        *slog.Logger
	selfUserID string

	mu           sync.Mutex
	aliasRoutes  map[uint64]route // SUBGROUP TrackAlias -> route
	fetchRoutes  map[uint64]route // FETCH RequestID -> route (catalog backfill)
	participants map[string]*remoteParticipant
	mediaSeen    map[string]bool // userID+"/"+kind -> first frame logged

	// Data streams can be accepted by acceptLoop BEFORE the
	// subscribe/fetch call that triggered them has registered its
	// route (the relay opens the FETCH data stream right after
	// FETCH_OK, which is exactly what unblocks our Fetch call). Park
	// such early streams here keyed by alias/RequestID and start
	// reading them as soon as the route is registered, so we never
	// drop the one-shot catalog object on the floor.
	pendingFetch    map[uint64]*session.IncomingFetchStream      // RequestID -> parked FETCH stream
	pendingSubgroup map[uint64][]*session.IncomingSubgroupStream // TrackAlias -> parked subgroup streams
}

func newSubscriber(ctx context.Context, sess *session.Session, log *slog.Logger, selfUserID string) *subscriber {
	ctx, cancel := context.WithCancel(ctx)
	return &subscriber{
		ctx:             ctx,
		cancel:          cancel,
		sess:            sess,
		log:             log,
		selfUserID:      selfUserID,
		aliasRoutes:     make(map[uint64]route),
		fetchRoutes:     make(map[uint64]route),
		participants:    make(map[string]*remoteParticipant),
		mediaSeen:       make(map[string]bool),
		pendingFetch:    make(map[uint64]*session.IncomingFetchStream),
		pendingSubgroup: make(map[uint64][]*session.IncomingSubgroupStream),
	}
}

// start launches namespace discovery and the inbound data-stream loop.
func (s *subscriber) start() {
	go s.discoverLoop()
	go s.acceptLoop()
}

func (s *subscriber) close() {
	s.cancel()
}

// discoverLoop subscribes to the "room" namespace prefix and reacts to
// NAMESPACE / NAMESPACE_DONE announcements.
func (s *subscriber) discoverLoop() {
	prefix := wire.Namespace("room")
	stream, err := s.sess.SubscribeNamespace(s.ctx, &message.SubscribeNamespace{
		TrackNamespacePrefix: prefix,
	})
	if err != nil {
		if s.ctx.Err() == nil {
			s.log.Error("SUBSCRIBE_NAMESPACE failed", "err", err.Error())
		}
		return
	}
	s.log.Info("subscribed to namespace prefix", "prefix", "/room")

	for {
		msg, err := message.Parse(stream)
		if err != nil {
			if s.ctx.Err() == nil && !errors.Is(err, io.EOF) {
				s.log.Debug("namespace stream closed", "err", err.Error())
			}
			return
		}
		switch m := msg.(type) {
		case *message.Namespace:
			s.onNamespace(prefix, m.TrackNamespaceSuffix)
		case *message.NamespaceDone:
			s.onNamespaceDone(m.TrackNamespaceSuffix)
		}
	}
}

// onNamespace handles a newly announced peer namespace. The relay forwards our
// own namespace back to us, so we exclude it by user id.
func (s *subscriber) onNamespace(prefix, suffix wire.TrackNamespace) {
	if len(suffix) == 0 {
		return
	}
	userID := string(suffix[0])
	if userID == s.selfUserID {
		return // ourselves
	}

	s.mu.Lock()
	if _, exists := s.participants[userID]; exists {
		s.mu.Unlock()
		return
	}
	fullNS := append(append(wire.TrackNamespace{}, prefix...), suffix...)
	s.participants[userID] = &remoteParticipant{id: userID, ns: fullNS}
	s.mu.Unlock()

	s.log.Info("peer discovered", "user", userID)
	if err := s.subscribeCatalog(userID, fullNS); err != nil {
		s.log.Error("subscribe catalog failed", "user", userID, "err", err.Error())
	}
}

func (s *subscriber) onNamespaceDone(suffix wire.TrackNamespace) {
	if len(suffix) == 0 {
		return
	}
	userID := string(suffix[0])

	s.mu.Lock()
	delete(s.participants, userID)
	for alias, r := range s.aliasRoutes {
		if r.participantID == userID {
			delete(s.aliasRoutes, alias)
		}
	}
	for id, r := range s.fetchRoutes {
		if r.participantID == userID {
			delete(s.fetchRoutes, id)
		}
	}
	s.mu.Unlock()

	s.log.Info("peer left", "user", userID)
	emitEvent(participantLeftEvent, RemoteLeft{ID: userID})
}

// subscribeCatalog subscribes to a peer's catalog track at the live edge and
// issues a relative Joining FETCH so the already-published catalog object is
// backfilled (FilterLargestObject alone only delivers future objects).
//
// The publisher's ordering is: emit catalog object → PublishNamespace.
// By the time we see the peer's namespace announce on the control
// stream, the publisher's catalog object has already reached the relay
// (it travelled before the announce did), so the relay's SUBSCRIBE_OK
// snapshot reliably captures hasLargest=true. The Joining FETCH then
// returns the cached catalog on the first try.
func (s *subscriber) subscribeCatalog(userID string, ns wire.TrackNamespace) error {
	subMsg := &message.Subscribe{
		Namespace:  ns,
		Name:       []byte(msf.CatalogTrackName),
		Parameters: message.Parameters{message.LargestObjectFilter()},
	}
	catSub, err := s.sess.Subscribe(s.ctx, subMsg)
	if err != nil {
		return fmt.Errorf("SUBSCRIBE catalog: %w", err)
	}
	s.addAliasRoute(catSub.TrackAlias(), route{userID, "catalog"})

	fetchMsg := &message.Fetch{
		FetchType: message.FetchTypeRelativeJoining,
		Joining:   &message.JoiningFetch{JoiningRequestID: subMsg.RequestID, JoiningStart: 0},
	}
	if _, err := s.sess.Fetch(s.ctx, fetchMsg); err != nil {
		// Non-fatal: the live SUBSCRIBE stays armed; if the
		// publisher emits a fresh catalog later, we'll receive it
		// that way.
		s.log.Warn("catalog joining FETCH failed", "user", userID, "err", err.Error())
		return nil
	}
	s.addFetchRoute(fetchMsg.RequestID, route{userID, "catalog"})
	return nil
}

// onCatalog parses a catalog object and, the first time, announces the peer to
// the frontend and subscribes to its media tracks.
func (s *subscriber) onCatalog(userID string, payload []byte) {
	var cat msf.Catalog
	if err := json.Unmarshal(payload, &cat); err != nil {
		s.log.Warn("parse catalog failed", "user", userID, "err", err.Error())
		return
	}

	s.mu.Lock()
	p := s.participants[userID]
	if p == nil || p.gotCatalog {
		s.mu.Unlock()
		return
	}
	p.gotCatalog = true
	ns := p.ns
	s.mu.Unlock()

	var video, audio *msf.Track
	for i := range cat.Tracks {
		t := &cat.Tracks[i]
		if t.Packaging != msf.PackagingLOC {
			continue
		}
		switch t.Role {
		case msf.RoleVideo:
			video = t
		case msf.RoleAudio:
			audio = t
		}
	}

	announce := RemoteParticipant{ID: userID}
	if video != nil {
		announce.Video = &VideoConfig{
			Codec: video.Codec, Width: video.Width, Height: video.Height,
			Framerate: video.Framerate, Bitrate: video.Bitrate,
		}
	}
	if audio != nil {
		announce.Audio = &AudioConfig{
			Codec: audio.Codec, Samplerate: audio.Samplerate,
			ChannelConfig: audio.ChannelConfig, Bitrate: audio.Bitrate,
		}
	}
	s.log.Info("peer catalog", "user", userID, "hasVideo", video != nil, "hasAudio", audio != nil)
	emitEvent(participantJoinedEvent, announce)

	if video != nil {
		if err := s.subscribeMedia(userID, ns, video.Name, "video"); err != nil {
			s.log.Error("subscribe video failed", "user", userID, "err", err.Error())
		}
	}
	if audio != nil {
		if err := s.subscribeMedia(userID, ns, audio.Name, "audio"); err != nil {
			s.log.Error("subscribe audio failed", "user", userID, "err", err.Error())
		}
	}
}

func (s *subscriber) subscribeMedia(userID string, ns wire.TrackNamespace, name, kind string) error {
	subMsg := &message.Subscribe{
		Namespace:  ns,
		Name:       []byte(name),
		Parameters: message.Parameters{message.LargestObjectFilter()},
	}
	sub, err := s.sess.Subscribe(s.ctx, subMsg)
	if err != nil {
		return fmt.Errorf("SUBSCRIBE %s: %w", kind, err)
	}
	s.addAliasRoute(sub.TrackAlias(), route{userID, kind})
	s.log.Info("subscribed to track", "user", userID, "kind", kind, "alias", sub.TrackAlias())
	return nil
}

// acceptLoop receives inbound data streams and dispatches their objects by the
// route registered for the stream's TrackAlias (subgroups) or RequestID (fetch).
func (s *subscriber) acceptLoop() {
	for {
		ds, err := s.sess.AcceptDataStream(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			if errors.Is(err, session.ErrPaddingStream) {
				continue
			}
			s.log.Debug("accept data stream ended", "err", err.Error())
			return
		}
		switch st := ds.(type) {
		case *session.IncomingSubgroupStream:
			// Park the stream if its alias route hasn't been
			// registered yet; addAliasRoute will replay it. This
			// closes the race where the relay's data stream beats
			// the Subscribe()-side route registration.
			s.mu.Lock()
			r, ok := s.aliasRoutes[st.Header.TrackAlias]
			if !ok {
				s.pendingSubgroup[st.Header.TrackAlias] = append(
					s.pendingSubgroup[st.Header.TrackAlias], st)
				s.mu.Unlock()
				continue
			}
			s.mu.Unlock()
			go s.readSubgroup(r, true, st)
		case *session.IncomingFetchStream:
			// Same race for the catalog joining FETCH: the relay
			// opens this uni-stream immediately after FETCH_OK, so
			// it can arrive before addFetchRoute runs. Park it
			// until the route exists rather than dropping the
			// one-shot catalog object.
			s.mu.Lock()
			r, ok := s.fetchRoutes[st.Header.RequestID]
			if !ok {
				s.pendingFetch[st.Header.RequestID] = st
				s.mu.Unlock()
				continue
			}
			s.mu.Unlock()
			go s.readFetch(r, true, st)
		}
	}
}

func (s *subscriber) readSubgroup(r route, known bool, st *session.IncomingSubgroupStream) {
	for {
		obj, err := st.ReadDecoded()
		if err != nil {
			return
		}
		if !known {
			continue // drain unknown stream (e.g. alias not yet registered)
		}
		switch r.kind {
		case "catalog":
			s.onCatalog(r.participantID, obj.Payload)
		case "video":
			s.forwardMedia(r.participantID, "video", obj.Payload, obj.Properties, obj.GroupID, obj.ObjectID)
		case "audio":
			s.forwardMedia(r.participantID, "audio", obj.Payload, obj.Properties, obj.GroupID, obj.ObjectID)
		}
	}
}

func (s *subscriber) readFetch(r route, known bool, st *session.IncomingFetchStream) {
	for {
		obj, err := st.ReadDecoded()
		if err != nil {
			return
		}
		if !known || obj.EndOfNonExistentRange || obj.EndOfUnknownRange {
			continue
		}
		if r.kind == "catalog" {
			s.onCatalog(r.participantID, obj.Payload)
		}
	}
}

// forwardMedia decodes the LOC wrapper to recover the codec timestamp and
// forwards the encoded payload to the frontend, tagged with its absolute MoQ
// coordinates so the player can restore decode order (see MediaChunk).
//
// A video object is a keyframe iff it is object 0 of its group: the publisher
// opens a fresh group on every H.264 keyframe, so object 0 is always the IDR.
// Opus frames are independently decodable, so audio is always "key".
func (s *subscriber) forwardMedia(participantID, kind string, payload, props []byte, groupID, objectID uint64) {
	keyframe := kind == "audio" || objectID == 0
	decoded, err := loc.Decode(props, payload)
	if err != nil {
		s.log.Debug("loc decode failed", "kind", kind, "err", err.Error())
		return
	}
	// Log the first frame of each track so the panel confirms media is flowing
	// from the relay into the backend (distinct from frontend decode/render).
	key := participantID + "/" + kind
	s.mu.Lock()
	if !s.mediaSeen[key] {
		s.mediaSeen[key] = true
		s.mu.Unlock()
		s.log.Info("receiving media", "user", participantID, "kind", kind, "keyframe", keyframe)
	} else {
		s.mu.Unlock()
	}
	emitEvent(mediaChunkEvent, MediaChunk{
		ParticipantID:   participantID,
		Kind:            kind,
		Data:            base64.StdEncoding.EncodeToString(decoded.Payload),
		TimestampMicros: decoded.Properties.Timestamp,
		Keyframe:        keyframe,
		GroupID:         groupID,
		ObjectID:        objectID,
	})
}

func (s *subscriber) addAliasRoute(alias uint64, r route) {
	s.mu.Lock()
	s.aliasRoutes[alias] = r
	parked := s.pendingSubgroup[alias]
	delete(s.pendingSubgroup, alias)
	s.mu.Unlock()
	// Replay any subgroup streams that were accepted before the route
	// existed (see acceptLoop).
	for _, st := range parked {
		go s.readSubgroup(r, true, st)
	}
}

func (s *subscriber) addFetchRoute(reqID uint64, r route) {
	s.mu.Lock()
	s.fetchRoutes[reqID] = r
	parked := s.pendingFetch[reqID]
	delete(s.pendingFetch, reqID)
	s.mu.Unlock()
	// Replay a FETCH stream that arrived before the route was
	// registered (the catalog-backfill race).
	if parked != nil {
		go s.readFetch(r, true, parked)
	}
}

// emitEvent dispatches a custom event to the frontend if the app is running.
func emitEvent(name string, data any) {
	if app := application.Get(); app != nil {
		app.Event.EmitEvent(&application.CustomEvent{Name: name, Data: data})
	}
}
