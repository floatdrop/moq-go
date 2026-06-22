package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/wailsapp/wails/v3/pkg/application"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
)

// logEventName is the Wails event carrying proxied backend log records.
const logEventName = "moq:log"

// LogEntry is one log record forwarded from the Go backend to the frontend.
// The binding generator turns this into a typed event payload.
type LogEntry struct {
	Time    string            `json:"time"`
	Level   string            `json:"level"`
	Message string            `json:"message"`
	Attrs   map[string]string `json:"attrs"`
}

// SessionService establishes and holds a MoQ (Media over QUIC Transport)
// client session on behalf of the frontend.
type SessionService struct {
	// appCtx stays valid for the lifetime of the app and is cancelled just
	// before shutdown. Captured in ServiceStartup.
	appCtx context.Context

	mu     sync.Mutex
	sess   *session.Session
	pub    *publisher
	sub    *subscriber
	userID string
	stats  *statsCollector
}

// ServiceStartup captures the application-scoped context.
func (s *SessionService) ServiceStartup(ctx context.Context, _ application.ServiceOptions) error {
	s.appCtx = ctx
	return nil
}

// ServiceShutdown closes any open session on app exit.
func (s *SessionService) ServiceShutdown() error {
	s.mu.Lock()
	sess, pub, sub := s.sess, s.pub, s.sub
	s.sess, s.pub, s.sub, s.stats = nil, nil, nil, nil
	s.mu.Unlock()
	if sub != nil {
		sub.close()
	}
	if pub != nil {
		pub.close()
	}
	if sess != nil {
		_ = sess.Close(moqt.SessionNoError, "shutting down")
	}
	return nil
}

// Leave tears down the current session, returning the UI to its initial state.
// It is safe to call when no session is open.
func (s *SessionService) Leave() error {
	s.mu.Lock()
	sess, pub, sub := s.sess, s.pub, s.sub
	s.sess, s.pub, s.sub, s.stats = nil, nil, nil, nil
	s.mu.Unlock()
	if sub != nil {
		sub.close()
	}
	if pub != nil {
		pub.close()
	}
	if sess == nil {
		return nil
	}
	return sess.Close(moqt.SessionNoError, "user left")
}

// Join establishes a MoQ session against the relay at addr. It blocks until
// the QUIC connection and MOQT SETUP handshake complete (or fail). Throughout
// establishment, every backend log line is proxied to the frontend as a
// "moq:log" event via the context logger, so the UI can render progress live.
func (s *SessionService) Join(addr string) error {
	// Route this establishment's logs to the frontend.
	logger := slog.New(newEventHandler(slog.LevelDebug))

	// Bound the handshake so an unreachable relay doesn't hang the UI forever.
	// The session does not retain this context past Client(), so cancelling it
	// once Join returns is safe.
	ctx, cancel := context.WithTimeout(s.appCtx, 30*time.Second)
	defer cancel()

	logger.Info("joining relay", "addr", addr)

	tlsCfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec — development tool
		NextProtos:         []string{"moq-00"},
	}
	// Collect QUIC transport metrics (RTT, loss, cwnd, throughput) for the
	// debug panel. quic-go v0.59 exposes these only through its qlog tracing
	// hook, so the collector doubles as the trace sink.
	stats := newStatsCollector()
	quicCfg := &quic.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 5 * time.Second,
		EnableDatagrams: true,
		Tracer:          stats.tracer,
	}

	logger.Debug("dialing QUIC", "addr", addr)
	qconn, err := quic.DialAddr(ctx, addr, tlsCfg, quicCfg)
	if err != nil {
		logger.Error("QUIC dial failed", "addr", addr, "err", err.Error())
		return fmt.Errorf("dial %s: %w", addr, err)
	}

	logger.Debug("QUIC connected, performing MOQT handshake")
	sess, err := session.Client(ctx, quicconn.New(qconn),
		session.WithImplementation("tlmst/0.1"),
	)
	if err != nil {
		logger.Error("MOQT handshake failed", "err", err.Error())
		return fmt.Errorf("moqt handshake: %w", err)
	}

	logger.Info("session established", "addr", addr)

	// Prepare the publisher. Namespace announcement and track publishing happen
	// later in StartPublishing; both must outlive Join, so the publisher runs on
	// the app-scoped context (not the handshake timeout) while keeping the same
	// frontend-proxying logger.
	pub := newPublisher(s.appCtx, sess, logger)

	// Swap in the new session, closing any previous one outside the lock.
	s.mu.Lock()
	old := s.sess
	oldPub := s.pub
	oldSub := s.sub
	s.sess = sess
	s.pub = pub
	s.sub = nil
	s.userID = pub.userID
	s.stats = stats
	s.mu.Unlock()
	if oldSub != nil {
		oldSub.close()
	}
	if oldPub != nil {
		oldPub.close()
	}
	if old != nil {
		_ = old.Close(moqt.SessionNoError, "replaced by new session")
	}

	return nil
}

// StartSubscribing begins discovering other participants via SUBSCRIBE_NAMESPACE
// and subscribing to their tracks. The frontend calls this from the call screen
// once its remote-media event listeners are attached.
func (s *SessionService) StartSubscribing() error {
	logger := slog.New(newEventHandler(slog.LevelDebug))

	s.mu.Lock()
	if s.sub != nil || s.sess == nil {
		s.mu.Unlock()
		return nil
	}
	sub := newSubscriber(s.appCtx, s.sess, logger, s.userID)
	s.sub = sub
	s.mu.Unlock()

	sub.start()
	return nil
}

// StartPublishing publishes the catalog, video, and audio tracks for the
// announced room. The frontend calls this once its WebCodecs encoders are
// configured, before sending any media chunks.
func (s *SessionService) StartPublishing(video VideoConfig, audio AudioConfig) error {
	s.mu.Lock()
	pub := s.pub
	s.mu.Unlock()
	if pub == nil {
		return fmt.Errorf("no active session")
	}
	return pub.start(video, audio)
}

// PublishVideoChunk forwards one H.264 access unit (the bytes of a WebCodecs
// EncodedVideoChunk) to the video track. timestampMicros is the chunk's
// presentation timestamp in microseconds. The data arrives as a base64 string
// on the wire; Wails (via encoding/json) decodes it into the []byte for us.
func (s *SessionService) PublishVideoChunk(data []byte, timestampMicros uint64, keyframe bool) error {
	s.mu.Lock()
	pub := s.pub
	s.mu.Unlock()
	if pub == nil {
		return fmt.Errorf("no active session")
	}
	return pub.publishVideo(data, timestampMicros, keyframe)
}

// PublishAudioChunk forwards one Opus frame (the bytes of a WebCodecs
// EncodedAudioChunk) to the audio track. The data arrives as a base64 string
// on the wire; Wails (via encoding/json) decodes it into the []byte for us.
func (s *SessionService) PublishAudioChunk(data []byte, timestampMicros uint64) error {
	s.mu.Lock()
	pub := s.pub
	s.mu.Unlock()
	if pub == nil {
		return fmt.Errorf("no active session")
	}
	return pub.publishAudio(data, timestampMicros)
}

// Stats returns a snapshot of the current QUIC connection's transport metrics
// for the debug panel. It is safe to call with no session open (it reports
// Connected=false and zeroed counters).
func (s *SessionService) Stats() ConnStats {
	s.mu.Lock()
	stats := s.stats
	connected := s.sess != nil
	s.mu.Unlock()
	if stats == nil {
		return ConnStats{Connected: connected}
	}
	snap := stats.Snapshot()
	snap.Connected = connected
	return snap
}

// eventHandler is an slog.Handler that forwards every record to the frontend
// as a Wails "moq:log" event instead of writing to an io.Writer.
type eventHandler struct {
	level slog.Leveler
	attrs []slog.Attr
	group string
}

func newEventHandler(level slog.Leveler) *eventHandler {
	return &eventHandler{level: level}
}

func (h *eventHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level.Level()
}

func (h *eventHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]string, len(h.attrs)+r.NumAttrs())
	put := func(a slog.Attr) {
		key := a.Key
		if h.group != "" {
			key = h.group + "." + key
		}
		attrs[key] = a.Value.Resolve().String()
	}
	for _, a := range h.attrs {
		put(a)
	}
	r.Attrs(func(a slog.Attr) bool {
		put(a)
		return true
	})

	entry := LogEntry{
		Time:    r.Time.Format(time.RFC3339Nano),
		Level:   r.Level.String(),
		Message: r.Message,
		Attrs:   attrs,
	}
	if app := application.Get(); app != nil {
		app.Event.EmitEvent(&application.CustomEvent{Name: logEventName, Data: entry})
	}
	return nil
}

func (h *eventHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	nh := *h
	nh.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &nh
}

func (h *eventHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	nh := *h
	if nh.group != "" {
		nh.group += "." + name
	} else {
		nh.group = name
	}
	return &nh
}
