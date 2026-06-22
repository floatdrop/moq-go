// Package session implements the MoQT session layer: SETUP handshake, control
// stream multiplexing, request-ID allocation, and graceful termination via
// GOAWAY (§3.3, §3.5, §10.3, §10.4 of draft-ietf-moq-transport-18).
//
// The package does not depend on a specific transport. It operates against the
// Conn interface, which any QUIC-like transport can satisfy.
package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// role identifies whether this endpoint initiated (client) or accepted
// (server) the underlying QUIC connection. role determines Request ID parity
// per §10.1: client IDs are even, server IDs are odd. The type is unexported
// because callers select a role by calling Client or Server rather than
// passing a value.
type role uint8

const (
	roleClient role = iota
	roleServer
)

// String returns "client" or "server".
func (r role) String() string {
	if r == roleServer {
		return "server"
	}
	return "client"
}

// Option configures a session opened via Client or Server. See WithPath,
// WithAuthority, WithImplementation, WithMaxAuthTokenCacheSize,
// WithTokenVerifier, WithSetupOption, and WithGrease for the available knobs.
type Option func(*config)

// config carries the resolved set of options applied to a single
// Client/Server call. Unexported so callers can only construct it via
// Option helpers.
type config struct {
	setupOptions                  []wire.KVPair
	knownMandatoryTrackProperties map[message.PropertyType]struct{}

	// maxAuthTokenCacheSize is the byte budget for the inbound
	// authorization-token alias cache (§10.2.2). It mirrors the value
	// advertised to the peer via MAX_AUTH_TOKEN_CACHE_SIZE and is captured
	// here by WithMaxAuthTokenCacheSize so open() can size the cache. The
	// default (0) prohibits alias registration per §10.3.1.3.
	maxAuthTokenCacheSize uint64

	// tokenVerifier is the optional application policy that turns a resolved
	// (Type, Value) authorization token into an allow/deny decision. nil
	// disables verification (all tokens are accepted by the transport; the
	// application is responsible for any out-of-band checks).
	tokenVerifier TokenVerifier
}

// WithImplementation sets the MOQT_IMPLEMENTATION SETUP option — a
// free-form identifier of this peer's implementation. Recommended for
// every peer; advisory in spec terms.
func WithImplementation(nameAndVersion string) Option {
	return func(c *config) {
		c.setupOptions = append(c.setupOptions, message.MOQTImplementationOption(nameAndVersion))
	}
}

// WithPath sets the PATH SETUP option (§10.3.1.2). Client-only — using
// this on Server is a protocol violation per the spec.
func WithPath(pathAndQuery string) Option {
	return func(c *config) {
		c.setupOptions = append(c.setupOptions, message.PathOption(pathAndQuery))
	}
}

// WithAuthority sets the AUTHORITY SETUP option (§10.3.1.1). Client-only.
func WithAuthority(authority string) Option {
	return func(c *config) {
		c.setupOptions = append(c.setupOptions, message.AuthorityOption(authority))
	}
}

// WithMaxAuthTokenCacheSize sets MAX_AUTH_TOKEN_CACHE_SIZE — the maximum
// byte size of the per-session authorization-token alias cache (§10.2.2).
//
// The same budget sizes the inbound TokenCache the session uses to process
// AUTHORIZATION_TOKEN aliases on request streams: maxBytes is both advertised
// to the peer in SETUP and used to bound how many REGISTER tokens the peer may
// install. The default (option absent) is 0, which prohibits alias
// registration entirely per §10.3.1.3.
func WithMaxAuthTokenCacheSize(maxBytes uint64) Option {
	return func(c *config) {
		c.maxAuthTokenCacheSize = maxBytes
		c.setupOptions = append(c.setupOptions, message.MaxAuthTokenCacheSizeOption(maxBytes))
	}
}

// WithTokenVerifier installs an application policy that authorizes resolved
// AUTHORIZATION_TOKEN tokens (§10.2.2). After the session resolves a request's
// tokens (handling REGISTER / USE_ALIAS / USE_VALUE / DELETE against the
// inbound cache), it invokes v.VerifyToken for each resolved token so the
// application can validate signatures, expiry, audience, and scope — concerns
// the transport deliberately leaves out (§13.3).
//
// Passing nil (or never calling this option) disables verification: tokens are
// still parsed and aliases are still maintained, but no allow/deny decision is
// made at the transport layer.
func WithTokenVerifier(v TokenVerifier) Option {
	return func(c *config) {
		c.tokenVerifier = v
	}
}

// WithSetupOption appends an arbitrary KVPair to the SETUP option set.
// Escape hatch for fields not yet covered by a typed helper.
func WithSetupOption(kv wire.KVPair) Option {
	return func(c *config) {
		c.setupOptions = append(c.setupOptions, kv)
	}
}

// WithGrease enables GREASE (§14): a random unknown SETUP option is injected
// into the outbound SETUP message to exercise the peer's tolerance of unknown
// values. GREASE values follow the pattern 0x7F * N + 0x9D and are always
// larger than all currently defined SETUP option types, so appending preserves
// the ascending-Type ordering required by §1.4.3.
func WithGrease() Option {
	return func(c *config) {
		c.setupOptions = append(c.setupOptions, message.GreaseSetupOption())
	}
}

// WithKnownMandatoryTrackProperties configures the set of Mandatory Track
// Property types (range 0x4000–0x7FFF per §2.5.1) that this endpoint
// understands. When the session receives Track Properties (in SUBSCRIBE_OK,
// FETCH_OK, or TRACK_STATUS_OK) containing a mandatory property not in this
// set, it returns *ErrUnsupportedMandatoryTrackProperty.
//
// If this option is never called, mandatory track property enforcement is
// disabled — all properties are forwarded without inspection. This is the
// correct default for relays and other forwarding endpoints.
//
// End subscribers that interpret track data should call this option to opt
// in to enforcement. Pass an empty (non-nil) map to reject all mandatory
// properties, or populate the map with the types you support.
func WithKnownMandatoryTrackProperties(types map[message.PropertyType]struct{}) Option {
	return func(c *config) {
		c.knownMandatoryTrackProperties = types
	}
}

// Session represents one MoQT session over a Conn after the SETUP handshake
// has completed. The Session owns the control-stream goroutines until Close
// is called.
type Session struct {
	conn Conn
	role role

	sendCtrl SendStream
	recvCtrl ReceiveStream

	peerOptions []wire.KVPair

	// Outgoing Request ID allocator: client starts at 0 (even), server at 1
	// (odd); each AllocRequestID advances by 2 (§10.1).
	nextRequestID atomic.Uint64

	// Outgoing Track Alias allocator. §11.1: aliases are scoped to the
	// publisher → subscriber direction of one session, so each end keeps an
	// independent counter for tracks it advertises to the peer. The first
	// allocation is 1, not 0: AllocOutboundTrackAlias reserves 0 as the
	// "unset, auto-allocate" sentinel used by Publish/OpenPublish/Reply (see
	// AllocOutboundTrackAlias). The spec does not constrain parity the way it
	// does for Request IDs.
	nextOutboundTrackAlias atomic.Uint64

	// Serialized writes onto the control stream. Producers send through
	// sendControl; the controlSendLoop drains and writes.
	controlOut chan message.Message

	mu             sync.Mutex
	goawayReceived *message.Goaway
	goawaySent     bool
	// goawayCh is closed when goawayReceived transitions from nil to set.
	goawayCh chan struct{}
	// goawayHandler is the optional callback registered via OnGoaway. It is
	// invoked exactly once, in its own goroutine, when the first GOAWAY
	// arrives from the peer. goawayFired guards the at-most-once invocation
	// across the handleGoaway and OnGoaway paths. Both are protected by mu.
	goawayHandler func(*message.Goaway)
	goawayFired   bool

	// Inbound Request ID high-water mark (§10.1). Protected by mu.
	// peerRequestIDSeen is false until the first inbound Request ID arrives.
	// Once set, every subsequent inbound ID must be strictly greater than
	// peerRequestIDMax (peer increments by 2 per request).
	peerRequestIDSeen bool
	peerRequestIDMax  uint64

	// Inbound Track Alias → track.Key mapping (§11.1). Protected by mu.
	// Populated via RegisterInboundTrackAlias when the peer assigns an alias
	// (SUBSCRIBE_OK or PUBLISH). A duplicate alias for a different track is
	// a DUPLICATE_TRACK_ALIAS session error.
	inboundAliases map[uint64]track.Key

	// knownMandatoryTrackProperties is the set of Mandatory Track Property
	// types (range 0x4000–0x7FFF) this endpoint supports. Configured via
	// WithKnownMandatoryTrackProperties. nil means none are known.
	knownMandatoryTrackProperties map[message.PropertyType]struct{}

	// tokenCache is the inbound authorization-token alias cache (§10.2.2).
	// AcceptRequest drives Register / Delete / Resolve on it from the
	// AUTHORIZATION_TOKEN parameters of each inbound request. Always non-nil
	// (sized 0 when MAX_AUTH_TOKEN_CACHE_SIZE was not negotiated, which
	// prohibits aliasing per §10.3.1.3). The cache has its own internal
	// mutex; it is not protected by mu.
	tokenCache *TokenCache

	// tokenVerifier is the optional application policy consulted for each
	// resolved token. nil disables verification. Set once at construction
	// via WithTokenVerifier; never mutated, so no lock is required.
	tokenVerifier TokenVerifier

	closeOnce sync.Once
	closeErr  error
	// done is closed when the session terminates for any reason.
	done chan struct{}
}

// Client performs the SETUP handshake from the client side (the initiator of
// the underlying QUIC connection) and returns a ready Session. Request IDs on
// this side are even (§10.1). If the handshake fails, conn is closed and the
// error is returned. On success the caller owns the Session and must Close it.
func Client(ctx context.Context, conn Conn, opts ...Option) (*Session, error) {
	return open(ctx, conn, opts, roleClient)
}

// Server performs the SETUP handshake from the server side (the acceptor of
// the underlying QUIC connection) and returns a ready Session. Request IDs on
// this side are odd (§10.1). Errors and ownership match Client.
func Server(ctx context.Context, conn Conn, opts ...Option) (*Session, error) {
	return open(ctx, conn, opts, roleServer)
}

func open(ctx context.Context, conn Conn, opts []Option, r role) (*Session, error) {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}

	s := &Session{
		conn: conn,
		role: r,
		// Control-stream traffic is sparse (only GOAWAY after SETUP per §10
		// table 5, and at most one outbound GOAWAY per session). A buffer of
		// 1 lets a sender hand off while the previous frame is being written
		// to the transport; anything larger just delays backpressure.
		controlOut:                    make(chan message.Message, 1),
		goawayCh:                      make(chan struct{}),
		done:                          make(chan struct{}),
		inboundAliases:                make(map[uint64]track.Key),
		knownMandatoryTrackProperties: cfg.knownMandatoryTrackProperties,
		tokenCache:                    NewTokenCache(cfg.maxAuthTokenCacheSize),
		tokenVerifier:                 cfg.tokenVerifier,
	}
	var first uint64
	if r == roleServer {
		first = 1
	}
	s.nextRequestID.Store(first)

	if err := s.handshake(ctx, cfg.setupOptions); err != nil {
		_ = conn.CloseWithError(uint64(moqt.SessionProtocolViolation), err.Error())
		return nil, err
	}

	go s.controlSendLoop()
	go s.controlRecvLoop()

	return s, nil
}

// PeerOptions returns the SETUP options the peer advertised. The returned
// slice aliases internal state and must not be mutated.
func (s *Session) PeerOptions() []wire.KVPair { return s.peerOptions }

// AllocRequestID returns the next outbound Request ID per §10.1.
func (s *Session) AllocRequestID() uint64 {
	return s.nextRequestID.Add(2) - 2
}

// AllocOutboundTrackAlias returns the next Track Alias to use when this side
// advertises a new track to the peer (§11.1). Aliases are independent across
// sessions, so callers must remap when forwarding between two sessions.
//
// Allocation starts at 1, never 0: [Session.Publish], [Session.OpenPublish],
// and the SUBSCRIBE_OK reply path treat a zero TrackAlias as "unset, allocate
// one for me". If this allocator returned 0, a caller that did the natural
// "alias := AllocOutboundTrackAlias(); Publish(&Publish{TrackAlias: alias})"
// would have its 0 silently re-allocated to a different value — and any data
// stream the caller then opened under the original 0 would carry an alias the
// peer never bound to the track (the relay drops it as an unknown alias). So 0
// is reserved as the sentinel and never handed out.
func (s *Session) AllocOutboundTrackAlias() uint64 {
	return s.nextOutboundTrackAlias.Add(1)
}

// ErrDuplicateTrackAlias is returned by RegisterInboundTrackAlias when the
// peer assigns a Track Alias that is already in use for a different track
// (§11.1). The caller MUST close the session with SessionDuplicateTrackAlias.
type ErrDuplicateTrackAlias struct {
	Alias    uint64
	Existing track.Key
	New      track.Key
}

func (e *ErrDuplicateTrackAlias) Error() string {
	return fmt.Sprintf(
		"moqt/session: Track Alias %d already in use for a different track — DUPLICATE_TRACK_ALIAS",
		e.Alias,
	)
}

// RegisterInboundTrackAlias records that the peer has assigned alias to the
// track identified by key. This MUST be called by the subscriber when it
// receives a SUBSCRIBE_OK (whose TrackAlias field is the alias) and by the
// server when it receives a PUBLISH (whose TrackAlias field is the alias).
//
// If alias is already registered for the same track (idempotent re-registration),
// nil is returned. If alias is already registered for a different track,
// *ErrDuplicateTrackAlias is returned and the caller MUST close the session
// with SessionDuplicateTrackAlias (§11.1).
func (s *Session) RegisterInboundTrackAlias(alias uint64, key track.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.inboundAliases[alias]; ok {
		if existing != key {
			return &ErrDuplicateTrackAlias{Alias: alias, Existing: existing, New: key}
		}
		return nil // idempotent
	}
	s.inboundAliases[alias] = key
	return nil
}

// UnregisterInboundTrackAlias removes a previously registered alias, freeing
// it for potential reuse. Callers should invoke this when the subscription or
// publication associated with alias has been fully torn down (e.g. after
// PUBLISH_DONE or subscription cancellation and a suitable grace period per
// §11.1: "Subscribers SHOULD retain sufficient state to quickly discard
// unwanted Objects").
//
// Unregistering an alias that was never registered is a no-op.
func (s *Session) UnregisterInboundTrackAlias(alias uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inboundAliases, alias)
}

// LookupInboundTrackAlias returns the track.Key bound to alias by an earlier
// [Session.RegisterInboundTrackAlias] call, or (zero, false) if the alias is
// not currently registered.
//
// This is the recipient-side companion of [Session.RegisterInboundTrackAlias].
// Inbound data streams (SUBGROUP_HEADER, ObjectDatagram, FETCH_HEADER objects)
// identify their track by the alias the publisher chose; consumers — most
// notably the relay's fanout and end-subscriber applications — use this
// method to recover the canonical track identity for routing or rendering.
func (s *Session) LookupInboundTrackAlias(alias uint64) (track.Key, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.inboundAliases[alias]
	return key, ok
}

// Done returns a channel that is closed when the session has terminated.
func (s *Session) Done() <-chan struct{} { return s.done }

// Err returns the close cause, or nil if the session was closed cleanly.
func (s *Session) Err() error { return s.closeErr }

// Close terminates the session, cancelling the control streams and closing
// the underlying connection with the given code (§3.5). Calling Close more
// than once is safe; only the first call's code takes effect.
func (s *Session) Close(code moqt.SessionErrorCode, reason string) error {
	s.closeOnce.Do(func() {
		close(s.done)
		// CancelRead first so the recv loop unblocks. The control stream
		// must not be FIN'd cleanly during session lifetime (§3.3); we
		// reset both directions instead.
		if s.recvCtrl != nil {
			s.recvCtrl.CancelRead(uint64(moqt.StreamResetSessionClosed))
		}
		if s.sendCtrl != nil {
			s.sendCtrl.CancelWrite(uint64(moqt.StreamResetSessionClosed))
		}
		s.closeErr = s.conn.CloseWithError(uint64(code), reason)
	})
	return s.closeErr
}

// GoawayReceived returns a channel that is closed when a GOAWAY arrives from
// the peer. After the channel closes, PeerGoaway returns the parsed message.
func (s *Session) GoawayReceived() <-chan struct{} { return s.goawayCh }

// PeerGoaway returns the GOAWAY most recently received from the peer, or nil
// if none has arrived.
func (s *Session) PeerGoaway() *message.Goaway {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.goawayReceived
}

// OnGoaway registers a callback invoked exactly once when the first GOAWAY
// arrives from the peer, passing the parsed message (whose NewSessionURI and
// Timeout drive client-side session migration per §3.6/§10.4). The handler
// runs in its own goroutine so it must not assume any ordering with other
// session activity, and it may safely block (e.g. to dial a new session and
// re-issue subscriptions) without stalling the control-receive loop.
//
// OnGoaway is level-triggered: if a GOAWAY has already been received when
// OnGoaway is called, the handler fires immediately. Only the most recently
// registered handler is retained, and the at-most-once guarantee is per
// session — a handler registered after the GOAWAY has already fired the
// previously registered one will itself fire (once) on registration.
//
// Passing a nil handler clears any previously registered callback (provided
// it has not yet fired).
func (s *Session) OnGoaway(handler func(*message.Goaway)) {
	s.mu.Lock()
	// If a GOAWAY already arrived and no handler has fired yet, run this one
	// now and mark it fired so handleGoaway won't double-invoke.
	if s.goawayReceived != nil && !s.goawayFired {
		g := s.goawayReceived
		s.goawayFired = true
		s.mu.Unlock()
		if handler != nil {
			go handler(g)
		}
		return
	}
	s.goawayHandler = handler
	s.mu.Unlock()
}

// SendGoaway sends a GOAWAY on the control stream and transitions the session
// to the draining state. newURI may be empty; timeout is the grace period
// before the local side may forcibly close the session with GOAWAY_TIMEOUT.
// Returns an error if GOAWAY has already been sent, or if the local role is
// client and newURI is non-empty (§10.4: "A client MUST NOT include a New
// Session URI").
func (s *Session) SendGoaway(timeout time.Duration, newURI string) error {
	if s.role == roleClient && newURI != "" {
		return errors.New("moqt/session: client MUST NOT send GOAWAY with New Session URI")
	}
	s.mu.Lock()
	if s.goawaySent {
		s.mu.Unlock()
		return errors.New("moqt/session: GOAWAY already sent")
	}
	s.goawaySent = true

	// §10.4: Request ID is "the smallest Request ID that was not or might
	// not have been processed." If we have seen at least one inbound
	// Request ID, the next unprocessed one is peerRequestIDMax + 2 (peer
	// increments by 2 per §10.1). If no inbound requests have arrived, use
	// the per-role minimum: 0 when we are the server (peer is client,
	// even IDs) or 1 when we are the client (peer is server, odd IDs).
	var watermark uint64
	if s.peerRequestIDSeen {
		watermark = s.peerRequestIDMax + 2
	} else if s.role == roleClient {
		watermark = 1
	}
	s.mu.Unlock()

	msg := &message.Goaway{
		NewSessionURI: []byte(newURI),
		//nolint:gosec // G115: timeout is non-negative; whole ms fits a varint.
		Timeout:      uint64(timeout / time.Millisecond),
		HasRequestID: true,
		RequestID:    watermark,
	}
	return s.sendControl(msg)
}

// sendControl queues a control message for the send loop. Blocks if the queue
// is full or the session is done.
func (s *Session) sendControl(msg message.Message) error {
	select {
	case s.controlOut <- msg:
		return nil
	case <-s.done:
		return errors.New("moqt/session: closed")
	}
}

// controlSendLoop serializes writes onto the send-control stream. It exits on
// session shutdown or on the first write error.
func (s *Session) controlSendLoop() {
	for {
		select {
		case msg := <-s.controlOut:
			if err := message.Marshal(s.sendCtrl, msg); err != nil {
				if s.sessionDoneAlready() {
					return
				}
				_ = s.Close(moqt.SessionInternalError, "control send failure")
				return
			}
		case <-s.done:
			return
		}
	}
}

// controlRecvLoop reads framed control messages off the recv-control stream
// and dispatches them. The loop owns shutdown on read failure unless the
// session is already terminating.
func (s *Session) controlRecvLoop() {
	for {
		msg, err := message.Parse(s.recvCtrl)
		if err != nil {
			if s.sessionDoneAlready() {
				return
			}
			if errors.Is(err, io.EOF) {
				// Peer closed the control stream cleanly. §3.3 forbids
				// this during the session lifetime; treat it as a
				// protocol violation.
				_ = s.Close(moqt.SessionProtocolViolation, "peer closed control stream")
				return
			}
			_ = s.Close(moqt.SessionProtocolViolation, err.Error())
			return
		}
		if err := s.dispatchControl(msg); err != nil {
			if s.sessionDoneAlready() {
				return
			}
			_ = s.Close(moqt.SessionProtocolViolation, err.Error())
			return
		}
	}
}

func (s *Session) sessionDoneAlready() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// dispatchControl handles a single control-stream message after SETUP. Per
// table 5 in §10, only GOAWAY is valid on the control stream after SETUP for
// the messages in scope; anything else is a protocol violation.
func (s *Session) dispatchControl(msg message.Message) error {
	switch m := msg.(type) {
	case *message.Goaway:
		return s.handleGoaway(m)
	case *message.Setup:
		return errors.New("duplicate SETUP on control stream")
	default:
		return fmt.Errorf("unexpected %s on control stream", msg.Type())
	}
}

// handleGoaway records a received GOAWAY and notifies any waiter on
// GoawayReceived. §10.4: a second GOAWAY on the same control stream MUST
// terminate the session with PROTOCOL_VIOLATION.
//
// §10.4 also defines an OPTIONAL per-request GOAWAY: a server MAY include a
// Request ID (decoded into m.HasRequestID / m.RequestID) to ask the peer to
// re-issue just that request against a new session. The session deliberately
// does not act on it — whether and how to re-issue a request is migration
// policy that belongs to the application, which receives the full message
// (Request ID included) via PeerGoaway / OnGoaway and can drive the re-issue.
func (s *Session) handleGoaway(m *message.Goaway) error {
	s.mu.Lock()
	if s.goawayReceived != nil {
		s.mu.Unlock()
		return errors.New("duplicate GOAWAY on control stream")
	}
	// §10.4: a client cannot direct a server to migrate, so a non-empty URI
	// from a client is a PROTOCOL_VIOLATION. From our perspective, that means
	// if we are the server we must reject a GOAWAY with a URI.
	if s.role == roleServer && len(m.NewSessionURI) > 0 {
		s.mu.Unlock()
		return errors.New("GOAWAY from client carries non-empty URI")
	}
	s.goawayReceived = m
	// Snapshot the registered handler under the lock and mark it fired so a
	// later OnGoaway call won't re-invoke it. Run it in its own goroutine
	// (outside the lock) so a blocking migration handler can't stall the
	// control-receive loop.
	var handler func(*message.Goaway)
	if s.goawayHandler != nil && !s.goawayFired {
		handler = s.goawayHandler
		s.goawayFired = true
	}
	s.mu.Unlock()
	close(s.goawayCh)
	if handler != nil {
		go handler(m)
	}
	return nil
}
