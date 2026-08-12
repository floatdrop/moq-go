// Package session implements the MoQT session layer: SETUP handshake, control
// stream multiplexing, request-ID allocation, and graceful termination via
// GOAWAY (§3.3, §3.5, §10.3, §10.4 of draft-ietf-moq-transport-19).
//
// The package does not depend on a specific transport. It operates against the
// Conn interface, which any QUIC-like transport can satisfy.
package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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

	// Inbound Request ID tracking (§10.1). Protected by mu.
	// peerRequestIDSeen is false until the first inbound Request ID arrives;
	// peerRequestIDMax is the high-water mark. The peer allocates IDs in +2
	// increments, but requests ride separate QUIC streams and can be
	// DELIVERED out of order, so an ID below the mark is not automatically a
	// duplicate: peerRequestIDGaps holds the not-yet-seen IDs below the mark
	// (bounded by maxTrackedRequestIDGaps) that a late-delivered request may
	// still legitimately claim. See [Session.CheckPeerRequestID].
	peerRequestIDSeen bool
	peerRequestIDMax  uint64
	peerRequestIDGaps map[uint64]struct{}

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

	// maxRequestUpdates is the per-request-stream limit on unacknowledged
	// inbound REQUEST_UPDATEs we advertised via MAX_REQUEST_UPDATES
	// (§10.3.1.7). 0 means unlimited. Set once at construction via
	// WithMaxRequestUpdates; read by NewRequestUpdateLimiter, so no lock.
	maxRequestUpdates uint64

	// maxFilterRanges is the total Range Filter range budget we advertised via
	// MAX_FILTER_RANGES (§10.3.1.6). 0 prohibits Range Filters. Set once at
	// construction via WithMaxFilterRanges; read via MaxFilterRanges(), no lock.
	maxFilterRanges uint64

	closeOnce sync.Once
	// closeErr holds the *ClosedError cause; atomic because Err may
	// be called at any time, not only after Done fires.
	closeErr atomic.Pointer[ClosedError]
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
		maxRequestUpdates:             cfg.maxRequestUpdates,
		maxFilterRanges:               cfg.maxFilterRanges,
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
	if code, err := s.checkPeerSetupOptions(); err != nil {
		_ = conn.CloseWithError(uint64(code), err.Error())
		return nil, err
	}

	go s.controlSendLoop()
	go s.controlRecvLoop()

	return s, nil
}

// PeerOptions returns the SETUP options the peer advertised. The returned
// slice aliases internal state and must not be mutated.
func (s *Session) PeerOptions() []wire.KVPair { return s.peerOptions }

// checkPeerSetupOptions enforces the receive side of the PATH (§10.3.1.2) and
// AUTHORITY (§10.3.1.1) setup options. Each says the option "MUST NOT be used
// by the server, or when WebTransport is used", and that a session receiving
// one from a server MUST be closed — with INVALID_PATH and INVALID_AUTHORITY
// respectively, whose numeric values come from the §3.5 registry. Returns that
// close code and the reason, or a nil error when the peer's options are
// acceptable.
//
// Only the server-sent case is enforced here, and deliberately so — the other
// two conditions each section names need machinery this package does not have:
//
//   - "on a WebTransport session" needs the Conn to say which transport it is,
//     and Conn has no such predicate. Adding one is a change to the central
//     interface that all three adapters must then implement.
//   - "does not support the path/authority" needs the server to be told which
//     paths and authorities it serves, which is configuration that does not
//     exist yet. Note this is the *server-side* check, and a server that never
//     learns its own names cannot make it.
//
// Enforcing the first alone is still worth doing: it is unambiguous, needs no
// new API, and it is the direction a conforming peer cannot protect us from,
// since a client has no other way to notice a server misusing these options.
func (s *Session) checkPeerSetupOptions() (moqt.SessionErrorCode, error) {
	if s.role != roleClient {
		return moqt.SessionNoError, nil
	}
	for _, opt := range s.peerOptions {
		switch message.SetupOption(opt.Type) {
		case message.SetupOptionPath:
			return moqt.SessionInvalidPath,
				errors.New("server sent a PATH setup option, which is client-only (§10.3.1.2)")
		case message.SetupOptionAuthority:
			return moqt.SessionInvalidAuthority,
				errors.New("server sent an AUTHORITY setup option, which is client-only (§10.3.1.1)")
		case message.SetupOptionAuthorizationToken,
			message.SetupOptionMaxAuthTokenCache,
			message.SetupOptionMaxFilterRanges,
			message.SetupOptionMOQTImplementation,
			message.SetupOptionMaxRequestUpdates:
			// Legal in both directions. Listed rather than folded into the
			// default so that adding a §10.3.1 option fails the exhaustive
			// linter until someone decides whether a server may send it.
		default:
			// §10.3 requires a receiver ignore options it does not recognize.
		}
	}
	return moqt.SessionNoError, nil
}

// MaxFilterRanges returns the MAX_FILTER_RANGES value this session advertised
// (§10.3.1.6) — the total Range Filter range budget it will accept on any one
// subscription or fetch. 0 prohibits Range Filters. The relay enforces this
// when validating a request's Range Filters against [message.RangeFilterSet.Validate].
func (s *Session) MaxFilterRanges() uint64 { return s.maxFilterRanges }

// AllocRequestID returns the next outbound Request ID per §10.1.
func (s *Session) AllocRequestID() uint64 {
	return s.nextRequestID.Add(2) - 2
}

// Done returns a channel that is closed when the session has terminated.
func (s *Session) Done() <-chan struct{} { return s.done }

// Err returns the close cause — a *ClosedError carrying the §3.5
// error code and reason of the first Close call — or nil when the session
// was closed cleanly (SessionNoError) or is still open. The value is
// published before Done is closed, so the natural pattern
// <-sess.Done(); sess.Err() is race-free.
func (s *Session) Err() error {
	// Explicit nil check: returning a nil *ClosedError directly
	// would produce a non-nil error interface.
	if e := s.closeErr.Load(); e != nil {
		return e
	}
	return nil
}

// ClosedError is the close cause stored by [Session.Close] and
// returned by [Session.Err] for a non-clean close.
type ClosedError struct {
	Code   moqt.SessionErrorCode
	Reason string
}

func (e *ClosedError) Error() string {
	return fmt.Sprintf("moqt/session: closed with code %#x: %s", uint64(e.Code), e.Reason)
}

// Close terminates the session, cancelling the control streams and closing
// the underlying connection with the given code (§3.5). Calling Close more
// than once is safe; only the first call's code takes effect. The returned
// error is the transport's close error (nil in the common case), NOT the
// close cause — that is what [Session.Err] reports.
func (s *Session) Close(code moqt.SessionErrorCode, reason string) error {
	var transportErr error
	s.closeOnce.Do(func() {
		// Publish the cause BEFORE closing done: Err must be safe to call
		// the moment Done fires.
		if code != moqt.SessionNoError {
			s.closeErr.Store(&ClosedError{Code: code, Reason: reason})
		}
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
		transportErr = s.conn.CloseWithError(uint64(code), reason)
	})
	return transportErr
}
