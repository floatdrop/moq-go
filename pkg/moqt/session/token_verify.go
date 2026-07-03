package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// ResolvedToken is a fully-resolved AUTHORIZATION_TOKEN (§10.2.2): the
// (Token Type, Token Value) pair an application policy needs to make an
// authorization decision, with all alias indirection already removed.
//
// The session produces a ResolvedToken for every REGISTER, USE_ALIAS, and
// USE_VALUE token on an inbound request (DELETE tokens carry no value and so
// produce none). USE_ALIAS tokens are resolved against the inbound TokenCache
// before the value is exposed, so a verifier never sees a bare alias.
//
// Value is owned by the caller (a fresh copy per resolution); mutating it does
// not affect the cache.
type ResolvedToken struct {
	// Type is the Token Type (§10.2.2) — an application-defined identifier
	// of the token scheme (e.g. a registry entry for a CAT or JWT profile).
	Type uint64
	// Value is the raw, opaque Token Value. Its interpretation is entirely
	// up to the TokenVerifier; the transport treats it as bytes (§13.3).
	Value []byte
}

// TokenVerifier is the application policy that authorizes resolved
// authorization tokens. The session invokes VerifyToken once per
// ResolvedToken carried by an inbound request, after the token has been
// resolved against the inbound cache.
//
// The transport deliberately defines no token format (§13.3); a verifier is
// where signature checking, expiry, audience, and scope validation live.
//
// Returning nil authorizes the token. Returning a non-nil error denies the
// request the token accompanied: wrap the error with [*TokenDeniedError] (or
// use [DenyToken]) to choose the REQUEST_ERROR code the peer receives —
// notably [moqt.RequestExpiredAuthToken] for an expired token per §10.2.2. A
// plain error denies with [moqt.RequestUnauthorized].
//
// VerifyToken must be safe for concurrent use: requests on a session are
// dispatched concurrently, so multiple goroutines may call it at once.
type TokenVerifier interface {
	VerifyToken(ctx context.Context, sess *Session, tok ResolvedToken) error
}

// TokenVerifierFunc adapts an ordinary function to the [TokenVerifier]
// interface, so a policy can be supplied inline without a named type.
type TokenVerifierFunc func(ctx context.Context, sess *Session, tok ResolvedToken) error

// VerifyToken calls f.
func (f TokenVerifierFunc) VerifyToken(ctx context.Context, sess *Session, tok ResolvedToken) error {
	return f(ctx, sess, tok)
}

// TokenCacheError is returned by [Session.AcceptRequest] when processing an
// inbound request's AUTHORIZATION_TOKEN parameters fails at the cache layer
// (§10.2.2). These are session-level faults: a malformed token, a duplicate
// REGISTER alias, a cache overflow, or a USE_ALIAS / DELETE referencing an
// unknown alias. Code is the SESSION_ERROR the caller should close the
// session with.
type TokenCacheError struct {
	// Code is the §10.2.2 SESSION_ERROR code to terminate the session with.
	Code moqt.SessionErrorCode
	// Err is the underlying cache or parse error, for diagnostics.
	Err error
}

// Error implements the error interface.
func (e *TokenCacheError) Error() string {
	return fmt.Sprintf("moqt/session: token cache error (session code 0x%X): %v", uint64(e.Code), e.Err)
}

// Unwrap exposes the underlying error for errors.Is / errors.As.
func (e *TokenCacheError) Unwrap() error { return e.Err }

// TokenDeniedError is returned by token verification to deny a single request
// with an explicit MoQT REQUEST_ERROR code. Unlike [TokenCacheError] it is a
// per-request rejection, not a session-level fault: the caller should reply
// REQUEST_ERROR and leave the session running.
//
// Code MUST be one of the §10.6 REQUEST_ERROR codes (see
// [moqt.RequestErrorCode]); the zero value collapses to
// [moqt.RequestUnauthorized].
type TokenDeniedError struct {
	// Code is the REQUEST_ERROR code to send. Zero ⇒ RequestUnauthorized.
	Code moqt.RequestErrorCode
	// Reason is the human-readable reason forwarded to the peer.
	Reason string
	// Err is the underlying verifier error, for diagnostics. Optional.
	Err error
}

// Error implements the error interface.
func (e *TokenDeniedError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("moqt/session: token denied (request code 0x%X): %s", uint64(e.RequestErrorCode()), e.Reason)
	}
	return fmt.Sprintf("moqt/session: token denied (request code 0x%X)", uint64(e.RequestErrorCode()))
}

// Unwrap exposes the underlying verifier error for errors.Is / errors.As.
func (e *TokenDeniedError) Unwrap() error { return e.Err }

// RequestErrorCode returns the REQUEST_ERROR code to send, substituting
// [moqt.RequestUnauthorized] for the zero value.
func (e *TokenDeniedError) RequestErrorCode() moqt.RequestErrorCode {
	if e.Code == 0 {
		return moqt.RequestUnauthorized
	}
	return e.Code
}

// DenyToken constructs a [*TokenDeniedError]. Use it from a [TokenVerifier]
// to reject a request with a specific REQUEST_ERROR code:
//
//	return session.DenyToken(moqt.RequestExpiredAuthToken, "token expired")
func DenyToken(code moqt.RequestErrorCode, reason string) error {
	return &TokenDeniedError{Code: code, Reason: reason}
}

// TokenCache returns the session's inbound authorization-token alias cache
// (§10.2.2). It is primarily exposed for inspection and tests; the session
// drives Register / Resolve / Delete on it automatically from inbound request
// parameters in [Session.AcceptRequest]. Always non-nil.
func (s *Session) TokenCache() *TokenCache { return s.tokenCache }

// processRequestTokens parses the AUTHORIZATION_TOKEN parameters of msg and
// applies each token to the inbound cache per §10.2.2, returning the resolved
// (Type, Value) tokens for any REGISTER / USE_ALIAS / USE_VALUE entries.
//
// Processing order matters: a REGISTER is committed to the cache immediately,
// honouring the §10.2.2 MUST that "an Alias which is registered ... MUST be
// added to the cache even if the message fails for some other reason." Because
// the cache mutation happens here — before the request is validated or
// authorized — a later rejection of the request does not roll the alias back.
//
// A cache-layer failure (malformed token, duplicate alias, overflow, unknown
// alias) is returned as a [*TokenCacheError] carrying the session-level
// SESSION_ERROR code the caller must close the session with.
func (s *Session) processRequestTokens(msg message.Message) ([]ResolvedToken, error) {
	ps, ok := messageParameters(msg)
	if !ok {
		return nil, nil
	}
	tokens, err := message.TokensFromParam(ps)
	if err != nil {
		return nil, &TokenCacheError{Code: moqt.SessionMalformedAuthToken, Err: err}
	}
	if len(tokens) == 0 {
		return nil, nil
	}

	var resolved []ResolvedToken
	for i := range tokens {
		t := &tokens[i]
		switch t.AliasType {
		case message.AliasTypeRegister:
			// §10.2.2: register before any further validation so the
			// alias persists even if the request is later rejected.
			if err := s.tokenCache.Register(t.TokenAlias, t.TokenType, t.TokenValue); err != nil {
				return nil, &TokenCacheError{Code: sessionCodeForCacheErr(err), Err: err}
			}
			resolved = append(resolved, ResolvedToken{
				Type:  t.TokenType,
				Value: append([]byte(nil), t.TokenValue...),
			})

		case message.AliasTypeUseAlias:
			typ, val, err := s.tokenCache.Resolve(t.TokenAlias)
			if err != nil {
				return nil, &TokenCacheError{Code: sessionCodeForCacheErr(err), Err: err}
			}
			resolved = append(resolved, ResolvedToken{Type: typ, Value: val})

		case message.AliasTypeUseValue:
			resolved = append(resolved, ResolvedToken{
				Type:  t.TokenType,
				Value: append([]byte(nil), t.TokenValue...),
			})

		case message.AliasTypeDelete:
			if err := s.tokenCache.Delete(t.TokenAlias); err != nil {
				return nil, &TokenCacheError{Code: sessionCodeForCacheErr(err), Err: err}
			}

		default:
			return nil, &TokenCacheError{
				Code: moqt.SessionMalformedAuthToken,
				Err:  fmt.Errorf("unknown token alias type 0x%X", uint64(t.AliasType)),
			}
		}
	}
	return resolved, nil
}

// VerifyRequestTokens runs the configured [TokenVerifier] over the tokens the
// session resolved for req (see [Request.Tokens]). It returns nil when no
// verifier is configured or every token is authorized, and a
// [*TokenDeniedError] (mappable to a REQUEST_ERROR) for the first denial.
//
// The relay calls this before dispatching a request; standalone session users
// can call it from their own request loop. It is safe to call with a req whose
// Tokens slice is empty.
func (s *Session) VerifyRequestTokens(ctx context.Context, req *Request) error {
	if s.tokenVerifier == nil || len(req.Tokens) == 0 {
		return nil
	}
	for _, tok := range req.Tokens {
		if err := s.tokenVerifier.VerifyToken(ctx, s, tok); err != nil {
			if denied, ok := errors.AsType[*TokenDeniedError](err); ok {
				return denied
			}
			return &TokenDeniedError{Code: moqt.RequestUnauthorized, Reason: err.Error(), Err: err}
		}
	}
	return nil
}

// sessionCodeForCacheErr maps a [TokenCache] error to its §10.2.2 SESSION_ERROR
// code. The cache wraps a sentinel via sessionErr, so errors.Is identifies
// which one. An unrecognised error defaults to MALFORMED_AUTH_TOKEN.
func sessionCodeForCacheErr(err error) moqt.SessionErrorCode {
	for _, c := range []moqt.SessionErrorCode{
		moqt.SessionDuplicateAuthTokenAlias,
		moqt.SessionAuthTokenCacheOverflow,
		moqt.SessionUnknownAuthTokenAlias,
	} {
		if errors.Is(err, sessionErr(c)) {
			return c
		}
	}
	return moqt.SessionMalformedAuthToken
}

// messageParameters returns the Parameters block of msg for the request
// message types that may carry AUTHORIZATION_TOKEN (§10.2.2). The second
// return is false for message types that have no Parameters block, so the
// caller can skip token processing entirely.
func messageParameters(msg message.Message) (message.Parameters, bool) {
	switch m := msg.(type) {
	case *message.Subscribe:
		return m.Parameters, true
	case *message.Publish:
		return m.Parameters, true
	case *message.Fetch:
		return m.Parameters, true
	case *message.TrackStatus:
		return m.Parameters, true
	case *message.PublishNamespace:
		return m.Parameters, true
	case *message.SubscribeNamespace:
		return m.Parameters, true
	case *message.SubscribeTracks:
		return m.Parameters, true
	case *message.RequestUpdate:
		return m.Parameters, true
	}
	return nil, false
}

// ProcessFollowupTokens resolves the AUTHORIZATION_TOKEN parameters (§10.2.2)
// of a follow-up message read off an established request stream — §10.2.2
// explicitly allows tokens on REQUEST_UPDATE, and a REGISTER alias "MUST be
// added to the cache even if the message fails for some other reason".
// AcceptRequest performs the same processing for a stream's FIRST message;
// any code that reads follow-ups directly (message.Parse on the stream) MUST
// route messages carrying parameters through here, or the peer's view of the
// token cache silently diverges and its next USE_ALIAS kills the session
// with UNKNOWN_AUTH_TOKEN_ALIAS.
//
// The error contract matches AcceptRequest: a *TokenCacheError carries the
// SESSION_ERROR code the caller must close the session with. Messages
// without parameters (or without token parameters) return (nil, nil).
func (s *Session) ProcessFollowupTokens(msg message.Message) ([]ResolvedToken, error) {
	return s.processRequestTokens(msg)
}
