package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
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
// A REGISTER is committed immediately, before the request is validated, so a
// later rejection does not roll it back (§10.2.2: "even if the message fails
// for other reasons").
//
// A cache-layer failure is returned as a [*TokenCacheError] carrying the code
// the caller must close the session with.
func (s *Session) processRequestTokens(msg message.Message) ([]ResolvedToken, error) {
	ps, ok := message.ParamsOf(msg)
	if !ok {
		return nil, nil
	}
	tokens, err := message.TokensFromParam(ps)
	if err != nil {
		// §10.2.2: an undecodable Token, including an unknown Alias Type,
		// closes with KEY_VALUE_FORMATTING_ERROR. This follows that MUST
		// over §3.5's MALFORMED_AUTH_TOKEN description.
		return nil, &TokenCacheError{Code: moqt.SessionKeyValueFormattingError, Err: err}
	}
	if len(tokens) == 0 {
		return nil, nil
	}

	var resolved []ResolvedToken
	for i := range tokens {
		tok, ok, err := s.applyToken(&tokens[i])
		if err != nil {
			return nil, err
		}
		if ok {
			resolved = append(resolved, tok)
		}
	}
	return resolved, nil
}

// applyToken applies one token to the inbound cache (§10.2.2) and returns what
// it resolves to; ok is false for a DELETE. A cache failure is a
// [*TokenCacheError].
func (s *Session) applyToken(t *message.Token) (tok ResolvedToken, ok bool, err error) {
	switch t.AliasType {
	case message.AliasTypeRegister:
		// §10.2.2: register before any further validation.
		if err := s.tokenCache.Register(t.TokenAlias, t.TokenType, t.TokenValue); err != nil {
			return ResolvedToken{}, false, &TokenCacheError{Code: sessionCodeForCacheErr(err), Err: err}
		}
		return ResolvedToken{Type: t.TokenType, Value: bytes.Clone(t.TokenValue)}, true, nil

	case message.AliasTypeUseAlias:
		typ, val, err := s.tokenCache.Resolve(t.TokenAlias)
		if err != nil {
			return ResolvedToken{}, false, &TokenCacheError{Code: sessionCodeForCacheErr(err), Err: err}
		}
		return ResolvedToken{Type: typ, Value: val}, true, nil

	case message.AliasTypeUseValue:
		return ResolvedToken{Type: t.TokenType, Value: bytes.Clone(t.TokenValue)}, true, nil

	case message.AliasTypeDelete:
		if err := s.tokenCache.Delete(t.TokenAlias); err != nil {
			return ResolvedToken{}, false, &TokenCacheError{Code: sessionCodeForCacheErr(err), Err: err}
		}
	}
	// No other Alias Type: Token.Parse rejects it.
	return ResolvedToken{}, false, nil
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
	return s.VerifyTokens(ctx, req.Tokens)
}

// VerifyTokens is [Session.VerifyRequestTokens] for tokens resolved by
// [Session.ProcessFollowupTokens], such as a REQUEST_UPDATE's (§10.2.2).
func (s *Session) VerifyTokens(ctx context.Context, toks []ResolvedToken) error {
	if s.tokenVerifier == nil || len(toks) == 0 {
		return nil
	}
	for _, tok := range toks {
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

// ProcessFollowupTokens resolves the AUTHORIZATION_TOKEN parameters (§10.2.2)
// of a follow-up message, such as a REQUEST_UPDATE, read off an established
// request stream. Code that reads follow-ups with message.Parse MUST route
// them through here, or the token cache diverges from the peer's.
//
// A *TokenCacheError carries the code the caller must close the session with.
// Messages without token parameters return (nil, nil).
func (s *Session) ProcessFollowupTokens(msg message.Message) ([]ResolvedToken, error) {
	return s.processRequestTokens(msg)
}

// SetupTokens returns copies of the resolved tokens the peer sent in
// AUTHORIZATION TOKEN options of its SETUP (§10.3.1.4). The session does not
// verify them.
func (s *Session) SetupTokens() []ResolvedToken {
	out := make([]ResolvedToken, len(s.setupTokens))
	for i, t := range s.setupTokens {
		out[i] = ResolvedToken{Type: t.Type, Value: bytes.Clone(t.Value)}
	}
	return out
}

// processSetupTokens applies the AUTHORIZATION TOKEN options in the peer's
// SETUP (§10.3.1.4) and keeps the resolved tokens for [Session.SetupTokens].
// Unlike a request, a server closes with PROTOCOL_VIOLATION on DELETE or
// USE_ALIAS (§10.2.2), and a REGISTER that overflows the cache is treated as
// USE_VALUE (§10.3.1.4).
//
// Assumption: a REGISTER that both repeats an alias and overflows closes with
// DUPLICATE_AUTH_TOKEN_ALIAS; the draft does not say which rule wins.
//
// Every error is a [*TokenCacheError] carrying the code to close with.
func (s *Session) processSetupTokens() error {
	for _, opt := range s.peerOptions {
		if message.SetupOption(opt.Type) != message.SetupOptionAuthorizationToken {
			continue
		}
		var t message.Token
		if err := t.Parse(opt.ByteVal); err != nil {
			// §10.2.2.
			return &TokenCacheError{Code: moqt.SessionKeyValueFormattingError,
				Err: fmt.Errorf("moqt/session: AUTHORIZATION TOKEN setup option: %w", err)}
		}
		if s.role == roleServer &&
			(t.AliasType == message.AliasTypeDelete || t.AliasType == message.AliasTypeUseAlias) {
			return &TokenCacheError{Code: moqt.SessionProtocolViolation,
				Err: fmt.Errorf("moqt/session: %s token in the client's SETUP (§10.2.2)", t.AliasType)}
		}
		tok, ok, err := s.applyToken(&t)
		if tce, isTCE := errors.AsType[*TokenCacheError](err); isTCE &&
			t.AliasType == message.AliasTypeRegister && tce.Code == moqt.SessionAuthTokenCacheOverflow {
			tok, ok, err = ResolvedToken{Type: t.TokenType, Value: bytes.Clone(t.TokenValue)}, true, nil
		}
		if err != nil {
			return err
		}
		if ok {
			s.setupTokens = append(s.setupTokens, tok)
		}
	}
	return nil
}

// SetupTokenAliases returns the aliases of the REGISTER tokens this endpoint
// sent in SETUP ([WithSetupToken]) that fit the peer's
// MAX_AUTH_TOKEN_CACHE_SIZE, in the order sent (§10.3.1.4). Only these may be
// referenced with USE_ALIAS.
func (s *Session) SetupTokenAliases() []uint64 { return slices.Clone(s.setupTokenAliases) }

// checkOutboundSetupTokens refuses setup tokens the peer would close the
// session over (§10.2.2): DELETE, USE_ALIAS, or an alias REGISTERed twice.
func checkOutboundSetupTokens(toks []message.Token) error {
	var registered []uint64
	for _, t := range toks {
		switch t.AliasType {
		case message.AliasTypeRegister:
			if slices.Contains(registered, t.TokenAlias) {
				return fmt.Errorf("moqt/session: AUTHORIZATION TOKEN setup option registers alias %d twice (§10.2.2)",
					t.TokenAlias)
			}
			registered = append(registered, t.TokenAlias)
		case message.AliasTypeUseValue:
		case message.AliasTypeDelete, message.AliasTypeUseAlias:
			return fmt.Errorf("moqt/session: AUTHORIZATION TOKEN setup option with %s (§10.2.2)", t.AliasType)
		default:
			return fmt.Errorf("moqt/session: AUTHORIZATION TOKEN setup option with %s", t.AliasType)
		}
	}
	return nil
}

// heldSetupAliases replays the peer's cache accounting ([TokenCache.Register])
// against its MAX_AUTH_TOKEN_CACHE_SIZE (§10.3.1.3; default 0) and returns
// the aliases of the REGISTERs that fit (§10.3.1.4).
func heldSetupAliases(toks []message.Token, peerOptions []wire.KVPair) []uint64 {
	var limit uint64
	for _, opt := range peerOptions {
		if message.SetupOption(opt.Type) == message.SetupOptionMaxAuthTokenCache {
			limit = opt.IntVal
		}
	}
	var (
		used uint64
		held []uint64
	)
	for _, t := range toks {
		if t.AliasType != message.AliasTypeRegister {
			continue
		}
		size := uint64(16) + uint64(len(t.TokenValue))
		if used+size > limit {
			continue
		}
		used += size
		held = append(held, t.TokenAlias)
	}
	return held
}
