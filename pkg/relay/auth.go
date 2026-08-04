package relay

import (
	"context"
	"errors"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// Authorizer is the relay's pluggable authorization hook. Every request
// handler in [pkg/relay] consults the authorizer before performing any state
// mutation; a non-nil return causes the relay to reply REQUEST_ERROR with
// the [DeniedError]'s mapped code (see [DeniedError.RequestErrorCode]).
//
// The interface is split per request type for two reasons:
//
//   - It lets a policy reject categories of request without having to
//     type-switch internally — the most common case is "this peer is allowed
//     to subscribe but not publish."
//   - It surfaces the parsed message to the policy, so token-based schemes
//     can inspect things like AUTH_TOKEN parameters or the target Track
//     Namespace without re-parsing the wire form.
//
// Every method receives:
//
//   - ctx: the per-request context. Authorizers may consult it for tracing,
//     cancellation, or token-cache lookups. The relay cancels ctx when the
//     request stream or the session terminates.
//   - sess: the MOQT session the request arrived on. Policies often inspect
//     [session.Session.PeerOptions] (for AUTHORITY / PATH / implementation
//     name) or session-scoped TLS / SETUP-time attestations.
//   - msg: the parsed request message. Authorizers MUST NOT mutate it.
//
// Returning nil grants the request; returning a non-nil error denies it.
// The relay treats any non-nil return as a denial regardless of error type,
// but wrapping with [*DeniedError] (or using [Deny] / [DenyReason]) gives
// the relay an explicit REQUEST_ERROR code to forward to the peer. A plain
// error (e.g. one returned from a downstream token-validation library)
// maps to [moqt.RequestUnauthorized] by default.
type Authorizer interface {
	AuthorizeSubscribe(ctx context.Context, sess *session.Session, msg *message.Subscribe) error
	AuthorizePublish(ctx context.Context, sess *session.Session, msg *message.Publish) error
	AuthorizePublishNamespace(ctx context.Context, sess *session.Session, msg *message.PublishNamespace) error
	AuthorizeFetch(ctx context.Context, sess *session.Session, msg *message.Fetch) error
	AuthorizeSubscribeNamespace(ctx context.Context, sess *session.Session, msg *message.SubscribeNamespace) error
	AuthorizeSubscribeTracks(ctx context.Context, sess *session.Session, msg *message.SubscribeTracks) error
	AuthorizeTrackStatus(ctx context.Context, sess *session.Session, msg *message.TrackStatus) error
}

// DeniedError is returned by an [Authorizer] method to deny a request with
// an explicit MoQT REQUEST_ERROR code. The relay maps Code directly onto the
// REQUEST_ERROR it sends downstream; Reason is forwarded as the human-readable
// reason string.
//
// Code MUST be one of the §10.6 / IANA §15.11.2 REQUEST_ERROR codes (see
// [moqt.RequestErrorCode]). If Code is the zero value, the relay substitutes
// [moqt.RequestUnauthorized] when forming the REQUEST_ERROR.
type DeniedError struct {
	Code   moqt.RequestErrorCode
	Reason string
}

// Error implements the error interface.
func (e *DeniedError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("relay: request denied (code %#x)", uint64(e.Code))
	}
	return fmt.Sprintf("relay: request denied (code %#x): %s", uint64(e.Code), e.Reason)
}

// RequestErrorCode returns the REQUEST_ERROR code the relay should use when
// rejecting the request. A zero value collapses to [moqt.RequestUnauthorized]
// — the spec's default rejection code for an authorization failure.
func (e *DeniedError) RequestErrorCode() moqt.RequestErrorCode {
	if e.Code == 0 {
		return moqt.RequestUnauthorized
	}
	return e.Code
}

// Deny is a constructor for [*DeniedError]. Use it when the policy already
// knows which REQUEST_ERROR code to surface:
//
//	return relay.Deny(moqt.RequestUnauthorized, "missing JWT")
func Deny(code moqt.RequestErrorCode, reason string) error {
	return &DeniedError{Code: code, Reason: reason}
}

// DenyReason is a convenience for the common case where the policy only
// wants to attach a human-readable reason and is happy with the default
// REQUEST_ERROR code ([moqt.RequestUnauthorized]).
func DenyReason(reason string) error {
	return &DeniedError{Code: moqt.RequestUnauthorized, Reason: reason}
}

// CodeForAuthorizerError extracts the REQUEST_ERROR code the relay should
// use when rejecting an authorization failure. If err wraps a [*DeniedError],
// its code is returned; otherwise the default [moqt.RequestUnauthorized] is
// returned so a policy that returns a plain error still gets a sensible
// MoQT reply rather than [moqt.RequestInternalError].
func CodeForAuthorizerError(err error) moqt.RequestErrorCode {
	if denied, ok := errors.AsType[*DeniedError](err); ok {
		return denied.RequestErrorCode()
	}
	return moqt.RequestUnauthorized
}

// ReasonForAuthorizerError extracts the human-readable reason for an
// authorization denial. If err wraps a [*DeniedError] and that error has a
// non-empty Reason field, the reason is returned; otherwise the error's
// Error() string is returned. This avoids leaking the internal "relay:
// request denied (code 0x1):" prefix into the wire reply.
func ReasonForAuthorizerError(err error) string {
	if denied, ok := errors.AsType[*DeniedError](err); ok && denied.Reason != "" {
		return denied.Reason
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

// AllowAllAuthorizer is the package's permissive default. Every method
// returns nil. It exists so unit tests, in-process integration tests, and
// pure-relay-of-relays topologies that defer authorization to a downstream
// layer can run without writing a custom policy.
//
// Production deployments SHOULD replace this with a token- or
// session-attestation-aware implementation via [Config.Authorizer]. The relay
// only invokes the authorizer once per request before any state mutation, so
// the cost of policy evaluation is bounded by the request rate rather than
// the object rate.
type AllowAllAuthorizer struct{}

var _ Authorizer = AllowAllAuthorizer{}

// AuthorizeSubscribe returns nil.
func (AllowAllAuthorizer) AuthorizeSubscribe(context.Context, *session.Session, *message.Subscribe) error {
	return nil
}

// AuthorizePublish returns nil.
func (AllowAllAuthorizer) AuthorizePublish(context.Context, *session.Session, *message.Publish) error {
	return nil
}

// AuthorizePublishNamespace returns nil.
func (AllowAllAuthorizer) AuthorizePublishNamespace(
	context.Context,
	*session.Session,
	*message.PublishNamespace,
) error {
	return nil
}

// AuthorizeFetch returns nil.
func (AllowAllAuthorizer) AuthorizeFetch(context.Context, *session.Session, *message.Fetch) error {
	return nil
}

// AuthorizeSubscribeNamespace returns nil.
func (AllowAllAuthorizer) AuthorizeSubscribeNamespace(
	context.Context,
	*session.Session,
	*message.SubscribeNamespace,
) error {
	return nil
}

// AuthorizeSubscribeTracks returns nil.
func (AllowAllAuthorizer) AuthorizeSubscribeTracks(context.Context, *session.Session, *message.SubscribeTracks) error {
	return nil
}

// AuthorizeTrackStatus returns nil.
func (AllowAllAuthorizer) AuthorizeTrackStatus(context.Context, *session.Session, *message.TrackStatus) error {
	return nil
}
