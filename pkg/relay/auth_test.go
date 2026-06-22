package relay_test

import (
	"context"
	"errors"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestAllowAllAuthorizer_AllMethodsAllow verifies that the default
// implementation lets every request type through. The relay (and tests) rely
// on this as the no-policy baseline.
func TestAllowAllAuthorizer_AllMethodsAllow(t *testing.T) {
	t.Parallel()
	a := relay.AllowAllAuthorizer{}
	ctx := context.Background()
	var sess *session.Session

	if err := a.AuthorizeSubscribe(ctx, sess, &message.Subscribe{}); err != nil {
		t.Errorf("AuthorizeSubscribe: %v", err)
	}
	if err := a.AuthorizePublish(ctx, sess, &message.Publish{}); err != nil {
		t.Errorf("AuthorizePublish: %v", err)
	}
	if err := a.AuthorizePublishNamespace(ctx, sess, &message.PublishNamespace{}); err != nil {
		t.Errorf("AuthorizePublishNamespace: %v", err)
	}
	if err := a.AuthorizeFetch(ctx, sess, &message.Fetch{}); err != nil {
		t.Errorf("AuthorizeFetch: %v", err)
	}
	if err := a.AuthorizeSubscribeNamespace(ctx, sess, &message.SubscribeNamespace{}); err != nil {
		t.Errorf("AuthorizeSubscribeNamespace: %v", err)
	}
	if err := a.AuthorizeSubscribeTracks(ctx, sess, &message.SubscribeTracks{}); err != nil {
		t.Errorf("AuthorizeSubscribeTracks: %v", err)
	}
	if err := a.AuthorizeTrackStatus(ctx, sess, &message.TrackStatus{}); err != nil {
		t.Errorf("AuthorizeTrackStatus: %v", err)
	}
}

// TestDeny_BuildsDeniedError covers the Deny constructor.
func TestDeny_BuildsDeniedError(t *testing.T) {
	t.Parallel()
	err := relay.Deny(moqt.RequestUnauthorized, "bad token")

	var denied *relay.DeniedError
	if !errors.As(err, &denied) {
		t.Fatalf("error type = %T, want *DeniedError", err)
	}
	if denied.Code != moqt.RequestUnauthorized {
		t.Errorf("Code = %v, want RequestUnauthorized", denied.Code)
	}
	if denied.Reason != "bad token" {
		t.Errorf("Reason = %q, want %q", denied.Reason, "bad token")
	}
	if want := "relay: request denied (code 0x1): bad token"; err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

// TestDenyReason_DefaultsToUnauthorized verifies the convenience constructor
// falls back to the canonical authorization code.
func TestDenyReason_DefaultsToUnauthorized(t *testing.T) {
	t.Parallel()
	err := relay.DenyReason("missing token")
	if got := relay.CodeForAuthorizerError(err); got != moqt.RequestUnauthorized {
		t.Fatalf("code = %v, want RequestUnauthorized", got)
	}
	if got := relay.ReasonForAuthorizerError(err); got != "missing token" {
		t.Fatalf("reason = %q", got)
	}
}

// TestCodeForAuthorizerError_PlainError verifies that a plain (non-DeniedError)
// returned from a policy still maps to RequestUnauthorized. This is the
// "third-party library returns an error" path.
func TestCodeForAuthorizerError_PlainError(t *testing.T) {
	t.Parallel()
	err := errors.New("token verify failed")
	if got := relay.CodeForAuthorizerError(err); got != moqt.RequestUnauthorized {
		t.Fatalf("code = %v, want RequestUnauthorized", got)
	}
	if got := relay.ReasonForAuthorizerError(err); got != "token verify failed" {
		t.Fatalf("reason = %q", got)
	}
}

// TestCodeForAuthorizerError_ZeroCode collapses to Unauthorized as documented
// on DeniedError.RequestErrorCode.
func TestCodeForAuthorizerError_ZeroCode(t *testing.T) {
	t.Parallel()
	err := &relay.DeniedError{Reason: "no code"}
	if got := relay.CodeForAuthorizerError(err); got != moqt.RequestUnauthorized {
		t.Fatalf("code = %v, want RequestUnauthorized", got)
	}
}

// TestRelay_DefaultsAuthorizerToAllowAll verifies the Config wiring: a
// nil Authorizer in Config is replaced with AllowAllAuthorizer by [relay.New].
// This is what makes the relay usable in tests without writing a policy.
func TestRelay_DefaultsAuthorizerToAllowAll(t *testing.T) {
	t.Parallel()
	r := relay.New(newPipeListener(), relay.Config{})
	if got := r.Authorizer(); got == nil {
		t.Fatal("Authorizer() returned nil")
	}
	if _, ok := r.Authorizer().(relay.AllowAllAuthorizer); !ok {
		t.Fatalf("default Authorizer = %T, want AllowAllAuthorizer", r.Authorizer())
	}
}

// TestRelay_HonoursCustomAuthorizer verifies that an injected Authorizer is
// preserved on the relay.
func TestRelay_HonoursCustomAuthorizer(t *testing.T) {
	t.Parallel()
	custom := &stubAuthorizer{}
	r := relay.New(newPipeListener(), relay.Config{Authorizer: custom})
	if got := r.Authorizer(); got != custom {
		t.Fatalf("Authorizer() = %v, want injected stub", got)
	}
}

// stubAuthorizer is a no-op recorder; used here to prove the relay holds
// onto exactly the pointer we hand it.
type stubAuthorizer struct{}

func (*stubAuthorizer) AuthorizeSubscribe(context.Context, *session.Session, *message.Subscribe) error {
	return nil
}
func (*stubAuthorizer) AuthorizePublish(context.Context, *session.Session, *message.Publish) error {
	return nil
}
func (*stubAuthorizer) AuthorizePublishNamespace(context.Context, *session.Session, *message.PublishNamespace) error {
	return nil
}
func (*stubAuthorizer) AuthorizeFetch(context.Context, *session.Session, *message.Fetch) error {
	return nil
}

func (*stubAuthorizer) AuthorizeSubscribeNamespace(
	context.Context,
	*session.Session,
	*message.SubscribeNamespace,
) error {
	return nil
}
func (*stubAuthorizer) AuthorizeSubscribeTracks(context.Context, *session.Session, *message.SubscribeTracks) error {
	return nil
}
func (*stubAuthorizer) AuthorizeTrackStatus(context.Context, *session.Session, *message.TrackStatus) error {
	return nil
}
