package session

import (
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Option configures a session opened via Client or Server. See WithPath,
// WithAuthority, WithImplementation, WithMaxAuthTokenCacheSize,
// WithTokenVerifier, and WithGrease for the available knobs.
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
