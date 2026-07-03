// Package uri parses and validates "moqt" URIs and their fragment identifiers
// as defined by draft-ietf-moq-transport-18 §3.1.1 and §3.1.2.
//
//	moqt-URI = "moqt" "://" authority path-abempty [ "?" query ]
//
// A parsed [URI] exposes everything the connection-setup paths need:
// [URI.HostPort] for dialing (applying the §3.1.1 default port of 443),
// [URI.Authority] and [URI.PathAndQuery] for the AUTHORITY / PATH Setup
// Options carried on a native-QUIC connection (§3.1.4 / §10.3.1), and
// [URI.HTTPSURL] for the https URL a WebTransport client connects to
// (§3.1.3). Fragments are parsed and validated but, per §3.1.2, are processed
// locally by the client and never transmitted to the server.
//
// The package depends only on the standard library so it can be used from any
// layer of the stack without pulling in the session machinery.
package uri

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// Scheme is the URI scheme defined for MOQT servers (§3.1.1).
const Scheme = "moqt"

// DefaultPort is used when the authority omits an explicit port (§3.1.1:
// "If the port is omitted in the URI, a default port of 443 is used").
const DefaultPort = "443"

// URI is a parsed, validated "moqt" URI (§3.1.1).
type URI struct {
	// Authority is the host[:port] exactly as supplied (no default port
	// filled in). The host subcomponent is guaranteed non-empty.
	Authority string

	// Host is the host subcomponent of the authority, without any port.
	Host string

	// Port is the explicit port from the authority, or [DefaultPort] when the
	// authority omitted one.
	Port string

	// Path is the path-abempty component: either empty or beginning with "/".
	Path string

	// RawQuery is the query component without its leading "?", empty when the
	// URI carried no query.
	RawQuery string

	// Fragment is the parsed fragment identifier, or nil when the URI carried
	// none. Per §3.1.2 the fragment is processed locally and never sent to
	// the server.
	Fragment *Fragment
}

// Fragment is a parsed moqt URI fragment identifier (§3.1.2):
//
//	moqt://example.com/app#<type>:<value>
type Fragment struct {
	// Type is the registered fragment type identifier: a non-empty string of
	// ASCII lowercase letters, digits, and hyphens (a-z, 0-9, -).
	Type string

	// Value is the type-specific value following the first colon. Its
	// semantics are defined by the specification that registers Type.
	Value string
}

// Parse parses and validates a "moqt" URI per §3.1.1 / §3.1.2. It returns an
// error when the scheme is not "moqt", the URI is not hierarchical, the
// authority has an empty host, or a present fragment does not match the
// "type:value" grammar.
func Parse(raw string) (*URI, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("uri: parse %q: %w", raw, err)
	}
	if u.Scheme != Scheme {
		return nil, fmt.Errorf("uri: scheme %q, want %q", u.Scheme, Scheme)
	}
	// A hierarchical URI ("scheme://...") leaves Opaque empty and fills Host.
	// An opaque form like "moqt:foo" is rejected.
	if u.Opaque != "" {
		return nil, fmt.Errorf("uri: %q is not hierarchical (expected %s://)", raw, Scheme)
	}
	host := u.Hostname()
	if host == "" {
		// §3.1.1: "The authority portion MUST NOT contain an empty host
		// portion."
		return nil, fmt.Errorf("uri: %q has an empty host", raw)
	}
	port := u.Port()
	if port == "" {
		port = DefaultPort
	}

	out := &URI{
		Authority: u.Host,
		Host:      host,
		Port:      port,
		// EscapedPath, not Path: the struct carries the RAW path-abempty
		// component. url.URL.Path is percent-DECODED — using it would turn
		// "/a%3Fb" into "/a?b", making the §3.1.4 PATH Setup Option
		// ambiguous and String()/HTTPSURL() emit invalid URIs.
		Path:     u.EscapedPath(),
		RawQuery: u.RawQuery,
	}

	// A '#' in the raw URI introduces a fragment component (§3.1.2). When
	// present it MUST match the "type:value" grammar, so a present-but-empty
	// or colon-less fragment is an error rather than silently ignored.
	if strings.IndexByte(raw, '#') >= 0 {
		frag, err := parseFragment(u.Fragment)
		if err != nil {
			return nil, err
		}
		out.Fragment = frag
	}

	return out, nil
}

// parseFragment validates the "type:value" grammar of §3.1.2 against the
// (percent-decoded) fragment text.
func parseFragment(frag string) (*Fragment, error) {
	typ, val, ok := strings.Cut(frag, ":")
	if !ok {
		return nil, fmt.Errorf("uri: fragment %q missing \"type:value\" colon (§3.1.2)", frag)
	}
	if !validFragmentType(typ) {
		return nil, fmt.Errorf(
			"uri: fragment type %q must be a non-empty run of ASCII [a-z0-9-] (§3.1.2)", typ)
	}
	return &Fragment{Type: typ, Value: val}, nil
}

// validFragmentType reports whether s is a non-empty run of ASCII lowercase
// letters, digits, and hyphens (§3.1.2).
func validFragmentType(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '-':
		default:
			return false
		}
	}
	return true
}

// HostPort returns the "host:port" string for dialing, applying the §3.1.1
// default port of 443 when the URI omitted one.
func (u *URI) HostPort() string {
	return net.JoinHostPort(u.Host, u.Port)
}

// PathAndQuery returns the path-abempty with the query appended, the value to
// carry in the PATH Setup Option (§3.1.4 / §10.3.1.2). It is empty when the
// URI has neither a path nor a query.
func (u *URI) PathAndQuery() string {
	if u.RawQuery == "" {
		return u.Path
	}
	return u.Path + "?" + u.RawQuery
}

// HTTPSURL converts the moqt URI to the https URL a WebTransport client
// connects to (§3.1.3): the scheme is replaced with https and the authority,
// path, and query are preserved. The fragment is omitted because it is
// processed locally and never sent to the server (§3.1.2).
func (u *URI) HTTPSURL() string {
	var b strings.Builder
	b.WriteString("https://")
	b.WriteString(u.Authority)
	b.WriteString(u.Path)
	if u.RawQuery != "" {
		b.WriteByte('?')
		b.WriteString(u.RawQuery)
	}
	return b.String()
}

// String reconstructs the moqt URI, including any fragment. The authority is
// emitted exactly as parsed, so a URI that omitted its port round-trips
// without a default port appearing.
func (u *URI) String() string {
	var b strings.Builder
	b.WriteString(Scheme)
	b.WriteString("://")
	b.WriteString(u.Authority)
	b.WriteString(u.Path)
	if u.RawQuery != "" {
		b.WriteByte('?')
		b.WriteString(u.RawQuery)
	}
	if u.Fragment != nil {
		b.WriteByte('#')
		b.WriteString(u.Fragment.Type)
		b.WriteByte(':')
		b.WriteString(u.Fragment.Value)
	}
	return b.String()
}
