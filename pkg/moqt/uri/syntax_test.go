package uri_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/uri"
)

// §10.3.1.1 / §10.3.1.2: the AUTHORITY and PATH Setup Options follow "the
// URI formatting rules [RFC3986]"; one that does not conform closes the
// session with MALFORMED_AUTHORITY / MALFORMED_PATH. §3.1.1 adds that the
// authority "MUST NOT contain an empty host portion".

func TestCheckAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in string
		ok bool
	}{
		{"example.com", true},
		{"example.com:4433", true},
		{"example.com:", true}, // RFC 3986: port = *DIGIT
		{"user:pw@example.com:443", true},
		{"a-b.c_d~e!$&'()*+,;=", true},
		{"ex%41mple.com", true},
		{"192.0.2.1:4433", true},
		{"[2001:db8::1]:4433", true},
		{"[::ffff:192.0.2.1]", true},
		{"[v7.a:b]", true},

		{"", false},                  // empty host (§3.1.1)
		{":443", false},              // empty host (§3.1.1)
		{"user@", false},             // empty host (§3.1.1)
		{"exa mple.com", false},      // space
		{"example.com/path", false},  // path in authority
		{"example.com?q", false},     // query in authority
		{"example.com#f", false},     // fragment in authority
		{"example.com:44a", false},   // non-digit port
		{"a@b@c", false},             // "@" in host
		{"ex%4", false},              // truncated pct-encoding
		{"ex%zz", false},             // bad hex
		{"[2001:db8::1", false},      // unterminated IP-literal
		{"[192.0.2.1]", false},       // IPv4 inside brackets
		{"[fe80::1%25eth0]", false},  // zone IDs are not RFC 3986
		{"[2001:db8::1]x", false},    // junk after IP-literal
		{"[v7.]", false},             // empty IPvFuture tail
		{"exämple.com", false},       // non-ASCII must be pct-encoded
		{"example.com:443:1", false}, // two ports
	} {
		err := uri.CheckAuthority(tc.in)
		if (err == nil) != tc.ok {
			t.Errorf("CheckAuthority(%q) = %v, want ok=%v", tc.in, err, tc.ok)
		}
	}
}

func TestCheckPathAndQuery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in string
		ok bool
	}{
		{"", true},
		{"/", true},
		{"/relay", true},
		{"/a/b/", true},
		{"//a", true}, // path-abempty allows empty segments
		{"/a:b@c!$&'()*+,;=-._~", true},
		{"/a%2Fb", true},
		{"/relay?x=1&y=2", true},
		{"?q", true},
		{"/p?a/b?c", true}, // query may hold "/" and "?"
		{"/p?", true},

		{"relay", false},   // must begin with "/" when non-empty
		{"/a b", false},    // space
		{"/a#f", false},    // fragment is never sent (§3.1.2)
		{"/a%2", false},    // truncated pct-encoding
		{"/a%g0", false},   // bad hex
		{"/a[b]", false},   // gen-delims not allowed in a segment
		{"/p?a b", false},  // space in query
		{"/ä", false},      // non-ASCII must be pct-encoded
		{"/a\x00b", false}, // control character
	} {
		err := uri.CheckPathAndQuery(tc.in)
		if (err == nil) != tc.ok {
			t.Errorf("CheckPathAndQuery(%q) = %v, want ok=%v", tc.in, err, tc.ok)
		}
	}
}
