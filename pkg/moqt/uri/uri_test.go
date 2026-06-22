package uri_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/uri"
)

func TestParse_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		raw          string
		wantHostPort string
		wantAuth     string
		wantPathQ    string
		wantHTTPS    string
		wantFragType string
		wantFragVal  string
	}{
		{
			name:         "host only, default port",
			raw:          "moqt://example.com",
			wantHostPort: "example.com:443",
			wantAuth:     "example.com",
			wantPathQ:    "",
			wantHTTPS:    "https://example.com",
		},
		{
			name:         "explicit port preserved",
			raw:          "moqt://example.com:4433/live",
			wantHostPort: "example.com:4433",
			wantAuth:     "example.com:4433",
			wantPathQ:    "/live",
			wantHTTPS:    "https://example.com:4433/live",
		},
		{
			name:         "path and query",
			raw:          "moqt://relay.example/app/room?token=abc",
			wantHostPort: "relay.example:443",
			wantAuth:     "relay.example",
			wantPathQ:    "/app/room?token=abc",
			wantHTTPS:    "https://relay.example/app/room?token=abc",
		},
		{
			name:         "well-known prefix",
			raw:          "moqt://example.com/.well-known/moqt",
			wantHostPort: "example.com:443",
			wantAuth:     "example.com",
			wantPathQ:    "/.well-known/moqt",
			wantHTTPS:    "https://example.com/.well-known/moqt",
		},
		{
			name:         "fragment processed locally, dropped from https",
			raw:          "moqt://example.com/app#loc:1.2",
			wantHostPort: "example.com:443",
			wantAuth:     "example.com",
			wantPathQ:    "/app",
			wantHTTPS:    "https://example.com/app",
			wantFragType: "loc",
			wantFragVal:  "1.2",
		},
		{
			name:         "fragment value may contain colons",
			raw:          "moqt://h/p#t-1:a:b:c",
			wantHostPort: "h:443",
			wantAuth:     "h",
			wantPathQ:    "/p",
			wantHTTPS:    "https://h/p",
			wantFragType: "t-1",
			wantFragVal:  "a:b:c",
		},
		{
			name:         "ipv6 literal authority",
			raw:          "moqt://[2001:db8::1]:9000/x",
			wantHostPort: "[2001:db8::1]:9000",
			wantAuth:     "[2001:db8::1]:9000",
			wantPathQ:    "/x",
			wantHTTPS:    "https://[2001:db8::1]:9000/x",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			u, err := uri.Parse(tc.raw)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.raw, err)
			}
			if got := u.HostPort(); got != tc.wantHostPort {
				t.Errorf("HostPort() = %q, want %q", got, tc.wantHostPort)
			}
			if u.Authority != tc.wantAuth {
				t.Errorf("Authority = %q, want %q", u.Authority, tc.wantAuth)
			}
			if got := u.PathAndQuery(); got != tc.wantPathQ {
				t.Errorf("PathAndQuery() = %q, want %q", got, tc.wantPathQ)
			}
			if got := u.HTTPSURL(); got != tc.wantHTTPS {
				t.Errorf("HTTPSURL() = %q, want %q", got, tc.wantHTTPS)
			}
			if tc.wantFragType == "" {
				if u.Fragment != nil {
					t.Errorf("Fragment = %+v, want nil", u.Fragment)
				}
			} else {
				if u.Fragment == nil {
					t.Fatalf("Fragment = nil, want type=%q value=%q", tc.wantFragType, tc.wantFragVal)
				}
				if u.Fragment.Type != tc.wantFragType || u.Fragment.Value != tc.wantFragVal {
					t.Errorf("Fragment = {%q, %q}, want {%q, %q}",
						u.Fragment.Type, u.Fragment.Value, tc.wantFragType, tc.wantFragVal)
				}
			}
		})
	}
}

func TestParse_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
	}{
		{"wrong scheme", "https://example.com/x"},
		{"no scheme", "example.com:4433"},
		{"opaque form", "moqt:example.com"},
		{"empty host", "moqt:///path"},
		{"fragment missing colon", "moqt://example.com/app#loc"},
		{"fragment empty", "moqt://example.com/app#"},
		{"fragment empty type", "moqt://example.com/app#:value"},
		{"fragment uppercase type", "moqt://example.com/app#Loc:1"},
		{"fragment underscore type", "moqt://example.com/app#lo_c:1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if u, err := uri.Parse(tc.raw); err == nil {
				t.Fatalf("Parse(%q) = %+v, want error", tc.raw, u)
			}
		})
	}
}

// TestString_RoundTrips checks that re-parsing String() yields an equivalent
// URI, and that an omitted port does not gain a default 443 on the way out.
func TestString_RoundTrips(t *testing.T) {
	t.Parallel()

	raws := []string{
		"moqt://example.com",
		"moqt://example.com:4433/live?x=1",
		"moqt://example.com/app#loc:1.2",
		"moqt://h/p#t-1:a:b:c",
	}
	for _, raw := range raws {
		u, err := uri.Parse(raw)
		if err != nil {
			t.Fatalf("Parse(%q): %v", raw, err)
		}
		if got := u.String(); got != raw {
			t.Errorf("String() = %q, want %q", got, raw)
		}
		if _, err := uri.Parse(u.String()); err != nil {
			t.Errorf("re-Parse(%q): %v", u.String(), err)
		}
	}
}
