package uri

import (
	"errors"
	"fmt"
	"net/netip"
	"strings"
)

// CheckAuthority reports whether s is an RFC 3986 authority (§3.2) with a
// non-empty host, the form the AUTHORITY Setup Option carries (§10.3.1.1,
// §3.1.1):
//
//	authority = [ userinfo "@" ] host [ ":" port ]
//
// Unlike [Parse] (net/url), it accepts nothing outside the RFC 3986 grammar:
// no raw non-ASCII, no zone identifiers.
func CheckAuthority(s string) error {
	hostport := s
	if userinfo, rest, found := strings.Cut(s, "@"); found {
		if !validChars(userinfo, ":", true) {
			return fmt.Errorf("uri: authority %q: invalid userinfo", s)
		}
		hostport = rest
	}
	host, port, err := splitHostPort(hostport)
	if err != nil {
		return fmt.Errorf("uri: authority %q: %w", s, err)
	}
	if host == "" {
		return fmt.Errorf("uri: authority %q has an empty host", s)
	}
	if strings.Trim(port, "0123456789") != "" {
		return fmt.Errorf("uri: authority %q: invalid port", s)
	}
	return nil
}

// CheckPathAndQuery reports whether s is an RFC 3986 path-abempty optionally
// followed by "?" and a query, the form the PATH Setup Option carries
// (§10.3.1.2):
//
//	path-abempty = *( "/" segment )
//	segment      = *pchar
//	query        = *( pchar / "/" / "?" )
func CheckPathAndQuery(s string) error {
	path, query, _ := strings.Cut(s, "?")
	if path != "" && path[0] != '/' {
		return fmt.Errorf("uri: path %q does not begin with \"/\"", s)
	}
	for seg := range strings.SplitSeq(path, "/") {
		if !validChars(seg, ":@", true) {
			return fmt.Errorf("uri: path %q: invalid segment %q", s, seg)
		}
	}
	if !validChars(query, ":@/?", true) {
		return fmt.Errorf("uri: path %q: invalid query", s)
	}
	return nil
}

// splitHostPort splits host [ ":" port ] and checks the host: an IP-literal,
// or a reg-name (which covers IPv4address: its characters are a subset).
func splitHostPort(hostport string) (host, port string, err error) {
	if !strings.HasPrefix(hostport, "[") {
		host, port, _ = strings.Cut(hostport, ":")
		if !validChars(host, "", true) {
			return "", "", errors.New("invalid host")
		}
		return host, port, nil
	}
	end := strings.IndexByte(hostport, ']')
	if end < 0 {
		return "", "", errors.New("unterminated IP-literal")
	}
	if !validIPLiteral(hostport[1:end]) {
		return "", "", errors.New("invalid IP-literal")
	}
	host, port = hostport[:end+1], hostport[end+1:]
	if port != "" && port[0] != ':' {
		return "", "", errors.New("junk after IP-literal")
	}
	return host, strings.TrimPrefix(port, ":"), nil
}

// validChars reports whether s consists of RFC 3986 unreserved characters,
// sub-delims, the bytes in extra, and — when pct is set — well-formed
// pct-encoded triplets.
func validChars(s, extra string, pct bool) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '%' && pct:
			if i+2 >= len(s) || !isHex(s[i+1]) || !isHex(s[i+2]) {
				return false
			}
			i += 2
		case isUnreserved(c), strings.IndexByte("!$&'()*+,;=", c) >= 0, strings.IndexByte(extra, c) >= 0:
		default:
			return false
		}
	}
	return true
}

// validIPLiteral checks the inside of "[...]":
//
//	IP-literal = "[" ( IPv6address / IPvFuture  ) "]"
//	IPvFuture  = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" )
func validIPLiteral(s string) bool {
	if s != "" && (s[0] == 'v' || s[0] == 'V') {
		ver, rest, found := strings.Cut(s[1:], ".")
		return found && ver != "" && strings.Trim(ver, "0123456789abcdefABCDEF") == "" &&
			rest != "" && validChars(rest, ":", false)
	}
	// RFC 3986 has no zone identifiers, which netip would accept.
	if strings.IndexByte(s, '%') >= 0 {
		return false
	}
	a, err := netip.ParseAddr(s)
	return err == nil && a.Is6()
}

func isUnreserved(c byte) bool {
	return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' ||
		c == '-' || c == '.' || c == '_' || c == '~'
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}
