package quicconn

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/netip"

	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// Dial opens a raw-QUIC connection to addr ("host:port") and returns it as a
// [session.Conn], ready for a client-side MOQT SETUP. It is the client-side
// counterpart of [NewListener], and the one dial path every MOQT client in this
// repo shares — the relay's cross-relay dialer and the demo/interop CLIs alike.
//
// The name in addr is resolved here rather than left to [quic.DialAddr], which
// resolves via [net.ResolveUDPAddr]. That helper returns the first *IPv4*
// address for a bare "host:port" and reaches for IPv6 only when the string
// carries a bracketed literal — it picks the family by looking for a '[' in the
// string (net.addrList.forResolve), not by what the resolver ranked first. So a
// dual-stack peer named by hostname is always dialed over IPv4, even where only
// the IPv6 path carries traffic: the Initials leave, nothing answers, and the
// dial fails with "timeout: no recent network activity" while the host is
// plainly reachable over IPv6.
//
// [net.Resolver.LookupIPAddr] instead returns every address in RFC 6724 order —
// the same ranking getaddrinfo / `getent ahosts` report — and each is tried in
// turn, so a host whose first address is unreachable still connects on the next.
// Candidates go back to [quic.DialAddr] as literals via [net.JoinHostPort],
// which brackets IPv6: that pins the family chosen here and costs no second
// lookup. ctx bounds the whole sequence, so a caller's dial timeout applies
// across all candidates rather than per candidate.
func Dial(ctx context.Context, addr string, tlsCfg *tls.Config, quicCfg *quic.Config) (session.Conn, error) {
	candidates, err := resolveDialCandidates(ctx, addr)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, candidate := range candidates {
		qc, err := quic.DialAddr(ctx, candidate, tlsCfg, quicCfg)
		if err == nil {
			return New(qc), nil
		}
		lastErr = fmt.Errorf("dial %s: %w", candidate, err)
		// A cancelled/expired ctx fails every remaining candidate the same way;
		// report the first real failure instead of the derived ones.
		if ctx.Err() != nil {
			break
		}
	}
	return nil, lastErr
}

// resolveDialCandidates expands a "host:port" into the addresses to dial, in the
// resolver's preferred order. A host that is already an IP literal resolves to
// itself: no DNS, and no chance of the family flipping under a caller that
// deliberately pinned one. [netip.ParseAddr] rather than [net.ParseIP] so a
// zone-scoped literal ("fe80::1%eth0") is recognized as one too.
func resolveDialCandidates(ctx context.Context, addr string) ([]string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return []string{addr}, nil
	}
	// LookupIPAddr reports an error rather than an empty slice when a name has
	// no addresses, so the dial loop above always has at least one candidate.
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	candidates := make([]string, len(ips))
	for i, ip := range ips {
		// IPAddr.String carries the zone; JoinHostPort adds the brackets.
		candidates[i] = net.JoinHostPort(ip.String(), port)
	}
	return candidates, nil
}
