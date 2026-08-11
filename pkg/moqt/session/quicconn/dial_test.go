package quicconn

import (
	"context"
	"net"
	"testing"
)

// TestResolveDialCandidatesPassesThroughLiterals pins the property that keeps a
// deliberately-pinned address pinned: an IP literal dials exactly what the caller
// wrote, with no DNS and no opportunity for the address family to change
// underneath it.
func TestResolveDialCandidatesPassesThroughLiterals(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{
		"127.0.0.1:4433",
		"[::1]:4433",
		"[2a02:6b8:c28:620:0:6f2d:0:f1]:443",
		"[fe80::1%eth0]:443", // zone-scoped: netip.ParseAddr accepts it, net.ParseIP would not
	} {
		got, err := resolveDialCandidates(context.Background(), addr)
		if err != nil {
			t.Fatalf("resolveDialCandidates(%q): %v", addr, err)
		}
		if len(got) != 1 || got[0] != addr {
			t.Errorf("resolveDialCandidates(%q) = %q, want [%q]", addr, got, addr)
		}
	}
}

// TestResolveDialCandidatesBracketsResolvedIPv6 is the regression guard for the
// bug this resolution exists to fix. quic.DialAddr resolves a bare "host:port"
// with net.ResolveUDPAddr, which picks the first *IPv4* address and reaches for
// IPv6 only when it sees a '[' in the string — so a dual-stack peer was always
// dialed over IPv4. Every candidate must therefore come back in a form that
// names one address unambiguously: parseable by SplitHostPort, and bracketed
// when it is IPv6.
func TestResolveDialCandidatesBracketsResolvedIPv6(t *testing.T) {
	t.Parallel()
	// localhost resolves from /etc/hosts (no network) and is dual-stack on most
	// systems, so this exercises the resolved path without a fixture. Which
	// families come back is the machine's business; the shape of each is not.
	got, err := resolveDialCandidates(context.Background(), "localhost:4433")
	if err != nil {
		t.Fatalf("resolveDialCandidates: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("resolveDialCandidates returned no candidates for localhost")
	}
	for _, candidate := range got {
		host, port, err := net.SplitHostPort(candidate)
		if err != nil {
			t.Errorf("SplitHostPort(%q): %v", candidate, err)
			continue
		}
		if port != "4433" {
			t.Errorf("candidate %q: port = %q, want 4433", candidate, port)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			t.Errorf("candidate %q: host %q is not an IP", candidate, host)
			continue
		}
		// An unbracketed IPv6 candidate is exactly the defect: SplitHostPort
		// above would have rejected it, so reaching here means it was bracketed.
		if ip.To4() == nil && candidate[0] != '[' {
			t.Errorf("candidate %q: IPv6 address is not bracketed", candidate)
		}
	}
}

// TestResolveDialCandidatesRejectsMissingPort keeps the failure at resolution
// time, where the address is still in hand, rather than inside a dial.
func TestResolveDialCandidatesRejectsMissingPort(t *testing.T) {
	t.Parallel()
	if _, err := resolveDialCandidates(context.Background(), "relay.example.com"); err == nil {
		t.Error("resolveDialCandidates without a port: got nil error, want failure")
	}
}
