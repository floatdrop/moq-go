package relaynet

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// TestQUICConfigAppliesHooksInOrder pins the two parts of WithQUICConfig's
// contract that the end-to-end tests cannot separate: hooks see the relay
// defaults already populated (so a caller changing one knob keeps the rest and
// inherits any default added later), and repeated hooks compose in the order
// they were passed rather than the last one silently replacing the others.
func TestQUICConfigAppliesHooksInOrder(t *testing.T) {
	t.Parallel()

	var seen []time.Duration
	cfg := configure([]Option{
		WithQUICConfig(func(c *quic.Config) {
			seen = append(seen, c.MaxIdleTimeout)
			c.MaxIdleTimeout = time.Minute
		}),
		WithQUICConfig(func(c *quic.Config) {
			seen = append(seen, c.MaxIdleTimeout)
			c.KeepAlivePeriod = time.Second
		}),
	}).quic

	want := []time.Duration{30 * time.Second, time.Minute}
	if !slices.Equal(seen, want) {
		t.Errorf("hooks observed %v; want %v (defaults first, then the previous hook's edit)", seen, want)
	}
	if cfg.MaxIdleTimeout != time.Minute || cfg.KeepAlivePeriod != time.Second {
		t.Errorf("cfg = {idle %v, keepalive %v}; want {1m0s, 1s} (both hooks applied)",
			cfg.MaxIdleTimeout, cfg.KeepAlivePeriod)
	}
	// Untouched defaults survive: that is why the hook mutates rather than replaces.
	if !cfg.EnableDatagrams || !cfg.EnableStreamResetPartialDelivery {
		t.Error("hooks dropped the MOQT defaults (§11.2, §11.3.2) they never touched")
	}
}

// Listen refuses a handle count it would not honor, on every OS, rather than
// quietly running with one.
func TestUDPSendWritersValidation(t *testing.T) {
	t.Parallel()
	tlsCfg, err := TLSConfig("", "", DualALPNs)
	if err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{-1, maxUDPSendWriters + 1} {
		if l, err := Listen("127.0.0.1:0", "/moq", tlsCfg, nil, WithUDPSendWriters(count)); err == nil {
			l.Close()
			t.Errorf("%d UDP send writers accepted", count)
		} else if !strings.Contains(err.Error(), "UDP send writers") {
			t.Errorf("%d: wrong validation error: %v", count, err)
		}
	}
	for _, count := range []int{0, 1, 2, maxUDPSendWriters} {
		l, err := Listen("127.0.0.1:0", "/moq", tlsCfg, nil, WithUDPSendWriters(count))
		if err != nil {
			t.Errorf("%d UDP send writers refused: %v", count, err)
			continue
		}
		l.Close()
	}
}
