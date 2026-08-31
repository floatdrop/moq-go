package relaynet

import (
	"slices"
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
	cfg := quicConfig([]Option{
		WithQUICConfig(func(c *quic.Config) {
			seen = append(seen, c.MaxIdleTimeout)
			c.MaxIdleTimeout = time.Minute
		}),
		WithQUICConfig(func(c *quic.Config) {
			seen = append(seen, c.MaxIdleTimeout)
			c.KeepAlivePeriod = time.Second
		}),
	})

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
		t.Error("hooks dropped the MOQT defaults (§11.3, §11.4.3) they never touched")
	}
}
