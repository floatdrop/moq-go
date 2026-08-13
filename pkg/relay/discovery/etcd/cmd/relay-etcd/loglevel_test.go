package main

import (
	"log/slog"
	"testing"
)

func TestParseLogLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"info":    slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		// Tolerated because these arrive from a ConfigMap or an env var,
		// where stray whitespace and capitals are ordinary. The padding is the
		// point of the case, so gocritic's suspicious-whitespace warning is
		// inverted here.
		//nolint:gocritic // mapKey: the whitespace is deliberate, see above
		"  INFO ": slog.LevelInfo,
		"Debug":   slog.LevelDebug,
	}
	for in, want := range cases {
		got, err := parseLogLevel(in)
		if err != nil {
			t.Errorf("parseLogLevel(%q) returned %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("parseLogLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

// Rejected rather than defaulted: a typo that quietly produced a different
// verbosity is found much later, while wondering where the logs went.
func TestParseLogLevelRejectsUnknown(t *testing.T) {
	for _, in := range []string{"verbose", "trace", "", "5", "infoo"} {
		if _, err := parseLogLevel(in); err == nil {
			t.Errorf("parseLogLevel(%q) = nil error, want a rejection", in)
		}
	}
}
