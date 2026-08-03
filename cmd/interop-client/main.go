// Command interop-client is a MoQT test client that drives the
// moq-interop-runner test cases against a relay, exercising this repository's
// session library from the client side (the inverse of cmd/relay, which is
// tested *by* third-party clients).
//
// It implements the runner's client contract
// (moq-interop-runner/docs/TEST-CLIENT-INTERFACE.md): RELAY_URL / TESTCASE /
// TLS_DISABLE_VERIFY / VERBOSE environment variables (overridable by flags),
// TAP version 14 on stdout, and exit codes 0 (all passed) / 1 (a failure) /
// 127 (unknown test).
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
)

// Canonical test identifiers and the shared track used by the multi-connection
// cases, matching the runner's TEST-CASES.md.
const (
	testNamespace = "moq-test/interop"
	testTrack     = "test-track"
)

type scenario struct {
	name string
	run  func(ctx context.Context, h *harness) error
}

// scenarios is the ordered list reported by --list and run when no specific
// TESTCASE is selected.
var scenarios = []scenario{
	{"setup-only", testSetupOnly},
	{"announce-only", testAnnounceOnly},
	{"publish-namespace-done", testPublishNamespaceDone},
	{"subscribe-error", testSubscribeError},
	{"announce-subscribe", testAnnounceSubscribe},
	{"subscribe-before-announce", testSubscribeBeforeAnnounce},
}

// perTestTimeout bounds each scenario; the multi-connection discovery cases get
// a longer budget than the single round-trips.
func perTestTimeout(name string) time.Duration {
	switch name {
	case "announce-subscribe", "subscribe-before-announce":
		return 8 * time.Second
	default:
		return 5 * time.Second
	}
}

func main() {
	relay := envOr("RELAY_URL", "https://localhost:4443")
	test := os.Getenv("TESTCASE")
	verbose := envBool("VERBOSE")
	insecure := envBool("TLS_DISABLE_VERIFY")
	var list bool

	// Flags override the environment defaults (per the interface contract).
	flag.StringVar(&relay, "relay", relay, "relay URL (moqt:// for raw QUIC, https:// for WebTransport)")
	flag.StringVar(&relay, "r", relay, "relay URL (shorthand)")
	flag.StringVar(&test, "test", test, "run a specific test (default: all)")
	flag.StringVar(&test, "t", test, "test (shorthand)")
	flag.BoolVar(&list, "list", false, "list available tests and exit")
	flag.BoolVar(&list, "l", false, "list (shorthand)")
	flag.BoolVar(&verbose, "verbose", verbose, "verbose diagnostics to stderr")
	flag.BoolVar(&verbose, "v", verbose, "verbose (shorthand)")
	flag.BoolVar(&insecure, "tls-disable-verify", insecure, "skip TLS certificate verification")
	flag.Parse()

	if list {
		for _, s := range scenarios {
			fmt.Fprintln(os.Stdout, s.name)
		}
		return
	}

	// Logs go to stderr so they never corrupt the TAP stream on stdout.
	level := slog.LevelError
	if verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(tint.NewTextHandler(os.Stderr, &tint.Options{
		Level:      level,
		TimeFormat: time.TimeOnly,
	})))

	selected := scenarios
	if test != "" {
		s, ok := findScenario(test)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown test %q\n", test)
			os.Exit(127)
		}
		selected = []scenario{s}
	}

	h := &harness{relayURL: relay, insecure: insecure}

	out := os.Stdout
	fmt.Fprintln(out, "TAP version 14")
	fmt.Fprintf(out, "# moq interop-client\n")
	fmt.Fprintf(out, "# Relay: %s\n", relay)
	fmt.Fprintf(out, "1..%d\n", len(selected))

	failed := false
	for i, s := range selected {
		num := i + 1
		ctx, cancel := context.WithTimeout(context.Background(), perTestTimeout(s.name))
		start := time.Now()
		err := s.run(ctx, h)
		dur := time.Since(start).Milliseconds()
		cancel()

		if err != nil {
			failed = true
			fmt.Fprintf(out, "not ok %d - %s\n", num, s.name)
			writeDiag(out, dur, err.Error())
		} else {
			fmt.Fprintf(out, "ok %d - %s\n", num, s.name)
			writeDiag(out, dur, "")
		}
	}

	if failed {
		os.Exit(1)
	}
}

// writeDiag emits a TAP YAML diagnostic block with the duration and, on
// failure, the message.
func writeDiag(w io.Writer, durMs int64, message string) {
	fmt.Fprintln(w, "  ---")
	fmt.Fprintf(w, "  duration_ms: %d\n", durMs)
	if message != "" {
		fmt.Fprintf(w, "  message: %q\n", message)
	}
	fmt.Fprintln(w, "  ...")
}

func findScenario(name string) (scenario, bool) {
	for _, s := range scenarios {
		if s.name == name {
			return s, true
		}
	}
	return scenario{}, false
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envBool treats "1" and "true" (the values the runner documents) as set.
func envBool(key string) bool {
	switch os.Getenv(key) {
	case "1", "true", "TRUE", "True":
		return true
	default:
		return false
	}
}
