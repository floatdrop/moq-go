package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// baseArgs is a runnable configuration: an ephemeral MOQT port, no health
// endpoint, and metrics off.
//
// Metrics stay off in every test here because newPromMetrics and
// registerBuildInfo both write to prometheus.DefaultRegisterer, which panics on
// a duplicate registration — so a metrics-enabled run is a once-per-process
// affair and cannot be a table entry. metrics.go's own collectors are covered
// by metrics_test.go against a private registry.
func baseArgs(etcdEndpoint string, extra ...string) []string {
	return append([]string{
		"-addr", "127.0.0.1:0",
		"-etcd-endpoints", etcdEndpoint,
		"-metrics=false",
	}, extra...)
}

// TestRun_RejectsBadConfiguration covers the argument and setup failures that
// previously called os.Exit, which is what made them untestable.
//
// Each one is a misconfiguration an operator can actually make from a
// Deployment manifest, and the thing being pinned is that the relay refuses to
// start and says why — rather than starting in a state where, say, every health
// probe 404s while the startup log cheerfully reports the endpoint as
// configured.
func TestRun_RejectsBadConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantMsg string
	}{
		{
			name:    "unparseable log level",
			args:    baseArgs("127.0.0.1:1", "-log-level", "chatty"),
			wantMsg: "chatty",
		},
		{
			name:    "unknown flag",
			args:    baseArgs("127.0.0.1:1", "-no-such-flag"),
			wantMsg: "no-such-flag",
		},
		{
			// r.URL.Path always begins with "/", so a path that does not would
			// 404 every probe.
			name:    "health path without a leading slash",
			args:    baseArgs("127.0.0.1:1", "-health-addr", "127.0.0.1:0", "-health-path", "healthz"),
			wantMsg: "must begin with /",
		},
		{
			name:    "unusable MOQT listen address",
			args:    []string{"-addr", "256.256.256.256:4433", "-metrics=false"},
			wantMsg: "listen",
		},
		{
			name:    "unusable health address",
			args:    baseArgs("127.0.0.1:1", "-health-addr", "256.256.256.256:8080"),
			wantMsg: "listen health",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			err := run(ctx, tt.args, io.Discard)
			if err == nil {
				t.Fatal("run accepted a configuration it cannot serve")
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error %q does not mention %q, so an operator cannot tell "+
					"which setting was wrong", err, tt.wantMsg)
			}
		})
	}
}

// TestSplitEndpoints covers the comma-separated -etcd-endpoints parsing.
func TestSplitEndpoints(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"127.0.0.1:2379", []string{"127.0.0.1:2379"}},
		{"a:1,b:2,c:3", []string{"a:1", "b:2", "c:3"}},
		{" a:1 , b:2 ", []string{"a:1", "b:2"}},
		{"", nil},
		{",", nil},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%q", tt.in), func(t *testing.T) {
			got := splitEndpoints(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("splitEndpoints(%q) = %v, want %v", tt.in, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("element %d = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestBuildInfo covers the version banner logged before any fatal setup, so a
// bug report about a relay that died on TLS still identifies which binary died.
// Under `go test` the module is built without VCS stamping, so the point is
// that it degrades to placeholders rather than panicking or returning blanks.
func TestBuildInfo(t *testing.T) {
	version, commit, commitTime, dirty := buildInfo()
	if version == "" || commit == "" || commitTime == "" {
		t.Errorf("buildInfo returned a blank field: version=%q commit=%q time=%q",
			version, commit, commitTime)
	}
	_ = dirty
	if errors.Is(nil, nil) && strings.TrimSpace(version) != version {
		t.Errorf("version %q has surrounding whitespace", version)
	}
}
