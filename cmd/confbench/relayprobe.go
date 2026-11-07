package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/floatdrop/moq-go/internal/conference"
)

// relayProbe reads the relay's side of a window: its metrics as the window
// opens and closes, and optionally a CPU profile covering it.
type relayProbe struct {
	metricsURL, pprofURL, profilePath string
	window                            time.Duration

	opened, closed     map[string]float64
	openedAt, closedAt time.Time
	err                error
	profile            chan error
}

func (p *relayProbe) onWindow(open bool) {
	if open && p.pprofURL != "" {
		p.profile = make(chan error, 1)
		go func() { p.profile <- p.fetchProfile() }()
	}
	if p.metricsURL == "" || p.err != nil {
		return
	}
	m, err := scrape(p.metricsURL)
	if err != nil {
		p.err = err
		return
	}
	if open {
		p.opened, p.openedAt = m, time.Now()
	} else {
		p.closed, p.closedAt = m, time.Now()
	}
}

// fetchProfile saves a CPU profile of one window to profilePath.
func (p *relayProbe) fetchProfile() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.window+30*time.Second)
	defer cancel()
	u, err := url.JoinPath(p.pprofURL, "debug/pprof/profile")
	if err != nil {
		return err
	}
	u += "?" + url.Values{"seconds": {strconv.Itoa(max(1, int(p.window.Seconds())))}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", u, resp.Status)
	}
	f, err := os.Create(p.profilePath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// relayStats is the relay's side of a window.
type relayStats struct {
	measured bool
	err      error
	// cpu is in cores; goroutines and sysBytes are as the window closed.
	cpu                  float64
	goroutines, sysBytes float64
	forwarded, dropped   float64
	profile              string
	profileErr           error
}

func (p *relayProbe) finish() relayStats {
	var s relayStats
	if p.profile != nil {
		if s.profileErr = <-p.profile; s.profileErr == nil {
			s.profile = p.profilePath
		}
	}
	switch {
	case p.metricsURL == "":
		return s
	case p.err != nil:
		s.err = p.err
		return s
	}
	s.measured = true
	delta := func(name string) float64 { return p.closed[name] - p.opened[name] }
	s.cpu = delta("process_cpu_seconds_total") / p.closedAt.Sub(p.openedAt).Seconds()
	s.goroutines = p.closed["go_goroutines"]
	s.sysBytes = p.closed["go_memstats_sys_bytes"]
	s.forwarded = delta("moqt_relay_objects_forwarded_total")
	s.dropped = delta("moqt_relay_objects_dropped_total")
	return s
}

func (s relayStats) write(w io.Writer) {
	switch {
	case s.err != nil:
		fmt.Fprintf(w, "relay         metrics unavailable: %v\n", s.err)
	case s.measured:
		fmt.Fprintf(w, "relay         cpu %.2f cores  memory %.0f MiB  goroutines %.0f  forwarded %.0f  dropped %.0f\n",
			s.cpu, s.sysBytes/(1<<20), s.goroutines, s.forwarded, s.dropped)
	}
	switch {
	case s.profileErr != nil:
		fmt.Fprintf(w, "relay profile failed: %v\n", s.profileErr)
	case s.profile != "":
		fmt.Fprintf(w, "relay profile %s (go tool pprof -top %s)\n", s.profile, s.profile)
	}
}

// scrape sums every sample of each metric in a Prometheus text exposition.
func scrape(u string) (map[string]float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", u, resp.Status)
	}
	return parseExposition(resp.Body)
}

func parseExposition(r io.Reader) (map[string]float64, error) {
	m := map[string]float64{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, _, _ := strings.Cut(line, "{")
		name, _, _ = strings.Cut(name, " ")
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			return nil, fmt.Errorf("metrics line %q: %w", line, err)
		}
		m[name] += v
	}
	return m, sc.Err()
}

// row is one ramp step.
type row struct {
	rep   *conference.Report
	relay relayStats
}

func writeSummary(w io.Writer, rows []row) {
	fmt.Fprintf(w, "%-13s %-11s %-11s %-10s %-10s %-10s %-12s %s\n",
		"participants", "served", "audio loss", "audio p99", "video p99", "relay cpu", "harness cpu", "pacing p99")
	for _, r := range rows {
		relayCPU := "-"
		if r.relay.measured {
			relayCPU = fmt.Sprintf("%.2f", r.relay.cpu)
		}
		fmt.Fprintf(w, "%-13d %-11s %-11s %-10s %-10s %-10s %-12.2f %s\n",
			r.rep.Participants, fmt.Sprintf("%d/%d", r.rep.Served, r.rep.Participants),
			fmt.Sprintf("%.3f%%", 100*r.rep.Audio.Loss),
			r.rep.Audio.Latency.P99.Round(100*time.Microsecond), r.rep.Video.Latency.P99.Round(100*time.Microsecond),
			relayCPU, r.rep.ProcessCPU, r.rep.Pacing.P99.Round(100*time.Microsecond))
	}
}
