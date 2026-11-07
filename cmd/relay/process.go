package main

import (
	"fmt"
	"io"
	"net/http"
	"runtime/metrics"
)

// withProcessMetrics serves next's exposition followed by the relay process's
// own figures, under the names Prometheus's client libraries give them, so a
// dashboard — or a capacity run (cmd/confbench -relay-metrics) — reads the
// relay's CPU, memory and goroutines beside its delivery counters.
func withProcessMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		writeProcessMetrics(w)
	})
}

func writeProcessMetrics(w io.Writer) {
	samples := []metrics.Sample{
		{Name: "/sched/goroutines:goroutines"},
		{Name: "/memory/classes/total:bytes"},
	}
	metrics.Read(samples)
	if cpu, ok := processCPU(); ok {
		writeSample(w, "process_cpu_seconds_total", "counter",
			"Total user and system CPU time spent in seconds.", cpu.Seconds())
	}
	writeSample(w, "go_goroutines", "gauge",
		"Number of goroutines that currently exist.", float64(samples[0].Value.Uint64()))
	writeSample(w, "go_memstats_sys_bytes", "gauge",
		"Number of bytes obtained from system.", float64(samples[1].Value.Uint64()))
}

func writeSample(w io.Writer, name, kind, help string, v float64) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %g\n", name, help, name, kind, name, v)
}
