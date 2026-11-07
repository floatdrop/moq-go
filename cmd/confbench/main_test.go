package main

import (
	"slices"
	"strings"
	"testing"
)

func TestBitrateFlag(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]int{"1.5M": 1_500_000, "48k": 48_000, "2G": 2_000_000_000, "64000": 64_000} {
		var b bitrate
		if err := b.Set(in); err != nil || int(b) != want {
			t.Errorf("Set(%q) = %d, %v; want %d", in, b, err, want)
		}
	}
	for _, in := range []string{"", "M", "fast", "-1M", "0"} {
		var b bitrate
		if err := b.Set(in); err == nil {
			t.Errorf("Set(%q) = %d, want an error", in, b)
		}
	}
}

func TestParseSteps(t *testing.T) {
	t.Parallel()
	got, err := parseSteps("250, 500,1000")
	if err != nil || !slices.Equal(got, []int{250, 500, 1000}) {
		t.Errorf("parseSteps = %v, %v", got, err)
	}
	for _, in := range []string{"", "250,", "0", "many"} {
		if _, err := parseSteps(in); err == nil {
			t.Errorf("parseSteps(%q): want an error", in)
		}
	}
}

// The relay's exposition (cmd/relay) is summed per metric across labels.
func TestParseExposition(t *testing.T) {
	t.Parallel()
	m, err := parseExposition(strings.NewReader(`# HELP moqt_relay_objects_forwarded_total x
# TYPE moqt_relay_objects_forwarded_total counter
moqt_relay_objects_forwarded_total{track="other",leg="local",subgroup="0"} 7
moqt_relay_objects_forwarded_total{track="catalog",leg="local",subgroup="0"} 3
process_cpu_seconds_total 12.5
go_memstats_sys_bytes 1.2863752e+07
`))
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]float64{
		"moqt_relay_objects_forwarded_total": 10,
		"process_cpu_seconds_total":          12.5,
		"go_memstats_sys_bytes":              12863752,
	} {
		if m[name] != want {
			t.Errorf("%s = %v, want %v", name, m[name], want)
		}
	}
	if _, err := parseExposition(strings.NewReader("broken notanumber\n")); err == nil {
		t.Error("want an error for a non-numeric sample")
	}
}
