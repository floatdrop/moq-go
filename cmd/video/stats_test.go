package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// at builds an arrival at a fixed base time plus offset milliseconds, so
// the ordering assertions do not depend on a clock.
func at(group, object uint64, ms int) arrival {
	base := time.Unix(1_700_000_000, 0)
	return arrival{
		Group:  group,
		Object: object,
		Bytes:  10,
		At:     base.Add(time.Duration(ms) * time.Millisecond),
	}
}

func TestRecorderCountsObjectsThatArriveAfterALaterOne(t *testing.T) {
	rec := &recorder{}
	// Group 1 overtakes the tail of Group 0 — which is what two subgroup
	// streams read on their own goroutines can do, and what a player has
	// to survive.
	for _, a := range []arrival{
		at(0, 0, 0), at(0, 1, 10), at(1, 0, 20), at(0, 2, 30), at(1, 1, 40),
	} {
		rec.add(a)
	}
	if rec.outOfOrder != 1 {
		t.Errorf("outOfOrder = %d, want 1 (object 0/2 arrived after 1/0)", rec.outOfOrder)
	}

	// sorted() must undo the interleaving: it is what the reassembled file
	// is written from, so a wrong order here is a corrupt output file.
	sorted := rec.sorted()
	want := []arrival{at(0, 0, 0), at(0, 1, 10), at(0, 2, 30), at(1, 0, 20), at(1, 1, 40)}
	for i := range want {
		if sorted[i].Group != want[i].Group || sorted[i].Object != want[i].Object {
			t.Fatalf("sorted[%d] = %d/%d, want %d/%d",
				i, sorted[i].Group, sorted[i].Object, want[i].Group, want[i].Object)
		}
	}
}

func TestRecorderDoesNotCountInOrderArrivalsAsReordered(t *testing.T) {
	rec := &recorder{}
	for g := range uint64(3) {
		for o := range uint64(4) {
			rec.add(at(g, o, int(g*4+o)))
		}
	}
	if rec.outOfOrder != 0 {
		t.Errorf("outOfOrder = %d, want 0", rec.outOfOrder)
	}
	if rec.bytes != 12*10 {
		t.Errorf("bytes = %d, want %d", rec.bytes, 12*10)
	}
}

func TestGaps(t *testing.T) {
	tests := []struct {
		name        string
		arrivals    []arrival
		wantObjects uint64
		wantGroups  uint64
	}{{
		name:     "nothing missing",
		arrivals: []arrival{at(0, 0, 0), at(0, 1, 1), at(1, 0, 2), at(1, 1, 3)},
	}, {
		name:        "an object lost mid-group",
		arrivals:    []arrival{at(0, 0, 0), at(0, 2, 1)},
		wantObjects: 1,
	}, {
		name:       "a whole group lost",
		arrivals:   []arrival{at(0, 0, 0), at(2, 0, 1)},
		wantGroups: 1,
	}, {
		name: "a group joined after its start",
		// Object 0 and 1 of group 1 never arrived, which is loss even
		// though nothing between two received objects is missing.
		arrivals:    []arrival{at(0, 0, 0), at(1, 2, 1)},
		wantObjects: 2,
	}, {
		name: "joined mid-broadcast",
		// The subscriber's first object is 7/3. Everything before it was
		// never sent to this subscriber, and counting it as loss would
		// make every late join look like a delivery failure.
		arrivals: []arrival{at(7, 3, 0), at(7, 4, 1), at(8, 0, 2)},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objects, groups := gaps(tc.arrivals)
			if objects != tc.wantObjects {
				t.Errorf("missing objects = %d, want %d", objects, tc.wantObjects)
			}
			if groups != tc.wantGroups {
				t.Errorf("missing groups = %d, want %d", groups, tc.wantGroups)
			}
		})
	}
}

func TestWriteMediaConcatenatesInSendOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.mp4")
	init := []byte("INIT")
	// Deliberately out of arrival order: writeMedia is handed the sorted
	// view, so the file must come out in send order regardless.
	sorted := []arrival{
		{Group: 0, Object: 0, Payload: []byte("a")},
		{Group: 0, Object: 1, Payload: []byte("b")},
		{Group: 1, Object: 0, Payload: []byte("c")},
	}
	digest, err := writeMedia(path, init, sorted)
	if err != nil {
		t.Fatalf("writeMedia: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, []byte("INITabc")) {
		t.Errorf("file = %q, want %q", got, "INITabc")
	}
	// The digest is what the source is compared against, so it must cover
	// the header as well as the payloads.
	if digest != sha256Of("INITabc") {
		t.Errorf("digest = %s, want the digest of the whole file", digest)
	}
}

func TestWriteMediaSkipsWhenNoPathIsGiven(t *testing.T) {
	digest, err := writeMedia("", []byte("INIT"), []arrival{{Payload: []byte("a")}})
	if err != nil {
		t.Fatalf("writeMedia: %v", err)
	}
	if digest != "" {
		t.Errorf("digest = %q, want empty: with no file there is nothing to hash", digest)
	}
}

func TestReportComparesAgainstTheSourceDigest(t *testing.T) {
	rec := &recorder{}
	rec.add(at(0, 0, 0))
	rec.add(at(0, 1, 10))

	var buf bytes.Buffer
	rec.report(&buf, broadcast{Digest: "abc", Objects: 2, Bytes: 20}, "abc")
	if !strings.Contains(buf.String(), "MATCHES the source") {
		t.Errorf("matching digests not reported as a match:\n%s", buf.String())
	}

	buf.Reset()
	rec.report(&buf, broadcast{Digest: "abc", Objects: 2, Bytes: 20}, "def")
	if !strings.Contains(buf.String(), "DIFFERS from the source") {
		t.Errorf("differing digests not reported as a mismatch:\n%s", buf.String())
	}
}

func TestReportSaysSoWhenNothingArrived(t *testing.T) {
	var buf bytes.Buffer
	(&recorder{}).report(&buf, broadcast{}, "")
	if !strings.Contains(buf.String(), "no objects received") {
		t.Errorf("empty run not reported:\n%s", buf.String())
	}
}

func TestPercentile(t *testing.T) {
	sorted := []time.Duration{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, tc := range []struct {
		p    float64
		want time.Duration
	}{{0, 1}, {0.5, 5}, {0.9, 9}, {1, 10}} {
		if got := percentile(sorted, tc.p); got != tc.want {
			t.Errorf("percentile(%.2f) = %v, want %v", tc.p, got, tc.want)
		}
	}
}
