package main

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"github.com/Eyevinn/mp4ff/mp4"

	"github.com/floatdrop/moq-go/pkg/moqt/msf"
)

// TestLiveWriterDropsWhatIsAlreadyLate is the ordering rule a pipe needs.
//
// Groups are read on their own goroutines, so arrivals interleave, and a
// player handed a fragment behind one it has already been given cannot do
// anything sensible with it. Dropping is what every player does with late
// data; the point is that the delivery report still counts it, because
// arriving late and not arriving are different failures.
func TestLiveWriterDropsWhatIsAlreadyLate(t *testing.T) {
	var buf bytes.Buffer
	live := newLiveWriter(mediaStdout, msf.PackagingCMAF, &buf, []byte("INIT"))
	if live == nil {
		t.Fatal("newLiveWriter returned nil for -out -")
	}

	live.add(arrival{Group: 0, Object: 0, Payload: []byte("a")})
	live.add(arrival{Group: 1, Object: 0, Payload: []byte("c")})
	live.add(arrival{Group: 0, Object: 1, Payload: []byte("b")}) // overtaken
	live.add(arrival{Group: 1, Object: 1, Payload: []byte("d")})
	if err := live.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got := buf.String(); got != "INITacd" {
		t.Errorf("stream = %q, want %q: the late object must not be written", got, "INITacd")
	}
	if late, stalled := live.dropped(); late != 1 || stalled != 0 {
		t.Errorf("dropped = (late %d, stalled %d), want (1, 0)", late, stalled)
	}
}

func TestNewLiveWriterOnlyForStdout(t *testing.T) {
	if newLiveWriter("/tmp/out.mp4", msf.PackagingCMAF, &bytes.Buffer{}, nil) != nil {
		t.Error("a file destination must not stream: it is written once, at the end")
	}
	if newLiveWriter("", msf.PackagingCMAF, &bytes.Buffer{}, nil) != nil {
		t.Error("no destination must not stream")
	}
}

// TestLiveWriterHandlesNoWriter covers the CMAF-to-a-file path calling into
// a nil writer, which is how subscribe avoids branching at every Object.
func TestLiveWriterHandlesNoWriter(t *testing.T) {
	var live *liveWriter
	live.add(arrival{Group: 0, Object: 0})
	if err := live.close(); err != nil {
		t.Errorf("close on a nil writer: %v", err)
	}
	if late, stalled := live.dropped(); late != 0 || stalled != 0 {
		t.Error("a nil writer cannot have dropped anything")
	}
}

// TestLegacyEmitterWaitsForAKeyframe covers the two things a legacy stream
// cannot start without: a decodable picture, and the parameter sets that
// configure the decoder — both of which arrive in the same access unit.
func TestLegacyEmitterWaitsForAKeyframe(t *testing.T) {
	const step = 41667
	slice := annexB([]byte{0x41, 0x9a, 0x00, 0x01})
	key := annexB(realSPS, realPPS, []byte{0x65, 0x88, 0x84, 0x00})

	var buf bytes.Buffer
	live := newLiveWriter(mediaStdout, legacyPackaging, &buf, nil)

	// Mid-GOP, which is where a live join lands: these reference a picture
	// that was never sent. Then a non-keyframe that does carry parameter
	// sets — enough to build a header, which is a different question from
	// whether there is a decodable picture to put after it, and only the
	// keyframe check separates the two. None of the three may be streamed.
	configured := annexB(realSPS, realPPS, []byte{0x41, 0x9a, 0x00, 0x01})
	live.add(arrival{Group: 0, Object: 0, Payload: legacyObject(0, slice)})
	live.add(arrival{Group: 0, Object: 1, Payload: legacyObject(step, slice)})
	live.add(arrival{Group: 0, Object: 2, Payload: legacyObject(2*step, configured)})

	live.add(arrival{Group: 1, Object: 0, Payload: legacyObject(3*step, key)})
	live.add(arrival{Group: 1, Object: 1, Payload: legacyObject(4*step, slice)})

	// Asserted after close, never during: writing happens on its own
	// goroutine, so the buffer mid-run says only how far it has got.
	if err := live.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("nothing was streamed at all")
	}

	back, err := mp4.DecodeFile(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("the streamed bytes do not parse as a file: %v", err)
	}
	if !back.IsFragmented() {
		t.Fatal("the streamed bytes are not a fragmented file")
	}
	samples := collectFragmentSamples(t, back)
	if len(samples) != 2 {
		t.Fatalf("streamed %d samples, want 2: only the keyframe and what followed it, "+
			"never the three frames ahead of it", len(samples))
	}
	if samples[0].Dur != step {
		t.Errorf("first duration = %d, want %d from the timestamp delta", samples[0].Dur, step)
	}
}

// TestLegacyEmitterSkipsAccessUnitsWithNoPicture covers the objects hang
// puts on the track that are not video — written out, they become samples
// a decoder rejects, which in this tool reads as corruption.
func TestLegacyEmitterSkipsAccessUnitsWithNoPicture(t *testing.T) {
	const step = 41667
	key := annexB(realSPS, realPPS, []byte{0x65, 0x88, 0x84, 0x00})
	// NALU type 23 is reserved and carries no slice; one Object in a few
	// hundred on bbb.hang is exactly this.
	reserved := annexB([]byte{0x17, 0x00, 0x01})

	var buf bytes.Buffer
	live := newLiveWriter(mediaStdout, legacyPackaging, &buf, nil)
	live.add(arrival{Group: 0, Object: 0, Payload: legacyObject(0, key)})
	live.add(arrival{Group: 0, Object: 1, Payload: legacyObject(step, reserved)})
	live.add(arrival{Group: 0, Object: 2, Payload: legacyObject(2*step, key)})
	if err := live.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	back, err := mp4.DecodeFile(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	samples := collectFragmentSamples(t, back)
	if len(samples) != 2 {
		t.Fatalf("streamed %d samples, want 2: the pictureless one must be skipped", len(samples))
	}
	for i, s := range samples {
		if !hasPicture(s.Data) {
			t.Errorf("sample %d carries no picture", i)
		}
	}
}

// TestFinishRunAcceptsAWriterThatWritesNothing covers the wiring for a live
// run, whose media has already gone out frame by frame. finishRun calls the
// writer unconditionally, so a run that had nothing left to write used to
// hand it nil and panic on the way out — after the stream itself was
// complete, which made it look like a fault in the media rather than in the
// wind-down.
func TestFinishRunAcceptsAWriterThatWritesNothing(t *testing.T) {
	rec := &recorder{}
	rec.add(at(0, 0, 0))
	var readers sync.WaitGroup
	var buf bytes.Buffer

	sorted, err := finishRun(t.Context(), rec, &readers, "", broadcast{}, noMedia, nil, &buf)
	if err != nil {
		t.Fatalf("finishRun: %v", err)
	}
	if len(sorted) != 1 {
		t.Errorf("reported %d objects, want 1", len(sorted))
	}
	if !strings.Contains(buf.String(), "delivery report") {
		t.Errorf("no report was written:\n%s", buf.String())
	}
	// No file, so no digest line to compare against a source.
	if strings.Contains(buf.String(), "digest") {
		t.Errorf("a run that wrote no file reported a digest:\n%s", buf.String())
	}
}
