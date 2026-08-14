package main

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Eyevinn/mp4ff/avc"
	"github.com/Eyevinn/mp4ff/mp4"
	"github.com/quic-go/quic-go/quicvarint"

	"github.com/floatdrop/moq-go/pkg/moqt/msf"
)

// Parameter sets taken off cdn.moq.pro/demo's bbb.hang — a real avc3
// stream's SPS and PPS, kept as literals because SetAVCDescriptor parses
// the SPS for the track dimensions and will not accept invented bytes.
var (
	realSPS = mustHex("6764001fac2484014016ec0440000003004000000c23c60c92")
	realPPS = mustHex("68ee32c8b0")
)

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

// annexB frames a set of NALUs with 4-byte start codes.
func annexB(nalus ...[]byte) []byte {
	var out []byte
	for _, n := range nalus {
		out = append(out, 0, 0, 0, 1)
		out = append(out, n...)
	}
	return out
}

// sampleOf converts an Annex-B access unit to the length-prefixed form the
// parser hands on, for the helpers that take a sample rather than an Object.
func sampleOf(annexB []byte) []byte {
	return avc.ConvertByteStreamToNaluSample(annexB)
}

// legacyObject encodes one Object the way the hang publisher does: a QUIC
// varint timestamp followed by the access unit. Encoded at the full eight
// bytes, which is the form the real stream uses and the one the wrong
// varint reader mis-decodes.
func legacyObject(ts uint64, au []byte) []byte {
	return append(quicvarint.AppendWithLen(nil, ts, 8), au...)
}

// TestParseLegacyObjectReadsAQUICVarint is the regression test for reading
// the timestamp with the wrong varint.
//
// MOQT's varint (draft-19 §1.4.1) is a leading-ones encoding and QUIC's is
// a two-bit length prefix. They overlap enough that MOQT's reader accepts
// these bytes and silently returns a different number — it took the 0xc0
// below as a three-byte encoding of zero, which stamped every frame 0 and
// left the reassembled file with no timeline at all. The header here is
// verbatim from cdn.moq.pro/demo.
func TestParseLegacyObjectReadsAQUICVarint(t *testing.T) {
	payload := mustHex("c000007499f300c9" + "0000000109f0")
	frame, err := parseLegacyObject(payload)
	if err != nil {
		t.Fatalf("parseLegacyObject: %v", err)
	}
	const want = 500799045833 // 0x7499f300c9
	if frame.Timestamp != want {
		t.Errorf("timestamp = %d, want %d", frame.Timestamp, want)
	}
	// The access unit comes back with its start codes already turned into
	// length fields, so a six-byte AUD reads as 00000002 09 f0.
	if !bytes.Equal(frame.Sample, mustHex("0000000209f0")) {
		t.Errorf("access unit = %x, want the bytes after the varint, length-prefixed", frame.Sample)
	}
}

// TestParseLegacyObjectDoesNotMutateTheObject pins the copy that keeps the
// Annex-B conversion off the recorded Object.
//
// avc.ConvertByteStreamToNaluSample rewrites four-byte start codes in place,
// and the Object it would rewrite is the one the delivery report still
// counts. The access unit here is what makes that corrupting rather than
// merely untidy: a one-byte NALU length-prefixes to 00 00 00 01, which is
// byte-identical to a start code, so a second conversion rewrites the length
// before it and mangles the sample.
func TestParseLegacyObjectDoesNotMutateTheObject(t *testing.T) {
	payload := legacyObject(1000, annexB([]byte{0x65}, []byte{0x41, 0x9a, 0x00}))
	before := bytes.Clone(payload)

	first, err := parseLegacyObject(payload)
	if err != nil {
		t.Fatalf("parseLegacyObject: %v", err)
	}
	if !bytes.Equal(payload, before) {
		t.Errorf("the Object was rewritten in place:\n before %x\n after  %x", before, payload)
	}

	// Parsing the same Object again must give the same sample; it does not
	// if the first pass consumed it.
	second, err := parseLegacyObject(payload)
	if err != nil {
		t.Fatalf("second parseLegacyObject: %v", err)
	}
	if !bytes.Equal(first.Sample, second.Sample) {
		t.Errorf("parsing twice gave different samples:\n first  %x\n second %x",
			first.Sample, second.Sample)
	}
}

func TestParseLegacyObjectRejectsAShortPayload(t *testing.T) {
	// 0xc0 promises eight bytes and only three are present.
	if _, err := parseLegacyObject([]byte{0xc0, 0x00, 0x00}); err == nil {
		t.Fatal("parseLegacyObject accepted a truncated timestamp")
	}
}

// TestWriteLegacyMediaProducesAPlayableFile drives the whole reassembly:
// Objects in, fragmented MP4 out, parsed back with the same sample count,
// timing and sync flags. The container has to be built from the parameter
// sets inside the bitstream, since a legacy track's catalog carries none.
func TestWriteLegacyMediaProducesAPlayableFile(t *testing.T) {
	const step = 41667 // µs, the 24 fps this packaging is used at
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	slice := []byte{0x41, 0x9a, 0x00, 0x01}

	arrivals := []arrival{
		{Group: 0, Object: 0, Payload: legacyObject(1000, annexB(realSPS, realPPS, idr))},
		{Group: 0, Object: 1, Payload: legacyObject(1000+step, annexB(slice))},
		{Group: 0, Object: 2, Payload: legacyObject(1000+2*step, annexB(slice))},
		{Group: 1, Object: 0, Payload: legacyObject(1000+3*step, annexB(realSPS, realPPS, idr))},
	}

	path := filepath.Join(t.TempDir(), "out.mp4")
	digest, err := writeLegacyMedia(path, arrivals)
	if err != nil {
		t.Fatalf("writeLegacyMedia: %v", err)
	}
	if digest == "" {
		t.Error("no digest returned for a file that was written")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	back, err := mp4.DecodeFile(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("the reassembled file does not parse: %v", err)
	}
	if !back.IsFragmented() {
		t.Fatal("the reassembled file is not fragmented")
	}

	trak := back.Init.Moov.Trak
	if trak.Mdia.Mdhd.Timescale != legacyTimescale {
		t.Errorf("timescale = %d, want %d so the µs timestamps need no rounding",
			trak.Mdia.Mdhd.Timescale, legacyTimescale)
	}
	// The dimensions come from the SPS, which is the only place they exist.
	if trak.Tkhd.Width>>16 != 1280 || trak.Tkhd.Height>>16 != 720 {
		t.Errorf("dimensions = %dx%d, want 1280x720 from the SPS",
			trak.Tkhd.Width>>16, trak.Tkhd.Height>>16)
	}
	if entry := trak.Mdia.Minf.Stbl.Stsd.AvcX; entry == nil || entry.Type() != "avc3" {
		t.Error("sample entry is not avc3, which is what a track with in-band parameter sets is")
	}

	samples := collectFragmentSamples(t, back)
	if len(samples) != len(arrivals) {
		t.Fatalf("wrote %d samples, want %d", len(samples), len(arrivals))
	}
	// Sync flags are asserted in TestLegacySampleFlagsMarksIDRs, not here:
	// with one sample per fragment they come back from the trun defaults
	// rather than from what was written, so a check here would pass either
	// way.
	if samples[0].Dur != step {
		t.Errorf("first sample duration = %d, want %d from the timestamp delta", samples[0].Dur, step)
	}
	// The first frame anchors the timeline, so decode times start at zero
	// however far into the broadcast the subscriber joined.
	if samples[0].DecodeTime != 0 {
		t.Errorf("first decode time = %d, want 0", samples[0].DecodeTime)
	}
	if samples[1].DecodeTime != step {
		t.Errorf("second decode time = %d, want %d", samples[1].DecodeTime, step)
	}
}

// collectFragmentSamples flattens every fragment's samples in order.
func collectFragmentSamples(t *testing.T, f *mp4.File) []mp4.FullSample {
	t.Helper()
	trex := f.Moov.Mvex.Trex
	var out []mp4.FullSample
	for _, seg := range f.Segments {
		for _, frag := range seg.Fragments {
			got, err := frag.GetFullSamples(trex)
			if err != nil {
				t.Fatalf("read fragment samples: %v", err)
			}
			out = append(out, got...)
		}
	}
	return out
}

// TestLegacySampleFlagsMarksIDRs tests the flag computation directly.
//
// Asserting it through the written file does not work: every fragment here
// holds one sample, so each is its own fragment's first sample, and the
// flags a reader gets back come from the trun/trex defaults rather than
// from what was set. The distinction only survives where it is made.
func TestLegacySampleFlagsMarksIDRs(t *testing.T) {
	idr := sampleOf(annexB([]byte{0x65, 0x88, 0x84, 0x00}))
	if flags := mp4.DecodeSampleFlags(legacySampleFlags(idr)); flags.SampleIsNonSync {
		t.Error("an access unit holding an IDR was not marked as a sync sample")
	}
	slice := sampleOf(annexB([]byte{0x41, 0x9a, 0x00, 0x01}))
	if flags := mp4.DecodeSampleFlags(legacySampleFlags(slice)); !flags.SampleIsNonSync {
		t.Error("a non-IDR access unit was marked as a sync sample")
	}
	// Parameter sets ahead of the IDR must not hide it — this is exactly
	// the shape a group-opening object has.
	withPS := sampleOf(annexB(realSPS, realPPS, []byte{0x65, 0x88, 0x84, 0x00}))
	if flags := mp4.DecodeSampleFlags(legacySampleFlags(withPS)); flags.SampleIsNonSync {
		t.Error("an IDR preceded by parameter sets was not marked as a sync sample")
	}
}

func TestWriteLegacyMediaNeedsParameterSets(t *testing.T) {
	// A stream joined mid-GOP, so no SPS/PPS has come round yet.
	arrivals := []arrival{
		{Group: 0, Object: 0, Payload: legacyObject(1000, annexB([]byte{0x41, 0x9a, 0x00}))},
	}
	_, err := writeLegacyMedia(filepath.Join(t.TempDir(), "out.mp4"), arrivals)
	if err == nil {
		t.Fatal("writeLegacyMedia built a file with no decoder configuration")
	}
}

func TestLegacySummaryDescribesTheStream(t *testing.T) {
	const step = 41667
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	slice := []byte{0x41, 0x9a, 0x00, 0x01}
	arrivals := []arrival{
		{Payload: legacyObject(0, annexB(realSPS, realPPS, idr))},
		{Payload: legacyObject(step, annexB(slice))},
		{Payload: legacyObject(2*step, annexB(slice))},
	}
	got := legacySummary(arrivals)
	for _, want := range []string{"3 frames", "1 keyframes", "24.00 fps"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q does not mention %q", got, want)
		}
	}
}

// TestParseBroadcastAcceptsALegacyTrackWithoutInitData covers the catalog
// half: a legacy track's parameter sets are in the bitstream, so there is
// no initRef to resolve and demanding one would reject every such stream.
func TestParseBroadcastAcceptsALegacyTrackWithoutInitData(t *testing.T) {
	live := true
	cat := msf.BeginBroadcast([]msf.Track{{
		Name:      "0.avc3",
		Packaging: legacyPackaging,
		IsLive:    &live,
		Role:      msf.RoleVideo,
		Codec:     "avc3.64001f",
	}}, testTime())

	got, err := parseBroadcast(cat, "bbb.hang", legacyPackaging)
	if err != nil {
		t.Fatalf("parseBroadcast: %v", err)
	}
	if got.Track.Name != "0.avc3" {
		t.Errorf("track = %q, want the legacy one", got.Track.Name)
	}
	if len(got.Init) != 0 {
		t.Errorf("init = %x, want none: a legacy track carries its own", got.Init)
	}
	if got.Namespace != "bbb.hang" {
		t.Errorf("namespace = %q, want the fallback", got.Namespace)
	}

	// And the CMAF reader must not silently pick it up.
	if _, err := parseBroadcast(cat, "bbb.hang", msf.PackagingCMAF); err == nil {
		t.Error("the CMAF path accepted a legacy track")
	}
}

// testTime keeps BeginBroadcast's generatedAt out of the assertions.
func testTime() time.Time { return time.Time{} }
