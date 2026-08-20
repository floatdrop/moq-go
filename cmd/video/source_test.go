package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/Eyevinn/mp4ff/mp4"
)

// syncFlags and deltaFlags are the trun sample flags for a sync sample and
// for one that depends on others.
//
// deltaFlags deliberately says nothing beyond "not a sync sample". A
// progressive file expresses its sample dependencies in stss and sdtp, and
// with no sdtp box there is nothing to recover a SampleDependsOn from — so
// a delta frame carrying one would make the two container shapes disagree
// for a reason that is about the fixture, not about the code under test.
var (
	syncFlags  = mp4.SampleFlags{SampleDependsOn: 2}.Encode()
	deltaFlags = mp4.SampleFlags{SampleIsNonSync: true}.Encode()
)

// synthSamples builds a run of samples whose sync pattern is given by
// pattern: 'I' for a sync sample, 'P' for one that is not, 'L' for a
// leading picture — one presented before the access point it follows in
// decode order, which is what an open GOP produces.
func synthSamples(pattern string) []mp4.FullSample {
	const dur = 3000
	samples := make([]mp4.FullSample, 0, len(pattern))
	for i, kind := range pattern {
		flags := deltaFlags
		if kind == 'I' {
			flags = syncFlags
		}
		// A leading picture presents two frames before it decodes, which
		// puts it ahead of the access point at the head of its group.
		var cto int32
		if kind == 'L' {
			cto = -2 * dur
		}
		samples = append(samples, mp4.FullSample{
			Flags: flags, Dur: dur, Size: 4, CompositionTimeOffset: cto,
			DecodeTime: uint64(i * dur),
			Data:       []byte{byte(i), 0, 0, 0},
		})
	}
	return samples
}

// groupsOf collapses chunks into the (Group, Object) coordinates they were
// assigned, as one string per Group.
func groupsOf(chunks []chunk) [][]uint64 {
	var out [][]uint64
	for _, c := range chunks {
		for uint64(len(out)) <= c.Group {
			out = append(out, nil)
		}
		out[c.Group] = append(out[c.Group], c.Object)
	}
	return out
}

func TestChunkSamplesStartsAGroupAtEverySyncSample(t *testing.T) {
	chunks, groups, err := chunkSamples(synthSamples("IPPIPPIP"), 1, 0)
	if err != nil {
		t.Fatalf("chunkSamples: %v", err)
	}
	if groups != 3 {
		t.Errorf("groups = %d, want 3", groups)
	}
	got := groupsOf(chunks)
	want := [][]uint64{{0, 1, 2}, {0, 1, 2}, {0, 1}}
	if len(got) != len(want) {
		t.Fatalf("got %d groups, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			t.Errorf("group %d objects = %v, want %v", i, got[i], want[i])
		}
	}
	// CMSF §3.4: every Group begins with a stream access point.
	for i, c := range chunks {
		if c.Object == 0 && !c.Sync {
			t.Errorf("chunk %d starts group %d without a sync sample", i, c.Group)
		}
	}
}

func TestChunkSamplesHonoursMinGroupObjects(t *testing.T) {
	// Sync samples every three frames, but a Group may not close below six
	// Objects — so only every second sync sample starts one, and the ones
	// in between stay mid-Group.
	chunks, groups, err := chunkSamples(synthSamples("IPPIPPIPPIPP"), 1, 6)
	if err != nil {
		t.Fatalf("chunkSamples: %v", err)
	}
	if groups != 2 {
		t.Fatalf("groups = %d, want 2", groups)
	}
	got := groupsOf(chunks)
	for i, objects := range got {
		if len(objects) != 6 {
			t.Errorf("group %d holds %d objects, want 6", i, len(objects))
		}
	}
	// The mid-Group sync sample is still a sync sample; it just does not
	// open a Group.
	if !chunks[3].Sync || chunks[3].Object != 3 {
		t.Errorf("chunk 3 = {sync:%v object:%d}, want a mid-group sync sample at object 3",
			chunks[3].Sync, chunks[3].Object)
	}
}

func TestChunkSamplesDropsFramesBeforeTheFirstSyncSample(t *testing.T) {
	chunks, groups, err := chunkSamples(synthSamples("PPIPP"), 1, 0)
	if err != nil {
		t.Fatalf("chunkSamples: %v", err)
	}
	if groups != 1 {
		t.Errorf("groups = %d, want 1", groups)
	}
	if len(chunks) != 3 {
		t.Fatalf("kept %d chunks, want 3 (the two leading delta frames dropped)", len(chunks))
	}
	if !chunks[0].Sync {
		t.Error("the first kept chunk is not a sync sample")
	}
	// The dropped frames are frames 0 and 1, so the first kept one carries
	// the third frame's payload.
	if chunks[0].Data == nil || !bytes.Contains(chunks[0].Data, []byte{2, 0, 0, 0}) {
		t.Error("the first kept chunk does not carry the third frame's payload")
	}
}

func TestChunkSamplesRejectsATrackWithNoSyncSample(t *testing.T) {
	if _, _, err := chunkSamples(synthSamples("PPPP"), 1, 0); err == nil {
		t.Fatal("chunkSamples accepted a track with no sync sample to open a Group with")
	}
}

// TestHasLeadingPictures is what stops a perfect delivery report from
// coexisting silently with a picture that breaks up at every Group
// boundary: an open-GOP input has to be noticed and said out loud, since
// no catalog field can express it.
func TestHasLeadingPictures(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    bool
	}{
		{"closed GOP", "IPPIPP", false},
		{"open GOP, every group", "ILPILP", true},
		{"open GOP, one group only", "IPPILP", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunks, _, err := chunkSamples(synthSamples(tc.pattern), 1, 0)
			if err != nil {
				t.Fatalf("chunkSamples: %v", err)
			}
			if got := hasLeadingPictures(chunks); got != tc.want {
				t.Errorf("hasLeadingPictures = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestOpenSourceSurfacesLeadingPictures checks the detection survives a
// round trip through a real container, since it reads composition times
// that ctts has to have carried.
func TestOpenSourceSurfacesLeadingPictures(t *testing.T) {
	open, err := openSource(writeSynthFile(t, "ILPILP", true), 0)
	if err != nil {
		t.Fatalf("openSource: %v", err)
	}
	if !open.LeadingPictures {
		t.Error("open-GOP file not reported as having leading pictures")
	}
	closed, err := openSource(writeSynthFile(t, "IPPIPP", true), 0)
	if err != nil {
		t.Fatalf("openSource: %v", err)
	}
	if closed.LeadingPictures {
		t.Error("closed-GOP file reported as having leading pictures")
	}
}

func TestOpenSourceReadsAFragmentedFile(t *testing.T) {
	path := writeSynthFile(t, "IPPIPP", true)

	src, err := openSource(path, 0)
	if err != nil {
		t.Fatalf("openSource: %v", err)
	}
	if len(src.Chunks) != 6 {
		t.Errorf("objects = %d, want 6", len(src.Chunks))
	}
	if src.Groups != 2 {
		t.Errorf("groups = %d, want 2", src.Groups)
	}
	if src.Width != 320 || src.Height != 180 {
		t.Errorf("dimensions = %dx%d, want 320x180", src.Width, src.Height)
	}
	assertReassembles(t, src)
}

func TestOpenSourceReadsAProgressiveFile(t *testing.T) {
	path := writeSynthFile(t, "IPPIPP", false)

	src, err := openSource(path, 0)
	if err != nil {
		t.Fatalf("openSource: %v", err)
	}
	if len(src.Chunks) != 6 {
		t.Errorf("objects = %d, want 6", len(src.Chunks))
	}
	if src.Groups != 2 {
		t.Errorf("groups = %d, want 2", src.Groups)
	}
	assertReassembles(t, src)
}

// TestOpenSourceAgreesAcrossContainerShapes is the check that the two
// readers are two ways of reading the same media rather than two
// behaviours: the same frames, packaged progressively and fragmented,
// must publish as the same Objects.
func TestOpenSourceAgreesAcrossContainerShapes(t *testing.T) {
	progressive, err := openSource(writeSynthFile(t, "IPPIPP", false), 0)
	if err != nil {
		t.Fatalf("openSource progressive: %v", err)
	}
	fragmented, err := openSource(writeSynthFile(t, "IPPIPP", true), 0)
	if err != nil {
		t.Fatalf("openSource fragmented: %v", err)
	}
	if progressive.SHA256 != fragmented.SHA256 {
		t.Errorf("digest differs between container shapes:\n progressive %s\n fragmented  %s",
			progressive.SHA256, fragmented.SHA256)
	}
}

// assertReassembles checks that the header and Objects a subscriber would
// be given parse back as a fragmented file holding every sample.
func assertReassembles(t *testing.T, src *source) {
	t.Helper()
	var buf bytes.Buffer
	buf.Write(src.Init)
	for i := range src.Chunks {
		buf.Write(src.Chunks[i].Data)
	}
	back, err := mp4.DecodeFile(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("reassembled media does not parse: %v", err)
	}
	if !back.IsFragmented() {
		t.Fatal("reassembled media is not a fragmented file")
	}
	var samples int
	for _, seg := range back.Segments {
		samples += len(seg.Fragments)
	}
	if samples != len(src.Chunks) {
		t.Errorf("reassembled media holds %d fragments, want %d", samples, len(src.Chunks))
	}
}

// writeSynthFile builds a single-track AVC file matching pattern and
// returns its path. When fragmented, the samples are written as movie
// fragments; otherwise as one mdat with sample tables describing it.
func writeSynthFile(t *testing.T, pattern string, fragmented bool) string {
	t.Helper()
	samples := synthSamples(pattern)
	path := filepath.Join(t.TempDir(), "clip.mp4")

	var buf bytes.Buffer
	if fragmented {
		writeFragmented(t, &buf, samples)
	} else {
		writeProgressive(t, &buf, samples)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// writeFragmented emits ftyp+moov followed by one movie fragment per
// sample, which is what a chunked CMAF file looks like.
func writeFragmented(t *testing.T, w *bytes.Buffer, samples []mp4.FullSample) {
	t.Helper()
	init := synthInit(t)
	if err := init.Encode(w); err != nil {
		t.Fatalf("encode init: %v", err)
	}
	for i := range samples {
		frag, err := mp4.CreateFragment(uint32(i+1), init.Moov.Trak.Tkhd.TrackID)
		if err != nil {
			t.Fatalf("create fragment: %v", err)
		}
		frag.AddFullSample(samples[i])
		if err := frag.Encode(w); err != nil {
			t.Fatalf("encode fragment: %v", err)
		}
	}
}

// writeProgressive emits ftyp+moov+mdat with sample tables pointing into
// the mdat, which is what a plain recorded file looks like.
func writeProgressive(t *testing.T, w *bytes.Buffer, samples []mp4.FullSample) {
	t.Helper()
	init := synthInit(t)
	trak := init.Moov.Trak
	// mvex signals a fragmented asset, and this file is not one.
	init.Moov.Children = slices.DeleteFunc(init.Moov.Children,
		func(b mp4.Box) bool { return b.Type() == "mvex" })
	init.Moov.Mvex = nil

	stbl := trak.Mdia.Minf.Stbl
	stbl.Stts = &mp4.SttsBox{}
	stbl.Stsz = &mp4.StszBox{SampleNumber: uint32(len(samples))}
	stbl.Stsc = &mp4.StscBox{}
	// One chunk, whose offset is filled in below once the boxes ahead of
	// it have their final size — which the entry itself contributes to.
	stbl.Stco = &mp4.StcoBox{ChunkOffset: []uint32{0}}
	stbl.Stss = &mp4.StssBox{}
	for _, box := range []mp4.Box{stbl.Stts, stbl.Stsc, stbl.Stsz, stbl.Stss, stbl.Stco} {
		stbl.AddChild(box)
	}
	if err := stbl.Stsc.AddEntry(1, uint32(len(samples)), 1); err != nil {
		t.Fatalf("stsc entry: %v", err)
	}

	mdat := &mp4.MdatBox{}
	for i := range samples {
		stbl.Stts.SampleCount = append(stbl.Stts.SampleCount, 1)
		stbl.Stts.SampleTimeDelta = append(stbl.Stts.SampleTimeDelta, samples[i].Dur)
		stbl.Stsz.SampleSize = append(stbl.Stsz.SampleSize, samples[i].Size)
		if samples[i].IsSync() {
			stbl.Stss.SampleNumber = append(stbl.Stss.SampleNumber, uint32(i+1))
		}
		mdat.AddSampleData(samples[i].Data)
	}

	// The one chunk starts where mdat's payload does.
	stbl.Stco.ChunkOffset[0] = uint32(init.Size() + mdat.HeaderSize())
	if err := init.Encode(w); err != nil {
		t.Fatalf("encode init: %v", err)
	}
	if err := mdat.Encode(w); err != nil {
		t.Fatalf("encode mdat: %v", err)
	}
}

// synthInit builds a one-track AVC init segment. The decoder
// configuration record is empty: nothing here decodes pictures, and an
// empty one is what [codecString] falls back on, so the codec string
// stays the bare sample entry name.
func synthInit(t *testing.T) *mp4.InitSegment {
	t.Helper()
	init := mp4.CreateEmptyInit()
	init.Moov.Mvhd.Timescale = 90000
	trak := init.AddEmptyTrack(90000, "video", "und")
	if trak == nil {
		t.Fatal("AddEmptyTrack returned nil")
	}
	entry := mp4.CreateVisualSampleEntryBox("avc1", 320, 180,
		&mp4.AvcCBox{
			AVCProfileIndication: 66,
			AVCLevelIndication:   30})
	trak.Mdia.Minf.Stbl.Stsd.AddChild(entry)
	return init
}

// TestChunkBytesContinuesTheTimelineOnARepeatPass is the fix for a stream
// that rewound every lap.
//
// A repeat pass used to resend the bytes encoded at load, so every
// fragment carried the decode time and the sequence number it had the
// first time round. A subscriber writing that out got a file whose time
// runs backwards once per loop: ffmpeg reads it as "DTS 0 < ... out of
// order" and a player stops there.
func TestChunkBytesContinuesTheTimelineOnARepeatPass(t *testing.T) {
	src, err := openSource(writeSynthFile(t, "IPPIPP", true), 0)
	if err != nil {
		t.Fatalf("openSource: %v", err)
	}
	if src.MediaDuration == 0 {
		t.Fatal("the source reports no media duration to advance the timeline by")
	}

	decodeTimes := func(pass int) []uint64 {
		t.Helper()
		var out []uint64
		for i := range src.Chunks {
			payload, err := src.chunkBytes(i, pass)
			if err != nil {
				t.Fatalf("chunkBytes(%d, %d): %v", i, pass, err)
			}
			f, err := mp4.DecodeFile(bytes.NewReader(append(bytes.Clone(src.Init), payload...)))
			if err != nil {
				t.Fatalf("parse chunk %d of pass %d: %v", i, pass, err)
			}
			frag := f.Segments[0].Fragments[0]
			out = append(out, frag.Moof.Traf.Tfdt.BaseMediaDecodeTime())
		}
		return out
	}

	first, second := decodeTimes(0), decodeTimes(1)
	for i := range first {
		want := first[i] + src.MediaDuration
		if second[i] != want {
			t.Fatalf("pass 2 chunk %d decodes at %d, want %d (pass 1 plus one duration)",
				i, second[i], want)
		}
	}
	// And the whole of pass two must sit after the whole of pass one, which
	// is what a player reads as one continuous timeline.
	if second[0] <= first[len(first)-1] {
		t.Errorf("pass 2 opens at %d, which is not after pass 1's last frame at %d",
			second[0], first[len(first)-1])
	}
}
