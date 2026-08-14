package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/Eyevinn/mp4ff/avc"
	"github.com/Eyevinn/mp4ff/mp4"
	"github.com/quic-go/quic-go/quicvarint"
)

// Reading a broadcast that is not ours.
//
// "legacy" is the packaging value the moq-lite/hang stack puts on a track
// whose Objects carry a bare codec bitstream rather than a container. It is
// not one of MSF §5.2.4's values and this package's catalog validation
// rejects it, which is correct — and useless if the point is to point this
// tool at a broadcast someone else is publishing and find out what the
// transport did to it.
//
// The Object layout, measured against cdn.moq.pro/demo's bbb.hang:
//
//	c0 00 00 74 99 f3 00 c9 | 00 00 00 01 09 f0 | 00 00 00 01 67 ...
//	└─ timestamp, µs ───────┘ └─ AUD ──────────┘ └─ SPS ──────────┘
//
// a QUIC varint timestamp in microseconds followed by one Annex-B access
// unit. Groups open on parameter sets and an IDR, and one Object is one
// frame — the same shape the CMAF path publishes, so everything the report
// measures carries over unchanged. Only the payload differs.

// legacyPackaging is the catalog packaging value this path reads. Kept as
// a plain string rather than added to pkg/moqt/msf: the draft does not
// define it and this package is not the place to imply that it does.
const legacyPackaging = "legacy"

// legacyTimescale is the timescale of the reassembled file, chosen to match
// the microsecond timestamps so no rounding is needed.
const legacyTimescale = 1_000_000

// legacyTrackID is the track the rebuilt container carries its samples on.
// mp4.InitSegment.AddEmptyTrack numbers from one, and the fragments have to
// name the same track the init segment declares.
const legacyTrackID = 1

// errShortLegacyObject reports an Object too small to hold its timestamp.
var errShortLegacyObject = errors.New("video: legacy object is shorter than its timestamp")

// legacyFrame is one decoded Object of a legacy-packaged track.
type legacyFrame struct {
	// Timestamp is the publisher's, in microseconds. Its origin is not
	// defined by anything readable here — deltas are what matter.
	Timestamp uint64
	// Sample is the access unit with its Annex-B start codes already
	// replaced by length fields, which is the form an MP4 sample takes and
	// the form every avc helper here reads.
	//
	// Converted once, at parse, and on a copy. avc.ConvertByteStreamToNaluSample
	// rewrites four-byte start codes in place and hands back the same slice,
	// so calling it twice on one buffer is destructive and calling it at all
	// would corrupt the recorded Object the report still counts. Doing it
	// once removes both hazards: this cost a run where every access unit was
	// converted to look for a picture and the parameter-set search that
	// followed then found no SPS in a stream that plainly had one.
	Sample []byte
}

// parseLegacyObject splits an Object into its timestamp and access unit.
//
// The timestamp is a QUIC varint (RFC 9000 §16), decoded with quic-go's
// own reader rather than with [wire.Reader.Varint] — MOQT's varint is the
// leading-ones encoding of draft-19 §1.4.1, a different format that reads
// these same bytes without complaining and returns the wrong number. On
// the measured stream it took 0xc0 as a three-byte encoding of zero, so
// every frame came out stamped 0 and the reassembled file had no timeline
// at all. The payload is moq-lite's, so QUIC's encoding is the one that
// applies; nothing about it is MOQT's to define.
func parseLegacyObject(payload []byte) (legacyFrame, error) {
	ts, n, err := quicvarint.Parse(payload)
	if err != nil {
		return legacyFrame{}, fmt.Errorf("%w: %w", errShortLegacyObject, err)
	}
	return legacyFrame{Timestamp: ts, Sample: avc.ConvertByteStreamToNaluSample(bytes.Clone(payload[n:]))}, nil
}

// writeLegacyMedia reassembles legacy-packaged Objects into a playable
// fragmented MP4 and returns its digest.
//
// The catalog cannot supply an initialization header for such a track and
// does not try to: the codec is avc3, which means the parameter sets travel
// in the bitstream, so the SPS and PPS opening the first Group are the
// header. That is also why this cannot be done by concatenating payloads
// the way the CMAF path does — the samples have to be converted out of
// Annex-B and given a container built from what was found inside them.
//
// An empty path skips the file, as in the CMAF case.
func writeLegacyMedia(path string, sorted []arrival) (string, error) {
	if path == "" {
		return "", nil
	}
	frames := decodableFrames(decodeLegacyFrames(sorted))
	if len(frames) == 0 {
		return "", errors.New(
			"video: no keyframe arrived, so there is no decodable point to start the file at " +
				"(a longer run will reach one — groups open on a keyframe)")
	}

	init, err := legacyInit(frames)
	if err != nil {
		return "", err
	}

	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("video: create %s: %w", path, err)
	}
	defer f.Close()

	digest := sha256.New()
	w := io.MultiWriter(f, digest)
	if err := init.Encode(w); err != nil {
		return "", fmt.Errorf("video: write init to %s: %w", path, err)
	}
	if err := writeLegacyFragments(w, frames, init.Moov.Trak.Tkhd.TrackID); err != nil {
		return "", fmt.Errorf("video: write %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("video: close %s: %w", path, err)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// decodeLegacyFrames parses every arrival, dropping those that do not hold
// a timestamp. A malformed Object is one frame lost, not a failed run —
// which is why this returns no error: parseLegacyObject fails only on a
// truncated timestamp, and skipping is the whole response to that.
func decodeLegacyFrames(sorted []arrival) []legacyFrame {
	frames := make([]legacyFrame, 0, len(sorted))
	for _, a := range sorted {
		frame, err := parseLegacyObject(a.Payload)
		if err != nil || len(frame.Sample) == 0 {
			continue
		}
		frames = append(frames, frame)
	}
	return frames
}

// decodableFrames reduces the received frames to the ones that make a
// playable file: from the first keyframe onward, and only those carrying a
// picture.
//
// Both exclusions are about the file, never about the report. Every Object
// counted in the delivery figures arrived and was supposed to; what is
// dropped here could not be turned into video, which is a different
// question from whether the transport delivered it.
func decodableFrames(frames []legacyFrame) []legacyFrame {
	return slices.DeleteFunc(fromFirstKeyframe(frames), func(f legacyFrame) bool {
		return !hasPicture(f.Sample)
	})
}

// hasPicture reports whether an access unit carries a coded picture.
//
// Not every Object does. Measured against cdn.moq.pro/demo, one access unit
// in a few hundred holds a single NALU of reserved type 23 and no slice —
// something the hang publisher puts on the track that is not video. Written
// into the file it becomes a sample the decoder rejects ("missing picture in
// access unit"), which reads as corruption in a tool whose whole purpose is
// telling corruption from clean delivery.
func hasPicture(sample []byte) bool {
	return slices.ContainsFunc(avc.FindNaluTypes(sample), func(t avc.NaluType) bool {
		return t == avc.NALU_IDR || t == avc.NALU_NON_IDR
	})
}

// fromFirstKeyframe drops the frames ahead of the first keyframe.
//
// A subscriber joining a live broadcast lands wherever the stream happens
// to be, so the first Objects it is given are usually mid-GOP frames whose
// reference picture was never sent to it. They arrived intact — the report
// counts them, and should, because delivering them is what the transport
// was asked to do — but they cannot be decoded, and writing them makes the
// output file open with a run of errors that reads as corruption. Measured
// against cdn.moq.pro: three undecodable access units at the head of one
// run, none at all in another that happened to join on a Group boundary.
//
// The publisher side drops the same frames for the same reason; see
// chunkSamples.
func fromFirstKeyframe(frames []legacyFrame) []legacyFrame {
	i := slices.IndexFunc(frames, func(f legacyFrame) bool {
		return avc.IsIDRSample(f.Sample)
	})
	if i < 0 {
		return nil
	}
	return frames[i:]
}

// legacyInit builds the CMAF header from the parameter sets carried in the
// stream, searching forward until it finds a frame holding both.
//
// The sample entry is avc3 rather than avc1 because that is what the track
// is: parameter sets in the bitstream, which avc3 exists to signal and avc1
// does not permit. They go into the entry as well, since a player that
// reads the sample description gets a decoder configured before the first
// sample rather than from it.
func legacyInit(frames []legacyFrame) (*mp4.InitSegment, error) {
	var spss, ppss [][]byte
	for _, frame := range frames {
		spss, ppss = avc.GetParameterSets(frame.Sample)
		if len(spss) > 0 && len(ppss) > 0 {
			break
		}
	}
	if len(spss) == 0 || len(ppss) == 0 {
		return nil, errors.New(
			"video: no SPS/PPS in the stream, so no decoder configuration can be built " +
				"(an avc1 track would carry them in the catalog's initDataList instead)")
	}

	init := mp4.CreateEmptyInit()
	init.Moov.Mvhd.Timescale = legacyTimescale
	trak := init.AddEmptyTrack(legacyTimescale, "video", "und")
	if trak == nil {
		return nil, errors.New("video: could not add a track to the init segment")
	}
	if err := trak.SetAVCDescriptor("avc3", spss, ppss, true); err != nil {
		return nil, fmt.Errorf("video: build avc descriptor: %w", err)
	}
	return init, nil
}

// writeLegacyFragments emits one movie fragment per frame, mirroring the
// one-Object-one-chunk shape the CMAF path publishes.
//
// Durations come from the timestamp deltas, which is the only timing the
// stream carries. The last frame has no successor to measure against and
// inherits the previous duration; with a single frame there is nothing to
// inherit, so it gets one tick rather than a zero-length sample that some
// players refuse.
func writeLegacyFragments(w io.Writer, frames []legacyFrame, trackID uint32) error {
	for i, frame := range frames {
		sample := mp4.FullSample{
			Sample: mp4.Sample{
				Flags: legacySampleFlags(frame.Sample),
				Dur:   legacyDuration(frames, i),
				// Set here because mp4ff records it verbatim: AddFullSample
				// copies Sample into the trun and never derives the size from
				// Data, so leaving it zero writes a file whose samples all
				// claim zero length while the mdat still holds the bytes.
				//nolint:gosec // G115: one Object's payload, bounded by the wire's own limits.
				Size: uint32(len(frame.Sample)),
			},
			DecodeTime: frame.Timestamp - frames[0].Timestamp,
			Data:       frame.Sample,
		}

		frag, err := mp4.CreateFragment(uint32(i+1), trackID)
		if err != nil {
			return fmt.Errorf("create fragment %d: %w", i+1, err)
		}
		frag.AddFullSample(sample)
		if err := frag.Encode(w); err != nil {
			return fmt.Errorf("encode fragment %d: %w", i+1, err)
		}
	}
	return nil
}

// legacyDuration returns frame i's duration in the media timescale.
func legacyDuration(frames []legacyFrame, i int) uint32 {
	switch {
	case i+1 < len(frames):
		return sampleDuration(frames[i].Timestamp, frames[i+1].Timestamp)
	case i > 0:
		return sampleDuration(frames[i-1].Timestamp, frames[i].Timestamp)
	default:
		return 1
	}
}

// maxSampleDuration caps a sample's duration at an hour of media time,
// which a trun records in 32 bits.
const maxSampleDuration = 3600 * legacyTimescale

// sampleDuration is the gap between two timestamps, clamped into what a
// sample duration can hold.
//
// The timestamps come off the wire from another implementation, so nothing
// guarantees they rise, or rise by a sane amount: a stall, a wrap, or a
// publisher that simply numbers them differently all land here. A run of
// frames spaced an hour apart is nonsense but produces a file; a silent
// 32-bit truncation produces one whose timeline is wrong in a way that
// reads as a delivery fault.
func sampleDuration(from, to uint64) uint32 {
	if to <= from {
		return 1
	}
	//nolint:gosec // G115: min() has already bounded this by maxSampleDuration, which fits in 32 bits.
	return uint32(min(to-from, maxSampleDuration))
}

// legacySampleFlags marks an access unit holding an IDR as a sync sample.
func legacySampleFlags(sample []byte) uint32 {
	if avc.IsIDRSample(sample) {
		return mp4.SampleFlags{SampleDependsOn: 2}.Encode()
	}
	return mp4.SampleFlags{SampleIsNonSync: true}.Encode()
}

// legacySummary describes what a legacy stream turned out to hold, for the
// line the subscriber logs once the run is over. Nothing about a foreign
// broadcast is known in advance, so this is the only place its shape is
// reported at all.
func legacySummary(sorted []arrival) string {
	frames := decodeLegacyFrames(sorted)
	if len(frames) == 0 {
		return "no decodable frames"
	}
	var keyframes int
	for _, frame := range frames {
		if avc.IsIDRSample(frame.Sample) {
			keyframes++
		}
	}
	span := frames[len(frames)-1].Timestamp - frames[0].Timestamp
	rate := "unknown"
	if span > 0 {
		rate = fmt.Sprintf("%.2f fps", float64(len(frames)-1)*legacyTimescale/float64(span))
	}
	return fmt.Sprintf("%d frames, %d keyframes, %s media, %s",
		len(frames), keyframes, formatMicros(span), rate)
}

// formatMicros renders a microsecond span in seconds.
func formatMicros(micros uint64) string {
	return fmt.Sprintf("%.3fs", float64(micros)/legacyTimescale)
}
