package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Eyevinn/mp4ff/av1"
	"github.com/Eyevinn/mp4ff/avc"
	"github.com/Eyevinn/mp4ff/hevc"
	"github.com/Eyevinn/mp4ff/mp4"
)

// errNoVideoTrack reports an input file with no video track to publish.
var errNoVideoTrack = errors.New("video: file has no video track")

// chunk is one CMAF chunk: a Movie Fragment Box followed by a Media Data
// Box holding a single coded frame. CMSF §3.3 requires each Object to
// contain at least one moof+mdat pair and to hold a single track, which
// this is the smallest legal form of.
//
// One frame per chunk rather than one GOP per chunk because the whole
// point of the tool is per-frame timing: an Object that spans a GOP
// reports one arrival time for two seconds of video and hides exactly
// the spikes we are looking for. It is also what a low-latency CMAF
// publisher emits, so nothing here is a diagnostic-only shape.
type chunk struct {
	// Data is the encoded moof+mdat, which is the Object payload verbatim.
	Data []byte
	// Group and Object are the MOQT coordinates this chunk is published at.
	Group  uint64
	Object uint64
	// Sync reports a chunk whose sample is a sync sample. Every Group
	// starts with one (CMSF §3.4).
	Sync bool
	// Presentation is the sample's composition time, in the track's media
	// timescale. Kept because it is the only reliable way to spot leading
	// pictures — see [hasLeadingPictures].
	Presentation int64
	// DecodeTime is the sample's absolute decode time in the track's media
	// timescale, which is what the publisher paces against.
	DecodeTime uint64
}

// source is a local video file re-packaged as a CMSF broadcast: one CMAF
// header, and the file's first video track as a flat list of chunks
// already assigned to Groups.
//
// The whole file is held in memory. These are debug clips, and the
// alternative — streaming chunks off disk as they are sent — would put
// file I/O inside the send loop being measured.
type source struct {
	// Init is the encoded CMAF header (ftyp+moov), which rides in the
	// catalog's initDataList (CMSF §3.1).
	Init []byte
	// Timescale is the track's media timescale, the unit of
	// chunk.DecodeTime.
	Timescale uint32
	// Codec is the RFC 6381 codec string for the catalog.
	Codec         string
	Width, Height uint32
	Framerate     float64
	Bitrate       uint64
	// Duration is the summed sample duration of the whole track.
	Duration time.Duration

	Chunks []chunk
	Groups uint64
	// LeadingPictures reports Groups that open on an access point with
	// leading pictures, which may make them SAP type 3 and so
	// non-conformant with CMSF §3.4. See [hasLeadingPictures] for what
	// this can and cannot establish; the publisher warns on it.
	LeadingPictures bool
	// SHA256 is the digest of Init followed by every chunk's Data in
	// order — the value a subscriber's own digest must equal for the
	// broadcast to have been delivered intact.
	SHA256 string
	// Bytes is the total size of Init plus every chunk's Data.
	Bytes int
}

// openSource reads path and re-packages its first video track.
//
// minGroupObjects is the smallest number of Objects a Group may hold: a
// sync sample reached before the current Group has that many becomes an
// ordinary mid-Group Object instead of starting a new one. Zero starts a
// Group at every sync sample. Groups still begin only at a sync sample
// either way, which is what CMSF §3.4 requires.
func openSource(path string, minGroupObjects int) (*source, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	parsed, err := mp4.DecodeFile(f)
	if err != nil {
		return nil, fmt.Errorf("video: decode %s: %w", path, err)
	}
	if parsed.Moov == nil {
		return nil, fmt.Errorf("video: %s has no moov box", path)
	}

	trak, err := videoTrak(parsed)
	if err != nil {
		return nil, err
	}
	entry, sampleEntryName, err := videoSampleEntry(trak)
	if err != nil {
		return nil, err
	}

	init, err := buildInit(parsed, trak, entry)
	if err != nil {
		return nil, err
	}
	var initBuf bytes.Buffer
	if err := init.Encode(&initBuf); err != nil {
		return nil, fmt.Errorf("video: encode init segment: %w", err)
	}

	samples, err := collectSamples(parsed, trak)
	if err != nil {
		return nil, err
	}
	if len(samples) == 0 {
		return nil, fmt.Errorf("video: %s has no video samples", path)
	}

	trackID := init.Moov.Trak.Tkhd.TrackID
	chunks, groups, err := chunkSamples(samples, trackID, minGroupObjects)
	if err != nil {
		return nil, err
	}

	timescale := trak.Mdia.Mdhd.Timescale
	src := &source{
		Init:            initBuf.Bytes(),
		Timescale:       timescale,
		Codec:           codecString(entry, sampleEntryName),
		Width:           uint32(entry.Width),
		Height:          uint32(entry.Height),
		Chunks:          chunks,
		Groups:          groups,
		LeadingPictures: hasLeadingPictures(chunks),
	}

	var mediaDur uint64
	var payload int
	for i := range samples {
		mediaDur += uint64(samples[i].Dur)
		payload += len(samples[i].Data)
	}
	src.Duration = time.Duration(mediaDur) * time.Second / time.Duration(timescale)
	if src.Duration > 0 {
		src.Framerate = float64(len(samples)) / src.Duration.Seconds()
		src.Bitrate = uint64(float64(payload) * 8 / src.Duration.Seconds())
	}

	digest := sha256.New()
	digest.Write(src.Init)
	src.Bytes = len(src.Init)
	for i := range chunks {
		digest.Write(chunks[i].Data)
		src.Bytes += len(chunks[i].Data)
	}
	src.SHA256 = hex.EncodeToString(digest.Sum(nil))
	return src, nil
}

// videoTrak returns the file's first video track.
func videoTrak(f *mp4.File) (*mp4.TrakBox, error) {
	for _, trak := range f.Moov.Traks {
		if trak.Mdia != nil && trak.Mdia.Hdlr != nil && trak.Mdia.Hdlr.HandlerType == "vide" {
			return trak, nil
		}
	}
	return nil, errNoVideoTrack
}

// videoSampleEntry returns the track's visual sample entry and its
// four-character box name, which together carry everything needed both to
// rebuild the CMAF header and to name the codec in the catalog.
func videoSampleEntry(trak *mp4.TrakBox) (*mp4.VisualSampleEntryBox, string, error) {
	if trak.Mdia.Minf == nil || trak.Mdia.Minf.Stbl == nil || trak.Mdia.Minf.Stbl.Stsd == nil {
		return nil, "", errors.New("video: track has no sample description")
	}
	stsd := trak.Mdia.Minf.Stbl.Stsd
	for _, entry := range []*mp4.VisualSampleEntryBox{
		stsd.AvcX, stsd.HvcX, stsd.Av01, stsd.VvcX, stsd.VpXX, stsd.Avs3, stsd.Mp4v,
	} {
		if entry != nil {
			return entry, entry.Type(), nil
		}
	}
	if stsd.Encv != nil {
		// An encrypted sample entry would need the CMSF §4 content-protection
		// half of the catalog and a key to be of any use; refused rather than
		// published as if it were clear.
		return nil, "", errors.New("video: encrypted video tracks (encv) are not supported")
	}
	return nil, "", errNoVideoTrack
}

// buildInit assembles the CMAF header for the published track: a fresh
// fragmented-file init segment carrying the input's own visual sample
// entry verbatim.
//
// Rebuilt rather than reused even when the input is already fragmented,
// because the published chunks are re-numbered onto this init's track ID.
// Copying the sample entry across keeps the decoder configuration —
// avcC/hvcC/av1C and everything beside them — exactly as the file had it.
func buildInit(f *mp4.File, trak *mp4.TrakBox, entry *mp4.VisualSampleEntryBox) (*mp4.InitSegment, error) {
	lang := trak.Mdia.Mdhd.GetLanguage()
	if trak.Mdia.Elng != nil {
		lang = trak.Mdia.Elng.Language
	}

	init := mp4.CreateEmptyInit()
	init.Moov.Mvhd.Timescale = f.Moov.Mvhd.Timescale
	//nolint:gosec // G115: mvhd duration comes from the local file; a wrong value makes a wrong hint in mehd, nothing more.
	init.Moov.Mvex.AddChild(&mp4.MehdBox{FragmentDuration: int64(f.Moov.Mvhd.Duration)})
	out := init.AddEmptyTrack(trak.Mdia.Mdhd.Timescale, "video", lang)
	if out == nil {
		return nil, errors.New("video: could not add a track to the init segment")
	}
	out.Mdia.Minf.Stbl.Stsd.AddChild(entry)
	// tkhd carries the display size as 16.16 fixed point; without it a
	// player has to fall back on the sample entry and some do not.
	out.Tkhd.Width = mp4.Fixed32(uint32(entry.Width) << 16)
	out.Tkhd.Height = mp4.Fixed32(uint32(entry.Height) << 16)
	return init, nil
}

// collectSamples reads the track's samples with their data, from either a
// progressive or an already-fragmented input.
func collectSamples(f *mp4.File, trak *mp4.TrakBox) ([]mp4.FullSample, error) {
	if f.IsFragmented() {
		return fragmentedSamples(f, trak)
	}
	return progressiveSamples(f, trak)
}

// fragmentedSamples reads the track's samples out of an already-fragmented
// file's movie fragments.
func fragmentedSamples(f *mp4.File, trak *mp4.TrakBox) ([]mp4.FullSample, error) {
	trex := trexFor(f.Moov.Mvex, trak.Tkhd.TrackID)
	if trex == nil {
		return nil, fmt.Errorf("video: fragmented file has no trex for track %d", trak.Tkhd.TrackID)
	}
	var samples []mp4.FullSample
	for _, seg := range f.Segments {
		for _, frag := range seg.Fragments {
			// Returns nil for a fragment that carries other tracks only.
			got, err := frag.GetFullSamples(trex)
			if err != nil {
				return nil, fmt.Errorf("video: read fragment samples: %w", err)
			}
			samples = append(samples, got...)
		}
	}
	return samples, nil
}

// trexFor returns the Track Extends Box for trackID, or nil.
func trexFor(mvex *mp4.MvexBox, trackID uint32) *mp4.TrexBox {
	if mvex == nil {
		return nil
	}
	for _, trex := range mvex.Trexs {
		if trex.TrackID == trackID {
			return trex
		}
	}
	return nil
}

// progressiveSamples reads the track's samples out of a non-fragmented
// file by walking its sample tables.
func progressiveSamples(f *mp4.File, trak *mp4.TrakBox) ([]mp4.FullSample, error) {
	if f.Mdat == nil {
		return nil, errors.New("video: progressive file has no mdat box")
	}
	stbl := trak.Mdia.Minf.Stbl
	if stbl.Stsz == nil || stbl.Stts == nil || stbl.Stsc == nil {
		return nil, errors.New("video: track is missing a sample table")
	}
	mdatStart := f.Mdat.PayloadAbsoluteOffset()
	mdatLen := uint64(len(f.Mdat.Data))

	count := stbl.Stsz.SampleNumber
	samples := make([]mp4.FullSample, 0, count)
	for nr := uint32(1); nr <= count; nr++ {
		offset, err := sampleOffset(stbl, nr)
		if err != nil {
			return nil, err
		}
		size := stbl.Stsz.GetSampleSize(int(nr))
		if offset < mdatStart || offset+uint64(size) > mdatStart+mdatLen {
			return nil, fmt.Errorf("video: sample %d lies outside mdat", nr)
		}
		decTime, dur := stbl.Stts.GetDecodeTime(nr)
		var cto int32
		if stbl.Ctts != nil {
			cto = stbl.Ctts.GetCompositionTimeOffset(nr)
		}
		samples = append(samples, mp4.FullSample{
			Sample: mp4.Sample{
				Flags:                 sampleFlags(stbl, nr),
				Dur:                   dur,
				Size:                  size,
				CompositionTimeOffset: cto,
			},
			DecodeTime: decTime,
			Data:       f.Mdat.Data[offset-mdatStart : offset-mdatStart+uint64(size)],
		})
	}
	return samples, nil
}

// sampleOffset returns the absolute file offset of sample nr: the offset
// of the chunk holding it plus the sizes of the samples ahead of it in
// that chunk.
func sampleOffset(stbl *mp4.StblBox, nr uint32) (uint64, error) {
	chunkNr, firstInChunk, err := stbl.Stsc.ChunkNrFromSampleNr(int(nr))
	if err != nil {
		return 0, fmt.Errorf("video: locate sample %d: %w", nr, err)
	}
	var offset uint64
	switch {
	case stbl.Stco != nil:
		offset = uint64(stbl.Stco.ChunkOffset[chunkNr-1])
	case stbl.Co64 != nil:
		offset = stbl.Co64.ChunkOffset[chunkNr-1]
	default:
		return 0, errors.New("video: track has neither stco nor co64")
	}
	for s := firstInChunk; s < int(nr); s++ {
		offset += uint64(stbl.Stsz.GetSampleSize(s))
	}
	return offset, nil
}

// sampleFlags translates a progressive track's sync-sample and dependency
// tables into the sample flags a trun box carries, so the fragments built
// from them report sync samples the way the source file did.
func sampleFlags(stbl *mp4.StblBox, nr uint32) uint32 {
	var flags mp4.SampleFlags
	if stbl.Stss != nil {
		// Absent stss, every sample is a sync sample and the zero value —
		// SampleIsNonSync false — already says so.
		isSync := stbl.Stss.IsSyncSample(nr)
		flags.SampleIsNonSync = !isSync
		if isSync {
			flags.SampleDependsOn = 2 // does not depend on others; sdtp may refine it
		}
	}
	if stbl.Sdtp != nil && int(nr) <= len(stbl.Sdtp.Entries) {
		entry := stbl.Sdtp.Entries[nr-1] // the table is zero-based, sample numbers are not
		flags.IsLeading = entry.IsLeading()
		flags.SampleDependsOn = entry.SampleDependsOn()
		flags.SampleHasRedundancy = entry.SampleHasRedundancy()
		flags.SampleIsDependedOn = entry.SampleIsDependedOn()
	}
	return flags.Encode()
}

// chunkSamples packages one sample per CMAF chunk and assigns the chunks
// to Groups, starting a Group at a sync sample once the open one holds at
// least minGroupObjects Objects.
//
// A leading run of non-sync samples is dropped: CMSF §3.4 requires a Group
// to begin with a SAP type 1 or 2 Object, and there is nothing else to do
// with frames that arrive before the first one.
func chunkSamples(samples []mp4.FullSample, trackID uint32, minGroupObjects int) ([]chunk, uint64, error) {
	chunks := make([]chunk, 0, len(samples))
	var group, object uint64
	var started bool
	seq := uint32(1)
	// Compared against the Object counter below, which is a Group-local
	// count and not an Object ID; a negative setting means "no minimum".
	minObjects := uint64(max(minGroupObjects, 0))

	for i := range samples {
		sample := samples[i]
		sync := sample.IsSync()
		switch {
		case !started:
			if !sync {
				continue // nothing decodable has appeared yet
			}
			started = true
		case sync && object >= minObjects:
			group++
			object = 0
		}

		// Each chunk is a self-contained fragment of one sample. The moof
		// sequence number counts chunks across the whole track, which is what
		// a reader concatenating them back into a file needs to see.
		frag, err := mp4.CreateFragment(seq, trackID)
		if err != nil {
			return nil, 0, fmt.Errorf("video: create fragment %d: %w", seq, err)
		}
		frag.AddFullSample(sample)
		var buf bytes.Buffer
		if err := frag.Encode(&buf); err != nil {
			return nil, 0, fmt.Errorf("video: encode fragment %d: %w", seq, err)
		}
		seq++

		chunks = append(chunks, chunk{
			Data:         buf.Bytes(),
			Group:        group,
			Object:       object,
			Sync:         sync,
			DecodeTime:   sample.DecodeTime,
			Presentation: sample.PresentationTime(),
		})
		object++
	}

	if !started {
		return nil, 0, errors.New("video: track has no sync sample to start a Group with")
	}
	return chunks, group + 1, nil
}

// hasLeadingPictures reports whether any Group opens on an access point
// that has leading pictures: samples that follow it in decode order but
// precede it in presentation order.
//
// Detected from composition times rather than from the is_leading field
// ISO/IEC 14496-12 §8.8.3.1 defines for exactly this, because ffmpeg
// writes is_leading as 0 ("unknown") on every sample of both open- and
// closed-GOP output — measured, not assumed — so a check reading it never
// fires on the files this tool is actually pointed at. Composition times
// come from ctts, which ffmpeg does write.
//
// What it can and cannot tell apart matters. Leading pictures make the
// access point SAP type 2 when they are decodable from it (RADL) and type
// 3 when they are not (RASL), and only type 3 breaks CMSF §3.4 — but
// which of the two a picture is cannot be read from the container at all,
// only from the slice headers inside the samples. So this is a flag for
// the operator, not a verdict: it says the Groups may open on a SAP type
// 3, which is the one input that would make a perfect delivery report
// coexist with a broken picture.
func hasLeadingPictures(chunks []chunk) bool {
	var groupStart int64
	var started bool
	for _, c := range chunks {
		if c.Object == 0 {
			groupStart, started = c.Presentation, true
			continue
		}
		if started && c.Presentation < groupStart {
			return true
		}
	}
	return false
}

// codecString renders the RFC 6381 codec string the catalog declares.
//
// Derived from the decoder configuration rather than from the sample
// entry name alone, because a player picks a decoder from the profile and
// level in it. Codecs whose string this cannot build fall back to the bare
// sample entry name, which is still a valid — if unspecific — value.
func codecString(entry *mp4.VisualSampleEntryBox, name string) string {
	switch {
	case entry.AvcC != nil && len(entry.AvcC.SPSnalus) > 0:
		sps, err := avc.ParseSPSNALUnit(entry.AvcC.SPSnalus[0], false)
		if err != nil {
			return name
		}
		return avc.CodecString(name, sps)
	case entry.HvcC != nil:
		spsNalus := entry.HvcC.GetNalusForType(hevc.NALU_SPS)
		if len(spsNalus) == 0 {
			return name
		}
		sps, err := hevc.ParseSPSNALUnit(spsNalus[0])
		if err != nil {
			return name
		}
		return hevc.CodecString(name, sps)
	case entry.Av1C != nil:
		// The sequence header carries the optional colour-configuration
		// suffix; without it the configuration record still yields the
		// mandatory "av01.P.LLT.DD" prefix, which is a complete codec string.
		if sh, err := entry.Av1C.SequenceHeader(); err == nil {
			return av1.CodecString(name, sh)
		}
		return entry.Av1C.CodecString(name)
	default:
		return name
	}
}
