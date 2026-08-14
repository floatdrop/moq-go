# video

Streams a local video file to a relay as a CMSF broadcast
([draft-ietf-moq-cmsf-01](https://datatracker.ietf.org/doc/draft-ietf-moq-cmsf/):
CMAF packaging on top of MSF `draft-ietf-moq-msf-01` and MoQ Transport
`draft-ietf-moq-transport-19`), and measures what comes back out.

It exists to answer one question about a suspected delivery fault: **is
the transport at fault, or the encoder feeding it?** Every byte the
publisher sends is known in advance, so the subscriber can say exactly
what arrived, in what order and how late — and can reassemble the Objects
it received into a file that either matches the source byte for byte or
does not. A clean run points at the capture and encode path; a dirty one
points at the transport.

## Usage

```
video [flags] publish | subscribe
```

Start the relay, then the two halves in either order:

```sh
go run ./cmd/relay                                    # :4433, self-signed cert
go run ./cmd/video -out /tmp/recv.mp4 subscribe
go run ./cmd/video -in clip.mp4 publish
```

| Flag | Default | Mode | Description |
|------|---------|------|-------------|
| `-addr` | `localhost:4433` | both | Relay address, `host:port` or a `moqt://` URI |
| `-ns` | `moq-example/video` | both | Track namespace |
| `-in` | — | publish | Video file to stream (required) |
| `-rate` | `1` | publish | Pacing multiplier; `0` sends as fast as the transport takes it |
| `-loop` | `1` | publish | Passes over the file; `0` repeats until interrupted |
| `-gop` | `0` | publish | Minimum Objects per Group; `0` starts a Group at every sync sample |
| `-delay` | `2s` | publish | Pause between the catalog and the first frame |
| `-out` | — | subscribe | Where to reassemble the received media |
| `-wait` | `30s` | subscribe | How long to wait for a publisher on the namespace |

Any MP4 the file reader understands works — progressive or already
fragmented, AVC / HEVC / AV1 / VP9. Only the first video track is
published; audio, subtitle and data tracks are ignored.

Connections use `InsecureSkipVerify: true` so they work against the
relay's ephemeral self-signed cert. `SIGINT` / `SIGTERM` ends either side
cleanly, and the subscriber writes its report either way — a run cut short
still says what arrived before it was.

## Reading the report

```
=== delivery report ===
objects   180 received, 0 missing (groups 6 received, 0 missing)
bytes     1516520 over 5.967s
order     0 objects arrived after a later one
latency   p50 939µs  p90 1.6ms  p99 3ms  max 3.7ms  (180 objects)
          slowest 0/25 3.7ms
          slowest 5/7 3.1ms
spacing   p50 33.4ms  p99 36.9ms  max 39.5ms  (mean 33.2ms)
digest    211deb6c63d4dcaf14bfa31e5b62b6682d0c2b0b085c54253df1118cff7c96d0
          MATCHES the source: every byte arrived, in order
```

- **objects / groups** — anything missing is loss, counted only between
  the first and last that arrived, so joining mid-broadcast is not
  reported as a failure.
- **order** — Objects that arrived after one the publisher sent later.
  One Group is one subgroup stream and streams are read concurrently, so
  a Group can overtake the tail of the one before it. That is inherent to
  MoQ, not a fault; it is here because a player that ignores it shows
  artefacts.
- **latency** — send to receive, per Object, from a producer-defined
  Object Property the publisher stamps (type `0x3800`, in the range §2.5
  reserves for application-specific use, which IANA will never allocate
  and a relay must forward unchanged). Only
  meaningful with both ends on one machine, since it subtracts two
  clocks. The `slowest` lines name the worst Objects by `group/object`,
  because a spike is a handful of Objects and an average hides it.
- **spacing** — gaps between successive arrivals in send order. A stall
  shows here even when latency cannot be measured.
- **digest** — SHA-256 of the CMAF header plus every Object payload in
  order, against the same value the publisher declared in the catalog.
  Needs `-out`, which is the only thing that retains payloads: without it
  a run keeps one small record per Object and not the media, which is what
  makes `-loop 0` soak testing practical.

The file `-out` writes is a playable fragmented MP4. If the digest matches
and it still shows artefacts, the artefacts were encoded into the source.

## How the file maps onto MoQ

| CMAF | MoQ | Draft |
|------|-----|-------|
| Init header (`ftyp`+`moov`) | catalog `initDataList`, inline base64 | CMSF §3.1 |
| One chunk (`moof`+`mdat`, one frame) | one Object | CMSF §3.3 |
| Fragment starting at a sync sample | one Group, one subgroup stream | CMSF §3.4 |

One frame per Object rather than one GOP is deliberate: an Object that
spans a GOP reports one arrival time for two seconds of video and hides
exactly the spikes this is looking for. It is also what a low-latency CMAF
publisher emits, so nothing about the shape is diagnostic-only.

`-gop N` widens Groups without breaking §3.4 — a sync sample reached
before the open Group holds `N` Objects becomes an ordinary mid-Group
Object rather than starting a new one. Use it to compare "many short
streams" against "few long ones".

The catalog declares `packaging: "cmaf"` (§3.5.1) and both SAP fields
(§3.5.2) as `2`. That is the only conformant value: both fields are
maxima, §3.4 pins a Group's first Object to SAP type 1 or 2 on a `cmaf`
track, and `pkg/moqt/msf` rejects anything outside that. Input that may
not satisfy §3.4 is flagged at publish time instead — see Limitations.

## Tracks

Both tracks live under `-ns`:

| Track name | Packaging | Contents |
|------------|-----------|----------|
| `catalog` | (MSF catalog) | The broadcast description, plus the CMAF header and the source's digest. One Object per Group. |
| `video` | `cmaf` | One CMAF chunk per Object, one Group per GOP. |

The catalog is published once at start (§5: a catalog Object SHOULD be
published only when track availability changes), so a subscriber that
connects later backfills it with a Joining FETCH (§5.1.3). The publisher
closes with the §11.3 terminator catalog, which is what ends the
subscriber's run and triggers its report.

## Limitations

- **Open-GOP input is warned about, not rejected.** A file whose Groups
  open on an access point with leading pictures may be starting them at
  SAP type 3, where CMSF §3.4 requires 1 or 2 — and it would then break up
  at every Group boundary for reasons belonging to the file while
  reporting a perfect digest, which is the exact false positive this tool
  exists not to produce. Whether those leading pictures are RASL
  (undecodable, SAP 3) or RADL (decodable, SAP 2) cannot be read from the
  container at all, only from slice headers, so the publisher flags the
  input rather than judging it. Re-encode with closed GOPs to rule the
  encoder out; if the artefacts survive that, the result means something.
  Detection reads composition times, not the `is_leading` field ISO/IEC
  14496-12 defines for it — ffmpeg writes `is_leading` as 0 ("unknown") on
  every sample of both open- and closed-GOP output, so a check reading it
  never fires on the files this is pointed at.
- **Video only.** Audio would double the tracks for no extra signal about
  video artefacts, and would complicate the byte comparison.
- **The whole file is held in memory** by the publisher, and by the
  subscriber too when `-out` is set. These are debug clips; streaming
  chunks off disk would put file I/O inside the loop being measured.
- **Latency needs one machine.** Across two, the send stamp and the
  receive stamp come from different clocks and only `spacing`, `order`
  and the loss counts mean anything.
- **The publisher lingers a second after its last write.** Closing a QUIC
  connection abandons whatever it has not had acknowledged, and without
  the pause the terminator catalog is exactly what gets dropped.
