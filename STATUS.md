# MoQT Implementation Status

Tracks this codebase's implementation of
[`draft-ietf-moq-transport-20`](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/20/)
(plus [`-loc-04`](https://datatracker.ietf.org/doc/draft-ietf-moq-loc/),
[`-msf-01`](https://datatracker.ietf.org/doc/draft-ietf-moq-msf/), and
[`-cmsf-01`](https://datatracker.ietf.org/doc/draft-ietf-moq-cmsf/) at the edges).

## Overall: ~95% complete

The wire codec, all control messages and parameters, data streams/datagrams, the
session lifecycle, and the relay are implemented and wired end to end. What remains is intentionally out of scope: behaviour the draft delegates to
the transport (congestion control, 0-RTT, communication/media security) and
*global* cross-session resource quotas, which sit above this library (per-session
caps and the `Authorizer` hook are the in-library surface).

**How the number is derived.** Each trackable feature below is scored
`DONE = 1`, `PARTIAL = 0.5`, `MISSING = 0`. Items the spec explicitly delegates
to QUIC/TLS or to deployment policy (most of §13) are scored on their hook/
surface-area completeness, not as protocol obligations. Legend:

- **DONE** — wire codec + session/relay behaviour both present and wired.
- **PARTIAL** — present but incomplete; see note.
- **MISSING** — not implemented.
- **N/A (transport)** — handled by the underlying QUIC/WebTransport stack.

## What's implemented

By package, bottom-up along the dependency stack:

- **`wire`** — byte-level codec: §1.4.1 leading-ones varints (distinct from
  QUIC's RFC 9000 varints), length-prefixed bytes, delta-encoded KV pairs
  (§1.4.3), track namespaces, reason phrases; an in-memory `Reader` and a
  streaming `Decoder` over one control-frame interface.
- **`message`** — typed control, request-stream, and data-stream messages with
  parameter negotiation: SETUP, GOAWAY, SUBSCRIBE, PUBLISH (+DONE/SKIPPED),
  FETCH (standalone + relative/absolute joining), TRACK_STATUS, REQUEST_UPDATE,
  the namespace messages, §11 object framing (subgroup/fetch/datagram),
  location filters, GREASE, and a parse-time `Validate` hook that rejects
  structurally-malformed messages.
- **`session`** — the SETUP handshake with version negotiation, control
  multiplexing and request-ID allocation, §3.5 Track-Alias management with
  collision detection, the request openers (`Publish`/`Subscribe`/`Fetch`/…) and
  the `AcceptRequest` responder, typed inbound data streams that resolve
  §11.4.2/§11.4.4 deltas to absolute IDs, GOAWAY, the §10.2.2 token cache, and
  pluggable transport via the `Conn` interface (`quicconn` + `wtconn` adapters).
- **`loc`** — `Object.Encode`/`Decode`: typed Timestamp/Timescale/VideoConfig/
  VideoFrameMarking/AudioConfig/AudioLevel properties with `Extras` passthrough
  for unknown IDs, an RFC 6464 audio-level codec, and AVC/HEVC NAL framing
  detection.
- **`msf`** — `Catalog`/`Track` JSON (independent and delta catalogs, with
  `Apply` replaying delta operations in document order and merging the
  root-level `initDataList`/`contentProtections` a delta carries), group-ID
  sequencing, the Media and Event Timeline record formats, the
  `BeginBroadcast`/`EndBroadcast*` workflow helpers, and `Catalog.Validate`.
  CMSF (`-cmsf-01`) adds `cmaf` packaging, the two max-SAP-starting-type track
  fields, the `SAPRecord` codec for SAP Type timeline tracks (§3.6.1), and the
  root-level `contentProtections` array with its DRM system metadata (§4.1) —
  validated down to the Table 3 scheme enum and the UUID forms §4.1.1.2 /
  §4.1.1.4.1 require. CMSF §4.2's `sinf`/`schm`/`schi`/`tenc` requirement lives
  inside the opaque Base64 init data, which this package does not parse.
- **`relay`** — routes objects through a track registry with per-subscription
  live fanout under a §8 slow-reader policy, merges multiple upstream publishers
  per track (§9.5) with §2.1 {Group, Object} dedup and survivor-continues
  failover, serves FETCHes from a per-track cache (stitching evicted ranges from
  an upstream FETCH), issues on-demand upstream SUBSCRIBEs to every matching
  publisher (local and, via a `DiscoveryStore` + `Dialer`, remote), reflects
  remote namespaces to local subscribers, gates requests through an `Authorizer`
  hook, emits telemetry through a `Metrics` hook, and drains sessions with
  GOAWAY.

## §1.4 Foundational structures

| §       | Feature                          | Status | Notes |
|---------|----------------------------------|--------|-------|
| 1.4.1   | Variable-length integers         | DONE   | Leading-ones encoding (§1.4.1, NOT QUIC's RFC 9000 varint) in `wire.AppendVarint`/`ParseVarint`/`ReadVarint`; used by `wire.Reader/Writer/StreamReader.Varint`. |
| 1.4.2   | Location structure               | DONE   | `message.Location` with `Compare`/`Less`; `KindLocation` param serialization. |
| 1.4.3   | Key-Value-Pair structure         | DONE   | `wire.KVPair`; even=varint / odd=length-prefixed; 0xFFFF cap, delta-overflow check. |
| 1.4.4   | Reason phrase structure          | DONE   | `wire.*.ReasonPhrase`; 1024-byte max enforced. |
| 1.5     | Namespace / track name encoding  | DONE   | `wire.TrackNamespace`; serialized-name parsing. |

## §3 Sessions

| §       | Feature                              | Status | Notes |
|---------|--------------------------------------|--------|-------|
| 3.1     | Session establishment                | DONE   | SETUP handshake in `handshake.go`. |
| 3.1.1   | MOQT URI scheme                      | DONE   | `pkg/moqt/uri` parses/validates `moqt://` (scheme, non-empty host, default port 443, well-known, https conversion); `video -addr` accepts a URI and feeds AUTHORITY/PATH options. |
| 3.1.2   | Fragment identifiers (`#type:value`) | DONE   | `uri.Parse` validates the `type:value` grammar (type ∈ [a-z0-9-]); fragment is kept local and dropped from the https URL. |
| 3.1.3   | Dereferencing a MOQT URI             | DONE   | A client offers either mapping's ALPN and the server picks: `relaynet.Listen` advertises `moqt-NN` + `h3` on one socket and dispatches on the negotiated protocol; `uri.HTTPSURL` derives the https form; `cmd/interop-client` switches adapter on the URL scheme. |
| 3.1.4   | WebTransport                         | DONE   | `wtconn` adapter (webtransport-go); MOQT identifiers offered as the WebTransport sub-protocol both ways. |
| 3.1.5   | Native QUIC                          | DONE   | `quicconn` adapter (quic-go). |
| 3.1.6   | Connection URL                       | DONE   | Descriptive: a track MAY have connection URLs, and the section defers their syntax and setup to the transport mapping — which §3.1.3-3.1.5 above implement. Nothing further is required of an endpoint. |
| 3.2     | Extension negotiation                | DONE   | SETUP options exchanged as KV pairs; peer options parsed. |
| 3.2.1   | Reserved namespaces                  | DONE   | `AcceptRequest` rejects an exact `.` first field with DOES_NOT_EXIST; other `.`-prefixed namespaces pass through to the application per spec. |
| 3.2.2   | Session-level tracks/namespaces      | DONE   | `.session` requests are rejected with DOES_NOT_EXIST before the application/relay sees them (no session-level extensions implemented), so relays never forward them; covers the empty-track-name rule. |
| 3.3     | Session initialization               | DONE   | Control streams + SETUP exchange; early data-stream buffering. A bidi stream opening with anything but the seven request messages closes the session with PROTOCOL_VIOLATION (`AcceptRequest`). |
| 3.3.2   | Graceful request stream closure      | DONE   | A FIN is not a cancel: the relay keeps a FIN'd request alive until the peer cancels, and FINs back to complete a finished FETCH. |
| 3.3.3   | Request cancellation / rejection     | DONE   | STOP_SENDING, stream resets, REQUEST_ERROR in `request.go`; `Close()` on request handles cancels. |
| 3.3.4   | Stream reset error codes             | DONE   | All codes in `errors.go` (`StreamReset*`). |
| 3.4     | Unidirectional stream types          | DONE   | SUBGROUP / FETCH / PADDING / SETUP type IDs dispatched. |
| 3.5     | Termination                          | DONE   | Session error codes; `Close()` sends CONNECTION_CLOSE w/ reason. |
| 3.6     | Migration (GOAWAY)                   | DONE   | `SendGoaway`/`OnGoaway`/`PeerGoaway`, new-session URI (draft-19 removed the Request-ID watermark field). |
| 3.3.1   | 0-RTT                                | N/A (transport) | No app-level 0-RTT handling; QUIC stack provides it. |
| 3.7     | Congestion control                   | N/A (transport) | No app-level pacing/bufferbloat logic (§3.7.1–3). |

## §5 Publishing and retrieving tracks

| §       | Feature                          | Status | Notes |
|---------|----------------------------------|--------|-------|
| 5.1     | Subscriptions                    | DONE   | Subscribe/Publish/OK/Error state machine in `pubsub.go`. |
| 5.1.1   | Subscription state management    | DONE   | REQUEST_ERROR / STOP_SENDING / PUBLISH_DONE handling + cleanup. |
| 5.1.2   | Location filters                 | DONE   | Every start/end form (unfiltered, Next Object, relative and absolute start, absolute range) + `Matches`. |
| 5.1.3   | Fill semantics                   | PARTIAL | Fill fetch streams from FILL_PARAMETERS on SUBSCRIBE / REQUEST_UPDATE (`handler_fill.go`). Fill streams do not inherit the subscription's Range Filters, and SUBSCRIBE_TRACKS opens none (see backlog). |
| 5.1.4   | Range filters                    | DONE    | Object filters (SUBGROUP/OBJECTID/PRIORITY/OBJECT_PROPERTY) enforced on SUBSCRIBE fanout, datagrams, and FETCH; TRACK_PROPERTY_FILTER gates PUBLISH forwarding on SUBSCRIBE_TRACKS; `MAX_FILTER_RANGES`/`INVALID_FILTER` gating in place. Object filters on a SUBSCRIBE_TRACKS apply to the subscriptions its forwarded PUBLISHes open. One documented carve-out (see Known protocol gaps): REQUEST_UPDATE whole-set replace vs per-type merge. |
| 5.1.5   | Combining filters                | DONE    | `ForwardDecision` ANDs Forward + Location + Range filters per object (§5.1.5); Range filters combine SetIDs via AND/OR. |
| 5.1.6   | Joining an ongoing track         | DONE   | A Location Filter plus FILL_PARAMETERS, served as a fill fetch stream (draft-20 removed the Joining FETCH). |
| 5.1.6.1 | Dynamically starting new groups  | DONE   | Relay forwards a downstream `NEW_GROUP_REQUEST` upstream per §10.2.19: included in the on-demand upstream SUBSCRIBE (no established upstream) or sent as an upstream REQUEST_UPDATE, gated on `DYNAMIC_GROUPS` support, Largest-Group, and outstanding-request bookkeeping. |
| 5.2     | Fetch state management           | DONE   | FETCH lifecycle. |

## §6 Namespace discovery

| §     | Feature                    | Status | Notes |
|-------|----------------------------|--------|-------|
| 6.1   | Subscribing to namespaces  | DONE   | `SubscribeNamespace` / `SubscribeTracks` / `ReadPublishSkipped`. |
| 6.2   | Publishing namespaces      | DONE   | `PublishNamespace`; NAMESPACE / NAMESPACE_DONE messages. |

## §7 Priorities

| §     | Feature                    | Status  | Notes |
|-------|----------------------------|---------|-------|
| 7.1   | Definitions                | DONE    | Subscriber/publisher priority + group order modeled. |
| 7.2   | Scheduling algorithm       | PARTIAL | `EffectiveStreamPriority` builds the composite `session.StreamPriority` (subscriber→publisher→group-order key→subgroup); FETCH ordering is group-order + Object-ID per §10.13. The fill-vs-subscription ordering of rules 3 and 4 (a subscription-delivered object first when the fill's Group Order differs; the fill-delivered one first within a group) is not implemented: fill streams carry no priority input. The datagram-wins tie-break (rule 4) holds by construction: datagrams bypass this priority key entirely and are sent as soon as ready, never queued behind a subgroup stream's priority. Transport knob is currently a no-op (quic-go exposes no per-stream priority API — [quic-go#437](https://github.com/quic-go/quic-go/issues/437)), so the order is computed and pushed through `session.PrioritizedSendStream` (propagation is test-covered) but not yet enforced on the wire. |
| 7.3   | Considerations for setting | DONE    | Relay honours subscriber/publisher priority on fanout. |

## §8 Delivery timeouts and data reliability

| §   | Feature                       | Status | Notes |
|-----|-------------------------------|--------|-------|
| 8   | Delivery timeouts / reliability| PARTIAL| OBJECT_DELIVERY_TIMEOUT enforced on subgroup streams in `session/datastream_out.go`; SUBGROUP_DELIVERY_TIMEOUT only where the transport reports acknowledgement (`session.DeliveryTrackingSendStream`), which no bundled adapter does, so not on quic-go or WebTransport; reset w/ `StreamResetDeliveryTimeout`. Datagrams are never dropped for either timeout (§8 "MUST drop the datagrams"). See Limitations. OBJECT_DELIVERY_TIMEOUT is measured per object from its receipt time (`WriteObjectReceivedAt`), not from stream open. `WithDeliveryTimeouts` takes the publisher's and subscriber's halves separately so the §12.1/§12.2 first-object override resolves within the publisher's half before `DeliveryTimeouts.Effective` takes the smaller of the two. The relay sources both sides — the publisher's Track Properties (decoded once onto the entry) and the subscriber's SUBSCRIBE parameters (§10.2.3/§10.2.4) — and passes them to every subgroup stream it opens downstream, resetting that stream alone with DELIVERY_TIMEOUT while the subscription continues. Not enforced on the raw `Write` path (no object boundaries) or inbound — see Limitations. |

## §9 Relays

| §     | Feature                              | Status | Notes |
|-------|--------------------------------------|--------|-------|
| 9.1   | Caching relays                       | DONE   | LRU+TTL object cache (`cache/cache.go`); updates limited to non-existence/properties. |
| 9.2   | Forward handling                     | DONE   | FORWARD flag honoured; Forward=0 pauses delivery. Upstream Forward is set to 1 only when a downstream subscriber forwards, else the relay pauses it (Forward=0) and resumes on the first forwarding subscriber. |
| 9.3   | Multiple publishers                  | DONE   | Per-track upstreams; dedup by `{GroupID, ObjectID}`. |
| 9.4   | Subscriber interactions              | DONE   | Upstream subscription established before SUBSCRIBE_OK; aggregation. |
| 9.4.1 | Graceful subscriber switchover       | DONE   | GOAWAY grace period (`GoawayTimeout`). |
| 9.5   | Publisher interactions               | DONE   | PUBLISH_NAMESPACE / PUBLISH with prefix matching (`namespace_registry.go`). |
| 9.5.1 | Graceful publisher switchover        | DONE   | Concurrent upstreams + cache dedup. |
| 9.6   | Relay track handling                 | DONE   | Properties captured once at track creation, forwarded opaquely. |
| 9.7   | Relay object handling                | DONE   | Objects forwarded verbatim except alias remap + Object-ID delta re-encode. |

## §10 Control messages

| §       | Message / option              | Type   | Status | Notes |
|---------|-------------------------------|--------|--------|-------|
| 10      | Message framing               | —      | DONE   | An unknown type, or a body that does not match its Length or fails validation, closes the session with PROTOCOL_VIOLATION (`message.ErrMalformedMessage`) wherever the session or the relay reads: the control stream, request openers, responses, follow-ups read by `RequestBroker.Serve` or `ReadPublishSkipped`, and the relay's own readers. A frame cut short by FIN or reset only ends its stream. A handle the application reads with `message.Parse` itself is checked only if it applies the rule (see Validation). |
| 10.1    | Request-ID parity/duplicates  | —      | DONE   | Enforced in `AcceptRequest` and on REQUEST_UPDATE: per-role parity, and duplicate detection that tolerates reordering. |
| 10.2    | Message parameters (20 types) | —      | DONE   | All 20 defined with correct kinds; unknown and duplicate parameters close the session; see §10.2.x below. |
| 10.2.1  | Parameter scope               | —      | DONE   | Per-message scope validation at every session receive point and the relay's own readers (`Parameters.CheckScope`). |
| 10.2.2  | AUTHORIZATION_TOKEN           | 0x03   | DONE   | 4 alias types; session token cache resolves inbound. |
| 10.2.3  | SUBGROUP_DELIVERY_TIMEOUT     | 0x06   | PARTIAL| Parsed and resolved; the stream reset is not enforced on the bundled transports, and datagrams are not dropped (see §8). |
| 10.2.4  | OBJECT_DELIVERY_TIMEOUT       | 0x02   | DONE   | |
| 10.2.5  | FILL_TIMEOUT                  | 0x0A   | DONE   | |
| 10.2.6  | RENDEZVOUS_TIMEOUT            | 0x04   | DONE   | |
| 10.2.7  | SUBSCRIBER_PRIORITY           | 0x20   | DONE   | |
| 10.2.8  | GROUP_ORDER                   | 0x22   | DONE   | Ascending/Descending validated. |
| 10.2.9  | LOCATION_FILTER               | 0x21   | DONE   | Overflow-checked. |
| 10.2.10 | SUBGROUP_FILTER               | 0x25   | DONE   | Enforced per object in the fanout/FETCH. |
| 10.2.11 | OBJECTID_FILTER               | 0x26   | DONE   | Enforced per object in the fanout/FETCH. |
| 10.2.12 | PRIORITY_FILTER               | 0x27   | DONE   | Enforced per object (subgroup priority); >255 rejected INVALID_FILTER. |
| 10.2.13 | OBJECT_PROPERTY_FILTER        | 0x28   | DONE   | Enforced per object against Object Properties; even property type. |
| 10.2.14 | TRACK_PROPERTY_FILTER         | 0x29   | DONE   | Gates PUBLISH forwarding on SUBSCRIBE_TRACKS against Track Properties; even property type. |
| 10.2.15 | FILL_PARAMETERS               | 0x23   | PARTIAL| Inner Table 6 scope and duplicates checked; omitted Range Filters are not inherited from the subscription (see §5.1.3). |
| 10.2.16 | EXPIRES                       | 0x08   | DONE   | |
| 10.2.17 | LARGEST_OBJECT                | 0x09   | DONE   | Monotonic constraint applied. |
| 10.2.18 | FORWARD                       | 0x10   | DONE   | |
| 10.2.19 | NEW_GROUP_REQUEST             | 0x32   | DONE   | |
| 10.2.20 | TRACK_NAMESPACE_PREFIX        | 0x34   | DONE   | Applied on REQUEST_UPDATE; SUBSCRIBE_NAMESPACE reconciles its announced set. |
| 10.2.21 | INCLUDE_PROPERTIES            | 0x35   | PARTIAL| Parsed and scope-checked; a value other than 0 or 1 closes the session. Not applied (see backlog). |
| 10.3    | SETUP                         | 0x2F00 | DONE   | Bidirectional handshake; options as KV pairs. |
| 10.3.1.1| AUTHORITY option              | 0x05   | PARTIAL| Sent (`WithAuthority`) and carried as a SETUP KV pair, but never validated on receipt: `SessionInvalidAuthority` is unused — see Limitations. |
| 10.3.1.2| PATH option                   | 0x01   | PARTIAL| Sent (`WithPath`) and carried as a SETUP KV pair, but never validated on receipt: `SessionInvalidPath` is unused — see Limitations. |
| 10.3.1.3| MAX_AUTH_TOKEN_CACHE_SIZE      | 0x04   | DONE   | Sizes the token cache. |
| 10.3.1.4| AUTHORIZATION_TOKEN (setup)   | 0x03   | PARTIAL| Received tokens are applied to the token cache as a request's are (REGISTER over the cache size is used as a value; DELETE / USE_ALIAS from a client closes the session) and exposed by `Session.SetupTokens`. Not sendable (see backlog). |
| 10.3.1.5| MOQT_IMPLEMENTATION           | 0x07   | DONE   | Advisory. |
| 10.3.1.6| MAX_FILTER_RANGES             | 0x06   | DONE   | `WithMaxFilterRanges` advertises it; relay rejects over-limit/prohibited filters with INVALID_FILTER. The relay advertises `relay.DefaultMaxFilterRanges` (16) rather than inheriting the session default of 0, which would prohibit the Range Filters it fully implements; `relay.Config.MaxFilterRanges` overrides, negative to prohibit. |
| 10.3.1.7| MAX_REQUEST_UPDATES           | 0x08   | DONE   | `WithMaxRequestUpdates` advertises the per-stream limit; enforced on inbound follow-ups via `RequestUpdateLimiter`, closing with `TOO_MANY_REQUEST_UPDATES` on overflow. |
| 10.4    | GOAWAY                        | 0x10   | DONE   | Same encoding on control and request streams (draft-19 dropped the Request ID field); callback. As recipient the relay initiates no new SUBSCRIBE, FETCH or PUBLISH to the peer and leaves closing the session to the sender. |
| 10.5    | REQUEST_OK                    | 0x07   | DONE   | Shared OK for PUBLISH/UPDATE/TRACK_STATUS/namespace reqs. |
| 10.6    | REQUEST_ERROR (+ Redirect)    | 0x05   | DONE   | Redirect required only when code==REDIRECT. |
| 10.7    | SUBSCRIBE                     | 0x03   | DONE   | |
| 10.8    | SUBSCRIBE_OK                  | 0x04   | DONE   | Registers inbound track alias. |
| 10.9    | REQUEST_UPDATE                | 0x02   | DONE   | A REQUEST_UPDATE opening a request stream closes the session with PROTOCOL_VIOLATION (`ErrUnexpectedRequestUpdate`). |
| 10.10   | PUBLISH_STATE_NOTIFY          | 0x22   | DONE   | Only the publisher may send it; enforced by brokers and the relay. |
| 10.11   | PUBLISH                       | 0x1D   | DONE   | |
| 10.12   | PUBLISH_DONE                  | 0x0B   | DONE   | Sent once every stream of the subscription has closed and no datagram send is in progress, with the exact Stream Count; written on its own goroutine, so subscribers do not wait on each other. |
| 10.13   | FETCH                         | 0x16   | DONE   | Standalone, the only kind in draft-20. |
| 10.14   | FETCH_OK                      | 0x18   | DONE   | An End Location before the FETCH's Start closes the session. A Start relative to the Largest Object is compared through End ≤ Largest; an End of {0,0} is let through, as it cannot be told apart from "no content yet". |
| 10.15   | TRACK_STATUS                  | 0x0D   | DONE   | Reply via REQUEST_OK, then FIN; any follow-up from the requester closes the session. |
| 10.16   | PUBLISH_NAMESPACE             | 0x06   | DONE   | |
| 10.17   | NAMESPACE                     | 0x08   | DONE   | Per namespace, counted over local and remote sources. |
| 10.18   | NAMESPACE_DONE                | 0x0E   | DONE   | Never before its NAMESPACE. |
| 10.19   | SUBSCRIBE_NAMESPACE           | 0x50   | DONE   | |
| 10.20   | SUBSCRIBE_TRACKS              | 0x51   | DONE   | §10.20.1: its SUBSCRIBE parameters become each forwarded PUBLISH's subscription; an out-of-range value closes the session (§10.2.8/§10.2.18). |
| 10.21   | PUBLISH_SKIPPED               | 0x0F   | DONE   | Prohibition scoped to a single PUBLISH (§6.1) — not sticky across re-PUBLISHes. |

## §11 Data streams and datagrams

| §        | Feature                              | Status  | Notes |
|----------|--------------------------------------|---------|-------|
| 11.1     | Track alias                          | DONE    | In subgroup header + datagram; validated. |
| 11.2     | Objects / object header              | DONE    | All header fields encoded. |
| 11.2.1.1 | Object status                        | DONE    | Normal / EndOfGroup / EndOfTrack. |
| 11.2.1.2 | Object properties                    | DONE    | Length-prefixed KV pairs. |
| 11.3     | Object datagram                      | DONE    | Type bit-fields + invalid-combo rejection. |
| 11.4     | Streams (subgroup / fetch)           | DONE    | Typed in/out subgroup + fetch streams. |
| 11.4.1   | Stream cancellation                  | DONE    | Bidi request-stream termination ends the request (handlers unregister on stream end); the relay sends PUBLISH_DONE on graceful subscription termination rather than abrupt reset. |
| 11.4.2   | Subgroup header + delta object IDs   | DONE    | All subgroup-ID modes; `ReadDecoded` resolves deltas. A subgroup stream whose Track Alias is not bound yet is held unread (§11.4.2 MAY buffer): `session.Demux` parks it, count-bounded; the relay waits up to 1 s (`IncomingSubgroupStream.AwaitInboundTrack`, at most 32 streams per session, the rest reset with EXCESSIVE_LOAD). The §11.4.2 MUST to give control streams connection flow control first is not met by the bundled transports, so enough early data can delay the SUBSCRIBE_OK until the relay's wait runs out and the streams are reset. |
| 11.4.3   | Closing subgroup streams             | DONE    | Relay forwards only the next object on a stream (gap → reset+reopen), FINs on clean inbound EOF, resets on inbound reset, resets with MALFORMED_TRACK after a terminal EndOfGroup/EndOfTrack object (§2.4.2), marks reliable boundaries for RESET_STREAM_AT (`SetReliableBoundary`, transport-gated on `EnableStreamResetPartialDelivery`), and resets (not FINs) in-flight subgroups whose group falls out of range after a narrowing REQUEST_UPDATE. |
| 11.4.4   | Fetch header                         | DONE    | Serialization Flags of 128 or more that are not an End of Range close the session. |
| 11.4.4.1 | Fetch flags                          | DONE    | All subgroup modes + delta/priority/properties/status flags. A first Object that references a prior Object's fields closes the session. |
| 11.4.4.2 | End of range                         | DONE    | Non-existent (0x8C) / unknown (0x10C) handled. An Object after a leading marker that references a prior Subgroup ID or Priority closes the session. |
| 11.5     | Padding streams & datagrams          | DONE    | Recognised type IDs silently discarded. |

## §12 MOQT properties

| §     | Property                       | Type | Status | Notes |
|-------|--------------------------------|------|--------|-------|
| 12.1  | SUBGROUP_DELIVERY_TIMEOUT      | 0x06 | PARTIAL| Track + Object Property; the first object of a subgroup overrides the Track-level value (§8 resolution in `message.DeliveryTimeouts`, enforced in `OutgoingSubgroupStream` where the transport reports acknowledgement — none of the bundled ones do, see §8). |
| 12.2  | OBJECT_DELIVERY_TIMEOUT        | 0x02 | DONE   | Track + Object Property; first-object override, as §12.1. |
| 12.3  | MAX_CACHE_DURATION             | 0x04 | DONE   | Lazy age-eviction in cache. |
| 12.4  | DEFAULT_PUBLISHER_PRIORITY     | 0x0E | DONE   | |
| 12.5  | DEFAULT_PUBLISHER_GROUP_ORDER  | 0x22 | DONE   | Validated. |
| 12.6  | DYNAMIC_GROUPS                 | 0x30 | DONE   | Property defined & scope-validated (flow: see §5.1.6.1). |
| 12.7  | Immutable properties           | 0x0B | DONE   | Relays cache & forward verbatim, never add. |
| 12.8  | Prior group ID gap             | 0x3C | DONE   | Object-scope; encoder in `msf/groupid.go`. |
| 12.9  | Prior object ID gap            | 0x3E | DONE   | Object-scope. |

## §13 Security considerations

Most of §13 is advice the draft delegates to the transport or to deployment
policy. This library provides the hooks; enforcement is the operator's.

| §      | Concern                          | Status | Notes |
|--------|----------------------------------|--------|-------|
| 13.1   | Subscription amplification       | DONE   | `Config.MaxSubscriptionsPerSession` caps concurrent subscriptions per session, rejecting excess with EXCESSIVE_LOAD before state mutation (0 = unlimited). |
| 13.2   | Communication security           | N/A (transport) | TLS 1.3 via QUIC/WebTransport. |
| 13.3   | Authorization                    | DONE   | `Authorizer` hook gates every request once before state mutation. |
| 13.3.2 | Replay attacks                   | PARTIAL| Session-scoped token cache; replay defence delegated to token scheme. |
| 13.4   | Media security                   | N/A    | Payloads opaque; E2EE (e.g. SFrame) is external. |
| 13.5   | Resource exhaustion              | DONE   | QUIC flow control + slow-reader reset (`fanout.go`) + per-session subscription/namespace caps; the publisher cancels lowest-priority streams on overload. Global cross-session quotas remain a deployment concern. |
| 13.6   | Timeouts                         | PARTIAL| Delivery timeouts enforced (§8), except SUBGROUP_DELIVERY_TIMEOUT on the bundled transports. |
| 13.6.1 | Idle connection handling         | PARTIAL| Keep-alive options documented; not enforced in-library. |
| 13.7   | Relay security                   | DONE   | §13.7.1: `Config.MaxNamespaceRequestsPerSession` bounds PUBLISH_NAMESPACE/SUBSCRIBE_NAMESPACE/SUBSCRIBE_TRACKS state per session (EXCESSIVE_LOAD). §13.7.2: the `Authorizer` hook gates short-prefix subscriptions. |
| 13.8   | Implementation fingerprinting    | DONE   | MOQT_IMPLEMENTATION optional/configurable. |

## §14 Grease

| §   | Feature | Status | Notes |
|-----|---------|--------|-------|
| 14  | GREASE  | DONE   | `IsGrease`/`GreaseValue`/`GreaseSetupOption`; unknown values ignored. |

## Limitations

Out of scope in the relay's cross-instance routing: multi-hop **loop detection**
(the only guard is skipping the relay's own `RelayAddr`), an upstream
**connection-health / redial policy** beyond dial-on-demand, and GOAWAY
**cascading**. `cmd/relay` stays single-instance by choice — the distributed
`DiscoveryStore` backend ships as its own binary out of tree, so the core module
never pulls in its client library.

Known protocol gaps, roughly ordered by how load-bearing they are:

- **Range Filter REQUEST_UPDATE semantics (§5.1.4)** — updating a
  subscription's Range Filters mid-stream replaces the *whole* filter set rather
  than the spec's per-parameter-type replace (non-zero Length) / remove
  (Length 0) with untouched types preserved. So a partial REQUEST_UPDATE wipes
  other filter types, and a Length-0 "remove" param is rejected as
  INVALID_FILTER instead of removing that type. Initial SUBSCRIBE/FETCH filtering
  and adding filters on update work; the per-type merge is a tracked follow-up.

- **PATH / AUTHORITY are sent but never validated on receipt (§10.3.1.1,
  §10.3.1.2)** — `WithPath` / `WithAuthority` emit the SETUP parameters, but
  nothing checks them on the receiving side: `SessionInvalidPath` (0x8) and
  `SessionInvalidAuthority` (0x19) are defined in `pkg/moqt/errors.go` and used
  nowhere. Enforcement is also per transport mapping, and `relaynet.Listen` does
  not record which mapping a session arrived on (it merges both into one accept
  queue) — recoverable by conn type, since `quicconn` and `wtconn` are distinct
  implementations, but not currently carried.
- **Delivery-timeout enforcement is outbound-only, subgroup-only, not on the
  raw path, and SUBGROUP_DELIVERY_TIMEOUT needs transport support (§8)** — the
  publisher side is wired end to end: `OutgoingSubgroupStream` resolves both
  timeouts (including the §12.1/§12.2 first-object override) and enforces
  OBJECT_DELIVERY_TIMEOUT, and the relay's fanout applies both halves to every
  subgroup stream it opens, sourcing the publisher's from the entry's Track
  Properties and the subscriber's from the SUBSCRIBE parameters. Four gaps
  remain. SUBGROUP_DELIVERY_TIMEOUT is enforced only on a transport that
  reports acknowledgement, which the bundled ones do not (see its own entry
  below). Datagrams are sent regardless of age: neither `SendDatagram` nor
  the relay's datagram forwarding drops an expired one, where §8 says the
  implementation "MUST drop the datagrams if the time elapsed exceeds
  OBJECT_DELIVERY_TIMEOUT" (SUBGROUP_DELIVERY_TIMEOUT acting the same way for
  datagrams). The raw `Write` escape hatch does not enforce OBJECT_DELIVERY_TIMEOUT
  at all: §8 measures it per object from that object's receipt, and a caller
  managing its own framing is the only party that knows either fact, so the
  check belongs to `WriteObjectReceivedAt`. And the inbound (subscriber-side)
  path enforces no timeout — a subscriber does not police how long the relay
  takes to deliver a subgroup it was promised.

  The relay's receipt time is also approximate. §8 names "the last header byte"
  of the object; the fanout passes `fwdObject.enqueuedAt`, stamped once the
  object has been read whole, deduped and cached, so the clock starts late by
  the object's inbound transfer time. The error is always lenient and scales
  with object size. Fixing it means recording the instant in the inbound read
  and carrying it separately from `enqueuedAt`, which cannot be reused: it is
  the `MaxFanoutLag` measurement, and that window means time spent queued rather
  than object age.

  Note that the §3.3.4 reset codes are not interchangeable here, and the fanout
  keeps them apart deliberately. A delivery timeout resets the one stream with
  `DELIVERY_TIMEOUT` ("a delivery timeout was exceeded for this stream") and
  leaves the subscription live; only the `MaxFanoutLag` window uses
  `TOO_FAR_BEHIND`, whose §3.3.4 definition says the subscription "is being
  terminated". Collapsing the two would make a per-subgroup timeout cost the
  subscriber the whole track — and it is precisely the survivable variant that
  lets a publisher stripe sheddable data across subgroups the relay may drop
  under load.

- **`MAX_REQUEST_UPDATES` enforcement is receive-side only, and cannot trip
  under our own processing (§10.3.1.7)** — we advertise the limit and enforce
  it on inbound follow-ups (`RequestUpdateLimiter`), but every follow-up reader
  (`RequestBroker.Serve`, the relay's per-stream loops) answers each
  REQUEST_UPDATE synchronously before reading the next, so a stream never holds
  more than one outstanding update and the check only ever fires against a peer
  that pipelines faster than a hypothetical async responder would drain. §10.3.1.7
  explicitly permits an immediate responder not to detect such a peer. We do not
  self-limit *outbound* REQUEST_UPDATEs against a peer's advertised value for the
  same reason: `UpdateRequest`/`RequestBroker.Update` are synchronous
  write-then-read, so they never exceed any limit ≥ 1.
- **Out-of-range GROUP_ORDER on FETCH (§10.2.8)** — the SUBSCRIBE and
  SUBSCRIBE_TRACKS paths now close the session with PROTOCOL_VIOLATION on an
  out-of-range GROUP_ORDER/FORWARD (§10.2.8/§10.2.18), but the FETCH path
  still reads a bad GROUP_ORDER as Ascending pending the same promotion.
- **Late publisher pickup (§9.5)** — multiple publishers per track are merged
  and deduplicated, and a local PUBLISH_NAMESPACE that arrives after a track's
  upstream set is established is SUBSCRIBEd for each matching track. Two
  deliberate deviations from "for each matching subscription": a track with no
  downstream subscriber is skipped (an on-demand upstream is released only
  when its last downstream leaves, so one opened then would never be), and so
  is a track the new publisher itself receives from the relay. Each later
  downstream SUBSCRIBE on the track asks every registered matching publisher
  that has no upstream for it, so a skipped one is picked up then. A refusal
  on that path holds for its §10.6.2 Retry Interval, or while the track entry
  and the publisher's registration last when the interval is 0. A *remote relay* that Discovery
  starts resolving later is not pulled in until the set drains and a fresh
  SUBSCRIBE re-establishes it.
- **Subscriber-priority scheduling (§7.2 / §10.2.7)** — fully plumbed but not
  enforced on the wire: the §7.2 composite key is computed
  (`EffectiveStreamPriority`) and pushed through `session.PrioritizedSendStream`,
  but quic-go and webtransport-go expose no per-stream priority API today
  ([quic-go#437](https://github.com/quic-go/quic-go/issues/437)), so the bundled
  adapters absorb the knob and quic-go round-robins instead. A REQUEST_UPDATE
  that changes priority mid-stream applies only to subsequently opened subgroups.
- **LOC encryption / SecureObjects and Private Properties** — intentionally out
  of scope pending a chosen SecureObjects revision. Some property IDs are
  draft-tentative (e.g. `PropAudioLevel = 0x0A`, pending IANA assignment).
- **MSF** — no timeline GZIP compression, content protection (§4.3), token
  authorization, or logs/analytics. No built-in ABR helper: every catalog field
  a selector needs is surfaced (AltGroup, Width/Height, Bitrate, RenderGroup,
  Depends, TemporalID, SpatialID), but variant-selection policy is the
  application's job.
- **Multi-publisher default priority downstream (§11.4.2, §12.4)** — the relay
  resolves each upstream's inherited DEFAULT_PUBLISHER_PRIORITY per alias, but
  forwards the DEFAULT_PRIORITY bit unchanged. A downstream subscriber therefore
  inherits the default from the SUBSCRIBE_OK it was sent, which carries the
  first publisher's properties.
- **PUBLISH_DONE waits on the subscription's streams (§10.12)** — as the draft
  requires, so it is as
  late as the slowest of them to close. A terminated subscription takes no new
  Object: its stream is reset at the next one, so while its upstream is live
  it waits at most for that. A publisher that ends a track but leaves a
  subgroup stream open, and sends nothing more on it, holds the relay's
  PUBLISH_DONE until that stream ends or its session does. A subscriber not
  reading its data streams holds its own: subgroup streams until the fanout
  writer's drain limit resets them, a fill fetch stream until the session
  ends (fill writes have no deadline). The goroutine that writes PUBLISH_DONE
  is not joined by `Relay.Stop`; it ends when its session does.
- **SUBGROUP_DELIVERY_TIMEOUT is not enforced on quic-go / WebTransport (§8)**
  — the reset needs to know when the peer has acknowledged the whole stream
  ("all data committed"). quic-go tracks that internally but exposes no API for
  it (quic-go#3291), and webtransport-go wraps quic-go. `OutgoingSubgroupStream`
  enforces the timeout only on a stream implementing
  `session.DeliveryTrackingSendStream`, which no bundled adapter does, so a
  subgroup stuck behind congestion is not reset with DELIVERY_TIMEOUT. The
  timer alone is not used instead: it would reset streams the peer already
  holds in full. Closing this needs an acknowledgement signal from quic-go,
  then `Finished()` in quicconn and wtconn.
- **TRACK_NAMESPACE_PREFIX encoding (§10.2.20)** — encoded length-prefixed, as
  moxygen, moqtail and libquicr do. The draft text reads as a bare Track
  Namespace. Open WG issue: moq-wg/moq-transport#1942.
- **Repeated AUTHORIZATION TOKENs (§10.2.2)** — a message "MAY" repeat the
  parameter "as long as the combination of Token Type and Token Value are
  unique after resolving any aliases". Uniqueness is not checked, in SETUP or
  in requests; the draft names no action for the receiver.
- **Handles the application reads itself (§10, §10.2.1, §10.9, §10.10)** —
  REQUEST_UPDATE / PUBLISH_STATE_NOTIFY roles and Message Parameter scope are
  enforced by brokers from typed handles'
  `Broker()`, by the session's own reads, and by the relay. Handles the
  application reads itself (the namespace handles, `FetchResponder`, or any
  stream read with `message.Parse`) are checked only if it calls
  `Session.CheckPeerParams`. Likewise for §10 framing: such a reader must close
  the session itself on an error wrapping `message.ErrMalformedMessage`, or read
  through `Session.NewRequestBroker(stream).Serve`, which does.
- **Duplicate upstream SUBSCRIBEs (§5.1)** — two to one publisher for one
  track can both go out when
  two downstream SUBSCRIBEs race for a track with no upstream yet, or one
  races a §9.5 late-publisher SUBSCRIBE (late-publisher SUBSCRIBEs themselves
  are deduplicated). §5.1 allows it; it costs a second upstream subscription.
  A Track Alias the publisher shares between them stays routed until both end.

### Draft-20 compliance review backlog

A full review against draft-ietf-moq-transport-20 (dated August 2026) found
the gaps below, which are still open. Items are grouped by area; each names the
rule it misses.

Found while fixing:

- **REQUEST_OK Track Properties on send (§10.5)** — receipt is enforced.
  `Request.Reply` still sends whatever it is given, so an application can emit a
  non-empty PUBLISH_OK.
- **AUTHORIZATION TOKEN setup option (§10.3.1.4)** — received tokens are
  applied, but none can be sent: there is no Option for it, and so no purge
  of a REGISTER the peer's MAX_AUTH_TOKEN_CACHE_SIZE could not hold.

Request lifecycle:

- The relay's REQUEST_UPDATE_OK to a downstream subscriber never carries
  LARGEST_OBJECT (§10.9.1, §10.2.17). The session's `Publication` handling
  does.

Validation:

- Object Properties are never validated on receipt: nested Immutable
  Properties, duplicate gap properties, and Mandatory Track Properties used as
  Object Properties (§12.7–§12.9, §2.5.1).
- OBJECT/SUBGROUP_DELIVERY_TIMEOUT inside Immutable Properties is ignored
  (§12.7).
- AUTHORITY / PATH are not validated against RFC 3986 (MALFORMED_AUTHORITY /
  MALFORMED_PATH, §10.3.1.1–2). The PATH / AUTHORITY entry in Limitations
  covers part of this.
- A zero-length Range Filter is rejected everywhere, including the initial
  SUBSCRIBE and FETCH, although §5.1.4 defines it as "no filter". The Range
  Filter REQUEST_UPDATE entry in Limitations covers the update case.
- A data stream that arrives before the control streams fails the handshake
  (§3.3 SHOULD buffer).

Relay:

- `Request.RejectError` always sends Retry Interval 0 ("SHOULD NOT be
  retried", §10.6.2). So the relay turns an upstream's "retry in N ms" into a
  permanent refusal when it passes the rejection downstream, and its
  EXCESSIVE_LOAD limit rejections never invite a retry.
- Mandatory Track Properties (§2.5.1) are enforced on PUBLISH and upstream
  SUBSCRIBE_OK (`Config.KnownMandatoryTrackProperties`), but not on an
  upstream FETCH_OK. There the MUST is unmet: the FETCH fails the way any failed
  upstream FETCH does (an unknown range), not with UNSUPPORTED_EXTENSION.
- MAX_CACHE_DURATION (§12.3) is enforced on cache reads and the live path, but:
  - FETCH and fill write their cache snapshot without re-checking age, so a
    blocked write can start sending an expired Object; a drop there needs an
    End of Unknown Range marker.
  - Objects that expire out of arrival order show up in FETCH as plain gaps,
    which assert non-existence, where §12.3 says their state is unknown.
  - It is per track, the first upstream's value (first-setter-wins
    Properties), where §12.3 says "this subscription or fetch".
  - A live writer opened before the track's Properties arrive (the #85
    window) does not enforce it.
- Namespace subscriptions:
  - a Discovery event the store drops (MemoryStore does, for a slow consumer)
    is not recovered until the watch restarts, so a remote namespace can be
    missing or linger; a restart sends NAMESPACE_DONE then NAMESPACE for every
    remote namespace still advertised;
  - a subscriber whose stream is blocked by flow control grows its message
    queue without bound; §10.19 lets the relay reset the stream instead.
- SUBSCRIBE_TRACKS:
  - FILL_PARAMETERS and NEW_GROUP_REQUEST on a SUBSCRIBE_TRACKS are accepted
    but do nothing: a forwarded PUBLISH's subscription gets no fill stream
    (§10.20.1 names FILL_PARAMETERS for joining);
  - a track that gains an upstream through the relay's own SUBSCRIBE, rather
    than an inbound PUBLISH, after the SUBSCRIBE_TRACKS arrived is not
    forwarded to it.
  - a TRACK_NAMESPACE_PREFIX update forwards nothing for tracks that already
    exist under the new prefix; only later PUBLISHes are forwarded;
  - INCLUDE_PROPERTIES=0 is ignored: forwarded PUBLISHes (and SUBSCRIBE_OK)
    still carry Track Properties (§10.2.21 SHOULD).
- Fill streams do not inherit the subscription's Range Filters (§5.1.3).
- Fill streams are not scheduled against their subscription (§7.2 rules 3
  and 4): a subscription-delivered object should go first when the fill's
  Group Order differs, and the fill-delivered one first within a group.
- Upstream PUBLISH_DONE codes are flattened to TRACK_ENDED; §10.12 asks for "a
  relevant status code".
- Duplicate objects from redundant upstreams are not compared (§9.1).
- A subgroup stream from which Objects were omitted because the subscription
  was paused (Forward State 0) ends with a FIN when the inbound subgroup does,
  where §11.4.3 lists "Omitting a Subgroup Object due to the subscriber's
  Forward State" among the cases that MUST reset the stream.
- After an inbound GOAWAY the relay stops initiating requests to that peer
  (§10.4) but, as its subscriber, neither unsubscribes ("A subscriber SHOULD
  individually unsubscribe from each existing subscription"), nor migrates to
  the New Session URI (§9.4.1), nor closes the session once no subscriptions
  remain (§3.6 RECOMMENDED). It waits for the sender to close.
