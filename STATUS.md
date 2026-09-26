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
  failover, serves FETCHes from a per-track cache (asking an upstream FETCH
  about what the cache cannot vouch for), issues on-demand upstream SUBSCRIBEs to every matching
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
| 3.3     | Session initialization               | DONE   | Control streams + SETUP exchange. Up to 32 data streams that arrive before the peer's control stream are held unread and delivered by `AcceptDataStream` once setup completes (more are refused with EXCESSIVE_LOAD); request streams wait in the transport until `AcceptRequest`. A bidi stream opening with anything but the seven request messages closes the session with PROTOCOL_VIOLATION (`AcceptRequest`). |
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
| 5.1.1   | Subscription state management    | DONE   | REQUEST_ERROR / STOP_SENDING / PUBLISH_DONE handling + cleanup. The relay resets a cancelled subscription's open subgroup and fill streams. |
| 5.1.2   | Location filters                 | DONE   | Every start/end form (unfiltered, Next Object, relative and absolute start, absolute range) + `Matches`. |
| 5.1.3   | Fill semantics                   | PARTIAL | Fill fetch streams from FILL_PARAMETERS on SUBSCRIBE / REQUEST_UPDATE (`handler_fill.go`), and on SUBSCRIBE_TRACKS, one per forwarded PUBLISH's subscription, keyed to the PUBLISH's Request ID (§10.1). A fill inherits the subscription's Range Filters; the ones inside FILL_PARAMETERS override per type. A cancelled subscription's open fills are reset (§5.1.3.1). Not done: scheduling fills against their subscription (§7.2, see Limitations). |
| 5.1.4   | Range filters                    | DONE    | Object filters (SUBGROUP/OBJECTID/PRIORITY/OBJECT_PROPERTY) enforced on SUBSCRIBE fanout, datagrams, and FETCH; TRACK_PROPERTY_FILTER gates PUBLISH forwarding on SUBSCRIBE_TRACKS; `MAX_FILTER_RANGES`/`INVALID_FILTER` gating in place. Object filters on a SUBSCRIBE_TRACKS apply to the subscriptions its forwarded PUBLISHes open. A zero-length filter is no filter; a subscription REQUEST_UPDATE replaces (or, zero-length, removes) the filter types it names and keeps the others. |
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
| 9.1   | Caching relays                       | DONE   | LRU+TTL object cache (`cache/cache.go`); updates limited to non-existence/properties. A duplicate of a cached Object with a different Forwarding Preference, Subgroup ID, Priority or Payload, or different Immutable Properties (§2.4.2, §12.7), ends the track as malformed (`relay/handler_duplicate.go`); not checked once the first copy left the cache — see Limitations. |
| 9.2   | Forward handling                     | DONE   | FORWARD flag honoured; Forward=0 pauses delivery. Upstream Forward is set to 1 only when a downstream subscriber forwards, else the relay pauses it (Forward=0) and resumes on the first forwarding subscriber. |
| 9.3   | Multiple publishers                  | DONE   | Per-track upstreams; dedup by `{GroupID, ObjectID}`. Upstreams of one Subgroup share one downstream stream per subscriber, with the first one's SUBGROUP_HEADER; a later one's Object Properties reopen it with PROPERTIES set, so none are dropped (§2.5). Like a §11.4.3 gap reopen, the reset keeps already-written Objects only where RESET_STREAM_AT is in use; always setting PROPERTIES would avoid it at a byte per Object. A stream sets FIRST_OBJECT only if it begins below every Object forwarded in its Subgroup (§2.2), remembered for the last 32 Groups across contributors. |
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
| 10.2.5  | FILL_TIMEOUT                  | 0x0A   | DONE   | The budget for a FETCH's or fill's upstream FETCH, its response included: when it runs out, what arrived is served and the rest is an End of Timed-Out Range; 0 asks no upstream. Default 5s. |
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
| 10.2.21 | INCLUDE_PROPERTIES            | 0x35   | DONE   | A value other than 0 or 1 closes the session. With 0 the relay sends empty Track Properties in SUBSCRIBE_OK, FETCH_OK, TRACK_STATUS_OK and forwarded PUBLISH, and writes the priority inline on that subscription's subgroups and datagrams, since the subscriber cannot inherit DEFAULT_PUBLISHER_PRIORITY. |
| 10.3    | SETUP                         | 0x2F00 | DONE   | Bidirectional handshake; options as KV pairs. |
| 10.3.1.1| AUTHORITY option              | 0x05   | PARTIAL| Sent (`WithAuthority`); refused from a server or over WebTransport (INVALID_AUTHORITY) and when not RFC 3986 syntax (MALFORMED_AUTHORITY, `uri.CheckAuthority`). Whether the server serves it is not checked — see Limitations. |
| 10.3.1.2| PATH option                   | 0x01   | PARTIAL| Sent (`WithPath`); refused from a server or over WebTransport (INVALID_PATH) and when not RFC 3986 syntax (MALFORMED_PATH, `uri.CheckPathAndQuery`). Whether the server serves it is not checked — see Limitations. |
| 10.3.1.3| MAX_AUTH_TOKEN_CACHE_SIZE      | 0x04   | DONE   | Sizes the token cache. |
| 10.3.1.4| AUTHORIZATION_TOKEN (setup)   | 0x03   | DONE   | Received tokens are applied to the token cache as a request's are (REGISTER over the cache size is used as a value; DELETE / USE_ALIAS from a client closes the session) and exposed by `Session.SetupTokens`. Sent with `WithSetupToken` (REGISTER / USE_VALUE only); `Session.SetupTokenAliases` reports the REGISTERs the peer's MAX_AUTH_TOKEN_CACHE_SIZE held, the rest purged. |
| 10.3.1.5| MOQT_IMPLEMENTATION           | 0x07   | DONE   | Advisory. |
| 10.3.1.6| MAX_FILTER_RANGES             | 0x06   | DONE   | `WithMaxFilterRanges` advertises it; relay rejects over-limit/prohibited filters with INVALID_FILTER. The relay advertises `relay.DefaultMaxFilterRanges` (16) rather than inheriting the session default of 0, which would prohibit the Range Filters it fully implements; `relay.Config.MaxFilterRanges` overrides, negative to prohibit. |
| 10.3.1.7| MAX_REQUEST_UPDATES           | 0x08   | DONE   | `WithMaxRequestUpdates` advertises the per-stream limit; enforced on inbound follow-ups via `RequestUpdateLimiter`, closing with `TOO_MANY_REQUEST_UPDATES` on overflow. |
| 10.4    | GOAWAY                        | 0x10   | DONE   | Same encoding on control and request streams (draft-19 dropped the Request ID field); callback. As recipient the relay initiates no new SUBSCRIBE, FETCH or PUBLISH to the peer and leaves closing the session to the sender. |
| 10.5    | REQUEST_OK                    | 0x07   | DONE   | Shared OK for PUBLISH/UPDATE/TRACK_STATUS/namespace reqs. Track Properties where they must be empty close the session on receipt and are refused on send (`ErrTrackPropertiesNotAllowed`). |
| 10.6    | REQUEST_ERROR (+ Redirect)    | 0x05   | DONE   | Redirect required only when code==REDIRECT. `Request.Reject` sends a Retry Interval; the relay invites a jittered ~1 s retry on EXCESSIVE_LOAD and passes an upstream SUBSCRIBE rejection on by meaning, Retry Interval kept. |
| 10.7    | SUBSCRIBE                     | 0x03   | DONE   | |
| 10.8    | SUBSCRIBE_OK                  | 0x04   | DONE   | Registers inbound track alias. |
| 10.9    | REQUEST_UPDATE                | 0x02   | DONE   | A REQUEST_UPDATE opening a request stream closes the session with PROTOCOL_VIOLATION (`ErrUnexpectedRequestUpdate`). |
| 10.10   | PUBLISH_STATE_NOTIFY          | 0x22   | DONE   | Only the publisher may send it; enforced by brokers and the relay. |
| 10.11   | PUBLISH                       | 0x1D   | DONE   | |
| 10.12   | PUBLISH_DONE                  | 0x0B   | DONE   | Sent once every stream of the subscription has closed and no datagram send is in progress, with the exact Stream Count; written on its own goroutine, so subscribers do not wait on each other. When a track's last upstream ends, its PUBLISH_DONE code reaches subscribers if it is about the track (TRACK_ENDED, MALFORMED_TRACK); codes about the relay's own upstream subscription become INTERNAL_ERROR. |
| 10.13   | FETCH                         | 0x16   | DONE   | Standalone, the only kind in draft-20. From the cache, a Location is non-existent only on a signal: a Prior Group or Object ID Gap, a Group's or the Track's end, or an upstream's FETCH. Other uncached Locations are FETCHed from a fetch-capable upstream in one span, within FILL_TIMEOUT, or else marked End of Unknown (or Timed-Out) Range. |
| 10.14   | FETCH_OK                      | 0x18   | DONE   | An End Location before the FETCH's Start closes the session. A Start relative to the Largest Object is compared through End ≤ Largest; an End of {0,0} is let through, as it cannot be told apart from "no content yet". |
| 10.15   | TRACK_STATUS                  | 0x0D   | DONE   | Reply via REQUEST_OK, then FIN; any follow-up from the requester closes the session. |
| 10.16   | PUBLISH_NAMESPACE             | 0x06   | DONE   | |
| 10.17   | NAMESPACE                     | 0x08   | DONE   | Per namespace, counted over local and remote sources. |
| 10.18   | NAMESPACE_DONE                | 0x0E   | DONE   | Never before its NAMESPACE. |
| 10.19   | SUBSCRIBE_NAMESPACE           | 0x50   | DONE   | |
| 10.20   | SUBSCRIBE_TRACKS              | 0x51   | DONE   | §10.20.1: its SUBSCRIBE parameters become each forwarded PUBLISH's subscription; an out-of-range value closes the session (§10.2.8/§10.2.18). A REQUEST_UPDATE merges into them for later PUBLISHes (§10.2.18: "Existing subscriptions are unaffected"), and existing tracks that newly match by prefix or Range Filter are forwarded then. |
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
| 11.4.3   | Closing subgroup streams             | DONE    | Relay forwards only the next object on a stream, otherwise reset+reopen: the next object is one ID greater, read next from the same upstream stream (only filtered-out objects between), or covered by its Prior Object ID Gap; an object the relay dropped, or one from another upstream, breaks the run. FINs on clean inbound EOF, resets on inbound reset, resets with MALFORMED_TRACK after a terminal EndOfGroup/EndOfTrack object (§2.4.2), marks reliable boundaries for RESET_STREAM_AT (`SetReliableBoundary`, transport-gated on `EnableStreamResetPartialDelivery`), and resets (not FINs) in-flight subgroups whose group falls out of range after a narrowing REQUEST_UPDATE. A subscription that skipped any Object of the Subgroup other than one before its Start Location (a filter, Forward State 0, a Start raised past Objects already sent, an inbox overflow with EXCESSIVE_LOAD, an expiry) gets resets, never a FIN, on that Subgroup's streams. Objects published before a subscription joined are treated as before its Start. |
| 11.4.4   | Fetch header                         | DONE    | Serialization Flags of 128 or more that are not an End of Range close the session. |
| 11.4.4.1 | Fetch flags                          | DONE    | All subgroup modes + delta/priority/properties/status flags. A first Object that references a prior Object's fields closes the session. |
| 11.4.4.2 | End of range                         | DONE    | Non-existent (0x8C) / unknown (0x10C) / timed-out (0x20C) handled; a marker covers the Locations after the previous element in the order the response carries them (see Limitations). An Object after a leading marker that references a prior Subgroup ID or Priority closes the session. |
| 11.5     | Padding streams & datagrams          | DONE    | Recognised type IDs silently discarded. |

## §12 MOQT properties

| §     | Property                       | Type | Status | Notes |
|-------|--------------------------------|------|--------|-------|
| 12.1  | SUBGROUP_DELIVERY_TIMEOUT      | 0x06 | PARTIAL| Track + Object Property; the first object of a subgroup overrides the Track-level value (§8 resolution in `message.DeliveryTimeouts`, enforced in `OutgoingSubgroupStream` where the transport reports acknowledgement — none of the bundled ones do, see §8). |
| 12.2  | OBJECT_DELIVERY_TIMEOUT        | 0x02 | DONE   | Track + Object Property; first-object override, as §12.1. |
| 12.3  | MAX_CACHE_DURATION             | 0x04 | DONE   | Per Object: each carries the value of the upstream it arrived through (captured with the Track Alias in `session.InboundTrack`), and is not forwarded live or served from the cache past it; a present 0 is never served from the cache. In FETCH and fill an expired Object is an End of Unknown Range, whether it expired before the snapshot or while the stream was written. |
| 12.4  | DEFAULT_PUBLISHER_PRIORITY     | 0x0E | DONE   | |
| 12.5  | DEFAULT_PUBLISHER_GROUP_ORDER  | 0x22 | DONE   | Validated. |
| 12.6  | DYNAMIC_GROUPS                 | 0x30 | DONE   | Property defined & scope-validated (flow: see §5.1.6.1). |
| 12.7  | Immutable properties           | 0x0B | DONE   | Relays cache & forward verbatim, never add. Property lookups search its contents too (`message.ExpandImmutable`), the mutable value winning: delivery timeouts, MAX_CACHE_DURATION, DYNAMIC_GROUPS, Mandatory Track Property screening, and property Range Filters. |
| 12.8  | Prior group ID gap             | 0x3C | PARTIAL| Object-scope; encoder in `msf/groupid.go`. More than one, or one past the Group ID, makes the track malformed (`message.CheckObjectProperties`), and the relay also ends the track for two values in one Group. Against the last 32 Groups of any upstream (`registry.TrackEntry.ClaimDelivered`), the relay neither forwards nor caches an Object in a Group announced absent; a gap covering a received Group is accepted (§2.1, §9.1, see Limitations). Not in upstream FETCH responses, nor in a session that is not a relay's. |
| 12.9  | Prior object ID gap            | 0x3E | PARTIAL| Object-scope. More than one, or one past the Object ID, makes the track malformed. The relay neither forwards nor caches an Object announced absent, and accepts a gap covering a received Object, as for §12.8. |

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

- **PATH / AUTHORITY: a server does not check it serves them (§10.3.1.1,
  §10.3.1.2)** — a PATH or AUTHORITY from a server, or over WebTransport,
  closes the session with INVALID_PATH / INVALID_AUTHORITY, and one that is
  not RFC 3986 syntax with MALFORMED_PATH / MALFORMED_AUTHORITY. The third
  INVALID_* condition, "the server does not support the specified path"
  (or authority), is not enforced: a server is never told which paths and
  authorities it serves. WebTransport is recognised by the `wtconn` adapter
  only; a third-party adapter that does not report it is treated as native
  QUIC.
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
  requires, so it is as late as the slowest of them to close. A terminated
  subscription takes no new Object: its stream is reset at the next one, so
  while its upstream is live it waits at most for that. A publisher that ends a
  track but leaves a subgroup stream open, and sends nothing more on it, holds
  the relay's PUBLISH_DONE until that stream ends or its session does. A
  subscriber not reading its data streams holds its own: subgroup streams until
  the fanout writer's drain limit resets them, a fill fetch stream until the
  session ends (fill writes have no deadline). The goroutine that writes
  PUBLISH_DONE is not joined by `Relay.Stop`; it ends when its session does.
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
- **Handles the application reads itself** — REQUEST_UPDATE /
  PUBLISH_STATE_NOTIFY roles (§10.9, §10.10) and Message Parameter scope
  (§10.2.1) are enforced by brokers from typed handles' `Broker()`, by the
  session's own reads, and by the relay. Handles the application reads itself
  (the namespace handles, `FetchResponder`, or any stream read with
  `message.Parse`) are checked only if it calls `Session.CheckPeerParams`.
  Likewise for §10 framing: such a reader must close the session itself on an
  error wrapping `message.ErrMalformedMessage`, or read through
  `Session.NewRequestBroker(stream).Serve`, which does.
- **Duplicate upstream SUBSCRIBEs (§5.1)** — two to one publisher for one track
  can both go out when two downstream SUBSCRIBEs race for a track with no
  upstream yet, or one races a §9.5 late-publisher SUBSCRIBE (late-publisher
  SUBSCRIBEs themselves are deduplicated). §5.1 allows it; it costs a second
  upstream subscription. A Track Alias the publisher shares between them stays
  routed until both end.
- **A rebuilt subgroup fanout forgets omissions (§11.4.3)** — when a
  subgroup's fanout state is torn down and rebuilt (the last inbound
  contributor left, then a replay or redundant upstream brings it back), its
  writers start afresh, so an omission recorded before the rebuild is
  forgotten and the rebuilt stream may FIN.
- **A refused upstream FETCH_OK ends only that fetch (§2.5.1)** — the relay
  answers FETCH_OK before stitching, so it resets the downstream fetch or fill
  stream and never takes the REQUEST_ERROR UNSUPPORTED_EXTENSION branch. The
  track's live subscription and cache-only FETCHes carry on, where §2.5.1's
  lead says the relay "MUST NOT process or forward that track".
- **Inbound GOAWAY, as the subscriber (§10.4, §9.4.1, §3.6)** — the relay
  stops initiating requests to that peer but neither unsubscribes ("A
  subscriber SHOULD individually unsubscribe from each existing
  subscription"), nor migrates to the New Session URI, nor closes the session
  once no subscriptions remain (§3.6 RECOMMENDED). It waits for the sender to
  close.
- **Malformed tracks (§2.4.2, §9.1, §12.8, §12.9)** — the session reports
  Object Properties that make a track malformed (`session.ErrMalformedTrack`),
  and the relay then ends the track: PUBLISH_DONE MALFORMED_TRACK to every
  downstream subscriber, its subscription to that publisher cancelled, the
  Objects triggering it not cached (removed, if earlier ones were). The relay
  also detects, on live subgroup and datagram Objects of any upstream, against
  the last 32 Groups (`registry.TrackEntry.ClaimDelivered`, `SubgroupEnded`,
  `RecordDuplicate`): §2.4.2's list — a Subgroup's Publisher Priority
  changing; an Object past a Subgroup's, Group's or Track's end, or two
  different ends; a duplicate that differs from the cached first copy (§9.1;
  Payloads and Immutable Properties compared only when both copies are
  Normal, since Normal may become End of Group) — and two Prior Group ID Gap
  values in one Group. An end is the first missing ID: an END_OF_GROUP or
  END_OF_TRACK status at M ends the Group at M (END_OF_TRACK the Track too),
  and a FIN (§11.4.3; the Group's too with END_OF_GROUP set, §11.4.2) or a
  datagram's END_OF_GROUP bit after Object N at N+1. Each end is also checked
  against the Objects already received, duplicates included. Interpretations,
  the first three chosen with the maintainer: §2.4.2 calls both the status
  Object at M and the Object N the "final Object", which would make the
  §9.1-equivalent M = N+1 two different finals; a Normal Object at an end is
  past it only if a FIN or bit set it — with status Objects alone it is the
  §9.1 existing-to-not-existing change or the §2.1 late Object; for the same
  reason a status end at M and a FIN or bit end at M+1 agree, ending at M; a
  datagram's END_OF_GROUP bit counts as a Group's end, which §2.4.2's
  non-exhaustive list does not name; and §2.4.2 item 7 (another Forwarding
  Preference) is read per Object, since it may vary within a Track (§11.2.1),
  so it is the §9.1 duplicate check. Not detected: in upstream FETCH
  responses (whose End of Track is not used either), in a session that is not
  a relay's, past the window, and a duplicate once the first copy left the
  cache (evicted or expired) or before a concurrent contributor cached it. An
  Object another upstream had claimed but not yet cached when a later end put
  it past that end stays cached. A downstream FETCH already being served from
  the cache when the track is found malformed is not reset: the relay does not
  track fetch streams per track. One interpretation: an Object with two
  Immutable Properties is treated as malformed, although §12.7 states "MUST
  NOT contain more than one instance" outside its list of malformed
  conditions.
- **Objects inside an announced gap are dropped, not malformed (§2.1, §9.1,
  §12.8, §12.9)** — an interpretation. §12.8 and §12.9 list "an Object with an
  ID within a previously communicated gap" and "a gap covering an Object it
  previously received" as malformed-track conditions, but §2.1 says the first
  "is not a protocol error and the Track is not malformed", and lets an Object
  go from existing to not existing. The relay follows §2.1: it neither
  forwards nor caches such an Object (§9.1 SHOULD NOT) and accepts the covering
  gap, keeping any cached copy of the Object it covers (§9.1 makes updating
  the cache a MAY). §9.1's specific SHOULD NOT is taken over §9.4's general
  "MUST NOT reorder or drop objects received on a multi-object stream".
  Checked on live subgroup and datagram Objects against the last 32 Groups;
  not in upstream FETCH responses, nor in a session that is not a relay's. The
  gap properties are forwarded unchanged, so a subscriber reading §12.9
  literally may still end the track itself.
- **LOC properties inside Immutable Properties (§12.7)** — `loc.Properties.Parse`
  reads only the mutable list, so a LOC Timestamp and the like placed inside
  Immutable Properties is not found. Filling the fields from there would make
  `Append` re-emit them in the mutable list, changing what a relay forwards.
- **A Group Order the subscriber cannot learn (§10.2.8, §10.2.21)** — a
  SUBSCRIBE that omits GROUP_ORDER takes the publisher's
  DEFAULT_PUBLISHER_GROUP_ORDER, and its fill fetch stream is written in that
  order (§11.4.4.1). With INCLUDE_PROPERTIES=0 the subscriber gets no Track
  Properties, and neither SUBSCRIBE_OK nor the FETCH_HEADER carries a Group
  Order, so it cannot tell a Descending fill from an Ascending one. A gap in the
  draft; a subscriber that asks for a fill avoids it by sending GROUP_ORDER.
- **FETCH End of Range markers in Descending order (§11.4.4.2)** — an
  interpretation. A marker covers "Locations between the last serialized
  Object, if any, and this Location"; the relay reads "between" in the order
  the response carries Locations (Groups in its Group Order, Object IDs
  ascending within a Group), so in Descending order a Group's unknown tail is
  marked at {G, 2^64-1} after its Objects. The draft does not say which order
  it means. Interop with moxygen and moqtail is unverified.
- **FETCH from the cache and publishers that do not mark Group ends (§2.1,
  §10.13)** — the relay treats a Group's tail as unknown unless an
  END_OF_GROUP or END_OF_TRACK status, an END_OF_GROUP bit on a FINed
  subgroup, or a gap Property says where it ends. A publisher that marks none
  gets an End of Unknown Range after every Group in a cached FETCH or fill, or,
  behind a fetch-capable upstream, an upstream FETCH for them. Group ends known
  only from a subgroup FIN are kept for the last 32 Groups.
- **Fill streams are not scheduled against their subscription (§7.2 rules 3
  and 4)** — a subscription-delivered Object should go first when the fill's
  Group Order differs, and the fill-delivered one first within a Group. The
  relay writes each stream as it is fed; ordering across them is a scheduler
  the relay does not have.
- **Duplicate Objects from redundant upstreams are not compared (§9.1)** —
  the first copy of each {Group, Object} is forwarded and later ones are
  dropped unread. Comparing them would detect a malformed track (§2.4.2
  condition 6), at a cost on every Object.

### Draft-20 compliance review backlog

A second full review against draft-ietf-moq-transport-20 (2026-09-26, at
`dbe571e`) found the gaps below. Each item names the rule it misses. Items
already listed as Limitations above are not repeated here.

Session layer:

- DUPLICATE_TRACK_ALIAS never closes the session: the relay answers REQUEST_ERROR
  MALFORMED_TRACK, `Session.Subscribe` and `AcceptPublish` return an error (§11.1).
  A session-layer subscriber also never releases an alias when its subscription
  ends.
- A GOAWAY on a request stream is ignored: a second one, or one carrying a New
  Session URI sent to a server, does not close the session (§10.4).
- A REQUEST_ERROR Redirect is dropped after parsing: a server receiving a Connect
  URI, or a Track Name on a namespace-scoped request, does not close the session,
  and the application cannot follow it (§10.6.1).
- A first response other than REQUEST_OK / REQUEST_ERROR to SUBSCRIBE_NAMESPACE or
  SUBSCRIBE_TRACKS does not close the session (§10.19, §10.20).
- `Publication`'s automatic PUBLISH_DONE UPDATE_FAILED is sent while its subgroup
  streams are open, `WriteObject` still succeeds after `Done`, and a subgroup
  opened concurrently with `Done` is missing from the Stream Count (§10.12).
- `Publication`'s REQUEST_UPDATE_OK carries LARGEST_OBJECT only for Objects it
  wrote itself, not the one its SUBSCRIBE_OK or PUBLISH reported (§10.2.17,
  §10.9.1).
- `ReadPublishSkipped` does not close the session on a REQUEST_UPDATE or
  PUBLISH_STATE_NOTIFY from the publisher (§10.9, §10.10), and nothing enforces
  NAMESPACE_DONE-before-NAMESPACE on a namespace subscription (§10.19).
- A second SUBSCRIBE_OK is handed to the application (§5.1 SHOULD close).
- A rejected request sends STOP_SENDING with INTERNAL_ERROR (§3.3.4 SHOULD use a
  relevant code).
- Mandatory Track Property enforcement is off unless configured (§2.5.1).
- FETCH Serialization Flags ≥ 128 are read as field bits before being rejected,
  so a reset or oversized length avoids the PROTOCOL_VIOLATION (§11.4.4).
- SETUP options are sorted unstably, so with more than 12 the Token order on the
  wire can differ from the order `heldSetupAliases` replays (§10.3.1.4).

Validation (values that MUST close the session):

- GROUP_ORDER on PUBLISH, inside FILL_PARAMETERS, and in `AcceptSubscribe` /
  `AcceptPublish` (§10.2.8); the FETCH case is the Limitation above.
- FORWARD in PUBLISH_STATE_NOTIFY, in a publisher's REQUEST_UPDATE, and in
  `AcceptPublish` (§10.2.18).
- DEFAULT_PUBLISHER_GROUP_ORDER outside {1, 2} and DYNAMIC_GROUPS above 1 in
  Track Properties (§12.5, §12.6).
- A LOCATION_FILTER whose StartGroup + EndGroupDelta overflows: REQUEST_ERROR
  MALFORMED_TRACK on SUBSCRIBE, REQUEST_UPDATE and SUBSCRIBE_TRACKS, INVALID_FILTER
  on FETCH, a fill reset in FILL_PARAMETERS (§5.1.2).

Relay:

- Any REQUEST_UPDATE turns INCLUDE_PROPERTIES=0 back off, so the subscriber
  resolves the wrong default Publisher Priority. §10.9: a parameter absent from
  REQUEST_UPDATE "remains unchanged", and INCLUDE_PROPERTIES cannot appear in
  one (§10.2.21, §12.4).
- A merged Subgroup FINs when one contributor ends cleanly although its Objects
  began after ones a reset contributor never delivered (§11.4.3).
- Replay streams (joiners, gap and properties reopens) lose the first Object's
  delivery-timeout override (§8, §12.1, §12.2).
- FETCH_OK never sets End Of Track (§10.14).
- A cancelled FETCH keeps writing its data stream (§5.2: "MUST reset").
- Objects from an upstream FETCH are exempt from MAX_CACHE_DURATION, and cached
  Objects age from when they were read whole rather than their beginning (§12.3).
- A fill range is evaluated against a later Largest Object than SUBSCRIBE_OK or
  REQUEST_UPDATE_OK reported (§5.1.3).
- TRACK_STATUS returns DOES_NOT_EXIST for a PUBLISHed track with no properties
  or Objects, which SUBSCRIBE accepts (§10.15: "treats it identically").
- A client cannot SUBSCRIBE to a track it publishes under its own
  PUBLISH_NAMESPACE (§5.1).
- RENDEZVOUS_TIMEOUT is ignored (§10.2.6 SHOULD hold the subscription; §9.5).
- REQUEST_ERROR MALFORMED_TRACK, defined for FETCH, answers SUBSCRIBE, PUBLISH
  and REQUEST_UPDATE failures (§10.6.2).
- The relay keeps initiating requests on a session it sent GOAWAY to (§10.4
  SHOULD avoid), and closes with GOAWAY_TIMEOUT when it sent none (§3.5).
- A PUBLISH can follow PUBLISH_SKIPPED for the same upstream PUBLISH after a
  prefix update moves away and back (§6.1).
- Upstream FETCHes to a publisher whose track is found malformed are not
  cancelled (§2.4.2).
- Filters are not aggregated upstream (§6.3.1 SHOULD).
- A REQUEST_UPDATE's AUTHORIZATION_TOKENs go through the TokenVerifier only on
  SUBSCRIBE_NAMESPACE and SUBSCRIBE_TRACKS; on SUBSCRIBE, FETCH and
  PUBLISH_NAMESPACE they are resolved but not verified (§10.2.2).

Documentation:

- Limitations: "Duplicate Objects … are not compared" is stale; the FETCH
  GROUP_ORDER entry omits PUBLISH and FILL_PARAMETERS; the LOC entry names
  `PropAudioLevel = 0x0A` (it is 0x0C); "Handles the application reads itself"
  says `CheckPeerParams` checks roles; "Inbound GOAWAY" omits request streams.
- Table rows 10.2.6, 10.2.8, 10.2.9, 10.2.15, 10.2.18, 10.2.21, 12.3,
  12.5 and 12.6 overstate what is done (see the items above), and the package
  summary still lists joining FETCH.
- `session/namespace.go` says NAMESPACE / NAMESPACE_DONE go on a
  PUBLISH_NAMESPACE stream (§10.17, §10.18).
- About a dozen stale `§` citations (padding, grease, fetch ordering, caching).

Open questions for interop: whether an End of Range marker carries an Object
Payload Length (Figure 28 vs §11.4.4.2), and whether EXPIRES may appear in
TRACK_STATUS_OK (§10.15 vs §10.2.16).
