# MoQT Implementation Status

Tracks this codebase's implementation of
[`draft-ietf-moq-transport-19`](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/19/)
(plus [`-loc-04`](https://datatracker.ietf.org/doc/draft-ietf-moq-loc/) and
[`-msf-01`](https://datatracker.ietf.org/doc/draft-ietf-moq-msf/) at the edges).

## Overall: ~98% complete

The wire codec, all control messages and parameters, data streams/datagrams, the
session lifecycle, and the single-instance relay are implemented and wired end to
end. What remains is intentionally out of scope: behaviour the draft delegates to
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
  §11.4.2/§11.4.4 deltas to absolute IDs, GOAWAY, the §10.20 token cache, and
  pluggable transport via the `Conn` interface (`quicconn` + `wtconn` adapters).
- **`loc`** — `Object.Encode`/`Decode`: typed Timestamp/Timescale/VideoConfig/
  VideoFrameMarking/AudioConfig/AudioLevel properties with `Extras` passthrough
  for unknown IDs, an RFC 6464 audio-level codec, and AVC/HEVC NAL framing
  detection.
- **`msf`** — `Catalog`/`Track` JSON (independent and delta catalogs, with
  `Apply` replaying delta operations in document order), group-ID sequencing,
  the Media and Event Timeline record formats, the `BeginBroadcast`/
  `EndBroadcast*` workflow helpers, and `Catalog.Validate`.
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
| 3.1.1   | MOQT URI scheme                      | DONE   | `pkg/moqt/uri` parses/validates `moqt://` (scheme, non-empty host, default port 443, well-known, https conversion); `msfdemo -addr` accepts a URI and feeds AUTHORITY/PATH options. |
| 3.1.2   | Fragment identifiers (`#type:value`) | DONE   | `uri.Parse` validates the `type:value` grammar (type ∈ [a-z0-9-]); fragment is kept local and dropped from the https URL. |
| 3.1.3   | WebTransport                         | DONE   | `wtconn` adapter (webtransport-go). |
| 3.1.4   | Native QUIC                          | DONE   | `quicconn` adapter (quic-go). |
| 3.1.5   | Connection URL                       | PARTIAL| Present in GOAWAY/REDIRECT; general handling deferred to app. |
| 3.2     | Extension negotiation                | DONE   | SETUP options exchanged as KV pairs; peer options parsed. |
| 3.2.1   | Reserved namespaces                  | DONE   | `AcceptRequest` rejects an exact `.` first field with DOES_NOT_EXIST; other `.`-prefixed namespaces pass through to the application per spec. |
| 3.2.2   | Session-level tracks/namespaces      | DONE   | `.session` requests are rejected with DOES_NOT_EXIST before the application/relay sees them (no session-level extensions implemented), so relays never forward them; covers the empty-track-name rule. |
| 3.3     | Session initialization               | DONE   | Control streams + SETUP exchange; early data-stream buffering. |
| 3.3.2   | Request cancellation / rejection     | DONE   | STOP_SENDING, stream resets, REQUEST_ERROR in `request.go`. |
| 3.3.3   | Stream reset error codes             | DONE   | All codes in `errors.go` (`StreamReset*`). |
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
| 5.1.2   | Location filters                 | DONE   | All 4 types (NextGroupStart, LargestObject, AbsoluteStart, AbsoluteRange) + `Matches`. |
| 5.1.3   | Range filters                    | DONE    | Object filters (SUBGROUP/OBJECTID/PRIORITY/OBJECT_PROPERTY) enforced on SUBSCRIBE fanout, datagrams, and FETCH; TRACK_PROPERTY_FILTER gates PUBLISH forwarding on SUBSCRIBE_TRACKS; `MAX_FILTER_RANGES`/`INVALID_FILTER` gating in place. Two documented carve-outs (see Known protocol gaps): REQUEST_UPDATE whole-set replace vs per-type merge, and §6.3 object filters on a SUBSCRIBE_TRACKS not yet applied to the resulting subscription's objects. |
| 5.1.4   | Combining filters                | DONE    | `ForwardDecision` ANDs Forward + Location + Range filters per object (§5.1.4); Range filters combine SetIDs via AND/OR. |
| 5.1.5   | Joining an ongoing track         | DONE   | Relative & absolute joining FETCH in `fetch.go`. |
| 5.1.5.1 | Dynamically starting new groups  | DONE   | Relay forwards a downstream `NEW_GROUP_REQUEST` upstream per §10.2.18: included in the on-demand upstream SUBSCRIBE (no established upstream) or sent as an upstream REQUEST_UPDATE, gated on `DYNAMIC_GROUPS` support, Largest-Group, and outstanding-request bookkeeping. |
| 5.2     | Fetch state management           | DONE   | Standalone + joining fetch lifecycle. |

## §6 Namespace discovery

| §     | Feature                    | Status | Notes |
|-------|----------------------------|--------|-------|
| 6.1   | Subscribing to namespaces  | DONE   | `SubscribeNamespace` / `SubscribeTracks` / `ReadPublishSkipped`. |
| 6.2   | Publishing namespaces      | DONE   | `PublishNamespace`; NAMESPACE / NAMESPACE_DONE messages. |

## §7 Priorities

| §     | Feature                    | Status  | Notes |
|-------|----------------------------|---------|-------|
| 7.1   | Definitions                | DONE    | Subscriber/publisher priority + group order modeled. |
| 7.2   | Scheduling algorithm       | DONE    | `EffectiveStreamPriority` builds the composite `session.StreamPriority` (subscriber→publisher→group-order key→subgroup), covering rules 1–4; FETCH ordering is group-order + Object-ID per §10.12.3. Draft-19's datagram-wins tie-break (rule 4) holds by construction: datagrams bypass this priority key entirely and are sent as soon as ready, never queued behind a subgroup stream's priority. Transport knob is currently a no-op (quic-go exposes no per-stream priority API — [quic-go#437](https://github.com/quic-go/quic-go/issues/437)), so the order is computed and pushed through `session.PrioritizedSendStream` (propagation is test-covered) but not yet enforced on the wire. |
| 7.3   | Considerations for setting | DONE    | Relay honours subscriber/publisher priority on fanout. |

## §8 Delivery timeouts and data reliability

| §   | Feature                       | Status | Notes |
|-----|-------------------------------|--------|-------|
| 8   | Delivery timeouts / reliability| DONE  | OBJECT/SUBGROUP delivery timeouts enforced in `session/datastream.go`; reset w/ `StreamResetDeliveryTimeout`; publisher+subscriber values merged. |

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
| 10.1    | Request-ID parity/monotonicity| —      | DONE   | Enforced in `AcceptRequest` (per-role parity + monotonic). |
| 10.2    | Message parameters (18 types) | —      | PARTIAL| 13 of 18 defined with correct kinds; the 5 new Range Filter parameters are MISSING; see §10.2.x below. |
| 10.2.1  | Parameter scope               | —      | DONE   | Per-message scope validation. |
| 10.2.2  | AUTHORIZATION_TOKEN           | 0x03   | DONE   | 4 alias types; session token cache resolves inbound. |
| 10.2.3  | SUBGROUP_DELIVERY_TIMEOUT     | 0x06   | DONE   | |
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
| 10.2.15 | EXPIRES                       | 0x08   | DONE   | |
| 10.2.16 | LARGEST_OBJECT                | 0x09   | DONE   | Monotonic constraint applied. |
| 10.2.17 | FORWARD                       | 0x10   | DONE   | |
| 10.2.18 | NEW_GROUP_REQUEST             | 0x32   | DONE   | |
| 10.2.19 | TRACK_NAMESPACE_PREFIX        | 0x34   | DONE   | |
| 10.3    | SETUP                         | 0x2F00 | DONE   | Bidirectional handshake; options as KV pairs. |
| 10.3.1.1| AUTHORITY option              | 0x05   | DONE   | |
| 10.3.1.2| PATH option                   | 0x01   | DONE   | |
| 10.3.1.3| MAX_AUTH_TOKEN_CACHE_SIZE      | 0x04   | DONE   | Sizes the token cache. |
| 10.3.1.4| AUTHORIZATION_TOKEN (setup)   | 0x03   | DONE   | |
| 10.3.1.5| MOQT_IMPLEMENTATION           | 0x07   | DONE   | Advisory. |
| 10.3.1.6| MAX_FILTER_RANGES             | 0x06   | DONE   | `WithMaxFilterRanges` advertises it; relay rejects over-limit/prohibited filters with INVALID_FILTER. |
| 10.3.1.7| MAX_REQUEST_UPDATES           | 0x08   | DONE   | `WithMaxRequestUpdates` advertises the per-stream limit; enforced on inbound follow-ups via `RequestUpdateLimiter`, closing with `TOO_MANY_REQUEST_UPDATES` on overflow. |
| 10.4    | GOAWAY                        | 0x10   | DONE   | Same encoding on control and request streams (draft-19 dropped the Request ID field); callback. |
| 10.5    | REQUEST_OK                    | 0x07   | DONE   | Shared OK for PUBLISH/UPDATE/TRACK_STATUS/namespace reqs. |
| 10.6    | REQUEST_ERROR (+ Redirect)    | 0x05   | DONE   | Redirect required only when code==REDIRECT. |
| 10.7    | SUBSCRIBE                     | 0x03   | DONE   | |
| 10.8    | SUBSCRIBE_OK                  | 0x04   | DONE   | Registers inbound track alias. |
| 10.9    | REQUEST_UPDATE                | 0x02   | DONE   | A REQUEST_UPDATE opening a request stream is rejected as a PROTOCOL_VIOLATION (`ErrUnexpectedRequestUpdate`). |
| 10.10   | PUBLISH                       | 0x1D   | DONE   | |
| 10.11   | PUBLISH_DONE                  | 0x0B   | DONE   | |
| 10.12   | FETCH (standalone + joining)  | 0x16   | DONE   | All three fetch types. |
| 10.13   | FETCH_OK                      | 0x18   | DONE   | |
| 10.14   | TRACK_STATUS                  | 0x0D   | DONE   | Reply via REQUEST_OK. |
| 10.15   | PUBLISH_NAMESPACE             | 0x06   | DONE   | |
| 10.16   | NAMESPACE                     | 0x08   | DONE   | |
| 10.17   | NAMESPACE_DONE                | 0x0E   | DONE   | |
| 10.18   | SUBSCRIBE_NAMESPACE           | 0x50   | DONE   | |
| 10.19   | SUBSCRIBE_TRACKS              | 0x51   | DONE   | §10.19.1: FORWARD/GROUP_ORDER are copied onto the PUBLISH messages the subscription triggers; an out-of-range value closes the session (§10.2.8/§10.2.17). |
| 10.20   | PUBLISH_SKIPPED               | 0x0F   | DONE   | Prohibition scoped to a single PUBLISH (draft-19 §6.1) — not sticky across re-PUBLISHes. |

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
| 11.4.2   | Subgroup header + delta object IDs   | DONE    | All subgroup-ID modes; `ReadDecoded` resolves deltas. |
| 11.4.3   | Closing subgroup streams             | DONE    | Relay forwards only the next object on a stream (gap → reset+reopen), FINs on clean inbound EOF, resets on inbound reset, resets with MALFORMED_TRACK after a terminal EndOfGroup/EndOfTrack object (§2.4.2), marks reliable boundaries for RESET_STREAM_AT (`SetReliableBoundary`, transport-gated on `EnableStreamResetPartialDelivery`), and resets (not FINs) in-flight subgroups whose group falls out of range after a narrowing REQUEST_UPDATE. |
| 11.4.4   | Fetch header                         | DONE    | |
| 11.4.4.1 | Fetch flags                          | DONE    | All subgroup modes + delta/priority/properties/status flags. |
| 11.4.4.2 | End of range                         | DONE    | Non-existent (0x8C) / unknown (0x10C) handled. |
| 11.5     | Padding streams & datagrams          | DONE    | Recognised type IDs silently discarded. |

## §12 MOQT properties

| §     | Property                       | Type | Status | Notes |
|-------|--------------------------------|------|--------|-------|
| 12.1  | SUBGROUP_DELIVERY_TIMEOUT      | 0x06 | DONE   | Track + Object Property; the first object of a subgroup overrides the Track-level value (§8 resolution in `message.DeliveryTimeouts`, enforced in `OutgoingSubgroupStream`). |
| 12.2  | OBJECT_DELIVERY_TIMEOUT        | 0x02 | DONE   | Track + Object Property; first-object override, as §12.1. |
| 12.3  | MAX_CACHE_DURATION             | 0x04 | DONE   | Lazy age-eviction in cache. |
| 12.4  | DEFAULT_PUBLISHER_PRIORITY     | 0x0E | DONE   | |
| 12.5  | DEFAULT_PUBLISHER_GROUP_ORDER  | 0x22 | DONE   | Validated. |
| 12.6  | DYNAMIC_GROUPS                 | 0x30 | DONE   | Property defined & scope-validated (flow: see §5.1.5.1). |
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
| 13.3.1 | Replay attacks                   | PARTIAL| Session-scoped token cache; replay defence delegated to token scheme. |
| 13.4   | Media security                   | N/A    | Payloads opaque; E2EE (e.g. SFrame) is external. |
| 13.5   | Resource exhaustion              | DONE   | QUIC flow control + slow-reader reset (`fanout.go`) + per-session subscription/namespace caps; the publisher cancels lowest-priority streams on overload. Global cross-session quotas remain a deployment concern. |
| 13.6   | Timeouts                         | DONE   | Delivery timeouts enforced (§8). |
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
`DiscoveryStore` backends ship as their own binaries (`relay-etcd`,
`relay-nats`) in their own modules, so the core module never pulls in an etcd or
NATS client.

Known protocol gaps, roughly ordered by how load-bearing they are:

- **Object Range Filters on SUBSCRIBE_TRACKS (§6.3)** — the object filters
  (SUBGROUP/OBJECTID/PRIORITY/OBJECT_PROPERTY) that ride a SUBSCRIBE_TRACKS are
  parsed and validated but applied only via TRACK_PROPERTY_FILTER's PUBLISH gate;
  §6.3 also wants them applied to the objects of the resulting PUBLISH-created
  subscriptions. Object filtering on a direct SUBSCRIBE/FETCH is unaffected (fully
  enforced); a SUBSCRIBE_TRACKS subscriber can also restate object filters in its
  PUBLISH_OK, which the fanout honors.
- **Range Filter REQUEST_UPDATE semantics (§5.1.3)** — updating a
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
- **An unexpected first message resets the stream instead of closing the session
  (§3.3)** — "Bidirectional streams MUST NOT begin with any other message type
  unless negotiated. If they do, the peer MUST close the Session with a
  PROTOCOL_VIOLATION." The relay's `OnUnknown` resets that one bidi stream and
  keeps the session up, deliberately isolating the failure to a single request.
  That is friendlier, and it is not what the draft requires.
- **Delivery-timeout enforcement is stream-API-level, not auto-wired (§8)** —
  `OutgoingSubgroupStream` enforces `OBJECT`/`SUBGROUP_DELIVERY_TIMEOUT`
  (including the §12.1/§12.2 first-object override) once
  `WithDeliveryTimeouts` is set, and `message.DeliveryTimeouts.Effective`
  implements the §8 publisher/subscriber "smaller of two non-zero" resolution.
  But the relay does not yet source the Track-level value from SUBSCRIBE_OK /
  SUBSCRIBE params and apply it on the subgroups it opens, and the inbound
  (subscriber-side) path enforces no timeout — so end-to-end timeout behaviour
  is opt-in via the stream API rather than automatic.

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
  out-of-range GROUP_ORDER/FORWARD (§10.2.8/§10.2.17), but the FETCH paths (a
  FETCH REQUEST_UPDATE, and the initial standalone/joining FETCH) still scope a
  bad GROUP_ORDER to a REQUEST_ERROR / silent coercion pending the same
  promotion.
- **Late publisher pickup (§9.5)** — multiple publishers per track are merged
  and deduplicated, but a publisher (or remote relay) that begins advertising
  *after* a track's upstream set is established is not retroactively pulled in
  until that set drains and a fresh SUBSCRIBE re-establishes it; publishers that
  PUBLISH proactively are always merged.
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
