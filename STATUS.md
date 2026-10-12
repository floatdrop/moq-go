# MoQT Implementation Status

Tracks this codebase's implementation of
[`draft-ietf-moq-transport-22`](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/22/)
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
to QUIC/TLS or to deployment policy (most of §15) are scored on their hook/
surface-area completeness, not as protocol obligations. Legend:

- **DONE** — wire codec + session/relay behaviour both present and wired.
- **PARTIAL** — present but incomplete; see note.
- **MISSING** — not implemented.
- **N/A (transport)** — handled by the underlying QUIC/WebTransport stack.

## What's implemented

By package, bottom-up along the dependency stack:

- **`wire`** — byte-level codec: §8.1 leading-ones varints (distinct from
  QUIC's RFC 9000 varints), length-prefixed bytes, delta-encoded KV pairs
  (§8.3), track namespaces, reason phrases; an in-memory `Reader` and a
  streaming `Decoder` over one control-frame interface.
- **`message`** — typed control, request-stream, and data-stream messages with
  parameter negotiation: SETUP, GOAWAY, SUBSCRIBE, PUBLISH (+DONE/SKIPPED),
  FETCH (draft-20 replaced Joining FETCH with fill fetch streams),
  TRACK_STATUS, REQUEST_UPDATE, the namespace messages, §11 object framing
  (subgroup/fetch/datagram), location filters, GREASE, and a parse-time
  `Validate` hook that rejects structurally-malformed messages.
- **`session`** — the SETUP handshake with version negotiation, control
  multiplexing and request-ID allocation, §3.1.3 Track-Alias management with
  collision detection, the request openers (`Publish`/`Subscribe`/`Fetch`/…) and
  the `AcceptRequest` responder, typed inbound data streams that resolve
  §11.3.1/§11.4.1 deltas to absolute IDs, GOAWAY, the §8.9 token cache, and
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
  live fanout under a §5.2 slow-reader policy, merges multiple upstream publishers
  per track (§7.3) with §2.1 {Group, Object} dedup and survivor-continues
  failover, serves FETCHes from a per-track cache (asking an upstream FETCH
  about what the cache cannot vouch for), issues on-demand upstream SUBSCRIBEs to every matching
  publisher (local and, via a `DiscoveryStore` + `Dialer`, remote), reflects
  remote namespaces to local subscribers, gates requests through an `Authorizer`
  hook, emits telemetry through a `Metrics` hook, and drains sessions with
  GOAWAY.

## §2 Object data model

| §       | Feature                              | Status | Notes |
|---------|--------------------------------------|--------|-------|
| 2.4.3   | Reserved namespaces                  | DONE   | `AcceptRequest` rejects an exact `.` first field with DOES_NOT_EXIST; other `.`-prefixed namespaces pass through to the application per spec. |

## §3 Publishing and receiving tracks

| §       | Feature                          | Status | Notes |
|---------|----------------------------------|--------|-------|
| 3.1     | Subscriptions                    | DONE   | Subscribe/Publish/OK/Error state machine in `subscribe.go` and `publish.go`. A second response to this side's SUBSCRIBE or PUBLISH closes the session with PROTOCOL_VIOLATION: a SUBSCRIBE_OK on a `Subscription`'s broker, and a REQUEST_OK / REQUEST_ERROR on any broker before this side sent an Update (after one it may answer an Update that gave up); see §9.5. |
| 3.1.1   | Pausing subscriptions            | DONE   | FORWARD=0 pauses delivery and FORWARD=1 resumes it, set by the SUBSCRIBE or PUBLISH and changed by REQUEST_UPDATE; control messages flow regardless. |
| 3.1.2   | Subscription state management    | DONE   | REQUEST_ERROR / STOP_SENDING / PUBLISH_DONE handling + cleanup. The relay resets a cancelled subscription's open subgroup and fill streams. |
| 3.1.3   | Track alias                      | DONE   | In subgroup header + datagram; validated. A SUBSCRIBE_OK or PUBLISH reusing the alias of a different registered Track closes the session with DUPLICATE_TRACK_ALIAS (`Session.RegisterInboundTrack`). A `Subscription` or `IncomingPublication` releases its alias once Terminated (§3.1): on its own or its broker's `Close`, when the publisher's FIN is read through it or its broker's `Serve`, or when `Serve` cancels the stream. |
| 3.1.3.1 | Unknown track alias              | PARTIAL | A subgroup stream whose Track Alias is not bound yet is held unread (MAY buffer): `session.Demux` parks it, count-bounded; the relay waits up to 1 s (`IncomingSubgroupStream.AwaitInboundTrack`, at most 32 streams per session, the rest reset with EXCESSIVE_LOAD). A datagram for an unknown alias is dropped. The MUST to give control streams connection flow control first is not met by the bundled transports, so enough early data can delay the SUBSCRIBE_OK until the relay's wait runs out and the streams are reset. |
| 3.1.4   | Largest Object                   | DONE   | Advances on an Object's first byte: the relay on reading its header (`IncomingSubgroupStream.ReadObjectHeader`), a `Publication` as an Object's write begins or a datagram is sent through `Publication.SendDatagram`. Reported in SUBSCRIBE_OK, PUBLISH, REQUEST_UPDATE_OK and TRACK_STATUS_OK, so it can name an Object still arriving; Next Object and Next Group follow from it. See Limitations for the readings taken. |
| 3.2     | Fetch                            | DONE   | Standalone FETCH (fill fetch streams replaced Joining FETCH in draft-20). Start beyond Largest Object, or no Objects published, is INVALID_RANGE; the range defaults to {0,0} through Largest Object. An Object in range the relay is still receiving is waited for, within FILL_TIMEOUT, and delivered in full (`registry.InFlight`); one still arriving when the budget runs out is an End of Timed-Out Range, and one abandoned mid-payload is waited for on another copy still arriving, else served from the cache, else asked of the upstream FETCH or marked End of Unknown Range. |
| 3.2.1   | Fetch object delivery            | DONE   | One unidirectional stream; Groups in the requested order, Object IDs ascending within a Group. |
| 3.2.2   | Gaps in a fetch stream           | DONE   | From the cache, a Location is non-existent only on a signal: a Prior Group or Object ID Gap, a Group's or the Track's end, or an upstream's FETCH. Other uncached Locations are marked End of Unknown (or Timed-Out) Range. Descending order follows draft-22's rule that a Group runs from Object 0 to 2^64-1 at both ends of the response. |
| 3.2.3   | Relay fetch handling             | DONE   | Uncached Locations are FETCHed from a fetch-capable upstream in one span within FILL_TIMEOUT; a failed or capped upstream FETCH leaves the rest marked unknown rather than failing the downstream FETCH. |
| 3.2.4   | Fetch state management           | DONE   | FETCH lifecycle. The relay resets the request and data streams with CANCELLED on a STOP_SENDING before its data stream FINs. |
| 3.3.1   | Location filters                 | DONE   | Every Location Filter Type (None, Relative Start, Absolute Start, Absolute Start with Group End, Absolute Range, Next Object) + `Matches`. |
| 3.3.2   | Range filters                    | DONE   | Object filters (SUBGROUP/OBJECTID/PRIORITY/OBJECT_PROPERTY) enforced on SUBSCRIBE fanout, datagrams, and FETCH; TRACK_PROPERTY_FILTER gates PUBLISH forwarding on SUBSCRIBE_TRACKS; `MAX_FILTER_RANGES`/`INVALID_FILTER` gating in place. Object filters on a SUBSCRIBE_TRACKS apply to the subscriptions its forwarded PUBLISHes open. A zero-length filter is no filter; a subscription REQUEST_UPDATE replaces (or, zero-length, removes) the filter types it names and keeps the others. |
| 3.3.3   | Combining filters                | DONE   | `ForwardDecision` ANDs Forward + Location + Range filters per object; Range filters combine SetIDs via AND/OR. |
| 3.4     | Fill semantics                   | PARTIAL | Fill fetch streams from FILL_PARAMETERS on SUBSCRIBE / REQUEST_UPDATE (`handler_fill.go`), and on SUBSCRIBE_TRACKS, one per forwarded PUBLISH's subscription, keyed to the PUBLISH's Request ID (§6.4.2.1). A fill inherits the subscription's Range Filters; the ones inside FILL_PARAMETERS override per type. A cancelled subscription's open fills are reset (§3.4.1). Not done: scheduling fills against their subscription (§5.1.2, see Limitations). |
| 3.5     | Joining an ongoing track         | DONE   | A Location Filter plus FILL_PARAMETERS, served as a fill fetch stream. |
| 3.5.1   | Dynamically starting new groups  | DONE   | Relay forwards a downstream `NEW_GROUP_REQUEST` upstream per §9.20.19: included in the on-demand upstream SUBSCRIBE (no established upstream) or sent as an upstream REQUEST_UPDATE, gated on `DYNAMIC_GROUPS` support, Largest-Group, and outstanding-request bookkeeping. |
| 3.6     | Subscribing to tracks by prefix  | DONE   | `SubscribeTracks`; the relay forwards a PUBLISH per matching track, excluding the subscriber's own, and existing tracks when the SUBSCRIBE_TRACKS arrives. |
| 3.6.2   | Parameters on SUBSCRIBE_TRACKS   | DONE   | Any SUBSCRIBE parameter is accepted and becomes each forwarded PUBLISH's initial subscription parameters. draft-22's §9.18 list is narrower and contradicts §3.6.2 (moq-wg/moq-transport#1963), so it is not enforced. |
| 3.6.3   | Skipped tracks                   | DONE   | `ReadPublishSkipped`; the prohibition is scoped to a single PUBLISH (see §9.19 below). |
| 3.7     | Mandatory track properties       | DONE   | A Mandatory Track Property (0x4000–0x7FFF) the endpoint does not know refuses the track with UNSUPPORTED_EXTENSION; the relay forwards only the types in `Config.KnownMandatoryTrackProperties` (none by default). One received as an Object Property makes the track malformed. A refused upstream FETCH_OK ends only that fetch (see Limitations). |

## §4 Publisher and namespace discovery

| §     | Feature                    | Status | Notes |
|-------|----------------------------|--------|-------|
| 4.1   | Publishing namespaces      | DONE   | `PublishNamespace`; the relay advertises namespaces only with NAMESPACE, never by forwarding PUBLISH_NAMESPACE downstream. |
| 4.2   | Subscribing to namespaces  | DONE   | `SubscribeNamespace`; NAMESPACE / NAMESPACE_DONE messages. |

## §5 Object transmission

| §     | Feature                    | Status  | Notes |
|-------|----------------------------|---------|-------|
| 5.1.1 | Priority definitions       | DONE    | Subscriber/publisher priority + group order modeled. |
| 5.1.2 | Scheduling algorithm       | PARTIAL | `EffectiveStreamPriority` builds the composite `session.StreamPriority` (subscriber→publisher→group-order key→subgroup); FETCH ordering is group-order + Object-ID per §3.2.1. The fill-vs-subscription ordering of rules 3 and 4 (a subscription-delivered object first when the fill's Group Order differs; the fill-delivered one first within a group) is not implemented: fill streams carry no priority input. The datagram-wins tie-break (rule 4) holds by construction: datagrams bypass this priority key entirely and are sent as soon as ready, never queued behind a subgroup stream's priority. Transport knob is currently a no-op (quic-go exposes no per-stream priority API — [quic-go#437](https://github.com/quic-go/quic-go/issues/437)), so the order is computed and pushed through `session.PrioritizedSendStream` (propagation is test-covered) but not yet enforced on the wire. |
| 5.1.3 | Considerations for setting | DONE    | Relay honours subscriber/publisher priority on fanout. |
| 5.2   | Delivery timeouts / reliability| PARTIAL| OBJECT_DELIVERY_TIMEOUT enforced on subgroup streams in `session/datastream_out.go`; SUBGROUP_DELIVERY_TIMEOUT only where the transport reports acknowledgement (`session.DeliveryTrackingSendStream`), which no bundled adapter does, so not on quic-go or WebTransport; reset w/ `StreamResetDeliveryTimeout`. Datagrams are never dropped for either timeout (§5.2 "MUST drop the datagrams"). See Limitations. OBJECT_DELIVERY_TIMEOUT is measured per object from its last header byte (`WriteObjectReceivedAt`), not from stream open. `WithDeliveryTimeouts` takes the publisher's and subscriber's halves separately so the §10.1/§10.2 first-object override resolves within the publisher's half before `DeliveryTimeouts.Effective` takes the smaller of the two. The relay sources both sides — the publisher's Track Properties (decoded once onto the entry) and the subscriber's SUBSCRIBE parameters (§9.20.3/§9.20.4) — and passes them to every subgroup stream it opens downstream (a replay stream, which starts past the Subgroup's first Object, gets that Object's override from the fanout), resetting that stream alone with DELIVERY_TIMEOUT while the subscription continues. Not enforced on the raw `Write` path (no object boundaries) or inbound — see Limitations. |

## §6 Sessions

| §       | Feature                              | Status | Notes |
|---------|--------------------------------------|--------|-------|
| 6.1     | MOQT URI scheme                      | DONE   | `pkg/moqt/uri` parses/validates `moqt://` (scheme, non-empty host, default port 443, well-known, https conversion); `video -addr` accepts a URI and feeds AUTHORITY/PATH options. |
| 6.1.1   | Fragment identifiers (`#type:value`) | DONE   | `uri.Parse` validates the `type:value` grammar (type ∈ [a-z0-9-]); fragment is kept local and dropped from the https URL. |
| 6.1.2   | Dereferencing a MOQT URI             | DONE   | A client offers either mapping's ALPN and the server picks: `relaynet.Listen` advertises `moqt-22` + `h3` on one socket and dispatches on the negotiated protocol; `uri.HTTPSURL` derives the https form; `cmd/interop-client` switches adapter on the URL scheme. |
| 6.2     | Session establishment                | DONE   | SETUP handshake in `handshake.go`; ALPN `moqt-22` fixes the version. |
| 6.2.1   | WebTransport                         | DONE   | `wtconn` adapter (webtransport-go); MOQT identifiers offered as the WebTransport sub-protocol both ways. |
| 6.2.2   | Native QUIC                          | DONE   | `quicconn` adapter (quic-go). |
| 6.3     | Session initialization               | DONE   | Control streams + SETUP exchange. Up to 32 data streams that arrive before the peer's control stream are held unread and delivered by `AcceptDataStream` once setup completes (more are refused with EXCESSIVE_LOAD); request streams wait in the transport until `AcceptRequest`. A bidi stream opening with anything but the seven request messages closes the session with PROTOCOL_VIOLATION (`AcceptRequest`). |
| 6.3.1   | 0-RTT                                | N/A (transport) | No app-level 0-RTT handling; QUIC stack provides it. |
| 6.3.2   | Extension negotiation                | DONE   | SETUP options exchanged as KV pairs; peer options parsed. |
| 6.4.1   | Unidirectional stream types          | DONE   | SUBGROUP / FETCH / PADDING / SETUP type IDs dispatched. |
| 6.4.2.1 | Request ID                           | DONE   | Enforced in `AcceptRequest` and on REQUEST_UPDATE: per-role parity, and duplicate detection that tolerates reordering. |
| 6.4.2.2 | Graceful request stream closure      | DONE   | A FIN is not a cancel: the relay keeps a FIN'd request alive until the peer cancels, and FINs back to complete a finished FETCH. |
| 6.4.2.3 | Request cancellation / rejection     | DONE   | STOP_SENDING, stream resets, REQUEST_ERROR in `request.go`; `Close()` on request handles cancels. |
| 6.5     | Session-level tracks/namespaces      | DONE   | `.session` requests are rejected with DOES_NOT_EXIST before the application/relay sees them (no session-level extensions implemented), so relays never forward them; covers the empty-track-name rule. |
| 6.6     | Termination                          | DONE   | `Close()` sends CONNECTION_CLOSE w/ reason; codes in §12.2 below. |
| 6.6.1   | Graceful session migration (GOAWAY)  | DONE   | `SendGoaway`/`OnGoaway`/`PeerGoaway`, new-session URI. |

## §7 Relays

| §     | Feature                              | Status | Notes |
|-------|--------------------------------------|--------|-------|
| 7.1   | Caching relays                       | DONE   | LRU+TTL object cache (`cache/cache.go`); updates limited to non-existence/properties. A duplicate of a cached Object with a different Delivery Mode, Subgroup ID, Priority or Payload, or different Immutable Properties (§12.1, §10.7), ends the track as malformed (`relay/handler_duplicate.go`); not checked once the first copy left the cache — see Limitations. |
| 7.2   | Paused subscription handling         | DONE   | FORWARD honoured; a paused subscription gets no Objects. The upstream is unpaused only while a downstream subscriber is, else the relay pauses it and resumes on the first unpaused subscriber. |
| 7.3   | Multiple publishers                  | DONE   | Per-track upstreams; dedup by `{GroupID, ObjectID}`. Upstreams of one Subgroup share one downstream stream per subscriber, with the first one's SUBGROUP_HEADER; a later one's Object Properties reopen it with PROPERTIES set, so none are dropped (§8.4). Like a §11.3.2 gap reopen, the reset keeps already-written Objects only where RESET_STREAM_AT is in use; always setting PROPERTIES would avoid it at a byte per Object. A stream sets FIRST_OBJECT only if it begins below every Object forwarded in its Subgroup (§2.2), remembered for the last 32 Groups across contributors. |
| 7.4   | Subscriber interactions              | DONE   | Upstream subscription established before SUBSCRIBE_OK; aggregation. |
| 7.4.1 | Graceful subscriber switchover       | DONE   | GOAWAY grace period (`GoawayTimeout`). |
| 7.5   | Resource protection in large namespaces | PARTIAL | Per-session caps (§15.7.1) and the `Authorizer` hook; filters are not aggregated upstream (see the review backlog). |
| 7.6   | Publisher interactions               | DONE   | PUBLISH_NAMESPACE / PUBLISH with prefix matching (`internal/registry/namespace.go`). |
| 7.6.1 | Graceful publisher switchover        | DONE   | Concurrent upstreams + cache dedup. |
| 7.7   | Relay track handling                 | DONE   | Properties captured once at track creation, forwarded opaquely. |
| 7.8   | Relay object handling                | DONE   | Objects forwarded verbatim except alias remap + Object-ID delta re-encode. |

## §8 Notational conventions and common structures

| §       | Feature                          | Status | Notes |
|---------|----------------------------------|--------|-------|
| 8.1     | Variable-length integers         | DONE   | Leading-ones encoding (§8.1, NOT QUIC's RFC 9000 varint) in `wire.AppendVarint`/`ParseVarint`/`ReadVarint`; used by `wire.Reader/Writer/StreamReader.Varint`. |
| 8.2     | Location structure               | DONE   | `message.Location` with `Compare`/`Less`; `KindLocation` param serialization. |
| 8.3     | Key-Value-Pair structure         | DONE   | `wire.KVPair`; even=varint / odd=length-prefixed; 0xFFFF cap, delta-overflow check. |
| 8.5     | Reason phrase structure          | DONE   | `wire.*.ReasonPhrase`; 1024-byte max enforced. |
| 8.6     | Range filter structure           | DONE   | `message.RangeFilter` codec; delta-encoded Ranges, Property Type for the property filters. |
| 8.7     | Track namespace structure        | DONE   | `wire.TrackNamespace`; 0–32 fields, each non-empty; 4,096-byte Full Track Name cap. |
| 8.8     | Namespace / track name encoding  | DONE   | Serialized-name parsing, strict (bijective) form enforced. |
| 8.9     | Authorization token compression  | DONE   | 4 alias types; session token cache resolves inbound. |

## §9 Control messages

| §       | Message / option              | Type   | Status | Notes |
|---------|-------------------------------|--------|--------|-------|
| 9       | Message framing               | —      | DONE   | An unknown type, or a body that does not match its Length or fails validation, closes the session with PROTOCOL_VIOLATION (`message.ErrMalformedMessage`) wherever the session or the relay reads: the control stream, request openers, responses, follow-ups read by `RequestBroker.Serve` or `ReadPublishSkipped`, and the relay's own readers. A frame cut short by FIN or reset only ends its stream. A handle the application reads with `message.Parse` itself is checked only if it applies the rule (see Validation). |
| 9.1     | SETUP                         | 0x2F00 | DONE   | Bidirectional handshake; options as KV pairs. |
| 9.1.1   | AUTHORITY option              | 0x05   | PARTIAL| Sent (`WithAuthority`); refused from a server or over WebTransport (INVALID_AUTHORITY) and when not RFC 3986 syntax (MALFORMED_AUTHORITY, `uri.CheckAuthority`). Whether the server serves it is not checked — see Limitations. |
| 9.1.2   | PATH option                   | 0x01   | PARTIAL| Sent (`WithPath`); refused from a server or over WebTransport (INVALID_PATH) and when not RFC 3986 syntax (MALFORMED_PATH, `uri.CheckPathAndQuery`). Whether the server serves it is not checked — see Limitations. |
| 9.1.3   | MAX_AUTH_TOKEN_CACHE_SIZE     | 0x04   | DONE   | Sizes the token cache. |
| 9.1.4   | AUTHORIZATION_TOKEN (setup)   | 0x03   | DONE   | Received tokens are applied to the token cache as a request's are (REGISTER over the cache size is used as a value; DELETE / USE_ALIAS from a client closes the session) and exposed by `Session.SetupTokens`. Sent with `WithSetupToken` (REGISTER / USE_VALUE only); `Session.SetupTokenAliases` reports the REGISTERs the peer's MAX_AUTH_TOKEN_CACHE_SIZE held, the rest purged. |
| 9.1.5   | MOQT_IMPLEMENTATION           | 0x07   | DONE   | Advisory. |
| 9.1.6   | MAX_FILTER_RANGES             | 0x06   | DONE   | `WithMaxFilterRanges` advertises it; relay rejects over-limit/prohibited filters with INVALID_FILTER. The relay advertises `relay.DefaultMaxFilterRanges` (16) rather than inheriting the session default of 0, which would prohibit the Range Filters it fully implements; `relay.Config.MaxFilterRanges` overrides, negative to prohibit. |
| 9.1.7   | MAX_REQUEST_UPDATES           | 0x08   | DONE   | `WithMaxRequestUpdates` advertises the per-stream limit; enforced on inbound follow-ups via `RequestUpdateLimiter`, closing with `TOO_MANY_REQUEST_UPDATES` on overflow. |
| 9.2     | GOAWAY                        | 0x10   | DONE   | Same encoding on control and request streams; callback. As recipient the relay initiates no new SUBSCRIBE, FETCH or PUBLISH to the peer and leaves closing the session to the sender. On a request stream (`RequestBroker.Serve` and the relay's readers after the response; `session.RequestGoaways` for callers that read one themselves) a second GOAWAY, or one with a New Session URI received by a server, closes the session with PROTOCOL_VIOLATION; a single one is handed to the reader, and neither side migrates the request. One before the response is checked the same way, the request still awaits its response, and the GOAWAY is the stream's first follow-up; bar PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE and SUBSCRIBE_TRACKS, whose response MUST come first (§4.1, §4.2, §3.6). |
| 9.3     | REQUEST_OK                    | 0x07   | DONE   | Shared OK for PUBLISH/UPDATE/TRACK_STATUS/namespace reqs. Track Properties where they must be empty close the session on receipt and are refused on send (`ErrTrackPropertiesNotAllowed`). |
| 9.4     | REQUEST_ERROR (+ Redirect)    | 0x05   | DONE   | Redirect required only when code==REDIRECT, and exposed as `RequestRejectedError.Redirect`; `Request.Reject` sends one. A Connect URI received by a server, or a Track Name for SUBSCRIBE_NAMESPACE / PUBLISH_NAMESPACE / SUBSCRIBE_TRACKS, closes the session (§9.4.1), and `Reject` refuses to send either; on a REQUEST_UPDATE's answer only the Connect URI is checked, as the reader does not know the request, and an update handler's REDIRECT is sent as INTERNAL_ERROR (§12.3 does not list REQUEST_UPDATE). The relay does not follow a Redirect: an upstream REDIRECT becomes INTERNAL_ERROR downstream. `Request.Reject` sends a Retry Interval; the relay invites a jittered ~1 s retry on EXCESSIVE_LOAD and passes an upstream SUBSCRIBE rejection on by meaning, Retry Interval kept. |
| 9.5     | REQUEST_UPDATE                | 0x02   | DONE   | A REQUEST_UPDATE opening a request stream closes the session with PROTOCOL_VIOLATION (`ErrUnexpectedRequestUpdate`). A REQUEST_OK / REQUEST_ERROR on a request stream where this side sent no REQUEST_UPDATE answers nothing and closes the session too, read by a `RequestBroker` or by the relay: a deliberate choice, as the draft names no rule for it on a stream this side answered. |
| 9.6     | SUBSCRIBE                     | 0x03   | DONE   | |
| 9.7     | SUBSCRIBE_OK                  | 0x04   | DONE   | Registers inbound track alias. |
| 9.8     | PUBLISH                       | 0x1D   | DONE   | |
| 9.9     | PUBLISH_DONE                  | 0x0B   | DONE   | Sent once every stream of the subscription has closed and no datagram send is in progress, with the exact Stream Count; written on its own goroutine, so subscribers do not wait on each other. When a track's last upstream ends, its PUBLISH_DONE code reaches subscribers if it is about the track (TRACK_ENDED, MALFORMED_TRACK); codes about the relay's own upstream subscription become INTERNAL_ERROR. Session `Publication.Done` resets the subgroups still open with CANCELLED, refuses later opens and writes (`ErrPublicationEnded`), and counts every subgroup opened, however the opens race it. |
| 9.10    | PUBLISH_STATE_NOTIFY          | 0x22   | DONE   | Only the publisher may send it; enforced by brokers and the relay. |
| 9.11    | FETCH                         | 0x16   | DONE   | The range rides in LOCATION_FILTER; see §3.2 above. |
| 9.12    | FETCH_OK                      | 0x18   | DONE   | End Location is inclusive. The relay sets End Of Track when the End Location is the Object an END_OF_TRACK status made the Track's final one; it does not learn a Track's end from an upstream FETCH_OK's End Of Track. An End Location before the FETCH's Start closes the session. A Start relative to the Largest Object is compared through End ≤ Largest; an End of {0,0} is let through, as it cannot be told apart from "no content yet". |
| 9.13    | TRACK_STATUS                  | 0x0D   | DONE   | Reply via REQUEST_OK, then FIN; any follow-up from the requester closes the session. The relay answers from an Established subscription, else forwards TRACK_STATUS to every candidate SUBSCRIBE would try, concurrently, and combines the answers as a SUBSCRIBE_OK would (the entry's Track Properties, else the first answer's; the largest LARGEST_OBJECT), or gives the refusal SUBSCRIBE would. A forwarding TRACK_STATUS counts against `Config.MaxSubscriptionsPerSession` (§15.1), and the round is bounded at 5s, a silent candidate counting as TIMEOUT (§15.6). Concurrent requests for a track share one round, each answered with its own INCLUDE_PROPERTIES; a requester's STOP_SENDING stops only its wait. Relay policy: not to the requester's own session while a TRACK_STATUS for the track to it is in flight (a loop, §4.1). |
| 9.14    | PUBLISH_NAMESPACE             | 0x06   | DONE   | Carries a Track Namespace Prefix, matched as a prefix (§2.4.2). |
| 9.15    | SUBSCRIBE_NAMESPACE           | 0x50   | DONE   | A first response other than REQUEST_OK / REQUEST_ERROR, a GOAWAY included, closes the session with PROTOCOL_VIOLATION. `NamespaceSubscription.Broker` closes it on a NAMESPACE_DONE for a suffix no NAMESPACE announced, and on a PUBLISH_STATE_NOTIFY or REQUEST_UPDATE from the publisher (§9.5, §9.10); `NamespaceSubscription.Update` updates the subscription through the broker. The NAMESPACE_DONE check resolves suffixes against the prefix in force, switching at the REQUEST_OK that accepts a TRACK_NAMESPACE_PREFIX update (§9.5.2); once any Update on the subscription gives up, responses no longer pair reliably, so the prefix is unknown and the check stops. A caller reading the stream with `message.Parse` gets none of these checks. |
| 9.16    | NAMESPACE                     | 0x08   | DONE   | Per namespace, counted over local and remote sources. |
| 9.17    | NAMESPACE_DONE                | 0x0E   | DONE   | Never before its NAMESPACE. |
| 9.18    | SUBSCRIBE_TRACKS              | 0x51   | DONE   | A first response other than REQUEST_OK / REQUEST_ERROR, a GOAWAY included, closes the session with PROTOCOL_VIOLATION. §3.6.2: its SUBSCRIBE parameters become each forwarded PUBLISH's subscription; an out-of-range value closes the session (§9.20.8/§9.20.18). A REQUEST_UPDATE merges into them for later PUBLISHes (§9.20.18: "Existing subscriptions are unaffected"), and existing tracks that newly match by prefix or Range Filter are forwarded then, unless skipped (§3.6.3). |
| 9.19    | PUBLISH_SKIPPED               | 0x0F   | DONE   | Prohibition scoped to a single PUBLISH (§3.6.3): the relay offers the track again only once a new upstream PUBLISH or SUBSCRIBE is added, not after a prefix update moves away and back. `TrackSubscription.ReadPublishSkipped` and its broker close the session on a PUBLISH_STATE_NOTIFY or REQUEST_UPDATE from the publisher (§9.5, §9.10); `ReadPublishSkipped` skips a single GOAWAY (§9.2). |
| 9.20    | Message parameters (20 types) | —      | DONE   | All 20 defined with their encodings; unknown and duplicate parameters close the session; see §9.20.x below. |
| 9.20.1  | Parameter scope               | —      | DONE   | Per-message scope validation at every session receive point and the relay's own readers (`Parameters.CheckScope`), from each parameter's "MAY appear in" list, widened by §3.6.2 for SUBSCRIBE_TRACKS. draft-22 also lists the allowed parameters per message; where the two disagree (moq-wg/moq-transport#1963) the wider reading is enforced, so no session closes on a parameter one of them allows. |
| 9.20.2  | AUTHORIZATION_TOKEN           | 0x03   | DONE   | Resolved through the token cache (§8.9 above). |
| 9.20.3  | SUBGROUP_DELIVERY_TIMEOUT     | 0x06   | PARTIAL| Parsed and resolved; the stream reset is not enforced on the bundled transports, and datagrams are not dropped (see §5.2). |
| 9.20.4  | OBJECT_DELIVERY_TIMEOUT       | 0x02   | DONE   | |
| 9.20.5  | FILL_TIMEOUT                  | 0x0A   | DONE   | The budget for a FETCH's or fill's upstream FETCH, its response included: when it runs out, what arrived is served and the rest is an End of Timed-Out Range; 0 asks no upstream. Default 5s. |
| 9.20.6  | RENDEZVOUS_TIMEOUT            | 0x04   | DONE   | The relay holds a SUBSCRIBE with no publisher, capped by `Config.MaxRendezvousTimeout` (30s), and answers TIMEOUT when the hold runs out. No publisher means none matched, or each answered DOES_NOT_EXIST, TIMEOUT or GOING_AWAY, failed at the transport, or is draining; any other refusal ends the hold with its error. The refusal a subscriber gets does not depend on the order candidates answer in: the highest ranked, and of equal rank the soonest retry. A PUBLISH of the track, or a covering namespace newly published here or advertised through Discovery, wakes the hold; a publisher that already answered is not asked again. Upstream SUBSCRIBEs to other relays carry what is left of the hold, and are cut short when a publisher arrives here. |
| 9.20.7  | SUBSCRIBER_PRIORITY           | 0x20   | DONE   | |
| 9.20.8  | GROUP_ORDER                   | 0x22   | DONE   | A value outside {1, 2} closes the session, wherever it appears, FILL_PARAMETERS included. |
| 9.20.9  | LOCATION_FILTER               | 0x21   | DONE   | Explicit Location Filter Type, no Length. An unknown Type, a value cut short, or an end Group overflowing 2^64-1 closes the session with PROTOCOL_VIOLATION. |
| 9.20.10 | SUBGROUP_FILTER               | 0x25   | DONE   | Enforced per object in the fanout/FETCH. |
| 9.20.11 | OBJECTID_FILTER               | 0x26   | DONE   | Enforced per object in the fanout/FETCH. |
| 9.20.12 | PRIORITY_FILTER               | 0x27   | DONE   | Enforced per object (subgroup priority); >255 rejected INVALID_FILTER. |
| 9.20.13 | OBJECT_PROPERTY_FILTER        | 0x28   | DONE   | Enforced per object against Object Properties; even property type. |
| 9.20.14 | TRACK_PROPERTY_FILTER         | 0x29   | DONE   | Gates PUBLISH forwarding on SUBSCRIBE_TRACKS against Track Properties; even property type. |
| 9.20.15 | FILL_PARAMETERS               | 0x23   | DONE   | Inner Table 7 scope and duplicates checked; contents that do not parse, or bytes left after them, close the session with PROTOCOL_VIOLATION, as for a malformed message; the fill inherits the subscription's Range Filters, those inside overriding per type (§3.4). SUBSCRIBER_PRIORITY inside it has no effect: fill streams carry no priority input (§5.1.2). |
| 9.20.16 | EXPIRES                       | 0x08   | DONE   | |
| 9.20.17 | LARGEST_OBJECT                | 0x09   | DONE   | Monotonic constraint applied. |
| 9.20.18 | FORWARD                       | 0x10   | DONE   | A value above 1 closes the session, in every message that may carry it. |
| 9.20.19 | NEW_GROUP_REQUEST             | 0x32   | DONE   | |
| 9.20.20 | TRACK_NAMESPACE_PREFIX        | 0x34   | DONE   | Applied on REQUEST_UPDATE; SUBSCRIBE_NAMESPACE reconciles its announced set. |
| 9.20.21 | INCLUDE_PROPERTIES            | 0x35   | DONE   | A value other than 0 or 1 closes the session. With 0 the relay sends empty Track Properties in SUBSCRIBE_OK, FETCH_OK, TRACK_STATUS_OK and forwarded PUBLISH, and writes the priority inline on that subscription's subgroups and datagrams, since the subscriber cannot inherit DEFAULT_PUBLISHER_PRIORITY. Nor can the subscriber learn a fill's Group Order when its SUBSCRIBE omitted GROUP_ORDER, a draft gap (see Limitations). |

## §10 MOQT properties

| §     | Property                       | Type | Status | Notes |
|-------|--------------------------------|------|--------|-------|
| 10.1  | SUBGROUP_DELIVERY_TIMEOUT      | 0x06 | PARTIAL| Track + Object Property; the first object of a subgroup overrides the Track-level value (§5.2 resolution in `message.DeliveryTimeouts`, enforced in `OutgoingSubgroupStream` where the transport reports acknowledgement — none of the bundled ones do, see §5.2). |
| 10.2  | OBJECT_DELIVERY_TIMEOUT        | 0x02 | DONE   | Track + Object Property; first-object override, as §10.1. |
| 10.3  | MAX_CACHE_DURATION             | 0x04 | PARTIAL| Per Object: each carries the value of the upstream it arrived through (captured with the Track Alias in `session.InboundTrack`), and is not forwarded live or served from the cache past it; a present 0 is never served from the cache. An Object stitched from an upstream FETCH carries that FETCH_OK's value, and a present 0 sets no limit on it. Objects age from when the relay read them whole, not from their beginning. An Object handed to a waiting FETCH or fill as it completes is delivered as live forwarding is, a present 0 not withholding it (see Limitations). In FETCH and fill an expired Object is an End of Unknown Range, whether it expired before the snapshot or while the stream was written. |
| 10.4  | DEFAULT_PUBLISHER_PRIORITY     | 0x0E | DONE   | |
| 10.5  | DEFAULT_PUBLISHER_GROUP_ORDER  | 0x22 | DONE   | A value outside {1, 2} closes the session, also inside Immutable Properties; an omitted one is Ascending. |
| 10.6  | DYNAMIC_GROUPS                 | 0x30 | DONE   | A value above 1 closes the session, also inside Immutable Properties. |
| 10.7  | Immutable properties           | 0x0B | DONE   | Relays cache & forward verbatim, never add. Property lookups search its contents too (`message.ExpandImmutable`), the mutable value winning: delivery timeouts, MAX_CACHE_DURATION, DYNAMIC_GROUPS, Mandatory Track Property screening, and property Range Filters. |
| 10.8  | Prior group ID gap             | 0x3C | PARTIAL| Object-scope; encoder in `msf/groupid.go`. More than one, or one past the Group ID, makes the track malformed (`message.CheckObjectProperties`), and the relay also ends the track for two values in one Group. Against the last 32 Groups of any upstream (`registry.TrackEntry.ClaimDelivered`), the relay neither forwards nor caches an Object in a Group announced absent; a gap covering a received Group is accepted (§2.1.2, §7.1, see Limitations). Not in upstream FETCH responses, nor in a session that is not a relay's. |
| 10.9  | Prior object ID gap            | 0x3E | PARTIAL| Object-scope. More than one, or one past the Object ID, makes the track malformed. The relay neither forwards nor caches an Object announced absent, and accepts a gap covering a received Object, as for §10.8. |

## §11 Data streams and datagrams

| §        | Feature                              | Status  | Notes |
|----------|--------------------------------------|---------|-------|
| 11.1     | Objects / object header              | DONE    | All header fields encoded. |
| 11.1.1   | Object status                        | DONE    | Normal / EndOfGroup / EndOfTrack. |
| 11.1.2   | Object properties                    | DONE    | Length-prefixed KV pairs. |
| 11.2.1   | Object datagram                      | DONE    | Type bit-fields + invalid-combo rejection. |
| 11.3     | Subgroup streams                     | DONE    | Typed in/out subgroup streams; a FIN mid-Object closes the session. |
| 11.3.1   | Subgroup header + delta object IDs   | DONE    | All subgroup-ID modes; `ReadDecoded` resolves deltas. Unknown Track Aliases: see §3.1.3.1. |
| 11.3.2   | Closing subgroup streams             | DONE    | Relay forwards only the next object on a stream, otherwise reset+reopen: the next object is one ID greater, read next from the same upstream stream (only filtered-out objects between), or covered by its Prior Object ID Gap; an object the relay dropped, or one from another upstream, breaks the run. FINs on clean inbound EOF, resets on inbound reset, resets with MALFORMED_TRACK after a terminal EndOfGroup/EndOfTrack object (§12.1), marks reliable boundaries for RESET_STREAM_AT (`SetReliableBoundary`, transport-gated on `EnableStreamResetPartialDelivery`), and resets (not FINs) in-flight subgroups whose group falls out of range after a narrowing REQUEST_UPDATE. A subscription that skipped any Object of the Subgroup other than one before its Start Location (a filter, a pause, a Start raised past Objects already sent, an inbox overflow with EXCESSIVE_LOAD, an expiry) gets resets, never a FIN, on that Subgroup's streams. Objects published before a subscription joined are treated as before its Start. With several upstreams on one Subgroup, a clean end FINs only if the contributors that ended cleanly delivered every Object from their start on and the Objects forwarded below that start form an unbroken run reaching it; otherwise, since a reset upstream may have held Objects nobody forwarded, the streams reset with CANCELLED. Object IDs forwarded out of order or not consecutive (a Group split across Subgroups) break that run, so there a clean upstream that started above Objects a peer forwarded resets too. The run counts from the lowest Object the dedup ledger saw forwarded, including through upstreams that already left, whose run is forgotten: a clean replay upstream arriving after them resets unless it covers from that Object, even when it did continue their run. Resets on a subscription's data streams do not end the subscription. |
| 11.4     | Fetch streams                        | DONE    | Typed in/out fetch streams. |
| 11.4.1   | Fetch header                         | DONE    | Serialization Flags of 128 or more that are not an End of Range close the session. |
| 11.4.1.1 | Fetch flags                          | DONE    | All subgroup modes + delta/priority/properties/status flags. A first Object that references a prior Object's fields closes the session. |
| 11.4.1.2 | End of range                         | DONE    | Non-existent (0x8C) / unknown (0x10C) / timed-out (0x20C) handled; a marker covers the Locations after the previous element in the order the response carries them (see Limitations). An Object after a leading marker that references a prior Subgroup ID or Priority closes the session. |
| 11.5     | Padding streams & datagrams          | DONE    | Recognised type IDs silently discarded. |

## §12 Error handling

| §     | Feature                        | Status | Notes |
|-------|--------------------------------|--------|-------|
| 12.1  | Malformed tracks               | DONE   | See Limitations for the conditions detected and the interpretations taken. |
| 12.2  | Session termination codes      | DONE   | All codes in `errors.go`. |
| 12.3  | Request error codes            | DONE   | All codes in `errors.go`. |
| 12.4  | Publish done codes             | DONE   | All codes in `errors.go`. |
| 12.5  | Stream reset error codes       | DONE   | All codes in `errors.go` (`StreamReset*`). |

## §13 Grease

| §   | Feature | Status | Notes |
|-----|---------|--------|-------|
| 13  | GREASE  | DONE   | `IsGrease`/`GreaseValue`/`GreaseSetupOption`; unknown values ignored. |

## §14 Transport considerations

| §     | Feature                    | Status | Notes |
|-------|----------------------------|--------|-------|
| 14.1  | Congestion control         | N/A (transport) | No app-level pacing/bufferbloat logic (§14.1.1–3). |

## §15 Security considerations

Most of §15 is advice the draft delegates to the transport or to deployment
policy. This library provides the hooks; enforcement is the operator's.

| §      | Concern                          | Status | Notes |
|--------|----------------------------------|--------|-------|
| 15.1   | Subscription amplification       | DONE   | `Config.MaxSubscriptionsPerSession` caps concurrent subscriptions per session, rejecting excess with EXCESSIVE_LOAD before state mutation (0 = unlimited). |
| 15.2   | Communication security           | N/A (transport) | TLS 1.3 via QUIC/WebTransport. |
| 15.3   | Authorization                    | DONE   | `Authorizer` hook gates every request once before state mutation. |
| 15.3.2 | Replay attacks                   | PARTIAL| Session-scoped token cache; replay defence delegated to token scheme. |
| 15.4   | Media security                   | N/A    | Payloads opaque; E2EE (e.g. SFrame) is external. |
| 15.5   | Resource exhaustion              | DONE   | QUIC flow control + slow-reader reset (`handler_fanout.go`) + per-session subscription/namespace caps; the publisher cancels lowest-priority streams on overload. Global cross-session quotas remain a deployment concern. |
| 15.6   | Timeouts                         | PARTIAL| Delivery timeouts enforced (§5.2), except SUBGROUP_DELIVERY_TIMEOUT on the bundled transports. |
| 15.6.1 | Idle connection handling         | PARTIAL| Keep-alive options documented; not enforced in-library. |
| 15.7   | Relay security                   | DONE   | §15.7.1: `Config.MaxNamespaceRequestsPerSession` bounds PUBLISH_NAMESPACE/SUBSCRIBE_NAMESPACE/SUBSCRIBE_TRACKS state per session (EXCESSIVE_LOAD). §15.7.2: the `Authorizer` hook gates short-prefix subscriptions. |
| 15.8   | Implementation fingerprinting    | DONE   | MOQT_IMPLEMENTATION optional/configurable. |

## Limitations

Out of scope in the relay's cross-instance routing: multi-hop **loop detection**
(the only guard is skipping the relay's own `RelayAddr`), an upstream
**connection-health / redial policy** beyond dial-on-demand, and GOAWAY
**cascading**. `cmd/relay` stays single-instance by choice — the distributed
`DiscoveryStore` backend ships as its own binary out of tree, so the core module
never pulls in its client library.

Known protocol gaps, roughly ordered by how load-bearing they are:

- **PATH / AUTHORITY: a server does not check it serves them (§9.1.1,
  §9.1.2)** — a PATH or AUTHORITY from a server, or over WebTransport,
  closes the session with INVALID_PATH / INVALID_AUTHORITY, and one that is
  not RFC 3986 syntax with MALFORMED_PATH / MALFORMED_AUTHORITY. The third
  INVALID_* condition, "the server does not support the specified path"
  (or authority), is not enforced: a server is never told which paths and
  authorities it serves. WebTransport is recognised by the `wtconn` adapter
  only; a third-party adapter that does not report it is treated as native
  QUIC.
- **Delivery-timeout enforcement is outbound-only, subgroup-only, not on the
  raw path, and SUBGROUP_DELIVERY_TIMEOUT needs transport support (§5.2)** — the
  publisher side is wired end to end: `OutgoingSubgroupStream` resolves both
  timeouts (including the §10.1/§10.2 first-object override) and enforces
  OBJECT_DELIVERY_TIMEOUT, and the relay's fanout applies both halves to every
  subgroup stream it opens, sourcing the publisher's from the entry's Track
  Properties and the subscriber's from the SUBSCRIBE parameters. Four gaps
  remain. SUBGROUP_DELIVERY_TIMEOUT is enforced only on a transport that
  reports acknowledgement, which the bundled ones do not (see its own entry
  below). Datagrams are sent regardless of age: neither `SendDatagram` nor
  the relay's datagram forwarding drops an expired one, where §5.2 says the
  implementation "MUST drop the datagrams if the time elapsed exceeds
  OBJECT_DELIVERY_TIMEOUT" (SUBGROUP_DELIVERY_TIMEOUT acting the same way for
  datagrams). The raw `Write` escape hatch does not enforce OBJECT_DELIVERY_TIMEOUT
  at all: §5.2 measures it per object from that object's receipt, and a caller
  managing its own framing is the only party that knows either fact, so the
  check belongs to `WriteObjectReceivedAt`. And the inbound (subscriber-side)
  path enforces no timeout — a subscriber does not police how long the relay
  takes to deliver a subgroup it was promised.

  The relay measures OBJECT_DELIVERY_TIMEOUT from the instant it read the
  object's header, the "last header byte" §5.2 names, stamped before any lock
  is taken and carried apart from `enqueuedAt`, the `MaxFanoutLag`
  measurement, which counts time spent queued rather than object age.

  Note that the §12.5 reset codes are not interchangeable here, and the fanout
  keeps them apart deliberately. A delivery timeout resets the one stream with
  `DELIVERY_TIMEOUT` ("a delivery timeout was exceeded for this stream") and
  leaves the subscription live; only the `MaxFanoutLag` window uses
  `TOO_FAR_BEHIND`, whose §12.5 definition says the subscription "is being
  terminated". Collapsing the two would make a per-subgroup timeout cost the
  subscriber the whole track — and it is precisely the survivable variant that
  lets a publisher stripe sheddable data across subgroups the relay may drop
  under load.

- **`MAX_REQUEST_UPDATES` enforcement is receive-side only, and cannot trip
  under our own processing (§9.1.7)** — we advertise the limit and enforce
  it on inbound follow-ups (`RequestUpdateLimiter`), but every follow-up reader
  (`RequestBroker.Serve`, the relay's per-stream loops) answers each
  REQUEST_UPDATE synchronously before reading the next, so a stream never holds
  more than one outstanding update and the check only ever fires against a peer
  that pipelines faster than a hypothetical async responder would drain. §9.1.7
  explicitly permits an immediate responder not to detect such a peer. We do not
  self-limit *outbound* REQUEST_UPDATEs against a peer's advertised value for the
  same reason: `UpdateRequest`/`RequestBroker.Update` are synchronous
  write-then-read, so they never exceed any limit ≥ 1.
- **Late publisher pickup (§7.6)** — multiple publishers per track are merged
  and deduplicated, and a local PUBLISH_NAMESPACE that arrives after a track's
  upstream set is established is SUBSCRIBEd for each matching track. Two
  deliberate deviations from "for each matching subscription": a track with no
  downstream subscriber is skipped (an on-demand upstream is released only
  when its last downstream leaves, so one opened then would never be), and so
  is a track the new publisher itself receives from the relay. Each later
  downstream SUBSCRIBE on the track asks every registered matching publisher
  that has no upstream for it, so a skipped one is picked up then. A refusal
  on that path holds for its §9.4.2 Retry Interval, or while the track entry
  and the publisher's registration last when the interval is 0. A *remote relay* that Discovery
  starts resolving later is not pulled in until the set drains and a fresh
  SUBSCRIBE re-establishes it.
- **Subscriber-priority scheduling (§5.1.2 / §9.20.7)** — fully plumbed but not
  enforced on the wire: the §5.1.2 composite key is computed
  (`EffectiveStreamPriority`) and pushed through `session.PrioritizedSendStream`,
  but quic-go and webtransport-go expose no per-stream priority API today
  ([quic-go#437](https://github.com/quic-go/quic-go/issues/437)), so the bundled
  adapters absorb the knob and quic-go round-robins instead. A REQUEST_UPDATE
  that changes priority mid-stream applies only to subsequently opened subgroups.
- **LOC encryption / SecureObjects and Private Properties** — intentionally out
  of scope pending a chosen SecureObjects revision. The property IDs are
  those draft-ietf-moq-loc-04 requests from IANA (§6.1), e.g. `PropAudioLevel =
  0x0C`, and may change until assigned.
- **MSF** — no timeline GZIP compression, content protection (§4.3), token
  authorization, or logs/analytics. No built-in ABR helper: every catalog field
  a selector needs is surfaced (AltGroup, Width/Height, Bitrate, RenderGroup,
  Depends, TemporalID, SpatialID), but variant-selection policy is the
  application's job.
- **Multi-publisher default priority downstream (§11.3.1, §10.4)** — the relay
  resolves each upstream's inherited DEFAULT_PUBLISHER_PRIORITY per alias, but
  forwards the DEFAULT_PRIORITY bit unchanged. A downstream subscriber therefore
  inherits the default from the SUBSCRIBE_OK it was sent, which carries the
  first publisher's properties.
- **PUBLISH_DONE waits on the subscription's streams (§9.9)** — as the draft
  requires, so it is as late as the slowest of them to close. A terminated
  subscription takes no new Object: its stream is reset at the next one, so
  while its upstream is live it waits at most for that. A publisher that ends a
  track but leaves a subgroup stream open, and sends nothing more on it, holds
  the relay's PUBLISH_DONE until that stream ends or its session does. A
  subscriber not reading its data streams holds its own: subgroup streams until
  the fanout writer's drain limit resets them, a fill fetch stream until the
  session ends (fill writes have no deadline). The goroutine that writes
  PUBLISH_DONE is not joined by `Relay.Stop`; it ends when its session does.
- **SUBGROUP_DELIVERY_TIMEOUT is not enforced on quic-go / WebTransport (§5.2)**
  — the reset needs to know when the peer has acknowledged the whole stream
  ("all data committed"). quic-go tracks that internally but exposes no API for
  it (quic-go#3291), and webtransport-go wraps quic-go. `OutgoingSubgroupStream`
  enforces the timeout only on a stream implementing
  `session.DeliveryTrackingSendStream`, which no bundled adapter does, so a
  subgroup stuck behind congestion is not reset with DELIVERY_TIMEOUT. The
  timer alone is not used instead: it would reset streams the peer already
  holds in full. Closing this needs an acknowledgement signal from quic-go,
  then `Finished()` in quicconn and wtconn.
- **TRACK_NAMESPACE_PREFIX encoding (§9.20.20)** — encoded length-prefixed, as
  moxygen, moqtail and libquicr do. The draft text reads as a bare Track
  Namespace. Open WG issue: moq-wg/moq-transport#1942.
- **Repeated AUTHORIZATION TOKENs (§8.9)** — a message "MAY" repeat the
  parameter "as long as the combination of Token Type and Token Value are
  unique after resolving any aliases". Uniqueness is not checked, in SETUP or
  in requests; the draft names no action for the receiver.
- **Handles the application reads itself** — REQUEST_UPDATE /
  PUBLISH_STATE_NOTIFY roles (§9.5, §9.10) and Message Parameter scope
  (§9.20.1) are enforced by brokers from typed handles' `Broker()`, by the
  session's own reads, and by the relay. On handles with no broker of their
  own (`NamespacePublication`, `IncomingNamespacePublication`,
  `IncomingNamespaceSubscription`, `IncomingTrackSubscription`,
  `FetchResponder`) or any stream read with `message.Parse`, the application
  checks both: roles itself, parameter scope with `Session.CheckPeerParams`. A
  bare `Session.NewRequestBroker` checks them only as configured
  (`PeerMessages`, `UpdateScope`).
  Likewise for §9 framing: such a reader must close the session itself on an
  error wrapping `message.ErrMalformedMessage`, or read through
  `Session.NewRequestBroker(stream).Serve`, which does.
- **Duplicate upstream SUBSCRIBEs (§3.1)** — two to one publisher for one track
  can both go out when two downstream SUBSCRIBEs race for a track with no
  upstream yet, or one races a §7.6 late-publisher SUBSCRIBE (late-publisher
  SUBSCRIBEs themselves are deduplicated). §3.1 allows it; it costs a second
  upstream subscription. A Track Alias the publisher shares between them stays
  routed until both end.
- **A rebuilt subgroup fanout forgets omissions (§11.3.2)** — when a
  subgroup's fanout state is torn down and rebuilt (the last inbound
  contributor left, then a replay or redundant upstream brings it back), its
  writers start afresh, so an omission recorded before the rebuild is
  forgotten and the rebuilt stream may FIN.
- **A refused upstream FETCH_OK ends only that fetch (§3.7)** — the relay
  answers FETCH_OK before stitching, so it resets the downstream fetch or fill
  stream (a FETCH's request stream too, with the same code: §6.4.2.3, the
  maintainer's choice) and never takes the REQUEST_ERROR UNSUPPORTED_EXTENSION
  branch. The track's live subscription and cache-only FETCHes carry on, where
  §3.7's lead says the relay "MUST NOT process or forward that track".
- **Inbound GOAWAY, as the subscriber (§9.2, §7.4.1, §6.6.1)** — the relay
  stops initiating requests to that peer but neither unsubscribes ("A
  subscriber SHOULD individually unsubscribe from each existing
  subscription"), nor migrates to the New Session URI, nor closes the session
  once no subscriptions remain (§6.6.1 RECOMMENDED). It waits for the sender to
  close. A GOAWAY on a request stream is checked (§9.2) and otherwise
  ignored: the relay neither re-issues that request (at the New Session URI,
  or on this session when none is given) nor closes the old stream, which the
  recipient SHOULD do.
- **Malformed tracks (§12.1, §7.1, §10.8, §10.9)** — the session reports
  Object Properties that make a track malformed (`session.ErrMalformedTrack`),
  and the relay then ends the track: PUBLISH_DONE MALFORMED_TRACK to every
  downstream subscriber; every downstream fetch stream (fill fetch streams
  included), and a FETCH's request stream, reset with MALFORMED_TRACK (§6.4.2.3,
  the maintainer's choice); its subscription and FETCHes to that publisher
  cancelled (an upstream FETCH to another publisher ends with its downstream
  stream, with CANCELLED); the Objects triggering it not cached (removed, if
  earlier ones were). The relay also detects, on live subgroup and datagram
  Objects of any upstream, against
  the last 32 Groups (`registry.TrackEntry.ClaimDelivered`, `SubgroupEnded`,
  `RecordDuplicate`): §12.1's list — a Subgroup's Publisher Priority
  changing; an Object past a Subgroup's, Group's or Track's end, or two
  different ends; a duplicate that differs from the cached first copy (§7.1;
  Payloads and Immutable Properties compared only when both copies are
  Normal, since Normal may become End of Group) — and two Prior Group ID Gap
  values in one Group. An end is the first missing ID: an END_OF_GROUP or
  END_OF_TRACK status at M ends the Group at M (END_OF_TRACK the Track too),
  and a FIN (§11.3.2; the Group's too with END_OF_GROUP set, §11.3.1) or a
  datagram's END_OF_GROUP bit after Object N at N+1. Each end is also checked
  against the Objects already received, duplicates included. Interpretations,
  the first three chosen with the maintainer: §12.1 calls both the status
  Object at M and the Object N the "final Object", which would make the
  §7.1-equivalent M = N+1 two different finals; a Normal Object at an end is
  past it only if a FIN or bit set it — with status Objects alone it is the
  §7.1 existing-to-not-existing change or the §2.1.2 late Object; for the same
  reason a status end at M and a FIN or bit end at M+1 agree, ending at M; a
  datagram's END_OF_GROUP bit counts as a Group's end, which §12.1's
  non-exhaustive list does not name; and §12.1 item 7 (another Delivery
  Mode) is read per Object, since it may vary within a Track (§2.1),
  so it is the §7.1 duplicate check. Not detected: in upstream FETCH
  responses (whose End of Track is not used either), in a session that is not
  a relay's, past the window, and a duplicate once the first copy left the
  cache (evicted or expired) or before a concurrent contributor cached it. An
  Object another upstream had claimed but not yet cached when a later end put
  it past that end stays cached. One interpretation: an Object with two
  Immutable Properties is treated as malformed, although §10.7 states "MUST
  NOT contain more than one instance" outside its list of malformed
  conditions.
- **Objects inside an announced gap are dropped, not malformed (§2.1.2, §7.1,
  §10.8, §10.9)** — an interpretation. §10.8 and §10.9 list "an Object with an
  ID within a previously communicated gap" and "a gap covering an Object it
  previously received" as malformed-track conditions, but §2.1.2 says the first
  "is not a protocol error and the Track is not malformed", and lets an Object
  go from existing to not existing. The relay follows §2.1.2: it neither
  forwards nor caches such an Object (§7.1 SHOULD NOT) and accepts the covering
  gap, keeping any cached copy of the Object it covers (§7.1 makes updating
  the cache a MAY). §7.1's specific SHOULD NOT is taken over §7.4's general
  "MUST NOT reorder or drop objects received on a multi-object stream".
  Checked on live subgroup and datagram Objects against the last 32 Groups;
  not in upstream FETCH responses, nor in a session that is not a relay's. The
  gap properties are forwarded unchanged, so a subscriber reading §10.9
  literally may still end the track itself.
- **LOC properties inside Immutable Properties (§10.7)** — `loc.Properties.Parse`
  reads only the mutable list, so a LOC Timestamp and the like placed inside
  Immutable Properties is not found. Filling the fields from there would make
  `Append` re-emit them in the mutable list, changing what a relay forwards.
- **A Group Order the subscriber cannot learn (§9.20.8, §9.20.21)** — a
  SUBSCRIBE that omits GROUP_ORDER takes the publisher's
  DEFAULT_PUBLISHER_GROUP_ORDER, and its fill fetch stream is written in that
  order (§3.2.1, §9.20.8). With INCLUDE_PROPERTIES=0 the subscriber gets no Track
  Properties, and neither SUBSCRIBE_OK nor the FETCH_HEADER carries a Group
  Order, so it cannot tell a Descending fill from an Ascending one. A gap in the
  draft; a subscriber that asks for a fill avoids it by sending GROUP_ORDER.
- **SUBGROUP_FILTER on datagrams (§3.3.2, §2.2)** — an interpretation. A
  Datagram Object "does not belong to a Subgroup in any way" (§2.2), and
  §3.3.2 does not say how a Subgroup filter treats one; the relay filters it
  as Subgroup 0.
- **FETCH End of Range markers in Descending order (§11.4.1.2)** — an
  interpretation. A marker covers "Locations between the last serialized
  Object, if any, and this Location"; the relay reads "between" in the order
  the response carries Locations (Groups in its Group Order, Object IDs
  ascending within a Group), so in Descending order a Group's unknown tail is
  marked at {G, 2^64-1} after its Objects. draft-22 §3.2.2 now defines
  Descending gaps the same way (a Group runs from Object 0 to 2^64-1, and a
  gap between Groups covers the tail of one, the skipped Groups, and the head
  of the next), but no test pins that rule yet. Interop with moxygen and
  moqtail is unverified.
- **FETCH from the cache and publishers that do not mark Group ends (§2.1.2,
  §3.2.2)** — the relay treats a Group's tail as unknown unless an
  END_OF_GROUP or END_OF_TRACK status, an END_OF_GROUP bit on a FINed
  subgroup, or a gap Property says where it ends. A publisher that marks none
  gets an End of Unknown Range after every Group in a cached FETCH or fill, or,
  behind a fetch-capable upstream, an upstream FETCH for them. Group ends known
  only from a subgroup FIN are kept for the last 32 Groups.
- **Largest Object on the first byte: readings taken (§3.1.4, §3.2)** —
  decided with the maintainer. Largest Object stays where it moved if that
  Object is then abandoned, and it moves even for an Object the relay already
  knows not to exist, which it then drops (inside an announced Prior Gap) or
  treats as making the track malformed (past a recorded End of Group or
  Track, §12.1). A subscriber with a relative start of
  0 (the Next Group) that joins while Object 0 of a Group is arriving starts
  at the following Group, which costs video a Group of join latency; Next
  Object plus a fill (§3.5) is the way to join without it. FILL_TIMEOUT 0 wins
  over §3.2's "includes that Object in full": the Object still arriving is an
  End of Timed-Out Range. A present MAX_CACHE_DURATION of 0, read as barring
  only serving from the cache, does not withhold an Object handed to a
  waiting FETCH as it completes; the relay's own cache TTL still applies. An
  Object goes live only to subscribers whose writer was open when its first
  header, from any upstream, was read; a later joiner gets it from its fill,
  so each Object reaches a subscriber once. One edge remains: when the first
  copy is abandoned and another upstream's copy starts only after the fill
  gave up on it, the fill may ask an upstream FETCH for it while that copy
  goes out live.
- **Per-message parameter lists (§9.20.1, §3.6.2)** — draft-22 lists the
  parameters each message allows, and the lists are narrower than the
  parameter definitions: SUBSCRIBE_TRACKS omits the SUBSCRIBE parameters
  §3.6.2 admits (LOCATION_FILTER and FILL_PARAMETERS included, which §3.6.2
  tells subscribers to send), and the delivery timeouts are allowed in any
  REQUEST_UPDATE by their definitions but only for a subscription by §9.5's
  list. Upstream tracks this as moq-wg/moq-transport#1963. Until it settles
  the receiver enforces the wider reading, so it never closes a session on a
  parameter one of the two statements allows.
- **Zero-length LOCATION_FILTER inside FILL_PARAMETERS (§3.4)** — draft-22
  §3.4 still describes the fill-the-whole-track filter as "zero-length", a
  leftover of draft-20's length-prefixed encoding; §9.20.9 has no lengths. It
  is read as Location Filter Type None (0x00).
- **Fill streams are not scheduled against their subscription (§5.1.2 rules 3
  and 4)** — a subscription-delivered Object should go first when the fill's
  Group Order differs, and the fill-delivered one first within a Group. The
  relay writes each stream as it is fed; ordering across them is a scheduler
  the relay does not have.

### Draft-20 compliance review backlog

A second full review against draft-ietf-moq-transport-20 (2026-09-26, at
`dbe571e`) found the gaps below. Each item names the rule it misses, cited by
its draft-22 section; draft-22 has not had a full review of its own yet. Items
already listed as Limitations above are not repeated here.

Relay:

- Objects age from when they were read whole rather than their beginning
  (§10.3).
- A client's SUBSCRIBE to a track it publishes gets DOES_NOT_EXIST while an
  upstream SUBSCRIBE for that track to the client is still pending, and its
  FETCH marks a hole unknown while a stitch FETCH for the track to it is in
  flight: the relay cannot tell either from its own request routed back to it
  by a relay peer (§4.1 has no loop protection), so it declines the second hop
  rather than loop. Self-subscriptions are otherwise "identical" (§3.1).
- Filters are not aggregated upstream (§7.5 SHOULD).
- A shared TRACK_STATUS round asks the candidates known when it started: a
  request joining it later is not answered by a publisher that arrived in
  between. A TRACK_STATUS loop through different sessions (the relay's
  request reaching it back over another one) is not detected, and its round
  waits out its 5s bound.

Open questions for interop: whether an End of Range marker carries an Object
Payload Length (Figure 29 vs §11.4.1.2), and whether EXPIRES may appear in
TRACK_STATUS_OK (§9.13 vs §9.20.16; moq-wg/moq-transport#1964 proposes
allowing it in every _OK).
