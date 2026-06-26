# MoQT Implementation Status

Tracks this codebase's implementation of
[`draft-ietf-moq-transport-18`](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/).

## Overall: ~98% complete

The wire codec, all control messages, all message parameters, data
streams/datagrams, the session lifecycle, and the single-instance relay are
implemented and wired end-to-end. Every previously-tracked gap has been closed;
what remains is intentionally out of scope — behaviour the draft delegates to
the transport (congestion control, 0-RTT, communication security, media
security) and *global* cross-session resource quotas, which sit above this
library (per-session caps and the `Authorizer` hook are the in-library surface).

**How this number is derived.** Each trackable feature below is scored
`DONE = 1`, `PARTIAL = 0.5`, `MISSING = 0`. Items the spec explicitly delegates
to QUIC/TLS or to deployment policy (most of §13) are scored as their hook/
surface-area completeness, not as protocol obligations. Legend:

- **DONE** — wire codec + session/relay behaviour both present and wired.
- **PARTIAL** — present but incomplete; see note.
- **MISSING** — not implemented.
- **N/A (transport)** — handled by the underlying QUIC/WebTransport stack, not
  this library; listed for completeness.

---

## Interoperability test status

Beyond the self-contained `go test ./...` suite (which round-trips through this
codebase's own codec), interop is tested in **both directions** against
independent draft-18 implementations via the
[moq-interop-runner](https://github.com/englishm/moq-interop-runner):

- **Relay direction** — our relay vs third-party clients: `make interop`,
  `make interop-matrix`.
- **Client direction** — our client (`cmd/interop-client`) vs third-party
  relays: `make interop-client`.

For the full registered matrices, use the runner:
`cd ../moq-interop-runner && make interop-relay RELAY=moq` and
`make interop-client CLIENT=moq`.

Last run: 2026-06-19, built from this tree.

### Relay direction (our relay ← third-party clients)

Client × transport matrix (`make interop-matrix`):

| Client | Transport | Pass | Skip | Fail | Notes |
|--------|-----------|------|------|------|-------|
| `moq-dev-rs` (moq-net) | QUIC | 4 | 2 | 0 | Skips `subscribe-error` / `subscribe-before-announce` — its consumer API can't subscribe before an announcement. |
| `moq-dev-rs` (moq-net) | WebTransport | 4 | 2 | 0 | Same as above. |
| `moq-dev-js` (moq-net, JS) | WebTransport | **6** | 0 | 0 | **Full coverage — all six cases exercised and passing.** WebTransport-only (no raw-QUIC mode). |
| `moq-rs-draft-18` (Cloudflare) | QUIC | 2 | 0 | 4 | *Advisory.* Its `moq-transport` enforces a Maximum Request ID that draft-18 removed, so it reports `too many requests` before the relay acts — a client/draft-version divergence, **not** a relay fault (relay logs are clean). |
| `moq-rs-draft-18` (Cloudflare) | WebTransport | 2 | 0 | 4 | Advisory, same cause. |

Every canonical case passes against at least one independent client, with no
relay-attributable failures. `moq-dev-js` over WebTransport covers all six;
`announce-subscribe` was unblocked by the §1.4.1 leading-ones varint fix.

### Client direction (our client → third-party relays)

`cmd/interop-client` implements all six cases over raw QUIC *and* WebTransport
with **no skips** (our session library is lower-level than the consumer APIs
that force skips elsewhere). Our client × relay (raw QUIC unless noted):

| Relay | Pass | Fail | Notes |
|-------|------|------|-------|
| our relay (loopback, `make interop-client`) | 6 | 0 | Regression guard — client and relay agree on all six, over QUIC and WebTransport. |
| `moq-dev` (kixelated) | 4 | 2 | Core flows pass (`setup-only`, `announce-only`, `publish-namespace-done`, `subscribe-error`). The two namespace-**discovery** cases fail: moq-dev uses moq-net's announce / "root" model instead of forwarding IETF `SUBSCRIBE_NAMESPACE` → `NAMESPACE`. |
| `moq-rs-draft-18` (Cloudflare) | 1 | 5 | Only `setup-only`; all request streams time out — the same `MAX_REQUEST_ID` divergence seen in the relay direction. |

Per-case client-direction coverage (✅ pass · ❌ fail):

| Test case | our relay | `moq-dev` | `moq-rs-draft-18` |
|-----------|-----------|-----------|-------------------|
| `setup-only` | ✅ | ✅ | ✅ |
| `announce-only` | ✅ | ✅ | ❌ |
| `publish-namespace-done` | ✅ | ✅ | ❌ |
| `subscribe-error` | ✅ | ✅ | ❌ |
| `announce-subscribe` | ✅ | ❌ | ❌ |
| `subscribe-before-announce` | ✅ | ❌ | ❌ |

Our client passes 6/6 against our spec-conformant relay on both transports; the
remaining failures are documented cross-implementation divergences (moq-dev's
announce model, moq-rs's `MAX_REQUEST_ID`), not client bugs.

---

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
| 3.6     | Migration (GOAWAY)                   | DONE   | `SendGoaway`/`OnGoaway`/`PeerGoaway`, Request-ID watermark, new-session URI. |
| 3.3.1   | 0-RTT                                | N/A (transport) | No app-level 0-RTT handling; QUIC stack provides it. |
| 3.7     | Congestion control                   | N/A (transport) | No app-level pacing/bufferbloat logic (§3.7.1–3). |

## §5 Publishing and retrieving tracks

| §       | Feature                          | Status | Notes |
|---------|----------------------------------|--------|-------|
| 5.1     | Subscriptions                    | DONE   | Subscribe/Publish/OK/Error state machine in `pubsub.go`. |
| 5.1.1   | Subscription state management    | DONE   | REQUEST_ERROR / STOP_SENDING / PUBLISH_DONE handling + cleanup. |
| 5.1.2   | Subscription filters             | DONE   | All 4 types (NextGroupStart, LargestObject, AbsoluteStart, AbsoluteRange) + `Matches`. |
| 5.1.3   | Joining an ongoing track         | DONE   | Relative & absolute joining FETCH in `fetch.go`. |
| 5.1.3.1 | Dynamically starting new groups  | DONE   | Relay forwards a downstream `NEW_GROUP_REQUEST` upstream per §10.2.13: included in the on-demand upstream SUBSCRIBE (no established upstream) or sent as an upstream REQUEST_UPDATE, gated on `DYNAMIC_GROUPS` support, Largest-Group, and outstanding-request bookkeeping. |
| 5.2     | Fetch state management           | DONE   | Standalone + joining fetch lifecycle. |

## §6 Namespace discovery

| §     | Feature                    | Status | Notes |
|-------|----------------------------|--------|-------|
| 6.1   | Subscribing to namespaces  | DONE   | `SubscribeNamespace` / `SubscribeTracks` / `ReadPublishBlocked`. |
| 6.2   | Publishing namespaces      | DONE   | `PublishNamespace`; NAMESPACE / NAMESPACE_DONE messages. |

## §7 Priorities

| §     | Feature                    | Status  | Notes |
|-------|----------------------------|---------|-------|
| 7.1   | Definitions                | DONE    | Subscriber/publisher priority + group order modeled. |
| 7.2   | Scheduling algorithm       | DONE    | `EffectiveStreamPriority` builds the composite `session.StreamPriority` (subscriber→publisher→group-order key→subgroup), covering rules 1–4; FETCH ordering is group-order + Object-ID per §10.12.3. Transport knob is currently a no-op (quic-go exposes no per-stream priority API — [quic-go#437](https://github.com/quic-go/quic-go/issues/437)), so the order is computed and pushed through `session.PrioritizedSendStream` (propagation is test-covered) but not yet enforced on the wire. |
| 7.3   | Considerations for setting | DONE    | Relay honours subscriber/publisher priority on fanout. |

## §8 Delivery timeouts and data reliability

| §   | Feature                       | Status | Notes |
|-----|-------------------------------|--------|-------|
| 8   | Delivery timeouts / reliability| DONE  | OBJECT/SUBGROUP delivery timeouts enforced in `session/datastream.go`; reset w/ `StreamResetDeliveryTimeout`; publisher+subscriber values merged. |

## §9 Relays

| §     | Feature                              | Status | Notes |
|-------|--------------------------------------|--------|-------|
| 9.1   | Caching relays                       | DONE   | LRU+TTL object cache (`cache/cache.go`); updates limited to non-existence/properties. |
| 9.2   | Forward handling                     | DONE   | FORWARD flag honoured; Forward=0 pauses delivery. |
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
| 10.2    | Message parameters (13 types) | —      | DONE   | All defined with correct kinds; see §10.2.x below. |
| 10.2.1  | Parameter scope               | —      | DONE   | Per-message scope validation. |
| 10.2.2  | AUTHORIZATION_TOKEN           | 0x03   | DONE   | 4 alias types; session token cache resolves inbound. |
| 10.2.3  | SUBGROUP_DELIVERY_TIMEOUT     | 0x06   | DONE   | |
| 10.2.4  | OBJECT_DELIVERY_TIMEOUT       | 0x02   | DONE   | |
| 10.2.5  | FILL_TIMEOUT                  | 0x0A   | DONE   | |
| 10.2.6  | RENDEZVOUS_TIMEOUT            | 0x04   | DONE   | |
| 10.2.7  | SUBSCRIBER_PRIORITY           | 0x20   | DONE   | |
| 10.2.8  | GROUP_ORDER                   | 0x22   | DONE   | Ascending/Descending validated. |
| 10.2.9  | SUBSCRIPTION_FILTER           | 0x21   | DONE   | Overflow-checked. |
| 10.2.10 | EXPIRES                       | 0x08   | DONE   | |
| 10.2.11 | LARGEST_OBJECT                | 0x09   | DONE   | Monotonic constraint applied. |
| 10.2.12 | FORWARD                       | 0x10   | DONE   | |
| 10.2.13 | NEW_GROUP_REQUEST             | 0x32   | DONE   | |
| 10.2.14 | TRACK_NAMESPACE_PREFIX        | 0x34   | DONE   | |
| 10.3    | SETUP                         | 0x2F00 | DONE   | Bidirectional handshake; options as KV pairs. |
| 10.3.1.1| AUTHORITY option              | 0x05   | DONE   | |
| 10.3.1.2| PATH option                   | 0x01   | DONE   | |
| 10.3.1.3| MAX_AUTH_TOKEN_CACHE_SIZE      | 0x04   | DONE   | Sizes the token cache. |
| 10.3.1.4| AUTHORIZATION_TOKEN (setup)   | 0x03   | DONE   | |
| 10.3.1.5| MOQT_IMPLEMENTATION           | 0x07   | DONE   | Advisory. |
| 10.4    | GOAWAY                        | 0x10   | DONE   | Control- vs request-stream encoding; callback + watermark. |
| 10.5    | REQUEST_OK                    | 0x07   | DONE   | Shared OK for PUBLISH/UPDATE/TRACK_STATUS/namespace reqs. |
| 10.6    | REQUEST_ERROR (+ Redirect)    | 0x05   | DONE   | Redirect required only when code==REDIRECT. |
| 10.7    | SUBSCRIBE                     | 0x03   | DONE   | |
| 10.8    | SUBSCRIBE_OK                  | 0x04   | DONE   | Registers inbound track alias. |
| 10.9    | REQUEST_UPDATE                | 0x02   | DONE   | |
| 10.10   | PUBLISH                       | 0x1D   | DONE   | |
| 10.11   | PUBLISH_DONE                  | 0x0B   | DONE   | |
| 10.12   | FETCH (standalone + joining)  | 0x16   | DONE   | All three fetch types. |
| 10.13   | FETCH_OK                      | 0x18   | DONE   | |
| 10.14   | TRACK_STATUS                  | 0x0D   | DONE   | Reply via REQUEST_OK. |
| 10.15   | PUBLISH_NAMESPACE             | 0x06   | DONE   | |
| 10.16   | NAMESPACE                     | 0x08   | DONE   | |
| 10.17   | NAMESPACE_DONE                | 0x0E   | DONE   | |
| 10.18   | SUBSCRIBE_NAMESPACE           | 0x50   | DONE   | |
| 10.19   | SUBSCRIBE_TRACKS              | 0x51   | DONE   | |
| 10.20   | PUBLISH_BLOCKED               | 0x0F   | DONE   | |

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
| 12.1  | SUBGROUP_DELIVERY_TIMEOUT      | 0x06 | DONE   | |
| 12.2  | OBJECT_DELIVERY_TIMEOUT        | 0x02 | DONE   | |
| 12.3  | MAX_CACHE_DURATION             | 0x04 | DONE   | Lazy age-eviction in cache. |
| 12.4  | DEFAULT_PUBLISHER_PRIORITY     | 0x0E | DONE   | |
| 12.5  | DEFAULT_PUBLISHER_GROUP_ORDER  | 0x22 | DONE   | Validated. |
| 12.6  | DYNAMIC_GROUPS                 | 0x30 | DONE   | Property defined & scope-validated (flow: see §5.1.3.1). |
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
