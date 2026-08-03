package relay

import (
	"context"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// handlePublishNamespace implements the PUBLISH_NAMESPACE flow (§6.2, §10.15):
//
//  1. Authorize.
//  2. Register the namespace in [registry.NamespaceRegistry].
//  3. Reply REQUEST_OK on the request stream.
//  4. Forward to every matching downstream SUBSCRIBE_NAMESPACE holder as a
//     NAMESPACE message (§9.5).
//  5. Block reading the request stream until the publisher cancels it
//     (FIN / RESET_STREAM, §6.2). On exit, unregister from the
//     registry.NamespaceRegistry and emit NAMESPACE_DONE to the same subscribers.
//
// The §9.5 "issue upstream SUBSCRIBE for matching downstream subs"
// optimisation is handled by the SUBSCRIBE handler's on-demand
// upstream subscribe path; here we only do forward-direction
// propagation.
func (h *sessionHandler) handlePublishNamespace(
	ctx context.Context,
	req *session.Request,
	msg *message.PublishNamespace,
) {
	if err := h.auth.AuthorizePublishNamespace(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "PublishNamespace", err)
		return
	}

	entry := h.names.RegisterPublisher(msg.Namespace, h.sess, req.Stream)
	defer h.names.UnregisterPublisher(entry)

	if err := req.Reply(&message.RequestOK{}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "PublishNamespace REQUEST_OK write failed",
			slog.String("err", err.Error()))
		return
	}

	// Forward to every matching downstream SUBSCRIBE_NAMESPACE holder.
	// Per §6.2 the relay MUST send NAMESPACE to subscribers whose
	// prefix matches OR is a prefix of the advertised namespace.
	subscribers := h.names.MatchSubscribers(msg.Namespace)
	notified := make([]*registry.SubscriberEntry, 0, len(subscribers))
	for _, sub := range subscribers {
		if sub.WantsTracks {
			// SUBSCRIBE_TRACKS holders get PUBLISH messages, not
			// NAMESPACE messages. They're tracked but not notified
			// here; handlePublish handles their PUBLISH forwarding.
			continue
		}
		if err := sub.WriteMessage(namespaceMessageFor(msg.Namespace, sub.Prefix)); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "NAMESPACE forward failed",
				slog.String("err", err.Error()))
			continue
		}
		notified = append(notified, sub)
	}

	// Block until the publisher cancels (request stream FIN/reset) or our
	// ctx is cancelled. Per §6.2 the bidi stream is the publisher's
	// keepalive for the advertisement; NAMESPACE / NAMESPACE_DONE
	// follow-ups from the publisher need no action (the §9.5 fanout keys
	// off tracks, not per-namespace sub-announcements), but REQUEST_UPDATEs
	// must be validated and answered. This handler goroutine is the only
	// writer on the publisher's stream after the REQUEST_OK above, so the
	// acks write directly.
	h.serveNamespaceFollowups(ctx, req.Stream, func(m message.Message) error {
		return message.Marshal(req.Stream, m)
	})

	// Emit NAMESPACE_DONE to every subscriber we previously notified.
	// Use the registry's CopySubscribers to refilter (handles subscribers
	// that unregistered while we were running), then intersect with
	// `notified` so we don't notify subscribers that never saw the
	// initial NAMESPACE.
	stillAlive := make(map[*registry.SubscriberEntry]struct{})
	for _, s := range h.names.CopySubscribers() {
		stillAlive[s] = struct{}{}
	}
	for _, sub := range notified {
		if _, ok := stillAlive[sub]; !ok {
			continue
		}
		if err := sub.WriteMessage(namespaceDoneMessageFor(msg.Namespace, sub.Prefix)); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "NAMESPACE_DONE forward failed",
				slog.String("err", err.Error()))
		}
	}
}

// handleSubscribeNamespace implements SUBSCRIBE_NAMESPACE (§6.1, §10.18):
//
//  1. Authorize.
//  2. Register in [registry.NamespaceRegistry] with WantsTracks=false.
//  3. Reply REQUEST_OK.
//  4. Emit one NAMESPACE for every currently-known publisher whose
//     advertised namespace extends our prefix (§6.1: the publisher MUST send
//     NAMESPACE for namespaces already known to it that match the prefix).
//  5. Block reading the request stream until the subscriber cancels it.
//
// New publisher arrivals during the subscription's lifetime are handled by
// the publisher's `handlePublishNamespace` (which fans out NAMESPACE).
func (h *sessionHandler) handleSubscribeNamespace(
	ctx context.Context,
	req *session.Request,
	msg *message.SubscribeNamespace,
) {
	if err := h.auth.AuthorizeSubscribeNamespace(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "SubscribeNamespace", err)
		return
	}

	// Reply REQUEST_OK before registering. Registration makes the entry
	// visible to MatchSubscribers, after which a concurrent publisher's
	// PUBLISH_NAMESPACE handler (or the Discovery watcher) may write NAMESPACE
	// to this stream; sending the OK first keeps it from racing those writes
	// (and §6.1 requires the OK to precede any NAMESPACE). The backlog scan
	// below still runs after registration, so no advertisement is missed.
	if err := req.Reply(&message.RequestOK{}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "SubscribeNamespace REQUEST_OK write failed",
			slog.String("err", err.Error()))
		return
	}

	// forward/groupOrder are ignored for a SUBSCRIBE_NAMESPACE (WantsTracks
	// false), which never triggers PUBLISH — pass the Forward default.
	entry := h.names.RegisterSubscriber(msg.TrackNamespacePrefix, h.sess, req.Stream, false /* wantsTracks */, true, 0)
	defer h.names.UnregisterSubscriber(entry)

	// Seed the subscriber with every namespace already known under this prefix,
	// each announced once. Local PUBLISH_NAMESPACE publishers first (snapshotted
	// so we don't hold the registry lock across stream writes); then, if
	// Discovery is configured, namespaces advertised by OTHER relays — so a
	// subscriber learns cross-relay namespaces advertised before it registered,
	// not only those that change afterwards. Own-relay Discovery entries are
	// skipped: the local pass already covered them. Writes go through
	// entry.WriteMessage so they serialise with concurrent forwards. New
	// arrivals during the subscription are handled live by handlePublishNamespace
	// and the Discovery watcher.
	seeded := make(map[string]struct{})
	emit := func(ns wire.TrackNamespace) error {
		k := namespaceKey(ns)
		if _, dup := seeded[k]; dup {
			return nil
		}
		seeded[k] = struct{}{}
		return entry.WriteMessage(namespaceMessageFor(ns, msg.TrackNamespacePrefix))
	}
	for _, pub := range h.names.CopyPublishers() {
		if !pub.Namespace.HasPrefix(msg.TrackNamespacePrefix) {
			continue
		}
		if err := emit(pub.Namespace); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "initial NAMESPACE write failed",
				slog.String("err", err.Error()))
			return
		}
	}
	if h.discovery != nil {
		infos, err := h.discovery.FindNamespacesUnder(ctx, msg.TrackNamespacePrefix)
		if err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "discovery namespace seed failed",
				slog.String("err", err.Error()))
		}
		for _, info := range infos {
			if info.RelayAddr == h.relayAddr {
				continue // our own advertisement — already seeded from local publishers
			}
			if err := emit(info.Prefix); err != nil {
				h.log.LogAttrs(ctx, slog.LevelDebug, "initial remote NAMESPACE write failed",
					slog.String("err", err.Error()))
				return
			}
		}
	}

	// REQUEST_OK acks go through entry.WriteMessage so they serialise with
	// the NAMESPACE / NAMESPACE_DONE notifications concurrent publisher
	// handlers write to this stream.
	h.serveNamespaceFollowups(ctx, req.Stream, entry.WriteMessage)
}

// handleSubscribeTracks implements SUBSCRIBE_TRACKS (§6.1, §10.19):
//
//  1. Authorize.
//  2. Register in [registry.NamespaceRegistry] with WantsTracks=true.
//  3. Reply REQUEST_OK.
//  4. Block reading the request stream until the subscriber cancels it.
//
// PUBLISH forwarding (the actual reason SUBSCRIBE_TRACKS exists) is the
// responsibility of `handlePublish` — it queries
// [registry.NamespaceRegistry.MatchSubscribers] on every inbound PUBLISH and routes
// to each WantsTracks=true entry whose prefix matches.
func (h *sessionHandler) handleSubscribeTracks(
	ctx context.Context,
	req *session.Request,
	msg *message.SubscribeTracks,
) {
	if err := h.auth.AuthorizeSubscribeTracks(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "SubscribeTracks", err)
		return
	}

	// §10.19.1: FORWARD/GROUP_ORDER on the SUBSCRIBE_TRACKS become the
	// defaults copied onto every PUBLISH this subscription triggers. Resolve
	// (and validate) before acking. An out-of-range value is a §10.2.8 /
	// §10.2.17 session-level PROTOCOL_VIOLATION.
	forward, groupOrder, err := subscribeTracksForwarding(msg.Parameters)
	if err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "SubscribeTracks parameter protocol violation",
			slog.String("err", err.Error()))
		_ = h.sess.Close(moqt.SessionProtocolViolation, err.Error())
		return
	}

	// Reply REQUEST_OK before registering, so the OK cannot race a
	// PUBLISH_SKIPPED that a concurrent publisher's PUBLISH handler
	// (emitPublishSkipped) may write to this stream once the entry is visible.
	if err := req.Reply(&message.RequestOK{}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "SubscribeTracks REQUEST_OK write failed",
			slog.String("err", err.Error()))
		return
	}

	entry := h.names.RegisterSubscriber(
		msg.TrackNamespacePrefix,
		h.sess,
		req.Stream,
		true, /* wantsTracks */
		forward,
		groupOrder,
	)
	defer h.names.UnregisterSubscriber(entry)

	// REQUEST_OK acks go through entry.WriteMessage so they serialise with
	// the PUBLISH_SKIPPED notifications concurrent PUBLISH handlers write
	// to this stream (emitPublishSkipped).
	h.serveNamespaceFollowups(ctx, req.Stream, entry.WriteMessage)
}

// subscribeTracksForwarding resolves the FORWARD (§10.2.17) and GROUP_ORDER
// (§10.2.8) parameters a SUBSCRIBE_TRACKS carries. §10.19.1 copies both onto
// the PUBLISH messages the subscription triggers: forward defaults to true
// (FORWARD omitted or 1; only 0 means "don't forward"); groupOrder is 0 when
// omitted (the publisher's default applies) or the Ascending/Descending value.
// An out-of-range value is a *paramProtocolViolation (§10.2.8 / §10.2.17: the
// caller MUST close the session), shared with installSubscribeParams.
func subscribeTracksForwarding(ps message.Parameters) (forward bool, groupOrder byte, err error) {
	if err := checkForwardParam(ps); err != nil {
		return false, 0, err
	}
	if err := checkGroupOrderParam(ps); err != nil {
		return false, 0, err
	}
	forward = true
	if p, ok := ps.Find(message.ParamForward); ok {
		forward = p.Byte != 0
	}
	if p, ok := ps.Find(message.ParamGroupOrder); ok {
		groupOrder = p.Byte
	}
	return forward, groupOrder, nil
}

// serveNamespaceFollowups holds a namespace request stream open (the §6.1 /
// §6.2 keepalive previously provided by session.DrainAndWait) while actually
// parsing the follow-ups: a peer REQUEST_UPDATE consumes a §10.1 Request ID
// (validated; violations are session-fatal), may carry §10.2.2 token
// parameters, and must be answered with the single REQUEST_OK §10.9 mandates
// — the relay keeps no mutable per-namespace-request parameters, so the
// update is acknowledged without further action. write supplies the
// stream's serialized writer (namespace streams are also written by
// concurrent notification fanouts). Other follow-ups (NAMESPACE,
// NAMESPACE_DONE, …) need no response and are ignored here.
func (h *sessionHandler) serveNamespaceFollowups(
	ctx context.Context,
	stream session.Stream,
	write func(message.Message) error,
) {
	updates := h.sess.NewRequestUpdateLimiter()
	readRequestStream(ctx, stream, func(m message.Message) bool {
		upd, ok := m.(*message.RequestUpdate)
		if !ok {
			return true
		}
		if !h.handleFollowupRequestID(ctx, upd) {
			return false
		}
		// §10.3.1.7: enforce the per-stream MAX_REQUEST_UPDATES limit.
		if !h.handleRequestUpdateLimit(ctx, updates) {
			return false
		}
		if !h.handleFollowupTokens(ctx, upd) {
			return false
		}
		if err := write(&message.RequestOK{}); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "namespace REQUEST_UPDATE_OK write failed",
				slog.String("err", err.Error()))
			// The handler unregisters the namespace state when this loop
			// returns; reset the read side so the peer learns reads
			// stopped rather than writing follow-ups into a void. (The
			// send side is left to the stream's owner — closing it here
			// could race a concurrent notification fanout write.)
			stream.CancelRead(uint64(moqt.StreamResetInternalError))
			return false
		}
		updates.Responded()
		return true
	})
}

// namespaceKey returns a canonical map key for a namespace tuple (its wire
// encoding), so the SUBSCRIBE_NAMESPACE seed can announce each namespace once
// across its local-publisher and Discovery passes.
func namespaceKey(ns wire.TrackNamespace) string {
	w := wire.NewWriter(nil)
	w.TrackNamespace(ns)
	return string(w.Bytes())
}

// namespaceMessageFor constructs a NAMESPACE wire message announcing the
// publisher's namespace under the subscriber's prefix. §10.16 carries only
// the suffix (the bytes beyond the prefix), so the relay strips the prefix
// portion before emitting.
//
// Example: publisher PUBLISH_NAMESPACE ("video", "cam1") + subscriber
// SUBSCRIBE_NAMESPACE ("video",) → NAMESPACE suffix ("cam1",).
func namespaceMessageFor(publisherNS, subscriberPrefix wire.TrackNamespace) *message.Namespace {
	suffix := publisherNS[len(subscriberPrefix):]
	return &message.Namespace{TrackNamespaceSuffix: append(wire.TrackNamespace(nil), suffix...)}
}

// namespaceDoneMessageFor constructs the NAMESPACE_DONE counterpart of
// [namespaceMessageFor]. Same suffix-stripping rule.
func namespaceDoneMessageFor(publisherNS, subscriberPrefix wire.TrackNamespace) *message.NamespaceDone {
	suffix := publisherNS[len(subscriberPrefix):]
	return &message.NamespaceDone{TrackNamespaceSuffix: append(wire.TrackNamespace(nil), suffix...)}
}
