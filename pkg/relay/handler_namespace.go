package relay

import (
	"context"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// handlePublishNamespace implements the PUBLISH_NAMESPACE flow (§6.2, §10.15):
//
//  1. Authorize.
//  2. Register the namespace in [NamespaceRegistry].
//  3. Reply REQUEST_OK on the request stream.
//  4. Forward to every matching downstream SUBSCRIBE_NAMESPACE holder as a
//     NAMESPACE message (§9.5).
//  5. Block reading the request stream until the publisher cancels it
//     (FIN / RESET_STREAM, §6.2). On exit, unregister from the
//     NamespaceRegistry and emit NAMESPACE_DONE to the same subscribers.
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
	notified := make([]*SubscriberEntry, 0, len(subscribers))
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
	// keepalive for the advertisement.
	session.DrainAndWait(ctx, req.Stream)

	// Emit NAMESPACE_DONE to every subscriber we previously notified.
	// Use the registry's CopySubscribers to refilter (handles subscribers
	// that unregistered while we were running), then intersect with
	// `notified` so we don't notify subscribers that never saw the
	// initial NAMESPACE.
	stillAlive := make(map[*SubscriberEntry]struct{})
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
//  2. Register in [NamespaceRegistry] with WantsTracks=false.
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

	entry := h.names.RegisterSubscriber(msg.TrackNamespacePrefix, h.sess, req.Stream, false /* wantsTracks */)
	defer h.names.UnregisterSubscriber(entry)

	// Emit NAMESPACE for every currently-known publisher whose namespace
	// matches this prefix. We snapshot the publishers list so we don't
	// hold the registry lock across stream writes. Writes go through
	// entry.WriteMessage so they serialise with concurrent forwards.
	for _, pub := range h.names.CopyPublishers() {
		if !pub.Namespace.HasPrefix(msg.TrackNamespacePrefix) {
			continue
		}
		if err := entry.WriteMessage(namespaceMessageFor(pub.Namespace, msg.TrackNamespacePrefix)); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "initial NAMESPACE write failed",
				slog.String("err", err.Error()))
			return
		}
	}

	session.DrainAndWait(ctx, req.Stream)
}

// handleSubscribeTracks implements SUBSCRIBE_TRACKS (§6.1, §10.19):
//
//  1. Authorize.
//  2. Register in [NamespaceRegistry] with WantsTracks=true.
//  3. Reply REQUEST_OK.
//  4. Block reading the request stream until the subscriber cancels it.
//
// PUBLISH forwarding (the actual reason SUBSCRIBE_TRACKS exists) is the
// responsibility of `handlePublish` — it queries
// [NamespaceRegistry.MatchSubscribers] on every inbound PUBLISH and routes
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

	// Reply REQUEST_OK before registering, so the OK cannot race a
	// PUBLISH_BLOCKED that a concurrent publisher's PUBLISH handler
	// (emitPublishBlocked) may write to this stream once the entry is visible.
	if err := req.Reply(&message.RequestOK{}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "SubscribeTracks REQUEST_OK write failed",
			slog.String("err", err.Error()))
		return
	}

	entry := h.names.RegisterSubscriber(msg.TrackNamespacePrefix, h.sess, req.Stream, true /* wantsTracks */)
	defer h.names.UnregisterSubscriber(entry)

	session.DrainAndWait(ctx, req.Stream)
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
