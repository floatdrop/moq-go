package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/lmittmann/tint"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func subscribe(ctx context.Context, addr string) error {
	slog.InfoContext(ctx, "connecting", "addr", addr)
	sess, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer sess.Close(moqt.SessionNoError, "bye")
	slog.InfoContext(ctx, "connected, sending SUBSCRIBE")

	// §5.1.6 "join a Track at the current Group": the Next Object filter
	// delivers only objects strictly after the current Largest Object, so the
	// live edge alone stays invisible until the next group lands. Pairing it
	// with FILL_PARAMETERS whose Location filter has StartGroup=1 fills the
	// current group from its start on a fill fetch stream (§5.1.3) — for the
	// clock that's exactly the latest timestamp the relay has cached.
	//
	// draft-20 replaced draft-19's Relative Joining FETCH with this.
	subMsg := &message.Subscribe{
		Namespace: wire.Namespace("moq-example"),
		Name:      []byte("clock"),
		Parameters: message.Parameters{
			message.NextObjectFilter(),
			message.FillParametersParam(message.Parameters{
				message.RelativeStartFilter(1),
			}),
		},
	}
	sub, err := sess.Subscribe(ctx, subMsg)
	if err != nil {
		return fmt.Errorf("SUBSCRIBE: %w", err)
	}
	defer sub.Close()

	// §10.2.17: the relay echoes LARGEST_OBJECT here when it has cached
	// state — log it so the demo shows what the fill resolved against.
	if p, found := sub.OK.Parameters.Find(message.ParamLargestObject); found {
		slog.InfoContext(ctx, "SUBSCRIBE_OK", "alias", sub.TrackAlias(),
			"largest_group", p.Group, "largest_object", p.Object)
	} else {
		slog.InfoContext(ctx, "SUBSCRIBE_OK", "alias", sub.TrackAlias(), "largest", "none")
	}

	// Watch the subscribe request stream for PUBLISH_DONE.
	dataCtx, cancelData := context.WithCancel(ctx)
	defer cancelData()
	go func() {
		slog.DebugContext(ctx, "watching request stream for PUBLISH_DONE")
		for {
			msg, err := message.Parse(sub)
			if err != nil {
				if errors.Is(err, io.EOF) {
					slog.DebugContext(ctx, "request stream EOF")
				} else {
					slog.WarnContext(ctx, "request stream error", tint.Err(err))
				}
				cancelData()
				return
			}
			slog.DebugContext(ctx, "request stream message", "type", fmt.Sprintf("%T", msg))
			if pd, ok := msg.(*message.PublishDone); ok {
				slog.InfoContext(ctx, "PUBLISH_DONE", "code", pd.StatusCode)
				cancelData()
				return
			}
		}
	}()

	// latest gates the display: an object only updates the shown time
	// when its Location is strictly greater than the last one printed.
	// This filters the race between the live SUBSCRIBE stream and the
	// Joining FETCH stream — both can deliver objects at SUBSCRIBE time,
	// and without the gate an older cached value could overwrite a newer
	// live one on the display.
	var latest latestSeen

	slog.DebugContext(ctx, "waiting for data streams")
	for {
		ds, err := sess.AcceptDataStream(dataCtx)
		if err != nil {
			if dataCtx.Err() != nil || ctx.Err() != nil {
				slog.DebugContext(ctx, "data stream loop done", tint.Err(err))
				return nil
			}
			if errors.Is(err, session.ErrPaddingStream) {
				slog.DebugContext(ctx, "padding stream ignored")
				continue
			}
			return fmt.Errorf("accept data stream: %w", err)
		}

		switch s := ds.(type) {
		case *session.IncomingSubgroupStream:
			slog.DebugContext(ctx, "subgroup stream",
				"alias", s.Header.TrackAlias,
				"group", s.Header.GroupID,
				"subgroup", s.Header.SubgroupID)
			readSubgroup(s, &latest)
		case *session.IncomingFetchStream:
			slog.DebugContext(ctx, "fetch stream", "request_id", s.Header.RequestID)
			readFetch(s, &latest)
		default:
			slog.WarnContext(ctx, "unknown data stream", "type", fmt.Sprintf("%T", ds))
		}
	}
}

// latestSeen tracks the highest (group, object) the subscriber has
// printed. Updates only when the new Location is strictly greater than
// the current one, which prevents an older cached object from a Joining
// FETCH overwriting a newer one delivered via the live SUBSCRIBE stream
// (or vice versa) when both arrive around the same time at subscribe.
type latestSeen struct {
	group, object uint64
	has           bool
}

func (l *latestSeen) record(group, object uint64) bool {
	if l.has && (group < l.group || (group == l.group && object <= l.object)) {
		return false
	}
	l.group, l.object, l.has = group, object, true
	return true
}

// readSubgroup drains a live SUBGROUP_HEADER stream via the session-layer
// decoder, which resolves §11.4.2 ObjectID deltas into absolute IDs.
func readSubgroup(s *session.IncomingSubgroupStream, latest *latestSeen) {
	for {
		obj, err := s.ReadDecoded()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Warn("read object error", tint.Err(err))
			}
			return
		}
		logTimeIfFresh(latest, obj.GroupID, obj.ObjectID, obj.Payload)
	}
}

// readFetch drains a FETCH_HEADER stream, letting the session-layer
// decoder reconstruct absolute (Group, Object) IDs from §11.4.4 deltas
// and passing each through latest.record before logging. We don't set
// s.GroupOrder — the joining FETCH defaults to ascending, which matches
// IncomingFetchStream.ReadDecoded's default.
func readFetch(s *session.IncomingFetchStream, latest *latestSeen) {
	for {
		obj, err := s.ReadDecoded()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Warn("read fetch object error", tint.Err(err))
			}
			return
		}
		// Skip §11.4.4.2 absence markers — for the clock demo we
		// only want real payloads.
		if obj.IsEndOfRange() {
			continue
		}
		logTimeIfFresh(latest, obj.GroupID, obj.ObjectID, obj.Payload)
	}
}

func logTimeIfFresh(latest *latestSeen, group, object uint64, payload []byte) {
	if !latest.record(group, object) {
		slog.Debug("dropped stale object",
			"group", group, "object", object, "time", string(payload))
		return
	}
	slog.Info("time", "group", group, "object", object, "value", string(payload))
}
