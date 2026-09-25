package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lmittmann/tint"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func publish(ctx context.Context, addr string) error {
	slog.InfoContext(ctx, "connecting", "addr", addr)
	sess, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer sess.Close(moqt.SessionNoError, "bye")
	slog.InfoContext(ctx, "connected, sending PUBLISH")

	pub, err := sess.Publish(ctx, &message.Publish{
		Namespace: wire.Namespace("moq-example"),
		Name:      []byte("clock"),
	})
	if err != nil {
		return fmt.Errorf("PUBLISH: %w", err)
	}
	slog.InfoContext(ctx, "PUBLISH_OK", "alias", pub.TrackAlias())

	// Serve the publish request stream through its broker: subscriber
	// REQUEST_UPDATEs are applied by the Publication's built-in handling
	// (FORWARD pauses and resumes OpenSubgroup) and answered as §10.9
	// requires, AUTHORIZATION_TOKEN parameters go through the session token
	// cache (§10.2.2), and — with the broker attached — the shutdown
	// [session.Publication.Done] below is automatically serialized against
	// those replies.
	go func() {
		err := pub.Broker().Serve(ctx, func(msg message.Message) bool {
			slog.DebugContext(ctx, "publish request stream message", "type", fmt.Sprintf("%T", msg))
			return true
		})
		if err != nil {
			slog.DebugContext(ctx, "publish request stream closed", tint.Err(err))
		}
	}()

	var groupID uint64
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "shutting down, sending PUBLISH_DONE")
			_ = pub.Done(moqt.PublishDoneTrackEnded, "")
			return nil

		case t := <-ticker.C:
			payload := []byte(t.UTC().Format(time.RFC3339))
			slog.DebugContext(ctx, "opening subgroup", "group", groupID)

			sg, err := pub.OpenSubgroup(message.SubgroupHeader{
				SubgroupIDMode: message.SubgroupIDImplicitZero,
				GroupID:        groupID,
			})
			if errors.Is(err, session.ErrForwardPaused) {
				// §5.1: no objects while the subscriber's Forward State is
				// 0. The clock keeps ticking; this second is skipped.
				groupID++
				continue
			}
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("open subgroup: %w", err)
			}
			slog.DebugContext(ctx, "writing object", "group", groupID)

			if err := sg.WriteObjectAt(0, &message.SubgroupObject{
				Payload: payload,
			}); err != nil {
				if errors.Is(err, session.ErrForwardPaused) {
					// Paused between open and write; the stream was reset.
					groupID++
					continue
				}
				sg.Cancel(moqt.StreamResetInternalError)
				return fmt.Errorf("write object: %w", err)
			}
			if err := sg.Close(); err != nil {
				return fmt.Errorf("close subgroup: %w", err)
			}

			slog.InfoContext(ctx, "sent", "group", groupID, "time", string(payload))
			groupID++
		}
	}
}
