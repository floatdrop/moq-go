package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/lmittmann/tint"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/loc"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/msf"
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

	live := true
	rg := 1
	cat := msf.BeginBroadcast([]msf.Track{
		{
			Name:        demoVideoName,
			Namespace:   demoNamespace,
			Packaging:   msf.PackagingLOC,
			IsLive:      &live,
			Role:        msf.RoleVideo,
			Codec:       "av01.0.08M.10.0.110.09",
			Width:       1920,
			Height:      1080,
			Framerate:   30,
			Bitrate:     1500000,
			RenderGroup: &rg,
		},
	}, time.Time{})
	if err := cat.Validate(); err != nil {
		return fmt.Errorf("catalog validate: %w", err)
	}
	catalogBytes, err := json.Marshal(cat)
	if err != nil {
		return fmt.Errorf("marshal catalog: %w", err)
	}

	// ----- Publish the catalog track ------------------------------------

	catAlias := sess.AllocOutboundTrackAlias()
	catStream, err := sess.Publish(ctx, &message.Publish{
		Namespace:  wire.Namespace(demoNamespace),
		Name:       []byte(msf.CatalogTrackName),
		TrackAlias: catAlias,
	})
	if err != nil {
		return fmt.Errorf("PUBLISH catalog: %w", err)
	}
	slog.InfoContext(ctx, "catalog PUBLISH_OK", "alias", catAlias)
	go drainRequestStream("catalog", catStream)

	// The catalog track is single-object-per-group: one Object at group 0, no Properties.
	if err := emitSubgroup(sess, catAlias, 0, nil, catalogBytes); err != nil {
		return fmt.Errorf("emit catalog object: %w", err)
	}

	// ----- Publish the video track --------------------------------------

	vidAlias := sess.AllocOutboundTrackAlias()
	vidStream, err := sess.Publish(ctx, &message.Publish{
		Namespace:  wire.Namespace(demoNamespace),
		Name:       []byte(demoVideoName),
		TrackAlias: vidAlias,
	})
	if err != nil {
		return fmt.Errorf("PUBLISH video: %w", err)
	}
	slog.InfoContext(ctx, "video PUBLISH_OK", "alias", vidAlias)
	go drainRequestStream("video", vidStream)

	seq := msf.NewGroupSequencer()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "shutting down, sending PUBLISH_DONE")
			_ = catStream.Done(moqt.PublishDoneTrackEnded, "")
			_ = vidStream.Done(moqt.PublishDoneTrackEnded, "")
			return nil

		case t := <-ticker.C:
			groupID := seq.Next()
			obj := loc.Object{
				Properties: loc.Properties{
					Timestamp:    uint64(t.UnixMilli()),
					HasTimestamp: true,
					Timescale:    1000,
					HasTimescale: true,
				},
				Payload: fmt.Appendf(nil, "synthetic-frame-%d", groupID),
			}
			props, payload := obj.Encode()
			if err := emitSubgroup(sess, vidAlias, groupID, props, payload); err != nil {
				return fmt.Errorf("emit video: %w", err)
			}
			slog.InfoContext(ctx, "sent video object", "group", groupID, "ts", t.UnixMilli())
		}
	}
}

// emitSubgroup opens one subgroup stream carrying a single Object.
// LOC-packaged tracks always produce one Object per chunk, and the
// catalog track is single-object-per-group too (§7.3 / §5).
func emitSubgroup(sess *session.Session, alias, groupID uint64, props, payload []byte) error {
	sg, err := sess.OpenSubgroup(message.SubgroupHeader{
		Properties:     len(props) > 0,
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		TrackAlias:     alias,
		GroupID:        groupID,
	})
	if err != nil {
		return fmt.Errorf("open subgroup: %w", err)
	}
	if err := sg.WriteObjectAt(0, &message.SubgroupObject{
		Properties: props,
		Payload:    payload,
	}); err != nil {
		sg.Cancel(moqt.StreamResetInternalError)
		return fmt.Errorf("write object: %w", err)
	}
	return sg.Close()
}

func drainRequestStream(label string, stream io.Reader) {
	for {
		msg, err := message.Parse(stream)
		if err != nil {
			slog.Debug("publish request stream closed", slog.String("track", label), tint.Err(err))
			return
		}
		slog.Debug("publish request stream message",
			"track", label, "type", fmt.Sprintf("%T", msg))
	}
}
