package relay_test

import (
	"fmt"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestPublishDone_UpstreamCodeByMeaning: §10.12 "The application SHOULD use a
// relevant status code in PUBLISH_DONE". When the track's last upstream ends,
// a code about the track reaches the downstream subscribers as is; one about
// the relay's own upstream subscription (it fell behind, its update failed,
// ...) says nothing true about theirs, and becomes INTERNAL_ERROR.
func TestPublishDone_UpstreamCodeByMeaning(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		upstream, want moqt.PublishDoneCode
	}{
		{moqt.PublishDoneTrackEnded, moqt.PublishDoneTrackEnded},
		{moqt.PublishDoneMalformedTrack, moqt.PublishDoneMalformedTrack},
		{moqt.PublishDoneInternalError, moqt.PublishDoneInternalError},
		{moqt.PublishDoneTooFarBehind, moqt.PublishDoneInternalError},
		{moqt.PublishDoneUpdateFailed, moqt.PublishDoneInternalError},
		{moqt.PublishDoneUnauthorized, moqt.PublishDoneInternalError},
	} {
		t.Run(fmt.Sprintf("%#x", uint64(tc.upstream)), func(t *testing.T) {
			t.Parallel()
			pubSess, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			pub := publishVideoTrack(t, pubSess, "cam1", 1)
			subReq := subscribeCam1Req(t, dialAnotherClient(t, pubSess))
			if err := pub.Done(tc.upstream, "upstream says"); err != nil {
				t.Fatalf("Done: %v", err)
			}
			if pd := awaitPublishDone(t, subReq); pd.StatusCode != tc.want {
				t.Fatalf("downstream PUBLISH_DONE %#x, want %#x for an upstream %#x",
					pd.StatusCode, tc.want, tc.upstream)
			}
		})
	}
}
