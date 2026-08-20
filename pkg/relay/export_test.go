package relay

import "github.com/floatdrop/moq-go/pkg/moqt/track"

// SetTestHookAfterAliasRegistered installs hook, to be called at the moment a Track Alias
// becomes routable on the SUBSCRIBE and PUBLISH paths, and returns a function restoring the previous value. See
// [testHookAfterAliasRegistered]; tests in package relay_test reach it
// through here.
func SetTestHookAfterAliasRegistered(hook func(track.FullTrackName)) (restore func()) {
	prev := testHookAfterAliasRegistered.Load()
	testHookAfterAliasRegistered.Store(&hook)
	return func() { testHookAfterAliasRegistered.Store(prev) }
}
