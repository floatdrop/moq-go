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

// SetTestHookEarlyStreamWaiting installs hook, to be called as a subgroup
// stream starts waiting for its Track Alias, and returns a function restoring
// the previous value. See [testHookEarlyStreamWaiting].
func SetTestHookEarlyStreamWaiting(hook func(alias uint64)) (restore func()) {
	prev := testHookEarlyStreamWaiting.Load()
	testHookEarlyStreamWaiting.Store(&hook)
	return func() { testHookEarlyStreamWaiting.Store(prev) }
}
