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

// SetTestHookBeforeDownstreamRegistered installs hook, to be called once a
// SUBSCRIBE has an upstream for its track and before its downstream is
// registered, and returns a function restoring the previous value. See
// [testHookBeforeDownstreamRegistered].
func SetTestHookBeforeDownstreamRegistered(hook func(track.FullTrackName)) (restore func()) {
	prev := testHookBeforeDownstreamRegistered.Load()
	testHookBeforeDownstreamRegistered.Store(&hook)
	return func() { testHookBeforeDownstreamRegistered.Store(prev) }
}

// SetTestHookBeforeFill installs hook, to be called as a fill is about to be
// evaluated, and returns a function restoring the previous value. See
// [testHookBeforeFill].
func SetTestHookBeforeFill(hook func(track.FullTrackName)) (restore func()) {
	prev := testHookBeforeFill.Load()
	testHookBeforeFill.Store(&hook)
	return func() { testHookBeforeFill.Store(prev) }
}

// SetTestHookBeforeForwardClaim installs hook, to be called as a forward of a
// track to a SUBSCRIBE_TRACKS holder is about to claim it, and returns a
// function restoring the previous value. See [testHookBeforeForwardClaim].
func SetTestHookBeforeForwardClaim(hook func(track.FullTrackName)) (restore func()) {
	prev := testHookBeforeForwardClaim.Load()
	testHookBeforeForwardClaim.Store(&hook)
	return func() { testHookBeforeForwardClaim.Store(prev) }
}
