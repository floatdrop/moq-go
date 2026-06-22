package relay

import "testing"

func TestSessionLimiter(t *testing.T) {
	t.Parallel()

	t.Run("zero max is unlimited", func(t *testing.T) {
		t.Parallel()
		var l sessionLimiter // maxSubs == maxNS == 0
		for range 1000 {
			if !l.acquireSub() {
				t.Fatal("acquireSub failed under unlimited")
			}
			if !l.acquireNamespace() {
				t.Fatal("acquireNamespace failed under unlimited")
			}
		}
		// release under unlimited is a no-op and must not panic.
		l.releaseSub()
		l.releaseNamespace()
	})

	t.Run("subscription cap is enforced and released", func(t *testing.T) {
		t.Parallel()
		l := sessionLimiter{maxSubs: 2}
		if !l.acquireSub() {
			t.Fatal("first sub acquire should succeed")
		}
		if !l.acquireSub() {
			t.Fatal("second sub acquire should succeed")
		}
		if l.acquireSub() {
			t.Fatal("third sub acquire over cap should fail")
		}
		l.releaseSub()
		if !l.acquireSub() {
			t.Fatal("sub acquire after release should succeed")
		}
		// The namespace counter is independent and unlimited here.
		if !l.acquireNamespace() {
			t.Fatal("namespace acquire should be unlimited when maxNS == 0")
		}
	})

	t.Run("namespace cap independent of subscription cap", func(t *testing.T) {
		t.Parallel()
		l := sessionLimiter{maxNS: 1}
		if !l.acquireNamespace() {
			t.Fatal("first namespace acquire should succeed")
		}
		if l.acquireNamespace() {
			t.Fatal("second namespace acquire over cap should fail")
		}
		if !l.acquireSub() {
			t.Fatal("sub acquire should be unlimited when maxSubs == 0")
		}
		l.releaseNamespace()
		if !l.acquireNamespace() {
			t.Fatal("namespace acquire after release should succeed")
		}
	})

	t.Run("release clamps at zero", func(t *testing.T) {
		t.Parallel()
		l := sessionLimiter{maxSubs: 1}
		l.releaseSub() // underflow guard: no effect at count 0
		if !l.acquireSub() {
			t.Fatal("acquire should succeed at count 0")
		}
		if l.acquireSub() {
			t.Fatal("second acquire should fail at cap 1")
		}
	})
}
