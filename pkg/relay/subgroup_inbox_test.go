package relay

import (
	"log/slog"
	"runtime"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

func pooledWriterForTest(p *subgroupInboxPool) *subgroupWriter {
	return &subgroupWriter{
		inbox: p.get(), inboxDone: make(chan struct{}), inboxPool: p,
		done: make(chan struct{}), metrics: NopMetrics{},
		log: slog.New(slog.DiscardHandler),
		sub: &registry.DownstreamSub{},
	}
}

func TestSubgroupInboxCloseDrainsBeforeReuse(t *testing.T) {
	p := newSubgroupInboxPool(4)
	w := pooledWriterForTest(p)
	original := w.inbox
	object := &message.SubgroupObject{}
	for i := range 4 {
		w.publish(fwdObject{obj: object, absID: uint64(i)})
	}
	w.close(false, 0)
	for i := range 4 {
		fwd, ok := w.nextObject()
		if !ok || fwd.absID != uint64(i) || fwd.obj != object {
			t.Fatalf("drain %d: %+v, %v", i, fwd, ok)
		}
	}
	if _, ok := w.nextObject(); ok {
		t.Fatal("closed inbox still produced an object")
	}
	w.releaseInbox()
	w.releaseInbox() // Never cache one channel twice.
	if w.inbox != nil || len(p.free) != 1 {
		t.Fatal("inbox was not detached exactly once")
	}
	if next := p.get(); next != original || len(next) != 0 {
		t.Fatal("inbox storage was not reused empty")
	}
}

func TestSubgroupInboxEarlyExitDropsReferences(t *testing.T) {
	p := newSubgroupInboxPool(4)
	w := pooledWriterForTest(p)
	original := w.inbox
	for range 4 {
		w.publish(fwdObject{obj: &message.SubgroupObject{}})
	}
	w.releaseInbox() // The lag/error exit need not consume the rest.
	if len(original) != 0 {
		t.Fatal("cached inbox retained queued objects")
	}
	w.close(true, 0) // Late parent close must not close the cached data channel.
	next := p.get()
	if next != original {
		t.Fatal("expected reuse")
	}
	next <- fwdObject{absID: 77}
	if got := <-next; got.absID != 77 {
		t.Fatal("late close affected reused channel")
	}
}

func TestSubgroupInboxLateProducerCannotReachReusedQueue(t *testing.T) {
	p := newSubgroupInboxPool(4)
	for range 200 {
		w := pooledWriterForTest(p)
		start := make(chan struct{})
		first := make(chan struct{})
		var producer sync.WaitGroup
		producer.Go(func() {
			<-start
			w.publish(fwdObject{absID: 99})
			close(first)
			for range 100 {
				runtime.Gosched()
				w.publish(fwdObject{absID: 99})
			}
		})
		close(start)
		<-first
		w.releaseInbox()
		reused := p.get()
		producer.Wait()
		w.publish(fwdObject{absID: 99})
		if len(reused) != 0 {
			t.Fatal("old producer wrote into a new subgroup")
		}
		p.put(reused)
	}
}

func TestSubgroupInboxCacheBoundAndClosedChannel(t *testing.T) {
	if newSubgroupInboxPool(maxPooledSubgroupQueueSize+1) != nil {
		t.Fatal("large queues should not be retained")
	}
	p := newSubgroupInboxPool(1)
	closed := make(chan fwdObject, 1)
	close(closed)
	p.put(closed)
	if len(p.free) != 0 {
		t.Fatal("cached a closed channel")
	}
	for range maxCachedSubgroupInboxes + 10 {
		p.put(make(chan fwdObject, 1))
	}
	if len(p.free) != maxCachedSubgroupInboxes {
		t.Fatal("unbounded cache", len(p.free))
	}
}

var benchmarkInboxWriter *subgroupWriter

func BenchmarkSubgroupInboxLifecycle(b *testing.B) {
	for _, reuse := range []bool{false, true} {
		name := "fresh"
		if reuse {
			name = "reused"
		}
		b.Run(name, func(b *testing.B) {
			p := newSubgroupInboxPool(defaultSendQueueSize)
			b.ReportAllocs()
			for b.Loop() {
				w := &subgroupWriter{done: make(chan struct{})}
				if reuse {
					w.inbox, w.inboxDone, w.inboxPool = p.get(), make(chan struct{}), p
				} else {
					w.inbox = make(chan fwdObject, defaultSendQueueSize)
				}
				w.close(false, 0)
				if _, ok := w.nextObject(); ok {
					b.Fatal("closed queue had an object")
				}
				w.releaseInbox()
				close(w.done)
				benchmarkInboxWriter = w
			}
		})
	}
}
