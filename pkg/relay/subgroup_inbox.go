package relay

// Retention is bounded per relay, and unusually large configured queues keep
// the original allocation path. Cached channels are open, empty, and have no
// object references. The writer owns its channel until releaseInbox completes.
const (
	maxCachedSubgroupInboxes   = 1024
	maxPooledSubgroupQueueSize = 256
)

type subgroupInboxPool struct {
	size int
	free chan chan fwdObject
}

func newSubgroupInboxPool(size int) *subgroupInboxPool {
	if size <= 0 || size > maxPooledSubgroupQueueSize {
		return nil
	}
	return &subgroupInboxPool{size: size, free: make(chan chan fwdObject, maxCachedSubgroupInboxes)}
}

func (p *subgroupInboxPool) get() chan fwdObject {
	select {
	case inbox := <-p.free:
		return inbox
	default:
		return make(chan fwdObject, p.size)
	}
}

func (p *subgroupInboxPool) put(inbox chan fwdObject) {
	if cap(inbox) != p.size {
		return
	}
	for {
		select {
		case _, ok := <-inbox:
			if !ok {
				return
			} // A legacy/test channel was closed; never reuse it.
		default:
			select {
			case p.free <- inbox:
			default: // The bounded cache is full; let this empty channel go.
			}
			return
		}
	}
}

func (w *subgroupWriter) nextObject() (fwdObject, bool) {
	if w.inboxDone == nil {
		fwd, ok := <-w.inbox
		return fwd, ok
	}
	// Avoid a two-channel select when data is already queued.
	select {
	case fwd, ok := <-w.inbox:
		return fwd, ok
	default:
	}
	select {
	case fwd, ok := <-w.inbox:
		return fwd, ok
	case <-w.inboxDone:
		// Closing stops producers, but queued objects still drain before FIN.
		select {
		case fwd, ok := <-w.inbox:
			return fwd, ok
		default:
			return fwdObject{}, false
		}
	}
}

func (w *subgroupWriter) releaseInbox() {
	if w.inboxPool == nil {
		return
	}
	// publish holds this lock through its nonblocking enqueue. A late producer
	// must see closed before the channel can belong to another subgroup.
	w.dropsMu.Lock()
	w.closed = true
	inbox := w.inbox
	w.inbox = nil
	w.dropsMu.Unlock()
	if inbox != nil {
		w.inboxPool.put(inbox)
	}
}
