package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// TestObjectDeliveryTimeout_FromLastHeaderByte: §5.2 has the relay "retain
// the time at which the last header byte of every object has been ...
// received from the upstream subscription" and measure OBJECT_DELIVERY_TIMEOUT
// from it. An Object whose payload trickles in for longer than the timeout is
// already past it when it completes, so the relay does not forward it.
func TestObjectDeliveryTimeout_FromLastHeaderByte(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess, message.ObjectDeliveryTimeoutParam(200*time.Millisecond))
	got := make(chan uint64, 8)
	go func() {
		for {
			ds, err := subSess.AcceptDataStream(t.Context())
			if err != nil {
				return
			}
			if in, ok := ds.(*session.IncomingSubgroupStream); ok {
				go func() {
					for {
						obj, err := in.ReadDecoded()
						if err != nil {
							return
						}
						got <- obj.ObjectID
					}
				}()
			}
		}
	}()

	sg, err := openSubgroupWaiting(t, pubSess, subgroupHeader(alias, 0))
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
		t.Fatalf("WriteObject 0: %v", err)
	}
	if _, err := sg.Write([]byte{0x00, 0x04, 'p', 'q'}); err != nil { // {0,1}, half
		t.Fatalf("Write {0,1} header: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := sg.Write([]byte{'r', 's'}); err != nil {
		t.Fatalf("Write rest of {0,1}: %v", err)
	}
	_ = sg.Close()

	deadline := time.After(time.Second)
	for {
		select {
		case id := <-got:
			if id == 1 {
				t.Fatal("{0,1} forwarded although its OBJECT_DELIVERY_TIMEOUT ran out while it arrived")
			}
		case <-deadline:
			return
		}
	}
}
