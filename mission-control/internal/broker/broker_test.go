package broker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeAcknowledger struct {
	mu     sync.Mutex
	acked  int
	nacked int
}

func (f *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked++
	return nil
}

func (f *fakeAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacked++
	return nil
}

func (f *fakeAcknowledger) Reject(tag uint64, requeue bool) error {
	return nil
}

func TestBrokerSafeInvokeCallsCallbackNormally(t *testing.T) {
	b := &Broker{log: testLogger()}
	fa := &fakeAcknowledger{}
	d := amqp.Delivery{Acknowledger: fa, DeliveryTag: 1}
	var called bool
	b.safeInvoke(func(delivery amqp.Delivery) {
		called = true
		delivery.Ack(false)
	}, d)
	if !called {
		t.Fatal("callback was not invoked")
	}
	if fa.acked != 1 {
		t.Fatalf("acked = %d, want 1", fa.acked)
	}
}

func TestBrokerSafeInvokeRecoversPanicAndNacks(t *testing.T) {
	b := &Broker{log: testLogger()}
	fa := &fakeAcknowledger{}
	d := amqp.Delivery{Acknowledger: fa, DeliveryTag: 1}
	b.safeInvoke(func(amqp.Delivery) {
		panic("boom")
	}, d)
	if fa.nacked != 1 {
		t.Fatalf("nacked = %d, want 1 after panicking callback", fa.nacked)
	}
	if fa.acked != 0 {
		t.Fatalf("acked = %d, want 0 after panicking callback", fa.acked)
	}
}

func TestBrokerForwardDeliveriesContinuesAfterPanic(t *testing.T) {
	b := &Broker{log: testLogger()}
	fa1 := &fakeAcknowledger{}
	fa2 := &fakeAcknowledger{}
	deliveries := make(chan amqp.Delivery, 2)
	processed := atomic.Int32{}
	deliveries <- amqp.Delivery{Acknowledger: fa1, DeliveryTag: 1}
	deliveries <- amqp.Delivery{Acknowledger: fa2, DeliveryTag: 2}
	close(deliveries)

	ctxDone := b.forwardDeliveries(context.Background(), deliveries, func(d amqp.Delivery) {
		if d.DeliveryTag == 1 {
			panic("boom")
		}
		d.Ack(false)
		processed.Add(1)
	})

	if ctxDone {
		t.Fatalf("ctxDone = %v, want false (channel closed, not ctx cancelled)", ctxDone)
	}
	if fa1.nacked != 1 {
		t.Fatalf("fa1.nacked = %d, want 1", fa1.nacked)
	}
	if fa2.acked != 1 {
		t.Fatalf("fa2.acked = %d, want 1", fa2.acked)
	}
	if processed.Load() != 1 {
		t.Fatalf("processed = %d, want 1", processed.Load())
	}
}

func TestBrokerForwardDeliveriesReturnsOnCtxDone(t *testing.T) {
	b := &Broker{log: testLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	deliveries := make(chan amqp.Delivery)
	cancel()

	ctxDone := b.forwardDeliveries(ctx, deliveries, func(amqp.Delivery) {})
	if !ctxDone {
		t.Fatal("forwardDeliveries should return true when ctx is done")
	}
}

func TestBrokerPublishSerializesConcurrentCalls(t *testing.T) {
	b := &Broker{log: testLogger()}
	var active int32
	var maxActive int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.publishSerialized(func() error {
				n := atomic.AddInt32(&active, 1)
				for {
					m := atomic.LoadInt32(&maxActive)
					if n <= m || atomic.CompareAndSwapInt32(&maxActive, m, n) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				atomic.AddInt32(&active, -1)
				return nil
			})
		}()
	}
	wg.Wait()
	if maxActive != 1 {
		t.Fatalf("max concurrent publishSerialized calls = %d, want 1 (pubMu must serialize Publish)", maxActive)
	}
}