package broker

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Broker is a reconnecting AMQP client shared by Commander and Soldier. On
// connection loss it redials with exponential backoff (capped at 30s) and
// redeclares queues before serving publishes and consumes again.
type Broker struct {
	url    string
	log    *slog.Logger
	queues []string

	mu   sync.RWMutex
	conn *amqp.Connection
	ch   *amqp.Channel

	// pubMu serializes Publish calls. amqp091-go's Connection.sendUnflushed
	// acquires its write mutex per frame, not per Publish call. A single
	// Publish emits multiple frames (method, header, body), so without this lock,
	// concurrent Publish calls across goroutines on a shared channel can
	// interleave frames on the wire, causing protocol violations.
	pubMu sync.Mutex
}

// New returns a Broker that declares the specified queues on every connect.
func New(url string, log *slog.Logger, queues []string) *Broker {
	return &Broker{url: url, log: log, queues: queues}
}

func (b *Broker) dial() error {
	conn, err := amqp.Dial(b.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}
	for _, q := range b.queues {
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			conn.Close()
			return err
		}
	}
	b.mu.Lock()
	b.conn, b.ch = conn, ch
	b.mu.Unlock()
	return nil
}

// Connect establishes first connection, then supervises it: when
// connection closes it reconnects (backoff capped at 30s) until ctx ends.
func (b *Broker) Connect(ctx context.Context) error {
	if err := b.connectWithBackoff(ctx); err != nil {
		return err
	}
	go b.supervise(ctx)
	return nil
}

func (b *Broker) connectWithBackoff(ctx context.Context) error {
	backoff := time.Second
	for {
		if err := b.dial(); err == nil {
			b.log.Info("broker connected")
			return nil
		} else {
			b.log.Warn("broker dial failed", "backoff", backoff, "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (b *Broker) supervise(ctx context.Context) {
	for {
		b.mu.RLock()
		conn := b.conn
		b.mu.RUnlock()

		closed := make(chan *amqp.Error, 1)
		conn.NotifyClose(closed)

		select {
		case <-ctx.Done():
			return
		case err := <-closed:
			b.log.Warn("broker connection closed", "err", err)
			if rerr := b.connectWithBackoff(ctx); rerr != nil {
				return
			}
		}
	}
}

func (b *Broker) channel() (*amqp.Channel, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.ch == nil {
		return nil, errors.New("broker not connected")
	}
	return b.ch, nil
}

// Publish sends body to queue. Safe for concurrent use.
func (b *Broker) Publish(ctx context.Context, queue string, body []byte, headers amqp.Table) error {
	ch, err := b.channel()
	if err != nil {
		return err
	}
	return b.publishSerialized(func() error {
		return ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
			ContentType: "application/json",
			DeliveryMode: amqp.Persistent,
			Headers: headers,
			Body: body,
		})
	})
}

func (b *Broker) publishSerialized(fn func() error) error {
	b.pubMu.Lock()
	defer b.pubMu.Unlock()
	return fn()
}

// Consume streams queue deliveries to the provided callback, which handles ack/nack for each
// delivery it receives. If the callback panics, the panic is recovered, logged, and the
// delivery is nacked without requeue. If prefetch > 0, QoS is configured before consuming.
//
// The returned channel is closed once the consumer has permanently stopped (when ctx is cancelled).
func (b *Broker) Consume(ctx context.Context, queue string, prefetch int, callback func(amqp.Delivery)) (<-chan struct{}, error) {
	ch, err := b.channel()
	if err != nil {
		return nil, err
	}
	if prefetch > 0 {
		if err := ch.Qos(prefetch, 0, false); err != nil {
			return nil, err
		}
	}
	return b.runConsumer(ctx, queue, prefetch, callback)
}

func (b *Broker) runConsumer(ctx context.Context, queue string, prefetch int, callback func(amqp.Delivery)) (<-chan struct{}, error) {
	ch, err := b.channel()
	if err != nil {
		return nil, err
	}
	if prefetch > 0 {
		if err := ch.Qos(prefetch, 0, false); err != nil {
			return nil, err
		}
	}
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if b.forwardDeliveries(ctx, deliveries, callback) {
				// ctx cancelled
				return
			}
			b.log.Warn("consumer channel closed, resubscribing", "queue", queue)
			next, ok := b.resubscribe(ctx, queue, prefetch)
			if !ok {
				// ctx cancelled in resubscribe
				return
			}
			deliveries = next
		}
	}()
	return done, nil
}

// forwardDeliveries drains deliveries into callback until either ctx
// cancelled (returns true) or deliveries channel closes, e.g. because
// connection was lost (returns false, so caller can resubscribe).
func (b *Broker) forwardDeliveries(ctx context.Context, deliveries <-chan amqp.Delivery, callback func(amqp.Delivery)) (ctxDone bool) {
	for {
		select {
		case <-ctx.Done():
			return true
		case d, ok := <-deliveries:
			if !ok {
				return false
			}
			b.safeInvoke(callback, d)
		}
	}
}

// safeInvoke calls callback, recovering panic so one bad delivery can't
// take down process. On panic delivery is nacked without requeue on
// callback's behalf, since callback never got chance to.
func (b *Broker) safeInvoke(callback func(amqp.Delivery), d amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			b.log.Error("consumer callback panicked", "err", r, "stack", string(debug.Stack()))
			d.Nack(false, false)
		}
	}()
	callback(d)
}

// resubscribe polls until it can obtain live channel (i.e. until
// supervise() has redialed) and re-subscribes to queue on it, or until ctx
// cancelled.
func (b *Broker) resubscribe(ctx context.Context, queue string, prefetch int) (<-chan amqp.Delivery, bool) {
	const retryInterval = time.Second
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(retryInterval):
		}
		ch, err := b.channel()
		if err != nil {
			continue
		}
		if prefetch > 0 {
			if err := ch.Qos(prefetch, 0, false); err != nil {
				continue
			}
		}
		deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
		if err != nil {
			continue
		}
		return deliveries, true
	}
}
