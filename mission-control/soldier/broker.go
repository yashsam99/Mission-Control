package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Broker is the Soldier's reconnecting AMQP client.
type Broker struct {
	url string
	log *slog.Logger

	mu   sync.RWMutex
	conn *amqp.Connection
	ch   *amqp.Channel

	// ordersDone is closed when ConsumeOrders's internal delivery-forwarding
	// goroutine actually returns (via ctx cancellation or the deliveries
	// channel closing). Callers use OrdersDone() to know it is safe to close
	// the missions channel without risking a send-on-closed-channel panic.
	ordersDone chan struct{}
}

func NewBroker(url string, log *slog.Logger) *Broker {
	return &Broker{url: url, log: log}
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
	for _, q := range []string{ordersQueue, statusQueue} {
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

func (b *Broker) connectWithBackoff(ctx context.Context) error {
	backoff := time.Second
	for {
		err := b.dial()
		if err == nil {
			b.log.Info("broker connected")
			return nil
		}
		b.log.Warn("broker dial failed, retrying", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (b *Broker) Connect(ctx context.Context) error {
	if err := b.connectWithBackoff(ctx); err != nil {
		return err
	}
	go func() {
		for {
			b.mu.RLock()
			conn := b.conn
			b.mu.RUnlock()
			closed := conn.NotifyClose(make(chan *amqp.Error, 1))
			select {
			case <-ctx.Done():
				return
			case err := <-closed:
				b.log.Warn("broker connection lost, reconnecting", "err", err)
				if rerr := b.connectWithBackoff(ctx); rerr != nil {
					return
				}
			}
		}
	}()
	return nil
}

func (b *Broker) channel() (*amqp.Channel, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.ch == nil {
		return nil, errors.New("broker not connected")
	}
	return b.ch, nil
}

func (b *Broker) PublishStatus(ctx context.Context, s StatusMessage, token string) error {
	ch, err := b.channel()
	if err != nil {
		return err
	}
	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(ctx, "", statusQueue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Headers:      amqp.Table{"authorization": token},
		Body:         body,
	})
}

// ConsumeOrders sets QoS to `prefetch` (= pool size) so the Soldier never holds
// more unacked orders than it has workers, then forwards deliveries onto the
// missions channel with an ack closure bound to each delivery. If the
// connection drops and Connect's supervisor goroutine redials, the returned
// consumer resubscribes to orders_queue on the new channel instead of
// silently going idle — see runOrdersConsumer.
func (b *Broker) ConsumeOrders(ctx context.Context, prefetch int, missions chan<- ackableMission) error {
	ch, err := b.channel()
	if err != nil {
		return err
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(ordersQueue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	b.ordersDone = make(chan struct{})
	go b.runOrdersConsumer(ctx, prefetch, missions, deliveries)
	return nil
}

// runOrdersConsumer forwards deliveries until ctx is cancelled. If deliveries
// closes because the underlying connection dropped (rather than ctx being
// done), it polls until a freshly-reconnected channel is available and
// re-subscribes to orders_queue, instead of returning and leaving the
// soldier permanently idle.
func (b *Broker) runOrdersConsumer(ctx context.Context, prefetch int, missions chan<- ackableMission, deliveries <-chan amqp.Delivery) {
	defer close(b.ordersDone)
	for {
		if b.forwardOrderDeliveries(ctx, deliveries, missions) {
			return
		}
		b.log.Warn("orders consumer channel closed, resubscribing to orders_queue")
		next, ok := b.resubscribeOrders(ctx, prefetch)
		if !ok {
			return
		}
		deliveries = next
	}
}

// forwardOrderDeliveries drains deliveries onto missions until either ctx is
// cancelled (returns true) or the deliveries channel closes, e.g. because the
// connection was lost (returns false, so the caller can resubscribe).
func (b *Broker) forwardOrderDeliveries(ctx context.Context, deliveries <-chan amqp.Delivery, missions chan<- ackableMission) (ctxDone bool) {
	for {
		select {
		case <-ctx.Done():
			return true
		case d, ok := <-deliveries:
			if !ok {
				return false
			}
			var m Mission
			if err := json.Unmarshal(d.Body, &m); err != nil {
				b.log.Error("bad order payload, dropping", "err", err)
				d.Nack(false, false)
				continue
			}
			delivery := d
			// Use a select (rather than a plain blocking send) so that a
			// ctx cancellation racing with a full/unread missions channel
			// can't leave this goroutine stuck trying to send after the
			// caller closes missions on shutdown.
			select {
			case missions <- ackableMission{mission: m, ack: func() { delivery.Ack(false) }}:
			case <-ctx.Done():
				return true
			}
		}
	}
}

// resubscribeOrders polls until it can obtain a live channel (i.e. until
// Connect's supervisor goroutine has redialed) and re-establish QoS plus a
// fresh orders_queue consumer on it, or until ctx is cancelled.
func (b *Broker) resubscribeOrders(ctx context.Context, prefetch int) (<-chan amqp.Delivery, bool) {
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
		if err := ch.Qos(prefetch, 0, false); err != nil {
			continue
		}
		deliveries, err := ch.Consume(ordersQueue, "", false, false, false, false, nil)
		if err != nil {
			continue
		}
		return deliveries, true
	}
}

// OrdersDone returns a channel that is closed once runOrdersConsumer has
// actually returned (i.e. ctx was cancelled — a lost connection alone just
// triggers a resubscribe, not a return). Callers must wait on this before
// closing the missions channel passed to ConsumeOrders, to avoid a
// send-on-closed-channel panic if that goroutine is mid-send.
func (b *Broker) OrdersDone() <-chan struct{} { return b.ordersDone }
