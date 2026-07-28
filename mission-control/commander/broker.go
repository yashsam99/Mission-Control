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

// OrderPublisher publishes mission orders to the broker.
type OrderPublisher interface {
	PublishOrder(ctx context.Context, m Mission) error
}

// Broker is a reconnecting AMQP client. On connection loss it re-dials with
// exponential backoff and re-declares queues before serving publishes again.
type Broker struct {
	url string
	log *slog.Logger

	mu   sync.RWMutex
	conn *amqp.Connection
	ch   *amqp.Channel
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

// Connect establishes the first connection, then supervises it: when the
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

func (b *Broker) supervise(ctx context.Context) {
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
				return // ctx cancelled
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

func (b *Broker) publish(ctx context.Context, queue string, body []byte, headers amqp.Table) error {
	ch, err := b.channel()
	if err != nil {
		return err
	}
	return ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Headers:      headers,
		Body:         body,
	})
}

func (b *Broker) PublishOrder(ctx context.Context, m Mission) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return b.publish(ctx, ordersQueue, body, nil)
}

func (b *Broker) PublishStatus(ctx context.Context, s StatusMessage, token string) error {
	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return b.publish(ctx, statusQueue, body, amqp.Table{"authorization": token})
}

// ConsumeStatus streams status_queue deliveries into handle. A nil error ACKs;
// a non-nil error nacks without requeue (poison/breach messages are dropped).
// If the connection drops and supervise() redials, the consumer resubscribes
// to status_queue on the new channel instead of going silent — see
// runStatusConsumer.
func (b *Broker) ConsumeStatus(ctx context.Context, handle func(amqp.Table, []byte) error) error {
	ch, err := b.channel()
	if err != nil {
		return err
	}
	deliveries, err := ch.Consume(statusQueue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go b.runStatusConsumer(ctx, handle, deliveries)
	return nil
}

// runStatusConsumer forwards deliveries until ctx is cancelled. If deliveries
// closes because the connection dropped (rather than ctx being done), it
// polls until a freshly-reconnected channel is available and re-subscribes
// to status_queue, instead of returning and leaving status updates unrecorded
// for the rest of the process's life.
func (b *Broker) runStatusConsumer(ctx context.Context, handle func(amqp.Table, []byte) error, deliveries <-chan amqp.Delivery) {
	for {
		if b.forwardStatusDeliveries(ctx, deliveries, handle) {
			return
		}
		b.log.Warn("status consumer channel closed, resubscribing to status_queue")
		next, ok := b.resubscribeStatus(ctx)
		if !ok {
			return
		}
		deliveries = next
	}
}

// forwardStatusDeliveries drains deliveries into handle until either ctx is
// cancelled (returns true) or the deliveries channel closes, e.g. because the
// connection was lost (returns false, so the caller can resubscribe).
func (b *Broker) forwardStatusDeliveries(ctx context.Context, deliveries <-chan amqp.Delivery, handle func(amqp.Table, []byte) error) (ctxDone bool) {
	for {
		select {
		case <-ctx.Done():
			return true
		case d, ok := <-deliveries:
			if !ok {
				return false
			}
			if herr := handle(d.Headers, d.Body); herr != nil {
				d.Nack(false, false)
			} else {
				d.Ack(false)
			}
		}
	}
}

// resubscribeStatus polls until it can obtain a live channel (i.e. until
// supervise() has redialed) and re-subscribe to status_queue on it, or until
// ctx is cancelled.
func (b *Broker) resubscribeStatus(ctx context.Context) (<-chan amqp.Delivery, bool) {
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
		deliveries, err := ch.Consume(statusQueue, "", false, false, false, false, nil)
		if err != nil {
			continue
		}
		return deliveries, true
	}
}
