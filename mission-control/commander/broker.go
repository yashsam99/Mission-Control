package main

import (
	"context"
	"encoding/json"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	"mission-control/internal/broker"
)

// OrderPublisher publishes mission orders to the broker.
type OrderPublisher interface {
	PublishOrder(ctx context.Context, m Mission) error
}

// Broker is Commander's thin adapter over the shared reconnecting AMQP
// client in internal/broker.
type Broker struct {
	inner *broker.Broker
}

func NewBroker(url string, log *slog.Logger) *Broker {
	return &Broker{inner: broker.New(url, log, []string{ordersQueue, statusQueue})}
}

func (b *Broker) Connect(ctx context.Context) error {
	return b.inner.Connect(ctx)
}

func (b *Broker) PublishOrder(ctx context.Context, m Mission) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return b.inner.Publish(ctx, ordersQueue, body, nil)
}

func (b *Broker) PublishStatus(ctx context.Context, s StatusMessage, token string) error {
	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return b.inner.Publish(ctx, statusQueue, body, amqp.Table{"authorization": token})
}

// ConsumeStatus streams status_queue deliveries into handle. A nil error
// ACKs; a non-nil error (or a panic inside handle, recovered by
// internal/broker) nacks without requeue.
func (b *Broker) ConsumeStatus(ctx context.Context, handle func(amqp.Table, []byte) error) error {
	_, err := b.inner.Consume(ctx, statusQueue, 0, func(d amqp.Delivery) {
		if err := handle(d.Headers, d.Body); err != nil {
			d.Nack(false, false)
			return
		}
		d.Ack(false)
	})
	return err
}
