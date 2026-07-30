package main

import (
	"context"
	"encoding/json"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	"mission-control/internal/broker"
)

// Broker is Soldier's thin adapter over the shared reconnecting AMQP client
// in internal/broker.
type Broker struct {
	log   *slog.Logger
	inner *broker.Broker

	ordersDone <-chan struct{}
}

func NewBroker(url string, log *slog.Logger) *Broker {
	return &Broker{log: log, inner: broker.New(url, log, []string{ordersQueue, statusQueue})}
}

func (b *Broker) Connect(ctx context.Context) error {
	return b.inner.Connect(ctx)
}

func (b *Broker) PublishStatus(ctx context.Context, s StatusMessage, token string) error {
	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return b.inner.Publish(ctx, statusQueue, body, amqp.Table{"authorization": token})
}

// ConsumeOrders sets QoS to prefetch (= pool size) so the Soldier never holds
// more unacked orders than it has workers, then forwards decoded missions
// onto missions with an ack closure bound to each delivery. A malformed
// order body is logged and nacked without requeue rather than crashing the
// consumer (and a panic decoding/forwarding it is recovered by
// internal/broker for the same reason).
func (b *Broker) ConsumeOrders(ctx context.Context, prefetch int, missions chan<- ackableMission) error {
	done, err := b.inner.Consume(ctx, ordersQueue, prefetch, func(d amqp.Delivery) {
		var m Mission
		if err := json.Unmarshal(d.Body, &m); err != nil {
			b.log.Error("bad order payload, dropping", "err", err)
			d.Nack(false, false)
			return
		}
		delivery := d
		select {
		case missions <- ackableMission{mission: m, ack: func() { delivery.Ack(false) }}:
		case <-ctx.Done():
		}
	})
	if err != nil {
		return err
	}
	b.ordersDone = done
	return nil
}

// OrdersDone returns a channel that is closed once the orders consumer has
// actually stopped (ctx cancelled — a lost connection alone just triggers a
// resubscribe). Callers must wait on this before closing the missions
// channel passed to ConsumeOrders, to avoid a send-on-closed-channel panic
// if that goroutine is mid-send.
func (b *Broker) OrdersDone() <-chan struct{} { return b.ordersDone }
