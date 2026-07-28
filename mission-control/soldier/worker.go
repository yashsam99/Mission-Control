package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// StatusPublisher publishes a Soldier status update carrying its current JWT.
type StatusPublisher interface {
	PublishStatus(ctx context.Context, s StatusMessage, token string) error
}

// Worker executes one mission at a time.
type Worker struct {
	id          string
	tokens      *TokenStore
	pub         StatusPublisher
	sleep       func() time.Duration
	successProb float32
	rnd         *rand.Rand
	log         *slog.Logger
}

// publish sends a status update for missionID. It deliberately uses a
// fresh, bounded-timeout context for the actual AMQP publish rather than
// the ctx passed in by execute: execute's ctx may already be cancelled
// (graceful shutdown lets an in-flight mission finish before its order is
// ACKed), and amqp091-go's PublishWithContext short-circuits on an
// already-cancelled context without publishing anything. Using ctx here
// would silently drop the terminal COMPLETED/FAILED status while the
// order still gets ACKed, leaving the mission stuck IN_PROGRESS forever.
func (w *Worker) publish(ctx context.Context, missionID, status string) {
	msg := StatusMessage{
		MissionID: missionID,
		Status:    status,
		WorkerID:  w.id,
		Timestamp: time.Now().UTC(),
	}
	pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.pub.PublishStatus(pubCtx, msg, w.tokens.Get()); err != nil {
		w.log.Error("failed to publish status", "mission_id", missionID, "status", status, "err", err)
	}
}

// execute runs the simulated mission and publishes IN_PROGRESS then a terminal
// state. It intentionally does NOT abort mid-work on ctx cancellation so that a
// graceful shutdown lets the in-flight mission finish before its order is ACKed.
func (w *Worker) execute(ctx context.Context, m Mission) {
	w.publish(ctx, m.ID, "IN_PROGRESS")
	time.Sleep(w.sleep())
	status := "COMPLETED"
	if w.rnd.Float32() >= w.successProb {
		status = "FAILED"
	}
	w.publish(ctx, m.ID, status)
	w.log.Info("mission finished", "mission_id", m.ID, "status", status, "worker", w.id)
}

// missionSleep is the production sleep: 5–14s inclusive.
func missionSleep() time.Duration {
	return time.Duration(rand.Intn(10)+5) * time.Second
}

// runWorkerPool starts `size` worker goroutines. Each drains the missions
// channel, executes the mission, then ACKs its order. wg tracks active
// missions so the caller can wait for a clean drain on shutdown.
func runWorkerPool(ctx context.Context, size int, missions <-chan ackableMission, newWorker func(id string) *Worker, wg *sync.WaitGroup) {
	for i := range size {
		w := newWorker(fmt.Sprintf("worker-%d", i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for am := range missions {
				w.execute(ctx, am.mission)
				am.ack()
			}
		}()
	}
}
