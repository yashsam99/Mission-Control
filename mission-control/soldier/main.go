package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Mission struct {
	ID        string    `json:"mission_id"`
	Objective string    `json:"objective"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type StatusMessage struct {
	MissionID string    `json:"mission_id"`
	Status    string    `json:"status"`
	WorkerID  string    `json:"worker_id"`
	Timestamp time.Time `json:"timestamp"`
}

// TokenStore is a lock-free holder for the current rotating JWT.
type TokenStore struct {
	v atomic.Value
}

func (t *TokenStore) Set(tok string) { t.v.Store(tok) }

func (t *TokenStore) Get() string {
	s, _ := t.v.Load().(string)
	return s
}

// fetchToken performs POST /auth with the bootstrap secret and returns the JWT.
func fetchToken(ctx context.Context, client *http.Client, commanderURL, bootstrap string) (string, error) {
	payload, _ := json.Marshal(struct {
		BootstrapSecret string `json:"bootstrap_secret"`
	}{bootstrap})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, commanderURL+"/auth", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("auth failed: status %d", resp.StatusCode)
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	if body.Token == "" {
		return "", errors.New("auth response missing token")
	}
	return body.Token, nil
}

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

func (w *Worker) publish(ctx context.Context, missionID, status string) {
	msg := StatusMessage{
		MissionID: missionID,
		Status:    status,
		WorkerID:  w.id,
		Timestamp: time.Now().UTC(),
	}
	if err := w.pub.PublishStatus(ctx, msg, w.tokens.Get()); err != nil {
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

const (
	ordersQueue = "orders_queue"
	statusQueue = "status_queue"
)

type ackableMission struct {
	mission Mission
	ack     func()
}

// runWorkerPool starts `size` worker goroutines. Each drains the missions
// channel, executes the mission, then ACKs its order. wg tracks active
// missions so the caller can wait for a clean drain on shutdown.
func runWorkerPool(ctx context.Context, size int, missions <-chan ackableMission, newWorker func(id string) *Worker, wg *sync.WaitGroup) {
	for i := 0; i < size; i++ {
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
// missions channel with an ack closure bound to each delivery.
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
	go func() {
		defer close(b.ordersDone)
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					return
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
					return
				}
			}
		}
	}()
	return nil
}

// OrdersDone returns a channel that is closed once ConsumeOrders's internal
// delivery-forwarding goroutine has actually returned. Callers must wait on
// this before closing the missions channel passed to ConsumeOrders, to avoid
// a send-on-closed-channel panic if that goroutine is mid-send.
func (b *Broker) OrdersDone() <-chan struct{} { return b.ordersDone }

const rotationInterval = 25 * time.Second

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	commanderURL := getenv("COMMANDER_URL", "http://localhost:8080")
	bootstrap := getenv("BOOTSTRAP_SECRET", "bootstrap")
	poolSize, err := strconv.Atoi(getenv("WORKER_POOL_SIZE", "10"))
	if err != nil || poolSize < 1 {
		poolSize = 10
	}
	httpClient := &http.Client{Timeout: 10 * time.Second}

	// Bootstrap: acquire the first token before doing anything else.
	tokens := &TokenStore{}
	firstTok, err := fetchToken(ctx, httpClient, commanderURL, bootstrap)
	if err != nil {
		log.Error("initial token acquisition failed", "err", err)
		os.Exit(1)
	}
	tokens.Set(firstTok)
	log.Info("bootstrap token acquired")

	// Rotation goroutine.
	go func() {
		ticker := time.NewTicker(rotationInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tok, err := fetchToken(ctx, httpClient, commanderURL, bootstrap)
				if err != nil {
					log.Warn("token rotation failed, keeping previous token", "err", err)
					continue
				}
				tokens.Set(tok)
				log.Info("Token Rotated")
			}
		}
	}()

	broker := NewBroker(getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"), log)
	if err := broker.Connect(ctx); err != nil {
		log.Error("broker connect failed", "err", err)
		os.Exit(1)
	}

	missions := make(chan ackableMission, poolSize)
	newWorker := func(id string) *Worker {
		return &Worker{
			id: id, tokens: tokens, pub: broker,
			sleep: missionSleep, successProb: 0.90,
			rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
			log: log,
		}
	}
	var wg sync.WaitGroup
	runWorkerPool(ctx, poolSize, missions, newWorker, &wg)

	if err := broker.ConsumeOrders(ctx, poolSize, missions); err != nil {
		log.Error("order consumer failed", "err", err)
		os.Exit(1)
	}
	log.Info("soldier ready", "workers", poolSize)

	<-ctx.Done()
	log.Info("shutting down soldier: draining in-flight missions")
	<-broker.OrdersDone() // wait for the consumer goroutine to actually stop before closing
	close(missions)
	wg.Wait()
	log.Info("drain complete")
}
