package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Mission is a unit of work tracked by the Commander.
type Mission struct {
	ID        string    `json:"mission_id"`
	Objective string    `json:"objective"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// MissionStore is a thread-safe in-memory mission registry.
type MissionStore struct {
	mu       sync.RWMutex
	missions map[string]Mission
}

// NewMissionStore returns an empty, ready-to-use MissionStore.
func NewMissionStore() *MissionStore {
	return &MissionStore{
		missions: make(map[string]Mission),
	}
}

// Create stores the mission, stamping it with the current time if not already set.
func (s *MissionStore) Create(mission Mission) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if mission.CreatedAt.IsZero() {
		mission.CreatedAt = time.Now().UTC()
	}
	mission.UpdatedAt = time.Now().UTC()
	s.missions[mission.ID] = mission
}

// UpdateStatus sets a new status; returns false if the mission is unknown.
func (s *MissionStore) UpdateStatus(id, status string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	mission, ok := s.missions[id]
	if !ok {
		return false
	}
	mission.Status = status
	mission.UpdatedAt = time.Now().UTC()
	s.missions[id] = mission
	return true
}

// Get returns the mission with the given id; the bool is false if unknown.
func (s *MissionStore) Get(id string) (Mission, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	mission, ok := s.missions[id]
	return mission, ok
}

// signToken issues an HS256 JWT valid for ttl.
func signToken(secret []byte, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		IssuedAt:  jwt.NewNumericDate(now),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
}

// validateToken returns nil only if the token is well-formed, HS256-signed
// with secret, and not expired.
func validateToken(secret []byte, tokenString string) error {
	_, err := jwt.ParseWithClaims(
		tokenString,
		&jwt.RegisteredClaims{},
		func(*jwt.Token) (any, error) { return secret, nil },
		jwt.WithValidMethods([]string{"HS256"}),
	)
	return err
}

const tokenTTL = 30 * time.Second

// StatusMessage is a state update published by a Soldier worker.
type StatusMessage struct {
	MissionID string    `json:"mission_id"`
	Status    string    `json:"status"`
	WorkerID  string    `json:"worker_id"`
	Timestamp time.Time `json:"timestamp"`
}

// OrderPublisher publishes mission orders to the broker.
type OrderPublisher interface {
	PublishOrder(ctx context.Context, m Mission) error
}

type Commander struct {
	store           *MissionStore
	publisher       OrderPublisher
	bootstrapSecret string
	jwtSecret       []byte
	log             *slog.Logger
}

func (c *Commander) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /missions", c.handleCreateMission)
	mux.HandleFunc("GET /missions/{id}", c.handleGetMission)
	mux.HandleFunc("POST /auth", c.handleAuth)
	mux.HandleFunc("GET /health", c.handleHealth)
	return mux
}

// writeJSON writes v as a JSON response with the given status code.
func (c *Commander) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		c.log.Error("failed to encode response", "err", err)
	}
}

func (c *Commander) handleCreateMission(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Objective string `json:"objective"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	mission := Mission{
		ID:        uuid.NewString(),
		Objective: req.Objective,
		Status:    "QUEUED",
		CreatedAt: time.Now().UTC(),
	}
	c.store.Create(mission)
	if err := c.publisher.PublishOrder(r.Context(), mission); err != nil {
		c.log.Error("failed to publish order", "mission_id", mission.ID, "err", err)
		http.Error(w, "failed to dispatch order", http.StatusServiceUnavailable)
		return
	}
	c.writeJSON(w, http.StatusAccepted, map[string]string{"mission_id": mission.ID})
}

func (c *Commander) handleGetMission(w http.ResponseWriter, r *http.Request) {
	mission, ok := c.store.Get(r.PathValue("id"))
	if !ok {
		http.Error(w, "mission not found", http.StatusNotFound)
		return
	}
	c.writeJSON(w, http.StatusOK, mission)
}

func (c *Commander) handleAuth(w http.ResponseWriter, r *http.Request) {
	var req struct {
		BootstrapSecret string `json:"bootstrap_secret"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if subtle.ConstantTimeCompare([]byte(req.BootstrapSecret), []byte(c.bootstrapSecret)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	tok, err := signToken(c.jwtSecret, tokenTTL)
	if err != nil {
		http.Error(w, "token generation failed", http.StatusInternalServerError)
		return
	}
	c.writeJSON(w, http.StatusOK, map[string]any{
		"token":      tok,
		"expires_in": int(tokenTTL.Seconds()),
	})
}

func (c *Commander) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

const (
	ordersQueue = "orders_queue"
	statusQueue = "status_queue"
)

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

func (c *Commander) handleStatus(headers amqp.Table, body []byte) error {
	token, _ := headers["authorization"].(string)
	if err := validateToken(c.jwtSecret, token); err != nil {
		c.log.Warn("SECURITY BREACH: rejected status message", "err", err)
		return fmt.Errorf("security breach: %w", err)
	}
	var s StatusMessage
	if err := json.Unmarshal(body, &s); err != nil {
		return err
	}
	if !c.store.UpdateStatus(s.MissionID, s.Status) {
		c.log.Warn("status for unknown mission", "mission_id", s.MissionID)
	}
	return nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	port := getenv("HTTP_PORT", "8080")
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(runHealthcheck(port))
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	broker := NewBroker(getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"), log)
	if err := broker.Connect(ctx); err != nil {
		log.Error("broker connect failed", "err", err)
		os.Exit(1)
	}

	c := &Commander{
		store:           NewMissionStore(),
		publisher:       broker,
		bootstrapSecret: getenv("BOOTSTRAP_SECRET", "bootstrap"),
		jwtSecret:       []byte(getenv("JWT_SECRET", "jwt-secret")),
		log:             log,
	}
	if err := broker.ConsumeStatus(ctx, c.handleStatus); err != nil {
		log.Error("status consumer failed", "err", err)
		os.Exit(1)
	}

	srv := &http.Server{Addr: net.JoinHostPort("", port), Handler: c.routes()}
	go func() {
		log.Info("commander HTTP listening", "port", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down commander")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
}

func runHealthcheck(port string) int {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/health")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return 0
	}
	return 1
}
