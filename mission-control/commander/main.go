package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
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

func main() {} // replaced in Task 5
