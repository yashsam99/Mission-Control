package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

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
		InstanceID      string `json:"instance_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if subtle.ConstantTimeCompare([]byte(req.BootstrapSecret), []byte(c.bootstrapSecret)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if _, err := uuid.Parse(req.InstanceID); err != nil {
		http.Error(w, "invalid instance_id", http.StatusBadRequest)
		return
	}
	tok, err := signToken(c.jwtSecret, tokenTTL, req.InstanceID)
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

func (c *Commander) handleStatus(headers amqp.Table, body []byte) error {
	token, _ := headers["authorization"].(string)
	claims, err := validateToken(c.jwtSecret, token)
	if err != nil {
		c.log.Warn("SECURITY BREACH: rejected status message", "err", err)
		return fmt.Errorf("security breach: %w", err)
	}
	var s StatusMessage
	if err := json.Unmarshal(body, &s); err != nil {
		return err
	}
	if !c.store.UpdateStatus(s.MissionID, s.Status) {
		c.log.Warn("status update rejected", "mission_id", s.MissionID, "status", s.Status,
			"instance", claims.Subject, "worker_id", s.WorkerID)
	}
	return nil
}
