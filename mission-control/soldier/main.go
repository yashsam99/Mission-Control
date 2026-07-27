package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
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

func main() {} // replaced in Task 9
