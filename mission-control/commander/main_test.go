package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestMissionStoreCreateAndGet(t *testing.T) {
	store := NewMissionStore()

	mission := Mission{
		ID:        "m1",
		Objective: "Secure the perimeter",
		Status:    "PENDING",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	store.Create(mission)

	retrieved, ok := store.Get("m1")
	if !ok {
		t.Fatal("expected to retrieve mission, got not found")
	}

	if retrieved.ID != mission.ID {
		t.Errorf("expected ID %q, got %q", mission.ID, retrieved.ID)
	}
	if retrieved.Objective != mission.Objective {
		t.Errorf("expected Objective %q, got %q", mission.Objective, retrieved.Objective)
	}
	if retrieved.Status != mission.Status {
		t.Errorf("expected Status %q, got %q", mission.Status, retrieved.Status)
	}
}

func TestMissionStoreUpdateStatus(t *testing.T) {
	store := NewMissionStore()

	mission := Mission{
		ID:        "m2",
		Objective: "Infiltrate the compound",
		Status:    "QUEUED",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	store.Create(mission)

	// Update status to IN_PROGRESS
	ok := store.UpdateStatus("m2", "IN_PROGRESS")
	if !ok {
		t.Fatal("expected UpdateStatus to succeed")
	}

	// Verify status was updated
	retrieved, ok := store.Get("m2")
	if !ok {
		t.Fatal("expected to retrieve mission after status update")
	}
	if retrieved.Status != "IN_PROGRESS" {
		t.Errorf("expected Status %q, got %q", "IN_PROGRESS", retrieved.Status)
	}

	// Try to update non-existent mission
	ok = store.UpdateStatus("non-existent", "COMPLETED")
	if ok {
		t.Fatal("expected UpdateStatus to fail for non-existent mission")
	}
}

func TestMissionStoreUpdateStatusTransitions(t *testing.T) {
	cases := []struct {
		from string
		to   string
		want bool
	}{
		{"QUEUED", "IN_PROGRESS", true},
		{"QUEUED", "COMPLETED", true},
		{"QUEUED", "FAILED", true},
		{"QUEUED", "QUEUED", false},
		{"IN_PROGRESS", "COMPLETED", true},
		{"IN_PROGRESS", "FAILED", true},
		{"IN_PROGRESS", "QUEUED", false},
		{"IN_PROGRESS", "IN_PROGRESS", false},
		{"COMPLETED", "IN_PROGRESS", false},
		{"COMPLETED", "COMPLETED", false},
		{"COMPLETED", "FAILED", false},
		{"FAILED", "COMPLETED", false},
		{"FAILED", "IN_PROGRESS", false},
		{"FAILED", "FAILED", false},
	}
	for _, tc := range cases {
		t.Run(tc.from+"->"+tc.to, func(t *testing.T) {
			store := NewMissionStore()
			store.Create(Mission{ID: "m", Status: tc.from})
			got := store.UpdateStatus("m", tc.to)
			if got != tc.want {
				t.Fatalf("UpdateStatus(%s -> %s) = %v, want %v", tc.from, tc.to, got, tc.want)
			}
			m, _ := store.Get("m")
			if tc.want && m.Status != tc.to {
				t.Fatalf("status = %q, want %q", m.Status, tc.to)
			}
			if !tc.want && m.Status != tc.from {
				t.Fatalf("status changed to %q on rejected transition, want unchanged %q", m.Status, tc.from)
			}
		})
	}
}

func TestMissionStoreConcurrent(t *testing.T) {
	store := NewMissionStore()
	const numGoroutines = 100

	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mission := Mission{
				ID:        "m" + string(rune('0'+idx/10)) + string(rune('0'+idx%10)),
				Objective: "Mission",
				Status:    "QUEUED",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			}
			store.Create(mission)
		}(i)
	}

	// Concurrent reads and writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := "m" + string(rune('0'+idx/10)) + string(rune('0'+idx%10))
			store.Get(id)
			store.UpdateStatus(id, "IN_PROGRESS")
		}(i)
	}

	wg.Wait()
}

func TestMissionStoreCreatedAtTimestamp(t *testing.T) {
	store := NewMissionStore()

	// Create a mission without setting CreatedAt (zero value)
	mission := Mission{
		ID:        "m_timestamp",
		Objective: "Test timestamp",
		Status:    "QUEUED",
		// CreatedAt intentionally left unset (zero value)
		// UpdatedAt intentionally left unset (zero value)
	}

	store.Create(mission)

	retrieved, ok := store.Get("m_timestamp")
	if !ok {
		t.Fatal("expected to retrieve mission after Create")
	}

	// Verify CreatedAt was stamped with non-zero value
	if retrieved.CreatedAt.IsZero() {
		t.Fatal("expected CreatedAt to be set by Create(), but got zero value")
	}

	// Verify UpdatedAt was also stamped
	if retrieved.UpdatedAt.IsZero() {
		t.Fatal("expected UpdatedAt to be set by Create(), but got zero value")
	}

	// Verify both timestamps are close to now
	now := time.Now().UTC()
	if retrieved.CreatedAt.After(now) {
		t.Errorf("CreatedAt is in the future: %v", retrieved.CreatedAt)
	}
	if retrieved.UpdatedAt.After(now) {
		t.Errorf("UpdatedAt is in the future: %v", retrieved.UpdatedAt)
	}
}

func TestSignAndValidateToken(t *testing.T) {
	secret := []byte("test-secret-key")
	ttl := 1 * time.Hour
	subject := uuid.NewString()

	token, err := signToken(secret, ttl, subject)
	if err != nil {
		t.Fatalf("signToken failed: %v", err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}

	claims, err := validateToken(secret, token)
	if err != nil {
		t.Fatalf("validateToken failed: %v", err)
	}
	if claims.Subject != subject {
		t.Fatalf("claims.Subject = %q, want %q", claims.Subject, subject)
	}
}

func TestValidateTokenMissingSubject(t *testing.T) {
	secret := []byte("test-secret-key")
	claims := jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		IssuedAt:  jwt.NewNumericDate(time.Now()),
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
	if err != nil {
		t.Fatalf("failed to build test token: %v", err)
	}
	if _, err := validateToken(secret, tok); err == nil {
		t.Fatal("expected validateToken to reject a token with no subject claim")
	}
}

func TestValidateTokenMalformedSubject(t *testing.T) {
	secret := []byte("test-secret-key")
	tok, err := signToken(secret, time.Hour, "not-a-uuid")
	if err != nil {
		t.Fatalf("signToken failed: %v", err)
	}
	if _, err := validateToken(secret, tok); err == nil {
		t.Fatal("expected validateToken to reject a non-UUID subject")
	}
}

func TestValidateTokenExpired(t *testing.T) {
	secret := []byte("test-secret-key")
	ttl := -1 * time.Second // already expired

	token, err := signToken(secret, ttl, uuid.NewString())
	if err != nil {
		t.Fatalf("signToken failed: %v", err)
	}

	if _, err := validateToken(secret, token); err == nil {
		t.Fatal("expected validateToken to fail for expired token, but got nil")
	}
}

func TestValidateTokenBadSignature(t *testing.T) {
	secret := []byte("test-secret-key")
	ttl := 1 * time.Hour

	token, err := signToken(secret, ttl, uuid.NewString())
	if err != nil {
		t.Fatalf("signToken failed: %v", err)
	}

	wrongSecret := []byte("wrong-secret")
	if _, err := validateToken(wrongSecret, token); err == nil {
		t.Fatal("expected validateToken to fail with wrong secret, but got nil")
	}
}

type fakePublisher struct{ published []Mission }

func (f *fakePublisher) PublishOrder(_ context.Context, m Mission) error {
	f.published = append(f.published, m)
	return nil
}

func newTestCommander() (*Commander, *fakePublisher) {
	fp := &fakePublisher{}
	c := &Commander{
		store:           NewMissionStore(),
		publisher:       fp,
		bootstrapSecret: "boot",
		jwtSecret:       []byte("jwt"),
		log:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return c, fp
}

func TestPostMissionQueuesAndPublishes(t *testing.T) {
	c, fp := newTestCommander()
	srv := httptest.NewServer(c.routes())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/missions", "application/json",
		strings.NewReader(`{"objective":"recon"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	var body struct {
		MissionID string `json:"mission_id"`
	}
	json.NewDecoder(resp.Body).Decode(&body)
	if body.MissionID == "" {
		t.Fatal("expected a mission_id in response")
	}
	if len(fp.published) != 1 {
		t.Fatalf("published %d orders, want 1", len(fp.published))
	}
	m, ok := c.store.Get(body.MissionID)
	if !ok || m.Status != "QUEUED" {
		t.Fatalf("stored mission = %+v, ok=%v; want QUEUED", m, ok)
	}
}

func TestGetMissionNotFound(t *testing.T) {
	c, _ := newTestCommander()
	srv := httptest.NewServer(c.routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/missions/nope")
	if err != nil {
		t.Fatalf("http.Get failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestAuthValidAndInvalidSecret(t *testing.T) {
	c, _ := newTestCommander()
	srv := httptest.NewServer(c.routes())
	defer srv.Close()

	instanceID := uuid.NewString()
	ok, err := http.Post(srv.URL+"/auth", "application/json",
		strings.NewReader(`{"bootstrap_secret":"boot","instance_id":"`+instanceID+`"}`))
	if err != nil {
		t.Fatalf("http.Post failed: %v", err)
	}
	defer ok.Body.Close()
	if ok.StatusCode != http.StatusOK {
		t.Fatalf("valid secret status = %d, want 200", ok.StatusCode)
	}
	var ar struct {
		Token string `json:"token"`
	}
	json.NewDecoder(ok.Body).Decode(&ar)
	claims, err := validateToken(c.jwtSecret, ar.Token)
	if err != nil {
		t.Fatalf("issued token invalid: %v", err)
	}
	if claims.Subject != instanceID {
		t.Fatalf("token subject = %q, want %q", claims.Subject, instanceID)
	}

	bad, err := http.Post(srv.URL+"/auth", "application/json",
		strings.NewReader(`{"bootstrap_secret":"wrong","instance_id":"`+instanceID+`"}`))
	if err != nil {
		t.Fatalf("http.Post failed: %v", err)
	}
	defer bad.Body.Close()
	if bad.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad secret status = %d, want 401", bad.StatusCode)
	}
}

func TestAuthRejectsInvalidInstanceID(t *testing.T) {
	c, _ := newTestCommander()
	srv := httptest.NewServer(c.routes())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/auth", "application/json",
		strings.NewReader(`{"bootstrap_secret":"boot","instance_id":"not-a-uuid"}`))
	if err != nil {
		t.Fatalf("http.Post failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHealth(t *testing.T) {
	c, _ := newTestCommander()
	srv := httptest.NewServer(c.routes())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("http.Get failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleStatusValidToken(t *testing.T) {
	c, _ := newTestCommander()
	c.store.Create(Mission{ID: "m1", Status: "QUEUED"})
	tok, _ := signToken(c.jwtSecret, 30*time.Second, uuid.NewString())
	body, _ := json.Marshal(StatusMessage{MissionID: "m1", Status: "COMPLETED"})

	if err := c.handleStatus(amqp.Table{"authorization": tok}, body); err != nil {
		t.Fatalf("handleStatus valid: %v", err)
	}
	m, _ := c.store.Get("m1")
	if m.Status != "COMPLETED" {
		t.Fatalf("status = %q, want COMPLETED", m.Status)
	}
}

func TestHandleStatusExpiredTokenIsBreach(t *testing.T) {
	c, _ := newTestCommander()
	c.store.Create(Mission{ID: "m1", Status: "QUEUED"})
	tok, _ := signToken(c.jwtSecret, -1*time.Second, uuid.NewString())
	body, _ := json.Marshal(StatusMessage{MissionID: "m1", Status: "COMPLETED"})

	if err := c.handleStatus(amqp.Table{"authorization": tok}, body); err == nil {
		t.Fatal("expected breach error for expired token")
	}
	m, _ := c.store.Get("m1")
	if m.Status != "QUEUED" {
		t.Fatalf("status = %q, want unchanged QUEUED", m.Status)
	}
}
