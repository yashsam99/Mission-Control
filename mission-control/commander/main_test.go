package main

import (
	"sync"
	"testing"
	"time"
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
		Status:    "PENDING",
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
	ok = store.UpdateStatus("non-existent", "COMPLETE")
	if ok {
		t.Fatal("expected UpdateStatus to fail for non-existent mission")
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
				Status:    "PENDING",
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
		Status:    "PENDING",
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

	token, err := signToken(secret, ttl)
	if err != nil {
		t.Fatalf("signToken failed: %v", err)
	}

	if token == "" {
		t.Fatal("expected non-empty token")
	}

	// Validate the token with the same secret
	err = validateToken(secret, token)
	if err != nil {
		t.Fatalf("validateToken failed: %v", err)
	}
}

func TestValidateTokenExpired(t *testing.T) {
	secret := []byte("test-secret-key")
	ttl := -1 * time.Second // already expired

	token, err := signToken(secret, ttl)
	if err != nil {
		t.Fatalf("signToken failed: %v", err)
	}

	// Validate should fail because token is expired
	err = validateToken(secret, token)
	if err == nil {
		t.Fatal("expected validateToken to fail for expired token, but got nil")
	}
}

func TestValidateTokenBadSignature(t *testing.T) {
	secret := []byte("test-secret-key")
	ttl := 1 * time.Hour

	token, err := signToken(secret, ttl)
	if err != nil {
		t.Fatalf("signToken failed: %v", err)
	}

	// Validate with wrong secret
	wrongSecret := []byte("wrong-secret")
	err = validateToken(wrongSecret, token)
	if err == nil {
		t.Fatal("expected validateToken to fail with wrong secret, but got nil")
	}
}
