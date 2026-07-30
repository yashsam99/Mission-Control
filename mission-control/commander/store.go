package main

import (
	"sync"
	"time"
)

// validTransitions defines the mission status state machine. A status
// update is only applied if it's a listed transition from the mission's
// current status — this is what stops a stale, duplicate, or out-of-order
// status message from corrupting an already-terminal mission.
var validTransitions = map[string]map[string]bool{
	"QUEUED": {
		"IN_PROGRESS": true,
		"COMPLETED":   true, // IN_PROGRESS can legitimately be lost/reordered in transit
		"FAILED":      true,
	},
	"IN_PROGRESS": {
		"COMPLETED": true,
		"FAILED":    true,
	},
	"COMPLETED": {}, // terminal
	"FAILED":    {}, // terminal
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

// UpdateStatus applies status if it is a valid transition from the mission's
// current status (see validTransitions); returns false if the mission is
// unknown or the transition isn't allowed.
func (s *MissionStore) UpdateStatus(id, status string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	mission, ok := s.missions[id]
	if !ok {
		return false
	}
	if !validTransitions[mission.Status][status] {
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

// Delete removes the mission with the given id from the store.
func (s *MissionStore) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.missions, id)
}
