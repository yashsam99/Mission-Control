package main

import (
	"sync"
	"time"
)

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
