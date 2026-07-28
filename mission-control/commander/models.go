package main

import "time"

// Mission is a unit of work tracked by the Commander.
type Mission struct {
	ID        string    `json:"mission_id"`
	Objective string    `json:"objective"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// StatusMessage is a state update published by a Soldier worker.
type StatusMessage struct {
	MissionID string    `json:"mission_id"`
	Status    string    `json:"status"`
	WorkerID  string    `json:"worker_id"`
	Timestamp time.Time `json:"timestamp"`
}
