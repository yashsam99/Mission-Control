package main

import "time"

const (
	ordersQueue      = "orders_queue"
	statusQueue      = "status_queue"
	rotationInterval = 25 * time.Second
)
