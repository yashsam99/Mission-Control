package main

import "time"

const (
	ordersQueue = "orders_queue"
	statusQueue = "status_queue"
	tokenTTL    = 30 * time.Second
)
