package main

import (
	"context"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	commanderURL := getenv("COMMANDER_URL", "http://localhost:8080")
	bootstrap := getenv("BOOTSTRAP_SECRET", "bootstrap")
	poolSize, err := strconv.Atoi(getenv("WORKER_POOL_SIZE", "10"))
	if err != nil || poolSize < 1 {
		poolSize = 10
	}
	httpClient := &http.Client{Timeout: 10 * time.Second}
	instanceID := uuid.NewString()

	// Bootstrap: acquire the first token before doing anything else.
	tokens := &TokenStore{}
	firstTok, err := fetchToken(ctx, httpClient, commanderURL, bootstrap, instanceID)
	if err != nil {
		log.Error("initial token acquisition failed", "err", err)
		os.Exit(1)
	}
	tokens.Set(firstTok)
	log.Info("bootstrap token acquired", "instance_id", instanceID)

	// Rotation goroutine.
	go func() {
		ticker := time.NewTicker(rotationInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tok, err := fetchToken(ctx, httpClient, commanderURL, bootstrap, instanceID)
				if err != nil {
					log.Warn("token rotation failed, keeping previous token", "err", err)
					continue
				}
				tokens.Set(tok)
				log.Info("Token Rotated")
			}
		}
	}()

	broker := NewBroker(getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"), log)
	if err := broker.Connect(ctx); err != nil {
		log.Error("broker connect failed", "err", err)
		os.Exit(1)
	}

	missions := make(chan ackableMission, poolSize)
	newWorker := func(id string) *Worker {
		return &Worker{
			id: id, tokens: tokens, pub: broker,
			sleep: missionSleep, successProb: 0.90,
			rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
			log: log,
		}
	}
	var wg sync.WaitGroup
	runWorkerPool(ctx, poolSize, missions, newWorker, &wg)

	if err := broker.ConsumeOrders(ctx, poolSize, missions); err != nil {
		log.Error("order consumer failed", "err", err)
		os.Exit(1)
	}
	log.Info("soldier ready", "workers", poolSize)

	<-ctx.Done()
	log.Info("shutting down soldier: draining in-flight missions")
	<-broker.OrdersDone() // wait for the consumer goroutine to actually stop before closing
	close(missions)
	wg.Wait()
	log.Info("drain complete")
}
