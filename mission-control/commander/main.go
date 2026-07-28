package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	port := getenv("HTTP_PORT", "8080")
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(runHealthcheck(port))
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	broker := NewBroker(getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"), log)
	if err := broker.Connect(ctx); err != nil {
		log.Error("broker connect failed", "err", err)
		os.Exit(1)
	}

	c := &Commander{
		store:           NewMissionStore(),
		publisher:       broker,
		bootstrapSecret: getenv("BOOTSTRAP_SECRET", "bootstrap"),
		jwtSecret:       []byte(getenv("JWT_SECRET", "jwt-secret")),
		log:             log,
	}
	if err := broker.ConsumeStatus(ctx, c.handleStatus); err != nil {
		log.Error("status consumer failed", "err", err)
		os.Exit(1)
	}

	srv := &http.Server{Addr: net.JoinHostPort("", port), Handler: c.routes()}
	go func() {
		log.Info("commander HTTP listening", "port", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down commander")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
}

func runHealthcheck(port string) int {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/health")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return 0
	}
	return 1
}
