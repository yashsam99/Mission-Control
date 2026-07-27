package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenStoreSetGet(t *testing.T) {
	var ts TokenStore
	if got := ts.Get(); got != "" {
		t.Fatalf("empty store Get = %q, want empty", got)
	}
	ts.Set("abc")
	if got := ts.Get(); got != "abc" {
		t.Fatalf("Get = %q, want abc", got)
	}
}

func TestTokenStoreConcurrent(t *testing.T) {
	var ts TokenStore
	ts.Set("seed")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); ts.Set("x") }()
		go func() { defer wg.Done(); _ = ts.Get() }()
	}
	wg.Wait()
}

func TestFetchTokenSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			BootstrapSecret string `json:"bootstrap_secret"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		if req.BootstrapSecret != "boot" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"token": "tok-123", "expires_in": 30})
	}))
	defer srv.Close()

	tok, err := fetchToken(context.Background(), srv.Client(), srv.URL, "boot")
	if err != nil {
		t.Fatalf("fetchToken: %v", err)
	}
	if tok != "tok-123" {
		t.Fatalf("token = %q, want tok-123", tok)
	}
}

func TestFetchTokenUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	if _, err := fetchToken(context.Background(), srv.Client(), srv.URL, "wrong"); err == nil {
		t.Fatal("expected error on 401")
	}
}

type capturedPublish struct {
	status StatusMessage
	token  string
}

type fakeStatusPublisher struct {
	mu   sync.Mutex
	msgs []capturedPublish
}

func (f *fakeStatusPublisher) PublishStatus(_ context.Context, s StatusMessage, token string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, capturedPublish{s, token})
	return nil
}

func newTestWorker(prob float32, seed int64) (*Worker, *fakeStatusPublisher) {
	ts := &TokenStore{}
	ts.Set("worker-token")
	fp := &fakeStatusPublisher{}
	w := &Worker{
		id:          "worker-0",
		tokens:      ts,
		pub:         fp,
		sleep:       func() time.Duration { return 0 },
		successProb: prob,
		rnd:         rand.New(rand.NewSource(seed)),
		log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return w, fp
}

func TestWorkerExecuteSuccess(t *testing.T) {
	w, fp := newTestWorker(1.0, 1) // prob 1.0 => always COMPLETED
	w.execute(context.Background(), Mission{ID: "m1"})

	if len(fp.msgs) != 2 {
		t.Fatalf("published %d messages, want 2", len(fp.msgs))
	}
	if fp.msgs[0].status.Status != "IN_PROGRESS" {
		t.Fatalf("first = %q, want IN_PROGRESS", fp.msgs[0].status.Status)
	}
	if fp.msgs[1].status.Status != "COMPLETED" {
		t.Fatalf("second = %q, want COMPLETED", fp.msgs[1].status.Status)
	}
	if fp.msgs[0].token != "worker-token" {
		t.Fatalf("token = %q, want worker-token", fp.msgs[0].token)
	}
}

func TestWorkerExecuteFailure(t *testing.T) {
	w, fp := newTestWorker(0.0, 1) // prob 0.0 => always FAILED
	w.execute(context.Background(), Mission{ID: "m1"})
	if fp.msgs[1].status.Status != "FAILED" {
		t.Fatalf("terminal = %q, want FAILED", fp.msgs[1].status.Status)
	}
}

func TestRunWorkerPoolExecutesAndAcks(t *testing.T) {
	missions := make(chan ackableMission, 3)
	var acked int32
	for i := 0; i < 3; i++ {
		id := i
		missions <- ackableMission{
			mission: Mission{ID: "m" + string(rune('0'+id))},
			ack:     func() { atomic.AddInt32(&acked, 1) },
		}
	}
	close(missions)

	fp := &fakeStatusPublisher{}
	newWorker := func(id string) *Worker {
		ts := &TokenStore{}
		ts.Set("t")
		return &Worker{
			id: id, tokens: ts, pub: fp,
			sleep:       func() time.Duration { return 0 },
			successProb: 1.0, rnd: rand.New(rand.NewSource(1)),
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
	}
	var wg sync.WaitGroup
	runWorkerPool(context.Background(), 2, missions, newWorker, &wg)
	wg.Wait()

	if got := atomic.LoadInt32(&acked); got != 3 {
		t.Fatalf("acked = %d, want 3", got)
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if len(fp.msgs) != 6 { // 3 missions * 2 status each
		t.Fatalf("status msgs = %d, want 6", len(fp.msgs))
	}
}
