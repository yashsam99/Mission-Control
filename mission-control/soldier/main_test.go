package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
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
