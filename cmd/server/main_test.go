package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

func TestBasicAuth_MissingCredentials(t *testing.T) {
	h := basicAuth(okHandler(), "admin", "secret")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Error("expected WWW-Authenticate challenge header")
	}
}

func TestBasicAuth_WrongCredentials(t *testing.T) {
	h := basicAuth(okHandler(), "admin", "secret")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "wrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestBasicAuth_WrongUsername(t *testing.T) {
	h := basicAuth(okHandler(), "admin", "secret")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("someone-else", "secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestBasicAuth_CorrectCredentials(t *testing.T) {
	h := basicAuth(okHandler(), "admin", "secret")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestNoCache_SetsHeaders(t *testing.T) {
	h := noCache(okHandler())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("Cache-Control"); got != "no-store, max-age=0" {
		t.Errorf("unexpected Cache-Control: %q", got)
	}
	if got := rec.Header().Get("Pragma"); got != "no-cache" {
		t.Errorf("unexpected Pragma: %q", got)
	}
}

func TestAccessLog_RecordsStatusAndBytes(t *testing.T) {
	h := accessLog(okHandler(), noopLogger())
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("unexpected body: %q", rec.Body.String())
	}
}

func TestAccessLogResponseWriter_DefaultsStatusOnWrite(t *testing.T) {
	rec := httptest.NewRecorder()
	w := &accessLogResponseWriter{ResponseWriter: rec}
	if _, err := w.Write([]byte("hi")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.status != http.StatusOK {
		t.Errorf("expected default status 200, got %d", w.status)
	}
	if w.bytes != 2 {
		t.Errorf("expected 2 bytes written, got %d", w.bytes)
	}
}

func TestAccessLogResponseWriter_WriteHeaderOnlyRecordsFirstCall(t *testing.T) {
	rec := httptest.NewRecorder()
	w := &accessLogResponseWriter{ResponseWriter: rec}
	w.WriteHeader(http.StatusCreated)
	w.WriteHeader(http.StatusInternalServerError)
	if w.status != http.StatusCreated {
		t.Errorf("expected first WriteHeader call to stick, got %d", w.status)
	}
}
