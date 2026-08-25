package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/vlussenburg/ghes-actions-monitor/internal/store"
)

type fakeStore struct {
	queueDepth     store.QueueDepth
	queueDepthErr  error
	inFlight       int
	inFlightErr    error
	outcomes       map[string]int
	outcomesErr    error
	recentRuns     []store.WorkflowRun
	recentRunsErr  error
	runnerSnaps    []store.RunnerSnapshot
	runnerSnapsErr error

	lastLimit int
	lastSince time.Time
}

func (f *fakeStore) QueueDepth(ctx context.Context) (store.QueueDepth, error) {
	return f.queueDepth, f.queueDepthErr
}

func (f *fakeStore) InFlightCount(ctx context.Context) (int, error) {
	return f.inFlight, f.inFlightErr
}

func (f *fakeStore) RecentRuns(ctx context.Context, limit int) ([]store.WorkflowRun, error) {
	f.lastLimit = limit
	return f.recentRuns, f.recentRunsErr
}

func (f *fakeStore) RecentOutcomes(ctx context.Context, since time.Time) (map[string]int, error) {
	f.lastSince = since
	return f.outcomes, f.outcomesErr
}

func (f *fakeStore) LatestRunnerSnapshots(ctx context.Context) ([]store.RunnerSnapshot, error) {
	return f.runnerSnaps, f.runnerSnapsErr
}

func TestHandleHealthz(t *testing.T) {
	s := &Server{Store: &fakeStore{}}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("unexpected body: %q", rec.Body.String())
	}
}

func TestHandleStatus_HappyPath(t *testing.T) {
	fixed := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	fs := &fakeStore{
		queueDepth: store.QueueDepth{Queued: 3, InProgress: 5},
		inFlight:   8,
		outcomes:   map[string]int{"success": 10, "failure": 2},
	}
	s := &Server{Store: fs, Org: "acme", Now: func() time.Time { return fixed }}

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var got statusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Org != "acme" || got.InFlight != 8 || got.QueueDepth.Queued != 3 || got.QueueDepth.InProgress != 5 {
		t.Errorf("unexpected status response: %+v", got)
	}
	if got.RecentOutcomes["success"] != 10 {
		t.Errorf("expected success=10, got %+v", got.RecentOutcomes)
	}
	if !fs.lastSince.Equal(fixed.Add(-time.Hour)) {
		t.Errorf("expected RecentOutcomes queried with 1h window, got since=%v", fs.lastSince)
	}
}

func TestHandleStatus_QueueDepthError(t *testing.T) {
	fs := &fakeStore{queueDepthErr: errors.New("db down")}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestHandleStatus_InFlightError(t *testing.T) {
	fs := &fakeStore{inFlightErr: errors.New("db down")}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestHandleStatus_OutcomesError(t *testing.T) {
	fs := &fakeStore{outcomesErr: errors.New("db down")}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestHandleRecentRuns_DefaultLimit(t *testing.T) {
	fs := &fakeStore{recentRuns: []store.WorkflowRun{{RunID: 1}}}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/recent", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if fs.lastLimit != 50 {
		t.Errorf("expected default limit 50, got %d", fs.lastLimit)
	}
}

func TestHandleRecentRuns_CustomLimit(t *testing.T) {
	fs := &fakeStore{}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/recent?limit=5", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if fs.lastLimit != 5 {
		t.Errorf("expected limit 5, got %d", fs.lastLimit)
	}
}

func TestHandleRecentRuns_InvalidLimitFallsBackToDefault(t *testing.T) {
	fs := &fakeStore{}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/recent?limit=notanumber", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if fs.lastLimit != 50 {
		t.Errorf("expected fallback to default limit 50, got %d", fs.lastLimit)
	}
}

func TestHandleRecentRuns_NegativeLimitFallsBackToDefault(t *testing.T) {
	fs := &fakeStore{}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/recent?limit=-5", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if fs.lastLimit != 50 {
		t.Errorf("expected fallback to default limit 50 for negative input, got %d", fs.lastLimit)
	}
}

func TestHandleRecentRuns_StoreError(t *testing.T) {
	fs := &fakeStore{recentRunsErr: errors.New("db down")}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/recent", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestHandleRunners_HappyPath(t *testing.T) {
	fs := &fakeStore{runnerSnaps: []store.RunnerSnapshot{{GroupName: "default", Total: 3}}}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runners", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body map[string][]store.RunnerSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(body["runner_groups"]) != 1 || body["runner_groups"][0].GroupName != "default" {
		t.Errorf("unexpected runner groups: %+v", body)
	}
}

func TestHandleRunners_StoreError(t *testing.T) {
	fs := &fakeStore{runnerSnapsErr: errors.New("db down")}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runners", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

type fakeRefresher struct {
	calls int
}

func (f *fakeRefresher) RefreshNow(ctx context.Context) {
	f.calls++
}

func TestHandleRefresh_NoRefresherConfigured(t *testing.T) {
	s := &Server{Store: &fakeStore{}}
	req := httptest.NewRequest(http.MethodPost, "/api/refresh", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when no refresher configured, got %d", rec.Code)
	}
}

func TestHandleRefresh_TriggersRefresh(t *testing.T) {
	fr := &fakeRefresher{}
	s := &Server{Store: &fakeStore{}, Refresher: fr}
	req := httptest.NewRequest(http.MethodPost, "/api/refresh", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if fr.calls != 1 {
		t.Errorf("expected RefreshNow to be called once, got %d", fr.calls)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if refreshed, _ := body["refreshed"].(bool); !refreshed {
		t.Errorf("expected refreshed=true in response, got %+v", body)
	}
}
