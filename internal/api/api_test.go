package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vlussenburg-org/ghes-actions-monitor/internal/store"
)

type fakeStore struct {
	queueDepth           store.QueueDepth
	queueDepthErr        error
	queueHistory         []store.QueueDepthSnapshot
	queueHistErr         error
	outcomes             map[string]int
	outcomesErr          error
	completedOutcomes    []store.CompletedRunOutcome
	completedOutcomesErr error
	recentRuns           []store.WorkflowRun
	recentRunsErr        error
	runnerSnaps          []store.RunnerSnapshot
	runnerSnapsErr       error
	zombieRuns           []store.WorkflowRun
	zombieRunsErr        error

	lastOpts                 store.RecentRunsOptions
	lastSince                time.Time
	lastHistorySince         time.Time
	lastOutcomesHistorySince time.Time
}

type fakeRunController struct {
	repo  string
	runID int64
	force bool
	err   error
}

func (f *fakeRunController) CancelWorkflowRun(ctx context.Context, repo string, runID int64, force bool) error {
	f.repo, f.runID, f.force = repo, runID, force
	return f.err
}

func (f *fakeStore) QueueDepth(ctx context.Context) (store.QueueDepth, error) {
	return f.queueDepth, f.queueDepthErr
}

func (f *fakeStore) QueueDepthHistory(ctx context.Context, since time.Time) ([]store.QueueDepthSnapshot, error) {
	f.lastHistorySince = since
	return f.queueHistory, f.queueHistErr
}

func (f *fakeStore) RecentRuns(ctx context.Context, opts store.RecentRunsOptions) ([]store.WorkflowRun, int, error) {
	f.lastOpts = opts
	if f.recentRunsErr != nil {
		return nil, 0, f.recentRunsErr
	}
	return f.recentRuns, len(f.recentRuns), nil
}

func (f *fakeStore) RecentOutcomes(ctx context.Context, since time.Time) (map[string]int, error) {
	f.lastSince = since
	return f.outcomes, f.outcomesErr
}

func (f *fakeStore) CompletedRunOutcomes(ctx context.Context, since time.Time) ([]store.CompletedRunOutcome, error) {
	f.lastOutcomesHistorySince = since
	return f.completedOutcomes, f.completedOutcomesErr
}

func (f *fakeStore) LatestRunnerSnapshots(ctx context.Context) ([]store.RunnerSnapshot, error) {
	return f.runnerSnaps, f.runnerSnapsErr
}

func (f *fakeStore) ZombieRuns(ctx context.Context, staleAfter time.Duration, now time.Time) ([]store.WorkflowRun, error) {
	return f.zombieRuns, f.zombieRunsErr
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

func TestHandleCancelRun(t *testing.T) {
	controller := &fakeRunController{}
	s := &Server{Store: &fakeStore{}, RunController: controller, Org: "acme"}
	req := httptest.NewRequest(http.MethodPost, "/api/runs/42/cancel", strings.NewReader(`{"repo":"acme/widgets","force":true}`))
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if controller.repo != "acme/widgets" || controller.runID != 42 || !controller.force {
		t.Fatalf("unexpected cancellation request: %+v", controller)
	}
}

func TestHandleCancelRun_RejectsDifferentOrg(t *testing.T) {
	controller := &fakeRunController{}
	s := &Server{Store: &fakeStore{}, RunController: controller, Org: "acme"}
	req := httptest.NewRequest(http.MethodPost, "/api/runs/42/cancel", strings.NewReader(`{"repo":"other/widgets","force":true}`))
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
	if controller.repo != "" {
		t.Fatalf("cancel should not have been called, got %+v", controller)
	}
}

func TestHandleCancelRun_PassesThroughUpstreamStatus(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"conflict", &codedError{status: http.StatusConflict, msg: "GitHub API 409: Cannot force cancel"}, http.StatusConflict},
		{"forbidden", &codedError{status: http.StatusForbidden, msg: "GitHub API 403: Resource not accessible"}, http.StatusForbidden},
		{"upstream 5xx collapses to 502", &codedError{status: http.StatusInternalServerError, msg: "boom"}, http.StatusBadGateway},
		{"plain error collapses to 502", errors.New("dial tcp: timeout"), http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controller := &fakeRunController{err: tc.err}
			s := &Server{Store: &fakeStore{}, RunController: controller, Org: "acme"}
			req := httptest.NewRequest(http.MethodPost, "/api/runs/42/cancel", strings.NewReader(`{"repo":"acme/widgets","force":true}`))
			rec := httptest.NewRecorder()
			s.Routes().ServeHTTP(rec, req)

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.err.Error()) {
				t.Errorf("body %q does not contain upstream message %q", rec.Body.String(), tc.err.Error())
			}
		})
	}
}

type codedError struct {
	status int
	msg    string
}

func (e *codedError) Error() string   { return e.msg }
func (e *codedError) StatusCode() int { return e.status }

func TestHandleStatus_HappyPath(t *testing.T) {
	fixed := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	fs := &fakeStore{
		queueDepth: store.QueueDepth{Queued: 3, InProgress: 5},
		outcomes:   map[string]int{"success": 10, "failure": 2},
		zombieRuns: []store.WorkflowRun{{RunID: 1}, {RunID: 2}},
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
	if got.ZombieCount != 2 {
		t.Errorf("expected zombie_count=2, got %d", got.ZombieCount)
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

func TestHandleStatus_ZombieRunsError(t *testing.T) {
	fs := &fakeStore{zombieRunsErr: errors.New("db down")}
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
	if fs.lastOpts.Limit != 50 {
		t.Errorf("expected default limit 50, got %d", fs.lastOpts.Limit)
	}
	if fs.lastOpts.Offset != 0 {
		t.Errorf("expected default offset 0, got %d", fs.lastOpts.Offset)
	}
	if !fs.lastOpts.Desc {
		t.Errorf("expected default sort direction desc")
	}
}

func TestHandleRecentRuns_CustomLimit(t *testing.T) {
	fs := &fakeStore{}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/recent?limit=5", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if fs.lastOpts.Limit != 5 {
		t.Errorf("expected limit 5, got %d", fs.lastOpts.Limit)
	}
}

func TestHandleRecentRuns_LimitCappedAt500(t *testing.T) {
	fs := &fakeStore{}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/recent?limit=10000", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if fs.lastOpts.Limit != 500 {
		t.Errorf("expected limit capped at 500, got %d", fs.lastOpts.Limit)
	}
}

func TestHandleRecentRuns_InvalidLimitFallsBackToDefault(t *testing.T) {
	fs := &fakeStore{}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/recent?limit=notanumber", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if fs.lastOpts.Limit != 50 {
		t.Errorf("expected fallback to default limit 50, got %d", fs.lastOpts.Limit)
	}
}

func TestHandleRecentRuns_NegativeLimitFallsBackToDefault(t *testing.T) {
	fs := &fakeStore{}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/recent?limit=-5", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if fs.lastOpts.Limit != 50 {
		t.Errorf("expected fallback to default limit 50 for negative input, got %d", fs.lastOpts.Limit)
	}
}

func TestHandleRecentRuns_PageAndSort(t *testing.T) {
	fs := &fakeStore{}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/recent?page=3&limit=10&sort=repo&order=asc&status=queued", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if fs.lastOpts.Offset != 20 {
		t.Errorf("expected offset 20 for page 3 limit 10, got %d", fs.lastOpts.Offset)
	}
	if fs.lastOpts.SortBy != "repo" {
		t.Errorf("expected sort=repo, got %q", fs.lastOpts.SortBy)
	}
	if fs.lastOpts.Desc {
		t.Errorf("expected order=asc to set Desc=false")
	}
	if fs.lastOpts.Status != "queued" {
		t.Errorf("expected status=queued, got %q", fs.lastOpts.Status)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(body["page"].(float64)) != 3 {
		t.Errorf("expected page=3 in response, got %+v", body["page"])
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

func TestHandleQueueDepthHistory_HappyPath(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	want := []store.QueueDepthSnapshot{
		{Queued: 1, InProgress: 2, CapturedAt: now.Add(-time.Hour)},
		{Queued: 0, InProgress: 3, CapturedAt: now},
	}
	fs := &fakeStore{queueHistory: want}
	s := &Server{Store: fs, Now: func() time.Time { return now }}
	req := httptest.NewRequest(http.MethodGet, "/api/queue-depth/history?hours=6", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !fs.lastHistorySince.Equal(now.Add(-6 * time.Hour)) {
		t.Errorf("expected since=%v, got %v", now.Add(-6*time.Hour), fs.lastHistorySince)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	hist, _ := body["history"].([]any)
	if len(hist) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(hist))
	}
}

func TestHandleQueueDepthHistory_DefaultsTo24Hours(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	fs := &fakeStore{}
	s := &Server{Store: fs, Now: func() time.Time { return now }}
	req := httptest.NewRequest(http.MethodGet, "/api/queue-depth/history", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if !fs.lastHistorySince.Equal(now.Add(-24 * time.Hour)) {
		t.Errorf("expected default since=%v, got %v", now.Add(-24*time.Hour), fs.lastHistorySince)
	}
}

func TestHandleQueueDepthHistory_CapsAt168Hours(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	fs := &fakeStore{}
	s := &Server{Store: fs, Now: func() time.Time { return now }}
	req := httptest.NewRequest(http.MethodGet, "/api/queue-depth/history?hours=99999", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if !fs.lastHistorySince.Equal(now.Add(-168 * time.Hour)) {
		t.Errorf("expected capped since=%v, got %v", now.Add(-168*time.Hour), fs.lastHistorySince)
	}
}

func TestHandleQueueDepthHistory_StoreError(t *testing.T) {
	fs := &fakeStore{queueHistErr: errors.New("db down")}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/queue-depth/history", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestHandleRunOutcomesHistory_HappyPath(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	want := []store.CompletedRunOutcome{
		{Conclusion: "success", CompletedAt: now.Add(-time.Hour)},
		{Conclusion: "failure", CompletedAt: now},
	}
	fs := &fakeStore{completedOutcomes: want}
	s := &Server{Store: fs, Now: func() time.Time { return now }}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/outcomes/history?hours=6", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !fs.lastOutcomesHistorySince.Equal(now.Add(-6 * time.Hour)) {
		t.Errorf("expected since=%v, got %v", now.Add(-6*time.Hour), fs.lastOutcomesHistorySince)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	outcomes, _ := body["outcomes"].([]any)
	if len(outcomes) != 2 {
		t.Fatalf("expected 2 outcome entries, got %d", len(outcomes))
	}
}

func TestHandleRunOutcomesHistory_DefaultsTo24Hours(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	fs := &fakeStore{}
	s := &Server{Store: fs, Now: func() time.Time { return now }}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/outcomes/history", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if !fs.lastOutcomesHistorySince.Equal(now.Add(-24 * time.Hour)) {
		t.Errorf("expected default since=%v, got %v", now.Add(-24*time.Hour), fs.lastOutcomesHistorySince)
	}
}

func TestHandleRunOutcomesHistory_CapsAt168Hours(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	fs := &fakeStore{}
	s := &Server{Store: fs, Now: func() time.Time { return now }}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/outcomes/history?hours=99999", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if !fs.lastOutcomesHistorySince.Equal(now.Add(-168 * time.Hour)) {
		t.Errorf("expected capped since=%v, got %v", now.Add(-168*time.Hour), fs.lastOutcomesHistorySince)
	}
}

func TestHandleRunOutcomesHistory_StoreError(t *testing.T) {
	fs := &fakeStore{completedOutcomesErr: errors.New("db down")}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/outcomes/history", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestHandleZombieRuns_HappyPath(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	want := []store.WorkflowRun{
		{RunID: 1, Repo: "org/a", Status: "queued", UpdatedAt: now.Add(-2 * time.Hour)},
	}
	fs := &fakeStore{zombieRuns: want}
	s := &Server{Store: fs, Now: func() time.Time { return now }}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/zombies?minutes=90", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	runs, _ := body["runs"].([]any)
	if len(runs) != 1 {
		t.Fatalf("expected 1 zombie run, got %d", len(runs))
	}
	if body["stale_after_minutes"] != float64(90) {
		t.Errorf("expected stale_after_minutes=90, got %v", body["stale_after_minutes"])
	}
}

func TestHandleZombieRuns_DefaultsTo60Minutes(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	fs := &fakeStore{}
	s := &Server{Store: fs, Now: func() time.Time { return now }}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/zombies", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["stale_after_minutes"] != float64(60) {
		t.Errorf("expected default stale_after_minutes=60, got %v", body["stale_after_minutes"])
	}
}

func TestHandleZombieRuns_CapsAt10080Minutes(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	fs := &fakeStore{}
	s := &Server{Store: fs, Now: func() time.Time { return now }}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/zombies?minutes=99999999", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["stale_after_minutes"] != float64(10080) {
		t.Errorf("expected capped stale_after_minutes=10080, got %v", body["stale_after_minutes"])
	}
}

func TestHandleZombieRuns_StoreError(t *testing.T) {
	fs := &fakeStore{zombieRunsErr: errors.New("db down")}
	s := &Server{Store: fs}
	req := httptest.NewRequest(http.MethodGet, "/api/runs/zombies", nil)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}
