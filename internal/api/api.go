// Package api exposes the HTTP surface of the GitHub Actions Monitor: JSON
// endpoints backing the dashboard (queue depth, recent runs, runner
// capacity, recent outcomes) and the webhook receiver mount point.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/vlussenburg-org/ghes-actions-monitor/internal/store"
)

// Store is the subset of store.Store the API needs.
type Store interface {
	QueueDepth(ctx context.Context) (store.QueueDepth, error)
	QueueDepthHistory(ctx context.Context, since time.Time) ([]store.QueueDepthSnapshot, error)
	InFlightCount(ctx context.Context) (int, error)
	RecentRuns(ctx context.Context, opts store.RecentRunsOptions) ([]store.WorkflowRun, int, error)
	RecentOutcomes(ctx context.Context, since time.Time) (map[string]int, error)
	LatestRunnerSnapshots(ctx context.Context) ([]store.RunnerSnapshot, error)
	ZombieRuns(ctx context.Context, staleAfter time.Duration, now time.Time) ([]store.WorkflowRun, error)
}

// Refresher lets the API trigger an immediate, out-of-band poll sweep (used
// by the dashboard's "force refresh" button). Optional: if unset,
// /api/refresh returns 503 rather than panicking.
type Refresher interface {
	RefreshNow(ctx context.Context)
}

// RunController performs mutating workflow-run actions using the configured
// administrative GitHub credential.
type RunController interface {
	CancelWorkflowRun(ctx context.Context, repo string, runID int64, force bool) error
}

type RateLimitProvider interface {
	RateLimitStatus() any
}

// Server wires up the monitor's HTTP handlers.
type Server struct {
	Store         Store
	Org           string
	GitHubBaseURL string
	Refresher     Refresher
	RunController RunController
	RateLimit     RateLimitProvider

	// Now allows tests to control the observed time; defaults to time.Now.
	Now func() time.Time
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now().UTC()
}

// Routes returns the configured http.ServeMux for the dashboard/API
// endpoints. It does not include the webhook receiver, which is mounted
// separately by cmd/server so its own signature-verification and org
// remain independently configurable.
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("GET /api/runs/recent", s.handleRecentRuns)
	mux.HandleFunc("GET /api/runners", s.handleRunners)
	mux.HandleFunc("GET /api/queue-depth/history", s.handleQueueDepthHistory)
	mux.HandleFunc("GET /api/runs/zombies", s.handleZombieRuns)
	mux.HandleFunc("POST /api/refresh", s.handleRefresh)
	mux.HandleFunc("POST /api/runs/{run_id}/cancel", s.handleCancelRun)
	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// statusResponse is the payload for GET /api/status: the org-wide snapshot
// of job queue depth, in-flight count, and recent build outcomes.
type statusResponse struct {
	Org            string           `json:"org"`
	GitHubBaseURL  string           `json:"github_base_url"`
	QueueDepth     store.QueueDepth `json:"queue_depth"`
	InFlight       int              `json:"in_flight"`
	RecentOutcomes map[string]int   `json:"recent_outcomes_1h"`
	GeneratedAt    time.Time        `json:"generated_at"`
	RateLimit      any              `json:"rate_limit,omitempty"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	now := s.now()

	depth, err := s.Store.QueueDepth(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to compute queue depth")
		return
	}
	inFlight, err := s.Store.InFlightCount(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to compute in-flight count")
		return
	}
	outcomes, err := s.Store.RecentOutcomes(ctx, now.Add(-time.Hour))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to compute recent outcomes")
		return
	}

	response := statusResponse{
		Org:            s.Org,
		GitHubBaseURL:  s.GitHubBaseURL,
		QueueDepth:     depth,
		InFlight:       inFlight,
		RecentOutcomes: outcomes,
		GeneratedAt:    now,
	}
	if s.RateLimit != nil {
		response.RateLimit = s.RateLimit.RateLimitStatus()
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleRecentRuns(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit := 50
	if v := q.Get("limit"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			limit = n
		}
	}
	if limit > 500 {
		limit = 500
	}

	page := 1
	if v := q.Get("page"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			page = n
		}
	}

	desc := true
	if v := q.Get("order"); v == "asc" {
		desc = false
	}

	runs, total, err := s.Store.RecentRuns(r.Context(), store.RecentRunsOptions{
		Limit:  limit,
		Offset: (page - 1) * limit,
		SortBy: q.Get("sort"),
		Desc:   desc,
		Status: q.Get("status"),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to fetch recent runs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"runs":  runs,
		"total": total,
		"page":  page,
		"limit": limit,
	})
}

func (s *Server) handleRunners(w http.ResponseWriter, r *http.Request) {
	snaps, err := s.Store.LatestRunnerSnapshots(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to fetch runner snapshots")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runner_groups": snaps})
}

// handleRefresh triggers an immediate, synchronous poll sweep (active runs,
// history, runners) so the dashboard doesn't have to wait for the next
// scheduled tick. Requires a Refresher to be configured; otherwise responds
// 503 since there's nothing to poll (e.g. no GitHub client configured).
func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if s.Refresher == nil {
		writeError(w, http.StatusServiceUnavailable, "refresh is not available: no poller configured")
		return
	}
	s.Refresher.RefreshNow(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"refreshed": true, "at": s.now()})
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	if s.RunController == nil {
		writeError(w, http.StatusServiceUnavailable, "cancel is not available")
		return
	}
	runID, err := strconv.ParseInt(r.PathValue("run_id"), 10, 64)
	if err != nil || runID <= 0 {
		writeError(w, http.StatusBadRequest, "run_id must be a positive integer")
		return
	}
	var request struct {
		Repo  string `json:"repo"`
		Force bool   `json:"force"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	parts := strings.Split(request.Repo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" ||
		strings.ContainsAny(request.Repo, " \t\r\n") {
		writeError(w, http.StatusBadRequest, "repo must be in owner/name format")
		return
	}
	if s.Org != "" && parts[0] != s.Org {
		writeError(w, http.StatusForbidden, "repo owner must match configured organization")
		return
	}
	if err := s.RunController.CancelWorkflowRun(r.Context(), request.Repo, runID, request.Force); err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("failed to cancel workflow run: %v", err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"cancelled": true, "force": request.Force})
}

// handleQueueDepthHistory returns the queued-vs-in-progress time series for
// GET /api/queue-depth/history?hours=N (default 24, capped at 168 = 7 days),
// used to render the dashboard's queue depth chart.
func (s *Server) handleQueueDepthHistory(w http.ResponseWriter, r *http.Request) {
	hours := 24
	if v := r.URL.Query().Get("hours"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			hours = n
		}
	}
	if hours > 168 {
		hours = 168
	}

	since := s.now().Add(-time.Duration(hours) * time.Hour)
	history, err := s.Store.QueueDepthHistory(r.Context(), since)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to fetch queue depth history")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"history": history})
}

// handleZombieRuns returns workflow runs that are still queued/in_progress
// but haven't been updated in a long time — GET /api/runs/zombies?minutes=N
// (default 60, capped at 10080 = 7 days).
func (s *Server) handleZombieRuns(w http.ResponseWriter, r *http.Request) {
	minutes := 60
	if v := r.URL.Query().Get("minutes"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			minutes = n
		}
	}
	if minutes > 10080 {
		minutes = 10080
	}

	runs, err := s.Store.ZombieRuns(r.Context(), time.Duration(minutes)*time.Minute, s.now())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to fetch zombie runs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs, "stale_after_minutes": minutes})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func parsePositiveInt(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, strconv.ErrSyntax
	}
	return n, nil
}
