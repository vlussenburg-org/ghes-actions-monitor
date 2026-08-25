// Package api exposes the HTTP surface of the GitHub Actions Monitor: JSON
// endpoints backing the dashboard (queue depth, recent runs, runner
// capacity, recent outcomes) and the webhook receiver mount point.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/vlussenburg/ghes-actions-monitor/internal/store"
)

// Store is the subset of store.Store the API needs.
type Store interface {
	QueueDepth(ctx context.Context) (store.QueueDepth, error)
	InFlightCount(ctx context.Context) (int, error)
	RecentRuns(ctx context.Context, limit int) ([]store.WorkflowRun, error)
	RecentOutcomes(ctx context.Context, since time.Time) (map[string]int, error)
	LatestRunnerSnapshots(ctx context.Context) ([]store.RunnerSnapshot, error)
}

// Server wires up the monitor's HTTP handlers.
type Server struct {
	Store Store
	Org   string

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
	QueueDepth     store.QueueDepth `json:"queue_depth"`
	InFlight       int              `json:"in_flight"`
	RecentOutcomes map[string]int   `json:"recent_outcomes_1h"`
	GeneratedAt    time.Time        `json:"generated_at"`
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

	writeJSON(w, http.StatusOK, statusResponse{
		Org:            s.Org,
		QueueDepth:     depth,
		InFlight:       inFlight,
		RecentOutcomes: outcomes,
		GeneratedAt:    now,
	})
}

func (s *Server) handleRecentRuns(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			limit = n
		}
	}

	runs, err := s.Store.RecentRuns(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to fetch recent runs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) handleRunners(w http.ResponseWriter, r *http.Request) {
	snaps, err := s.Store.LatestRunnerSnapshots(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to fetch runner snapshots")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runner_groups": snaps})
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
