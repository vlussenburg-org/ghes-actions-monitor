// Package webhook implements the inbound org webhook receiver
// (POST /webhook/github). GitHub delivers workflow_run events here in
// real time; this is the monitor's most reliable signal, since it keeps
// working even when the regular REST API is degraded. The delivery format
// (payload shape, X-Hub-Signature-256 header) is identical for GHES and
// GHEC, so this package requires no instance-specific handling.
package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/vlussenburg-org/ghes-actions-monitor/internal/store"
)

// Store is the subset of store.Store the webhook handler needs, allowing
// tests to substitute a fake.
type Store interface {
	UpsertWorkflowRun(ctx context.Context, r store.WorkflowRun) error
	QueueDepth(ctx context.Context) (store.QueueDepth, error)
	RecordQueueDepthSnapshot(ctx context.Context, snap store.QueueDepthSnapshot) error
}

// Handler receives and processes GitHub org webhook deliveries.
type Handler struct {
	Secret string
	Store  Store
	Logger *slog.Logger
	// Now allows tests to control the observed time; defaults to time.Now.
	Now func() time.Time

	// lastSnapshotMinute holds the unix-minute of the most recent queue-depth
	// snapshot, used to coalesce the expensive per-webhook recomputation down
	// to at most once per wall-clock minute. Accessed atomically across
	// concurrent deliveries.
	lastSnapshotMinute atomic.Int64
}

// workflowRunPayload is the subset of the workflow_run webhook event this
// monitor cares about.
type workflowRunPayload struct {
	Action      string `json:"action"`
	WorkflowRun struct {
		ID         int64  `json:"id"`
		Name       string `json:"name"`
		Status     string `json:"status"`
		Conclusion string `json:"conclusion"`
		Event      string `json:"event"`
		HeadBranch string `json:"head_branch"`
		UpdatedAt  string `json:"updated_at"`
	} `json:"workflow_run"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

// ServeHTTP implements http.Handler. It verifies the HMAC-SHA256 signature
// (X-Hub-Signature-256), and for workflow_run events, records the run's
// current state in the store.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 5<<20)) // 5MB cap
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	if h.Secret != "" {
		sig := r.Header.Get("X-Hub-Signature-256")
		if !validSignature(h.Secret, body, sig) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
	}

	event := r.Header.Get("X-GitHub-Event")
	switch event {
	case "workflow_run":
		h.handleWorkflowRun(r.Context(), body, w)
	case "ping":
		w.WriteHeader(http.StatusOK)
	default:
		// Unhandled event types are acknowledged but ignored.
		w.WriteHeader(http.StatusOK)
	}
}

func (h *Handler) handleWorkflowRun(ctx context.Context, body []byte, w http.ResponseWriter) {
	var payload workflowRunPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	capturedAt := h.now()
	updatedAt := capturedAt
	if t, err := time.Parse(time.RFC3339, payload.WorkflowRun.UpdatedAt); err == nil {
		updatedAt = t
	}

	run := store.WorkflowRun{
		RunID:      payload.WorkflowRun.ID,
		Repo:       payload.Repository.FullName,
		Name:       payload.WorkflowRun.Name,
		Status:     payload.WorkflowRun.Status,
		Conclusion: payload.WorkflowRun.Conclusion,
		Event:      payload.WorkflowRun.Event,
		HeadBranch: payload.WorkflowRun.HeadBranch,
		Source:     "webhook",
		UpdatedAt:  updatedAt,
	}

	if err := h.Store.UpsertWorkflowRun(ctx, run); err != nil {
		h.logger().Error("failed to store workflow run", "error", err, "run_id", run.RunID)
		http.Error(w, "failed to record event", http.StatusInternalServerError)
		return
	}

	// Recording the run's state above is the delivery's critical work and is
	// now durable. The queue-depth snapshot is only a per-minute time series
	// for the trend chart, so recomputing it on every delivery is wasteful:
	// QueueDepth scans the latest state of every run, and under a webhook storm
	// those full scans stack up, exhaust the request's time budget, and fail
	// the delivery — causing GitHub to retry and, worse, the run state we just
	// stored to look lost. Coalesce the computation to at most once per
	// wall-clock minute (the snapshot's own bucket granularity) and never fail
	// the delivery on a snapshot error.
	h.maybeSnapshotQueueDepth(ctx, capturedAt)

	w.WriteHeader(http.StatusOK)
}

// maybeSnapshotQueueDepth recomputes and records the queue-depth snapshot at
// most once per wall-clock minute across all concurrent deliveries. The first
// delivery to observe a new minute claims it via CAS and does the work; the
// rest skip the expensive QueueDepth scan entirely. Snapshot failures are
// logged but never propagated, since the run state is already persisted and
// the snapshot only feeds the trend chart.
func (h *Handler) maybeSnapshotQueueDepth(ctx context.Context, capturedAt time.Time) {
	minute := capturedAt.UTC().Truncate(time.Minute).Unix()
	prev := h.lastSnapshotMinute.Load()
	if prev == minute || !h.lastSnapshotMinute.CompareAndSwap(prev, minute) {
		return
	}
	depth, err := h.Store.QueueDepth(ctx)
	if err != nil {
		h.logger().Error("failed to compute queue depth after webhook", "error", err)
		return
	}
	if err := h.Store.RecordQueueDepthSnapshot(ctx, store.QueueDepthSnapshot{
		Queued: depth.Queued, InProgress: depth.InProgress, CapturedAt: capturedAt,
	}); err != nil {
		h.logger().Error("failed to record queue depth snapshot after webhook", "error", err)
	}
}

func (h *Handler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now().UTC()
}

func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

// validSignature reports whether sigHeader ("sha256=<hex>") is a valid
// HMAC-SHA256 signature of body under secret, using constant-time
// comparison to avoid timing side-channels.
func validSignature(secret string, body []byte, sigHeader string) bool {
	const prefix = "sha256="
	if len(sigHeader) <= len(prefix) || sigHeader[:len(prefix)] != prefix {
		return false
	}
	expectedHex := sigHeader[len(prefix):]
	expected, err := hex.DecodeString(expectedHex)
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	computed := mac.Sum(nil)

	return hmac.Equal(computed, expected)
}
