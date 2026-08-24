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
	"time"

	"github.com/vlussenburg/ghes-actions-monitor/internal/store"
)

// Store is the subset of store.Store the webhook handler needs, allowing
// tests to substitute a fake.
type Store interface {
	UpsertWorkflowRun(ctx context.Context, r store.WorkflowRun) error
}

// Handler receives and processes GitHub org webhook deliveries.
type Handler struct {
	Secret string
	Store  Store
	Logger *slog.Logger
	// Now allows tests to control the observed time; defaults to time.Now.
	Now func() time.Time
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

	updatedAt := h.now()
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

	w.WriteHeader(http.StatusOK)
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
