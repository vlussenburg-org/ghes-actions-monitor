package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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
	runs          []store.WorkflowRun
	snapshots     []store.QueueDepthSnapshot
	queueDepth    store.QueueDepth
	failErr       error
	snapshotErr   error
	queueDepthErr error
}

func (f *fakeStore) UpsertWorkflowRun(ctx context.Context, r store.WorkflowRun) error {
	if f.failErr != nil {
		return f.failErr
	}
	f.runs = append(f.runs, r)
	return nil
}

func (f *fakeStore) QueueDepth(ctx context.Context) (store.QueueDepth, error) {
	return f.queueDepth, f.queueDepthErr
}

func (f *fakeStore) RecordQueueDepthSnapshot(ctx context.Context, snap store.QueueDepthSnapshot) error {
	if f.snapshotErr != nil {
		return f.snapshotErr
	}
	f.snapshots = append(f.snapshots, snap)
	return nil
}

func sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func workflowRunBody(t *testing.T) []byte {
	t.Helper()
	payload := map[string]any{
		"action": "completed",
		"workflow_run": map[string]any{
			"id":          123,
			"name":        "CI",
			"status":      "completed",
			"conclusion":  "success",
			"event":       "push",
			"head_branch": "main",
			"updated_at":  "2024-01-01T00:00:00Z",
		},
		"repository": map[string]any{
			"full_name": "acme/widgets",
		},
	}
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestServeHTTP_MethodNotAllowed(t *testing.T) {
	h := &Handler{Store: &fakeStore{}}
	req := httptest.NewRequest(http.MethodGet, "/webhook/github", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestServeHTTP_ValidSignature_WorkflowRun(t *testing.T) {
	secret := "s3cr3t"
	body := workflowRunBody(t)
	fs := &fakeStore{}
	h := &Handler{Secret: secret, Store: fs}

	req := httptest.NewRequest(http.MethodPost, "/webhook/github", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "workflow_run")
	req.Header.Set("X-Hub-Signature-256", sign(secret, body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(fs.runs) != 1 {
		t.Fatalf("expected 1 stored run, got %d", len(fs.runs))
	}
	got := fs.runs[0]
	if got.RunID != 123 || got.Repo != "acme/widgets" || got.Status != "completed" || got.Conclusion != "success" {
		t.Errorf("unexpected stored run: %+v", got)
	}
	if !got.UpdatedAt.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("unexpected updated_at: %v", got.UpdatedAt)
	}
	if len(fs.snapshots) != 1 || fs.snapshots[0].Queued != 0 || fs.snapshots[0].InProgress != 0 {
		t.Errorf("expected one queue-depth snapshot, got %+v", fs.snapshots)
	}
}

func TestServeHTTP_InvalidSignature(t *testing.T) {
	secret := "s3cr3t"
	body := workflowRunBody(t)
	fs := &fakeStore{}
	h := &Handler{Secret: secret, Store: fs}

	req := httptest.NewRequest(http.MethodPost, "/webhook/github", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "workflow_run")
	req.Header.Set("X-Hub-Signature-256", "sha256=deadbeef")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
	if len(fs.runs) != 0 {
		t.Errorf("expected no runs stored for invalid signature")
	}
}

func TestServeHTTP_MissingSignatureHeader(t *testing.T) {
	secret := "s3cr3t"
	body := workflowRunBody(t)
	h := &Handler{Secret: secret, Store: &fakeStore{}}

	req := httptest.NewRequest(http.MethodPost, "/webhook/github", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "workflow_run")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for missing signature, got %d", rec.Code)
	}
}

func TestServeHTTP_NoSecretConfigured_SkipsVerification(t *testing.T) {
	body := workflowRunBody(t)
	fs := &fakeStore{}
	h := &Handler{Store: fs} // no secret

	req := httptest.NewRequest(http.MethodPost, "/webhook/github", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "workflow_run")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(fs.runs) != 1 {
		t.Fatalf("expected 1 stored run, got %d", len(fs.runs))
	}
}

func TestServeHTTP_PingEvent(t *testing.T) {
	h := &Handler{Store: &fakeStore{}}
	req := httptest.NewRequest(http.MethodPost, "/webhook/github", strings.NewReader("{}"))
	req.Header.Set("X-GitHub-Event", "ping")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for ping, got %d", rec.Code)
	}
}

func TestServeHTTP_UnhandledEvent(t *testing.T) {
	h := &Handler{Store: &fakeStore{}}
	req := httptest.NewRequest(http.MethodPost, "/webhook/github", strings.NewReader("{}"))
	req.Header.Set("X-GitHub-Event", "installation")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for unhandled event, got %d", rec.Code)
	}
}

func TestServeHTTP_InvalidJSONPayload(t *testing.T) {
	h := &Handler{Store: &fakeStore{}}
	req := httptest.NewRequest(http.MethodPost, "/webhook/github", strings.NewReader("not json"))
	req.Header.Set("X-GitHub-Event", "workflow_run")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid JSON, got %d", rec.Code)
	}
}

func TestServeHTTP_StoreError(t *testing.T) {
	body := workflowRunBody(t)
	fs := &fakeStore{failErr: errors.New("db down")}
	h := &Handler{Store: fs}

	req := httptest.NewRequest(http.MethodPost, "/webhook/github", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "workflow_run")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on store error, got %d", rec.Code)
	}
}

func TestServeHTTP_MissingUpdatedAt_UsesNow(t *testing.T) {
	fixed := time.Date(2030, 5, 5, 0, 0, 0, 0, time.UTC)
	fs := &fakeStore{}
	h := &Handler{Store: fs, Now: func() time.Time { return fixed }}

	payload := map[string]any{
		"action": "queued",
		"workflow_run": map[string]any{
			"id":     1,
			"status": "queued",
		},
		"repository": map[string]any{"full_name": "acme/widgets"},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/webhook/github", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "workflow_run")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(fs.runs) != 1 || !fs.runs[0].UpdatedAt.Equal(fixed) {
		t.Fatalf("expected fallback to Now(), got %+v", fs.runs)
	}
}

func TestValidSignature_MalformedHex(t *testing.T) {
	if validSignature("secret", []byte("body"), "sha256=not-hex!!") {
		t.Error("expected false for malformed hex signature")
	}
}

func TestValidSignature_MissingPrefix(t *testing.T) {
	if validSignature("secret", []byte("body"), "deadbeef") {
		t.Error("expected false for signature missing sha256= prefix")
	}
}
