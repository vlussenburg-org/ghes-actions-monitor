package ghclient

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vlussenburg/ghes-actions-monitor/internal/config"
)

func TestNew_NoCredentials(t *testing.T) {
	cfg := config.Config{Org: "acme", IsGHEC: true}
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if c.App != nil {
		t.Errorf("expected nil App client without credentials")
	}

	if c.Admin != nil {
		t.Errorf("expected nil Admin client without credentials")
	}
}

func TestRateLimitTracker_RecordsHeaders(t *testing.T) {
	reset := time.Now().Add(time.Hour).Unix()
	tracker := &RateLimitTracker{base: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		header := http.Header{}
		header.Add("X-RateLimit-Remaining", "12")
		header.Add("X-RateLimit-Limit", "5000")
		header.Add("X-RateLimit-Reset", strconv.FormatInt(reset, 10))
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: http.NoBody}, nil
	})}
	if _, err := tracker.RoundTrip(httptest.NewRequest(http.MethodGet, "https://example.test", nil)); err != nil {
		t.Fatal(err)
	}
	status := tracker.RateLimitStatus().(RateLimitStatus)
	if status.Remaining != 12 || status.Limit != 5000 || status.ResetAt.Unix() != reset {
		t.Fatalf("unexpected rate-limit status: %+v", status)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestNew_AdminTokenOnly_GHES(t *testing.T) {
	cfg := config.Config{
		Org:         "acme",
		GHESBaseURL: "https://ghes.example.com",
		IsGHEC:      false,
		AdminToken:  "fake-pat",
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.Admin == nil {
		t.Fatal("expected non-nil Admin client")
	}
	if !strings.Contains(c.Admin.BaseURL.String(), "ghes.example.com/api/v3") {
		t.Errorf("expected admin client base URL to target GHES appliance, got %s", c.Admin.BaseURL.String())
	}
}

func TestNew_AdminTokenOnly_GHEC(t *testing.T) {
	cfg := config.Config{
		Org:        "acme",
		IsGHEC:     true,
		AdminToken: "fake-pat",
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.Admin == nil {
		t.Fatal("expected non-nil Admin client")
	}
	if !strings.Contains(c.Admin.BaseURL.String(), "api.github.com") {
		t.Errorf("expected admin client base URL to target api.github.com, got %s", c.Admin.BaseURL.String())
	}
}

// generateTestKey produces a small RSA key and its PEM encoding for use in
// App-auth tests.
func generateTestKey(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der := x509.MarshalPKCS1PrivateKey(key)
	block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: der}
	return key, string(pem.EncodeToMemory(block))
}

func TestNew_AppClient_InvalidKey(t *testing.T) {
	cfg := config.Config{
		Org:               "acme",
		IsGHEC:            true,
		AppID:             1,
		AppInstallationID: 2,
		AppPrivateKeyPEM:  "not-a-valid-pem",
	}
	if _, err := New(cfg); err == nil {
		t.Fatal("expected error for invalid private key PEM")
	}
}

func TestAppTransport_MintsAndCachesInstallationToken(t *testing.T) {
	_, pemKey := generateTestKey(t)

	var tokenCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/access_tokens") {
			tokenCalls++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"token":      "installation-token-123",
				"expires_at": time.Now().Add(time.Hour).Format(time.RFC3339),
			})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/orgs/acme/actions/runner-groups") {
			auth := r.Header.Get("Authorization")
			if auth != "Bearer installation-token-123" {
				t.Errorf("expected installation token on API request, got %q", auth)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 0, "runner_groups": []any{}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	cfg := config.Config{
		Org:               "acme",
		GHESBaseURL:       srv.URL,
		IsGHEC:            false,
		AppID:             123,
		AppInstallationID: 456,
		AppPrivateKeyPEM:  pemKey,
	}

	clients, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if clients.App == nil {
		t.Fatal("expected non-nil App client")
	}

	ctx := t.Context()
	_, _, err = clients.App.Actions.ListOrganizationRunnerGroups(ctx, "acme", nil)
	if err != nil {
		t.Fatalf("ListRunnerGroups: %v", err)
	}
	_, _, err = clients.App.Actions.ListOrganizationRunnerGroups(ctx, "acme", nil)
	if err != nil {
		t.Fatalf("ListRunnerGroups (2nd call): %v", err)
	}

	if tokenCalls != 1 {
		t.Errorf("expected installation token to be minted once (cached), got %d calls", tokenCalls)
	}
}

func TestRateLimitTracker_RateLimited(t *testing.T) {
	future := time.Now().Add(time.Hour)

	t.Run("exhausted budget", func(t *testing.T) {
		tr := &RateLimitTracker{}
		tr.status = RateLimitStatus{Remaining: 0, Limit: 5000, ResetAt: future}
		limited, resetAt := tr.RateLimited()
		if !limited || !resetAt.Equal(future) {
			t.Fatalf("want limited until %v, got %v %v", future, limited, resetAt)
		}
	})

	t.Run("budget available", func(t *testing.T) {
		tr := &RateLimitTracker{}
		tr.status = RateLimitStatus{Remaining: 42, Limit: 5000, ResetAt: future}
		if limited, _ := tr.RateLimited(); limited {
			t.Fatal("want not limited when remaining > 0")
		}
	})

	t.Run("explicit backoff", func(t *testing.T) {
		tr := &RateLimitTracker{backoffUntil: future}
		tr.status = RateLimitStatus{Remaining: 100}
		limited, resetAt := tr.RateLimited()
		if !limited || !resetAt.Equal(future) {
			t.Fatalf("want backoff until %v, got %v %v", future, limited, resetAt)
		}
	})

	t.Run("expired reset", func(t *testing.T) {
		tr := &RateLimitTracker{}
		tr.status = RateLimitStatus{Remaining: 0, ResetAt: time.Now().Add(-time.Minute)}
		if limited, _ := tr.RateLimited(); limited {
			t.Fatal("want not limited once the reset time has passed")
		}
	})
}

func TestRateLimitTracker_RateLimitStatusUnknown(t *testing.T) {
	tr := &RateLimitTracker{}
	if got := tr.RateLimitStatus(); got != nil {
		t.Fatalf("expected nil status before headers are observed, got %+v", got)
	}
}
