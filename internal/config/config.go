// Package config loads GitHub Actions Monitor configuration from environment
// variables. It is intentionally simple (no external config libraries) so the
// app can run as a single static binary in a container with only env vars set.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the monitor.
type Config struct {
	// Port the HTTP server listens on.
	Port string

	// GHESBaseURL is the base URL of the GitHub Enterprise Server instance,
	// e.g. "https://ghes.example.com". The REST API is served under
	// "<GHESBaseURL>/api/v3" and uploads under "<GHESBaseURL>/api/uploads".
	//
	// Leave unset (or set GITHUB_HOST=github.com) to target GitHub.com /
	// GHEC instead, in which case the standard api.github.com and
	// uploads.github.com endpoints are used. See GHEC_TODO.md for the
	// handful of GHEC-only behaviors (e.g. audit-log API shape, OAuth
	// device flow endpoints) that still need dedicated support.
	GHESBaseURL string

	// IsGHEC reports whether this instance targets GitHub.com/GHEC rather
	// than a GitHub Enterprise Server appliance. Derived from GHESBaseURL.
	IsGHEC bool

	// Org is the GitHub organization to monitor.
	Org string

	// GitHub App credentials used for least-privilege polling (runner pools,
	// workflow runs/jobs). Optional for the initial launch — set
	// GITHUB_ADMIN_TOKEN instead to use a single admin PAT for everything.
	AppID             int64
	AppInstallationID int64
	AppPrivateKeyPEM  string

	// AdminToken is a PAT used for all GitHub API calls when no GitHub App
	// is configured. This lets the MVP launch without dealing with GitHub
	// App/OAuth setup; swap in App credentials later for least privilege.
	AdminToken string

	// WebhookSecret validates inbound org webhook deliveries (HMAC SHA-256).
	WebhookSecret string

	// Poll intervals.
	RunnerPollInterval   time.Duration
	WorkflowPollInterval time.Duration

	// DBPath is the local SQLite database file path.
	DBPath string
}

// Load reads configuration from environment variables, applying sane
// defaults, and validates required fields for GHES operation.
func Load() (Config, error) {
	c := Config{
		Port:          getEnvDefault("PORT", "8080"),
		GHESBaseURL:   strings.TrimRight(os.Getenv("GHES_BASE_URL"), "/"),
		Org:           os.Getenv("GITHUB_ORG"),
		IsGHEC:        isGHECBaseURL(os.Getenv("GHES_BASE_URL")),
		AdminToken:    os.Getenv("GITHUB_ADMIN_TOKEN"),
		WebhookSecret: os.Getenv("GITHUB_WEBHOOK_SECRET"),
		DBPath:        getEnvDefault("DB_PATH", "data/monitor.db"),
	}

	var err error
	c.AppID, err = getEnvInt64("GITHUB_APP_ID", 0)
	if err != nil {
		return Config{}, err
	}
	c.AppInstallationID, err = getEnvInt64("GITHUB_APP_INSTALLATION_ID", 0)
	if err != nil {
		return Config{}, err
	}
	c.AppPrivateKeyPEM = os.Getenv("GITHUB_APP_PRIVATE_KEY")

	c.RunnerPollInterval, err = getEnvDuration("RUNNER_POLL_INTERVAL", 60*time.Second)
	if err != nil {
		return Config{}, err
	}
	c.WorkflowPollInterval, err = getEnvDuration("WORKFLOW_POLL_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}

	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Validate ensures the minimum configuration required to run is present.
// GHES_BASE_URL may be omitted entirely to target GitHub.com/GHEC; Org is
// always required, and exactly one auth method (admin token or GitHub App)
// must be configured.
func (c Config) Validate() error {
	if c.Org == "" {
		return fmt.Errorf("GITHUB_ORG is required")
	}
	if c.AdminToken == "" && !c.HasAppCredentials() {
		return fmt.Errorf("either GITHUB_ADMIN_TOKEN or GITHUB_APP_ID/GITHUB_APP_INSTALLATION_ID/GITHUB_APP_PRIVATE_KEY must be set")
	}
	return nil
}

// RESTBaseURL returns the REST API base URL: "<GHESBaseURL>/api/v3/" for a
// GHES appliance, or "https://api.github.com/" for GitHub.com/GHEC.
func (c Config) RESTBaseURL() string {
	if c.IsGHEC {
		return "https://api.github.com/"
	}
	return c.GHESBaseURL + "/api/v3/"
}

// UploadBaseURL returns the upload base URL: "<GHESBaseURL>/api/uploads/"
// for GHES, or "https://uploads.github.com/" for GitHub.com/GHEC.
func (c Config) UploadBaseURL() string {
	if c.IsGHEC {
		return "https://uploads.github.com/"
	}
	return c.GHESBaseURL + "/api/uploads/"
}

// isGHECBaseURL reports whether the configured base URL (if any) refers to
// GitHub.com/GHEC rather than a dedicated GHES appliance. An empty value
// defaults to GHEC so the app is usable out of the box against GitHub.com.
func isGHECBaseURL(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return true
	}
	trimmed := strings.TrimRight(strings.ToLower(raw), "/")
	return trimmed == "https://github.com" || trimmed == "http://github.com" || trimmed == "github.com"
}

// HasAppCredentials reports whether GitHub App auth is fully configured.
func (c Config) HasAppCredentials() bool {
	return c.AppID != 0 && c.AppInstallationID != 0 && c.AppPrivateKeyPEM != ""
}

func getEnvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt64(key string, def int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return n, nil
}

func getEnvDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return d, nil
}
