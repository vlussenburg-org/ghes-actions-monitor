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
	// cache usage, workflow runs/jobs).
	AppID             int64
	AppInstallationID int64
	AppPrivateKeyPEM  string

	// AdminToken is a separate, higher-privilege PAT used only for org-wide
	// data unavailable to the GitHub App (installed-apps inventory, audit
	// log). It is deliberately scoped to as few calls as possible.
	AdminToken string

	// WebhookSecret validates inbound org webhook deliveries (HMAC SHA-256).
	WebhookSecret string

	// SlackWebhookURL receives incident notifications.
	SlackWebhookURL string

	// Poll intervals.
	RunnerPollInterval   time.Duration
	WorkflowPollInterval time.Duration
	AppInventoryInterval time.Duration
	HealthProbeInterval  time.Duration
	IncidentEvalInterval time.Duration

	// DBPath is the local SQLite database file path.
	DBPath string

	// OktaIssuer/OktaClientID gate the dashboard behind Okta OIDC login.
	// Left empty in the initial admin-PAT-only launch; auth middleware
	// no-ops until configured.
	OktaIssuer   string
	OktaClientID string
}

// Load reads configuration from environment variables, applying sane
// defaults, and validates required fields for GHES operation.
func Load() (Config, error) {
	c := Config{
		Port:            getEnvDefault("PORT", "8080"),
		GHESBaseURL:     strings.TrimRight(os.Getenv("GHES_BASE_URL"), "/"),
		Org:             os.Getenv("GITHUB_ORG"),
		IsGHEC:          isGHECBaseURL(os.Getenv("GHES_BASE_URL")),
		AdminToken:      os.Getenv("GITHUB_ADMIN_TOKEN"),
		WebhookSecret:   os.Getenv("GITHUB_WEBHOOK_SECRET"),
		SlackWebhookURL: os.Getenv("SLACK_WEBHOOK_URL"),
		DBPath:          getEnvDefault("DB_PATH", "data/monitor.db"),
		OktaIssuer:      os.Getenv("OKTA_ISSUER"),
		OktaClientID:    os.Getenv("OKTA_CLIENT_ID"),
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
	c.AppInventoryInterval, err = getEnvDuration("APP_INVENTORY_INTERVAL", time.Hour)
	if err != nil {
		return Config{}, err
	}
	c.HealthProbeInterval, err = getEnvDuration("HEALTH_PROBE_INTERVAL", 2*time.Minute)
	if err != nil {
		return Config{}, err
	}
	c.IncidentEvalInterval, err = getEnvDuration("INCIDENT_EVAL_INTERVAL", 30*time.Second)
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
// always required.
func (c Config) Validate() error {
	if c.Org == "" {
		return fmt.Errorf("GITHUB_ORG is required")
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
