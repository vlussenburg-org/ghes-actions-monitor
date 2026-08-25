// Package ghclient builds authenticated GitHub REST API clients for the
// monitor. It supports two credentials, mirroring the "how it works" design:
//
//   - A GitHub App identity (least privilege) used for routine polling:
//     runner pools, cache usage, workflow runs/jobs.
//   - A separate admin PAT, used only for org-wide data the App cannot see
//     (installed-apps inventory, audit log).
//
// Both clients transparently target either a GHES appliance or GitHub.com/
// GHEC based on config.Config, via go-github's NewClient(...).WithEnterpriseURLs.
package ghclient

import (
	"context"
	"crypto/rsa"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/go-github/v66/github"

	"github.com/vlussenburg/ghes-actions-monitor/internal/config"
)

// Clients bundles the two authenticated clients the monitor needs.
type Clients struct {
	// App is authenticated as the GitHub App installation (least privilege).
	// Nil if no App credentials are configured.
	App *github.Client

	// Admin is authenticated with the admin PAT, used only for the org app
	// inventory / audit log. Nil if no admin token is configured.
	Admin          *github.Client
	RateLimit      *RateLimitTracker
	AppRateLimit   *RateLimitTracker
	AdminRateLimit *RateLimitTracker
}

type RateLimitStatus struct {
	Remaining int       `json:"remaining"`
	Limit     int       `json:"limit"`
	ResetAt   time.Time `json:"reset_at"`
}

type RateLimitTracker struct {
	mu           sync.RWMutex
	status       RateLimitStatus
	backoffUntil time.Time
	base         http.RoundTripper
}

func (t *RateLimitTracker) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.wait(req.Context()); err != nil {
		return nil, err
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if resp != nil {
		t.mu.Lock()
		if v, e := strconv.Atoi(resp.Header.Get("X-RateLimit-Remaining")); e == nil {
			t.status.Remaining = v
		}
		if v, e := strconv.Atoi(resp.Header.Get("X-RateLimit-Limit")); e == nil {
			t.status.Limit = v
		}
		if v, e := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64); e == nil {
			t.status.ResetAt = time.Unix(v, 0).UTC()
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			if v, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil {
				t.backoffUntil = time.Now().Add(time.Duration(v) * time.Second)
			} else if !t.status.ResetAt.IsZero() {
				t.backoffUntil = t.status.ResetAt
			}
		}
		t.mu.Unlock()
	}
	return resp, err
}

func (t *RateLimitTracker) wait(ctx context.Context) error {
	t.mu.RLock()
	until := t.backoffUntil
	if t.status.Remaining <= 0 && !t.status.ResetAt.IsZero() && t.status.ResetAt.After(until) {
		until = t.status.ResetAt
	}
	t.mu.RUnlock()
	if until.IsZero() || !until.After(time.Now()) {
		return nil
	}
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *RateLimitTracker) RateLimitStatus() any {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.status.Limit == 0 && t.status.ResetAt.IsZero() {
		return nil
	}
	return t.status
}

// RateLimited reports whether the budget is currently spent, along with the
// time it is expected to reset. Callers use this to skip optional work rather
// than issuing requests that are certain to fail.
func (t *RateLimitTracker) RateLimited() (bool, time.Time) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	now := time.Now()
	if t.backoffUntil.After(now) {
		return true, t.backoffUntil
	}
	if t.status.Remaining <= 0 && t.status.ResetAt.After(now) {
		return true, t.status.ResetAt
	}
	return false, time.Time{}
}

// New builds the configured clients for the given instance (GHES or GHEC,
// as resolved by cfg). The App client, if configured, uses an
// auto-refreshing installation-token transport (see appTransport below).
func New(cfg config.Config) (*Clients, error) {
	c := &Clients{}
	if cfg.HasAppCredentials() {
		c.AppRateLimit = &RateLimitTracker{}
		appClient, err := newAppClient(cfg, c.AppRateLimit)
		if err != nil {
			return nil, fmt.Errorf("build app client: %w", err)
		}
		c.App = appClient
	}

	if cfg.AdminToken != "" {
		c.AdminRateLimit = &RateLimitTracker{}
		adminClient, err := newTokenClient(cfg, cfg.AdminToken, c.AdminRateLimit)
		if err != nil {
			return nil, fmt.Errorf("build admin client: %w", err)
		}
		c.Admin = adminClient
	}

	if c.AppRateLimit != nil {
		c.RateLimit = c.AppRateLimit
	} else {
		c.RateLimit = c.AdminRateLimit
	}
	return c, nil
}

// newTokenClient builds a go-github client authenticated with a static
// bearer token (used for the admin PAT), pointed at the resolved GHES/GHEC
// REST and upload base URLs.
func newTokenClient(cfg config.Config, token string, tracker *RateLimitTracker) (*github.Client, error) {
	base := github.NewClient(&http.Client{Transport: tracker}).WithAuthToken(token)
	return withEnterpriseURLs(base, cfg)
}

// withEnterpriseURLs points a go-github client at the resolved REST/upload
// base URLs for either a GHES appliance or GitHub.com/GHEC. For GHEC this is
// a no-op (default URLs already correct); for GHES it rewrites to the
// appliance's /api/v3 and /api/uploads paths.
func withEnterpriseURLs(base *github.Client, cfg config.Config) (*github.Client, error) {
	if cfg.IsGHEC {
		return base, nil
	}
	client, err := base.WithEnterpriseURLs(cfg.RESTBaseURL(), cfg.UploadBaseURL())
	if err != nil {
		return nil, fmt.Errorf("configure enterprise URLs: %w", err)
	}
	return client, nil
}

// newAppClient builds a client authenticated as the configured GitHub App
// installation, using appTransport to mint and cache short-lived
// installation access tokens (POST /app/installations/{id}/access_tokens).
func newAppClient(cfg config.Config, tracker *RateLimitTracker) (*github.Client, error) {
	key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(cfg.AppPrivateKeyPEM))
	if err != nil {
		return nil, fmt.Errorf("parse app private key: %w", err)
	}

	// Build a plain client (pointed at the right instance) to mint
	// installation tokens against, then wrap its transport so every
	// request carries a valid, auto-refreshed installation token.
	seed := github.NewClient(&http.Client{Transport: tracker})
	seed, err = withEnterpriseURLs(seed, cfg)
	if err != nil {
		return nil, err
	}

	t := &appTransport{
		base:           tracker,
		client:         seed,
		appID:          cfg.AppID,
		installationID: cfg.AppInstallationID,
		key:            key,
	}

	httpClient := &http.Client{Transport: t}
	appClient := github.NewClient(httpClient)
	return withEnterpriseURLs(appClient, cfg)
}

// appTransport is an http.RoundTripper that mints a JWT for the GitHub App,
// exchanges it for an installation access token (caching until near
// expiry), and attaches it as a Bearer token on every outgoing request.
type appTransport struct {
	base           http.RoundTripper
	client         *github.Client
	appID          int64
	installationID int64
	key            *rsa.PrivateKey

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

// RoundTrip implements http.RoundTripper.
func (t *appTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.installationToken(req.Context())
	if err != nil {
		return nil, fmt.Errorf("get installation token: %w", err)
	}
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+token)
	return t.base.RoundTrip(req)
}

// installationToken returns a cached installation token, refreshing it if
// it is missing or within 2 minutes of expiry.
func (t *appTransport) installationToken(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.token != "" && time.Until(t.expiresAt) > 2*time.Minute {
		return t.token, nil
	}

	appJWT, err := t.mintAppJWT()
	if err != nil {
		return "", err
	}

	// Use a short-lived plain client authenticated with the App JWT to call
	// POST /app/installations/{installation_id}/access_tokens.
	jwtClient := t.client.WithAuthToken(appJWT)
	installTok, _, err := jwtClient.Apps.CreateInstallationToken(ctx, t.installationID, nil)
	if err != nil {
		return "", fmt.Errorf("create installation token: %w", err)
	}

	t.token = installTok.GetToken()
	t.expiresAt = installTok.GetExpiresAt().Time
	return t.token, nil
}

// mintAppJWT builds a short-lived (9 minute) JWT identifying the GitHub App,
// per https://docs.github.com/apps/creating-github-apps/authenticating-with-a-github-app/authenticating-as-a-github-app.
func (t *appTransport) mintAppJWT() (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		IssuedAt:  jwt.NewNumericDate(now.Add(-30 * time.Second)),
		ExpiresAt: jwt.NewNumericDate(now.Add(9 * time.Minute)),
		Issuer:    fmt.Sprintf("%d", t.appID),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(t.key)
	if err != nil {
		return "", fmt.Errorf("sign app jwt: %w", err)
	}
	return signed, nil
}
