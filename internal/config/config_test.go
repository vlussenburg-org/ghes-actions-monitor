package config

import (
	"testing"
	"time"
)

func TestLoad_MissingRequired(t *testing.T) {
	t.Setenv("GHES_BASE_URL", "")
	t.Setenv("GITHUB_ORG", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when GITHUB_ORG is unset")
	}
}

func TestLoad_MissingAuth(t *testing.T) {
	t.Setenv("GHES_BASE_URL", "https://ghes.example.com")
	t.Setenv("GITHUB_ORG", "snowflake-eng")
	t.Setenv("GITHUB_ADMIN_TOKEN", "")
	t.Setenv("GITHUB_APP_ID", "")
	t.Setenv("GITHUB_APP_INSTALLATION_ID", "")
	t.Setenv("GITHUB_APP_PRIVATE_KEY", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when neither admin token nor app credentials are set")
	}
}

func TestLoad_GHECDefaultWhenBaseURLUnset(t *testing.T) {
	t.Setenv("GHES_BASE_URL", "")
	t.Setenv("GITHUB_ORG", "snowflake-eng")
	t.Setenv("GITHUB_ADMIN_TOKEN", "fake-pat")

	c, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.IsGHEC {
		t.Errorf("expected IsGHEC=true when GHES_BASE_URL is unset")
	}
	if got, want := c.RESTBaseURL(), "https://api.github.com/"; got != want {
		t.Errorf("RESTBaseURL() = %q, want %q", got, want)
	}
	if got, want := c.UploadBaseURL(), "https://uploads.github.com/"; got != want {
		t.Errorf("UploadBaseURL() = %q, want %q", got, want)
	}
}

func TestLoad_GHECExplicitGitHubCom(t *testing.T) {
	t.Setenv("GHES_BASE_URL", "https://github.com")
	t.Setenv("GITHUB_ORG", "snowflake-eng")
	t.Setenv("GITHUB_ADMIN_TOKEN", "fake-pat")

	c, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.IsGHEC {
		t.Errorf("expected IsGHEC=true for https://github.com")
	}
	if got, want := c.RESTBaseURL(), "https://api.github.com/"; got != want {
		t.Errorf("RESTBaseURL() = %q, want %q", got, want)
	}
}

func TestLoad_GHESBaseURL(t *testing.T) {
	t.Setenv("GHES_BASE_URL", "https://ghes.example.com/")
	t.Setenv("GITHUB_ORG", "snowflake-eng")
	t.Setenv("GITHUB_ADMIN_TOKEN", "fake-pat")

	c, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.IsGHEC {
		t.Errorf("expected IsGHEC=false for a GHES appliance URL")
	}
}

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("GHES_BASE_URL", "https://ghes.example.com/")
	t.Setenv("GITHUB_ORG", "snowflake-eng")
	t.Setenv("GITHUB_APP_ID", "")
	t.Setenv("GITHUB_APP_INSTALLATION_ID", "")
	t.Setenv("GITHUB_ADMIN_TOKEN", "fake-pat")

	c, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.GHESBaseURL != "https://ghes.example.com" {
		t.Errorf("expected trimmed base URL, got %q", c.GHESBaseURL)
	}
	if c.Org != "snowflake-eng" {
		t.Errorf("unexpected org: %q", c.Org)
	}
	if c.Port != "8080" {
		t.Errorf("expected default port 8080, got %q", c.Port)
	}
	if c.RunnerPollInterval != 10*time.Minute {
		t.Errorf("unexpected default runner poll interval: %v", c.RunnerPollInterval)
	}
	if c.WorkflowPollInterval != 5*time.Minute {
		t.Errorf("unexpected default workflow poll interval: %v", c.WorkflowPollInterval)
	}
	if c.HasAppCredentials() {
		t.Errorf("expected no app credentials configured")
	}
	if got, want := c.RESTBaseURL(), "https://ghes.example.com/api/v3/"; got != want {
		t.Errorf("RESTBaseURL() = %q, want %q", got, want)
	}
	if got, want := c.UploadBaseURL(), "https://ghes.example.com/api/uploads/"; got != want {
		t.Errorf("UploadBaseURL() = %q, want %q", got, want)
	}
}

func TestLoad_InvalidDuration(t *testing.T) {
	t.Setenv("GHES_BASE_URL", "https://ghes.example.com")
	t.Setenv("GITHUB_ORG", "snowflake-eng")
	t.Setenv("GITHUB_ADMIN_TOKEN", "fake-pat")
	t.Setenv("RUNNER_POLL_INTERVAL", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for invalid duration")
	}
}

func TestLoad_InvalidInt(t *testing.T) {
	t.Setenv("GHES_BASE_URL", "https://ghes.example.com")
	t.Setenv("GITHUB_ORG", "snowflake-eng")
	t.Setenv("GITHUB_ADMIN_TOKEN", "fake-pat")
	t.Setenv("GITHUB_APP_ID", "not-a-number")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for invalid GITHUB_APP_ID")
	}
}

func TestLoad_FullAppCredentials(t *testing.T) {
	t.Setenv("GHES_BASE_URL", "https://ghes.example.com")
	t.Setenv("GITHUB_ORG", "snowflake-eng")
	t.Setenv("GITHUB_APP_ID", "123")
	t.Setenv("GITHUB_APP_INSTALLATION_ID", "456")
	t.Setenv("GITHUB_APP_PRIVATE_KEY", "fake-pem")

	c, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.HasAppCredentials() {
		t.Errorf("expected app credentials to be configured")
	}
	if c.AppID != 123 || c.AppInstallationID != 456 {
		t.Errorf("unexpected app ids: %d %d", c.AppID, c.AppInstallationID)
	}
}
