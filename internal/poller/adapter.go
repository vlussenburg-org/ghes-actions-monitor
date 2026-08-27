package poller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/go-github/v66/github"
)

// GHClientAdapter adapts a *github.Client to the poller's GitHubClient
// interface, handling pagination for org repos and translating "active
// workflow runs" into the underlying ListRepositoryWorkflowRuns calls.
type GHClientAdapter struct {
	Client *github.Client
}

// CancelWorkflowRun requests a normal or force cancellation for a workflow
// run. Force cancellation bypasses workflow conditions that keep jobs alive.
func (a *GHClientAdapter) CancelWorkflowRun(ctx context.Context, repo string, runID int64, force bool) error {
	parts := strings.Split(repo, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid repository %q", repo)
	}
	action := "cancel"
	if force {
		action = "force-cancel"
	}
	path := fmt.Sprintf("repos/%s/%s/actions/runs/%d/%s",
		url.PathEscape(parts[0]), url.PathEscape(parts[1]), runID, action)
	req, err := a.Client.NewRequest(http.MethodPost, path, nil)
	if err != nil {
		return err
	}
	_, err = a.Client.Do(ctx, req, nil)
	var accepted *github.AcceptedError
	if errors.As(err, &accepted) {
		return nil
	}
	return asGitHubAPIError(err)
}

// GitHubAPIError carries the upstream GitHub REST status code and message so
// callers can surface the exact failure instead of a generic error. It
// deliberately exposes StatusCode() rather than the go-github type, keeping
// package boundaries interface-driven.
type GitHubAPIError struct {
	Status  int
	Message string
	Err     error
}

func (e *GitHubAPIError) Error() string {
	if e.Message == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("GitHub API %d: %s", e.Status, e.Message)
}

func (e *GitHubAPIError) StatusCode() int { return e.Status }

func (e *GitHubAPIError) Unwrap() error { return e.Err }

// asGitHubAPIError converts a go-github *ErrorResponse into a GitHubAPIError,
// preserving the upstream status code and any per-error detail. Other errors
// (transport failures, context cancellation) pass through unchanged.
func asGitHubAPIError(err error) error {
	var resp *github.ErrorResponse
	if !errors.As(err, &resp) || resp.Response == nil {
		return err
	}
	msg := resp.Message
	details := make([]string, 0, len(resp.Errors))
	for _, e := range resp.Errors {
		if e.Message != "" {
			details = append(details, e.Message)
		}
	}
	if len(details) > 0 {
		msg = strings.TrimSpace(msg + " (" + strings.Join(details, "; ") + ")")
	}
	if msg == "" {
		msg = http.StatusText(resp.Response.StatusCode)
	}
	return &GitHubAPIError{Status: resp.Response.StatusCode, Message: msg, Err: err}
}

// ListRepos returns the names (not full_name) of every repo in the org,
// paginating through the full result set.
func (a *GHClientAdapter) ListRepos(ctx context.Context, org string) ([]string, error) {
	var names []string
	opts := &github.RepositoryListByOrgOptions{
		ListOptions: github.ListOptions{PerPage: 100},
	}
	for {
		repos, resp, err := a.Client.Repositories.ListByOrg(ctx, org, opts)
		if err != nil {
			return nil, fmt.Errorf("list repos for org %s: %w", org, err)
		}
		for _, r := range repos {
			names = append(names, r.GetName())
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return names, nil
}

// ListActiveWorkflowRuns returns workflow runs that are currently queued or
// in progress for the given repo.
func (a *GHClientAdapter) ListActiveWorkflowRuns(ctx context.Context, owner, repo string) ([]*github.WorkflowRun, error) {
	var all []*github.WorkflowRun
	for _, status := range []string{"queued", "in_progress"} {
		opts := &github.ListWorkflowRunsOptions{
			Status:      status,
			ListOptions: github.ListOptions{PerPage: 100},
		}
		for {
			runs, resp, err := a.Client.Actions.ListRepositoryWorkflowRuns(ctx, owner, repo, opts)
			if err != nil {
				return nil, fmt.Errorf("list workflow runs for %s/%s (status=%s): %w", owner, repo, status, err)
			}
			all = append(all, runs.WorkflowRuns...)
			if resp.NextPage == 0 {
				break
			}
			opts.Page = resp.NextPage
		}
	}
	return all, nil
}

// ListRecentWorkflowRuns returns one page of the most recent workflow runs
// for the given repo from the last 24 hours, regardless of status, so
// completed/historic runs are captured too (not just active ones).
// Deliberately unpaginated for now — a single 100-result page per repo is
// enough for the MVP's "recent history" view.
func (a *GHClientAdapter) ListRecentWorkflowRuns(ctx context.Context, owner, repo string) ([]*github.WorkflowRun, error) {
	since := time.Now().Add(-24 * time.Hour).Format("2006-01-02")
	opts := &github.ListWorkflowRunsOptions{
		Created:     ">=" + since,
		ListOptions: github.ListOptions{PerPage: 100},
	}
	runs, _, err := a.Client.Actions.ListRepositoryWorkflowRuns(ctx, owner, repo, opts)
	if err != nil {
		return nil, fmt.Errorf("list recent workflow runs for %s/%s: %w", owner, repo, err)
	}
	return runs.WorkflowRuns, nil
}

// ListRunnerGroups returns every runner group configured for the org.
func (a *GHClientAdapter) ListRunnerGroups(ctx context.Context, org string) ([]*github.RunnerGroup, error) {
	groups, _, err := a.Client.Actions.ListOrganizationRunnerGroups(ctx, org, &github.ListOrgRunnerGroupOptions{
		ListOptions: github.ListOptions{PerPage: 100},
	})
	if err != nil {
		return nil, fmt.Errorf("list runner groups for org %s: %w", org, err)
	}
	return groups.RunnerGroups, nil
}

// ListRunnerGroupRunners returns every runner assigned to the given group.
func (a *GHClientAdapter) ListRunnerGroupRunners(ctx context.Context, org string, groupID int64) ([]*github.Runner, error) {
	runners, _, err := a.Client.Actions.ListRunnerGroupRunners(ctx, org, groupID, &github.ListOptions{PerPage: 100})
	if err != nil {
		return nil, fmt.Errorf("list runners for group %d: %w", groupID, err)
	}
	return runners.Runners, nil
}
