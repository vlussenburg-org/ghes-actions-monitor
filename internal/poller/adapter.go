package poller

import (
	"context"
	"fmt"

	"github.com/google/go-github/v66/github"
)

// GHClientAdapter adapts a *github.Client to the poller's GitHubClient
// interface, handling pagination for org repos and translating "active
// workflow runs" into the underlying ListRepositoryWorkflowRuns calls.
type GHClientAdapter struct {
	Client *github.Client
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
		runs, _, err := a.Client.Actions.ListRepositoryWorkflowRuns(ctx, owner, repo, opts)
		if err != nil {
			return nil, fmt.Errorf("list workflow runs for %s/%s (status=%s): %w", owner, repo, status, err)
		}
		all = append(all, runs.WorkflowRuns...)
	}
	return all, nil
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
