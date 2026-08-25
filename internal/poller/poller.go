// Package poller periodically calls the GitHub REST API to fill in what the
// webhook event feed doesn't carry: a full sweep of active (queued/
// in_progress) workflow runs across every repo in the org (as a backstop
// for missed or delayed webhook deliveries), a slower sweep of each repo's
// recent (last 7 days) run history regardless of status (so completed runs
// show up even without a webhook), and runner group capacity for the org.
package poller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/go-github/v66/github"

	"github.com/vlussenburg/ghes-actions-monitor/internal/store"
)

// Store is the subset of store.Store the poller needs.
type Store interface {
	UpsertWorkflowRun(ctx context.Context, r store.WorkflowRun) error
	RecordRunnerSnapshot(ctx context.Context, r store.RunnerSnapshot) error
}

// GitHubClient is the subset of the go-github client surface the poller
// depends on, allowing tests to substitute a fake without spinning up an
// HTTP server for every case.
type GitHubClient interface {
	ListRepos(ctx context.Context, org string) ([]string, error)
	ListActiveWorkflowRuns(ctx context.Context, owner, repo string) ([]*github.WorkflowRun, error)
	ListRecentWorkflowRuns(ctx context.Context, owner, repo string) ([]*github.WorkflowRun, error)
	ListRunnerGroups(ctx context.Context, org string) ([]*github.RunnerGroup, error)
	ListRunnerGroupRunners(ctx context.Context, org string, groupID int64) ([]*github.Runner, error)
}

// Poller drives periodic API polling.
type Poller struct {
	Client GitHubClient
	Store  Store
	Org    string
	Logger *slog.Logger

	WorkflowInterval time.Duration
	RunnerInterval   time.Duration
	HistoryInterval  time.Duration

	// Now allows tests to control the observed time; defaults to time.Now.
	Now func() time.Time
}

// RefreshNow runs one immediate pass of every poll sweep (active workflow
// runs, recent/historic workflow runs, and runner capacity), bypassing the
// configured intervals. Used to back a manual "force refresh" action so
// staleness isn't limited by the poll ticker cadence.
func (p *Poller) RefreshNow(ctx context.Context) {
	p.pollWorkflowRuns(ctx)
	p.pollHistory(ctx)
	p.pollRunners(ctx)
}

// Run blocks, polling workflow runs and runner capacity on their configured
// intervals until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	workflowTicker := time.NewTicker(p.intervalOrDefault(p.WorkflowInterval, 30*time.Second))
	defer workflowTicker.Stop()
	runnerTicker := time.NewTicker(p.intervalOrDefault(p.RunnerInterval, 60*time.Second))
	defer runnerTicker.Stop()
	historyTicker := time.NewTicker(p.intervalOrDefault(p.HistoryInterval, 5*time.Minute))
	defer historyTicker.Stop()

	// Run once immediately so the dashboard has data without waiting a full
	// interval after startup.
	p.pollWorkflowRuns(ctx)
	p.pollRunners(ctx)
	p.pollHistory(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-workflowTicker.C:
			p.pollWorkflowRuns(ctx)
		case <-runnerTicker.C:
			p.pollRunners(ctx)
		case <-historyTicker.C:
			p.pollHistory(ctx)
		}
	}
}

func (p *Poller) intervalOrDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

func (p *Poller) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now().UTC()
}

func (p *Poller) logger() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return slog.Default()
}

// pollWorkflowRuns sweeps every repo in the org for active (queued/
// in_progress) workflow runs and records their current state. This
// backstops the webhook feed: even if a delivery is lost, the next sweep
// picks up the run's true state.
func (p *Poller) pollWorkflowRuns(ctx context.Context) {
	repos, err := p.Client.ListRepos(ctx, p.Org)
	if err != nil {
		p.logger().Error("poll: failed to list repos", "error", err)
		return
	}

	for _, repo := range repos {
		runs, err := p.Client.ListActiveWorkflowRuns(ctx, p.Org, repo)
		if err != nil {
			p.logger().Error("poll: failed to list workflow runs", "repo", repo, "error", err)
			continue
		}
		for _, r := range runs {
			if err := p.Store.UpsertWorkflowRun(ctx, toStoreRun(p.Org, repo, r, p.now())); err != nil {
				p.logger().Error("poll: failed to store workflow run", "repo", repo, "run_id", r.GetID(), "error", err)
			}
		}
	}
}

// pollRunners records a capacity snapshot for every runner group in the
// org.
func (p *Poller) pollRunners(ctx context.Context) {
	groups, err := p.Client.ListRunnerGroups(ctx, p.Org)
	if err != nil {
		p.logger().Error("poll: failed to list runner groups", "error", err)
		return
	}

	for _, g := range groups {
		runners, err := p.Client.ListRunnerGroupRunners(ctx, p.Org, g.GetID())
		if err != nil {
			p.logger().Error("poll: failed to list runners for group", "group", g.GetName(), "error", err)
			continue
		}

		var busy, idle int
		for _, r := range runners {
			if r.GetBusy() {
				busy++
			} else {
				idle++
			}
		}

		snap := store.RunnerSnapshot{
			GroupName:  g.GetName(),
			Total:      len(runners),
			Busy:       busy,
			Idle:       idle,
			CapturedAt: p.now(),
		}
		if err := p.Store.RecordRunnerSnapshot(ctx, snap); err != nil {
			p.logger().Error("poll: failed to record runner snapshot", "group", g.GetName(), "error", err)
		}
	}
}

// pollHistory sweeps every repo in the org for its recent (last 7 days)
// workflow runs regardless of status, so completed/historic runs show up
// in the dashboard even without a webhook delivering them live. Runs on a
// slower interval than pollWorkflowRuns since it's a heavier, less
// time-sensitive sweep.
func (p *Poller) pollHistory(ctx context.Context) {
	repos, err := p.Client.ListRepos(ctx, p.Org)
	if err != nil {
		p.logger().Error("poll: failed to list repos for history sweep", "error", err)
		return
	}

	for _, repo := range repos {
		runs, err := p.Client.ListRecentWorkflowRuns(ctx, p.Org, repo)
		if err != nil {
			p.logger().Error("poll: failed to list recent workflow runs", "repo", repo, "error", err)
			continue
		}
		for _, r := range runs {
			if err := p.Store.UpsertWorkflowRun(ctx, toStoreRun(p.Org, repo, r, p.now())); err != nil {
				p.logger().Error("poll: failed to store historic workflow run", "repo", repo, "run_id", r.GetID(), "error", err)
			}
		}
	}
}

// toStoreRun maps a go-github WorkflowRun to the store's representation.
func toStoreRun(org, repo string, r *github.WorkflowRun, fallbackTime time.Time) store.WorkflowRun {
	updatedAt := fallbackTime
	if r.UpdatedAt != nil {
		updatedAt = r.UpdatedAt.Time
	}
	return store.WorkflowRun{
		RunID:      r.GetID(),
		Repo:       fmt.Sprintf("%s/%s", org, repo),
		Name:       r.GetName(),
		Status:     r.GetStatus(),
		Conclusion: r.GetConclusion(),
		Event:      r.GetEvent(),
		HeadBranch: r.GetHeadBranch(),
		Source:     "poll",
		UpdatedAt:  updatedAt,
	}
}
