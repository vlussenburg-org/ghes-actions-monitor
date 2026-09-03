// Package poller periodically calls the GitHub REST API to fill in what the
// webhook event feed doesn't carry: a spot-check sweep of workflow runs,
// scoped to only the repos webhooks say have had recent job activity (as a
// backstop for missed or delayed webhook deliveries on repos that are
// actually in use), a manual refresh-only sweep of each repo's recent run
// history regardless of status, and runner group capacity for the org. If
// the spot-check window ever finds no recently active repos at all (a fresh
// install with an empty store, or a quiet window with no webhook deliveries
// yet), it falls back to one one-time full-org history sweep to bootstrap
// the store.
package poller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/go-github/v66/github"

	"github.com/vlussenburg-org/ghes-actions-monitor/internal/store"
)

// Store is the subset of store.Store the poller needs.
type Store interface {
	UpsertWorkflowRun(ctx context.Context, r store.WorkflowRun) error
	CloseStaleActiveRuns(ctx context.Context, repos []string, activeRunIDs map[int64]struct{}, completedAt time.Time) error
	// CloseConcludedActiveRuns repairs runs whose latest recorded state is
	// still queued/in_progress but already carries a terminal conclusion
	// (e.g. skipped), which GitHub's active-run list filters can return.
	// It closes them out from data already held (no API call) and returns
	// the number repaired.
	CloseConcludedActiveRuns(ctx context.Context) (int, error)
	RecordRunnerSnapshot(ctx context.Context, r store.RunnerSnapshot) error
	QueueDepth(ctx context.Context) (store.QueueDepth, error)
	RecordQueueDepthSnapshot(ctx context.Context, snap store.QueueDepthSnapshot) error
	// RecentlyActiveRepos returns repos that have had a workflow_run event
	// (any status — webhooks record every transition) recorded since the
	// given time. The poller uses this, rather than every repo in the org,
	// to target its per-cycle REST spot-check sweep at repos webhooks say
	// are actually seeing job activity.
	RecentlyActiveRepos(ctx context.Context, since time.Time) ([]string, error)
	// ZombieRuns returns runs whose latest recorded state is still queued
	// or in_progress but hasn't been updated in over staleAfter. The
	// poller uses this to find runs the repo-activity-scoped spot-check
	// sweep will never revisit (because their repo has gone quiet) and
	// reconcile them directly by run ID instead.
	ZombieRuns(ctx context.Context, staleAfter time.Duration, now time.Time) ([]store.WorkflowRun, error)
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
	// GetWorkflowRun fetches a single run by ID directly, regardless of
	// age or which repo it belongs to. Returns (nil, nil) if GitHub no
	// longer has the run (e.g. deleted), which is a valid terminal state.
	GetWorkflowRun(ctx context.Context, owner, repo string, runID int64) (*github.WorkflowRun, error)
}

// RateLimiter reports whether the shared GitHub API budget is currently
// exhausted. The poller consults it before starting a sweep so an exhausted
// budget skips the sweep entirely instead of emitting one error per repo.
type RateLimiter interface {
	// RateLimited reports whether requests are currently blocked, and when
	// the budget is expected to reset.
	RateLimited() (bool, time.Time)
}

// Poller drives periodic API polling.
type Poller struct {
	Client      GitHubClient
	Store       Store
	Org         string
	Logger      *slog.Logger
	RateLimiter RateLimiter

	WorkflowInterval time.Duration
	RunnerInterval   time.Duration
	// SpotCheckWindow bounds which repos are considered "recently active"
	// for the per-cycle workflow-run sweep: only repos with a workflow_run
	// recorded (any status) within this window are polled, rather than
	// every repo in the org. Defaults to 24h.
	SpotCheckWindow time.Duration
	// StaleRunReconcileAfter bounds how long a run may sit as queued or
	// in_progress before pollStaleRuns fetches its true current state
	// directly by run ID. This catches runs the repo-activity-scoped
	// spot-check sweep (pollWorkflowRuns) can never revisit once a repo
	// stops seeing new webhook activity, plus runs older than the
	// date-bounded history sweep's window. Defaults to 3h.
	StaleRunReconcileAfter time.Duration

	// Now allows tests to control the observed time; defaults to time.Now.
	Now func() time.Time

	// bootstrapped is set once pollWorkflowRuns has run pollHistory as a
	// one-time fallback to seed the store when the spot-check window found
	// no recently active repos (e.g. a fresh install with an empty store,
	// or a quiet window with no webhook deliveries yet).
	bootstrapped atomic.Bool
}

// RefreshNow runs one immediate pass of every poll sweep (active workflow
// runs, recent/historic workflow runs, stale-run reconciliation, and
// runner capacity), bypassing the configured intervals. Used to back a
// manual "force refresh" action so staleness isn't limited by the poll
// ticker cadence.
func (p *Poller) RefreshNow(ctx context.Context) {
	p.pollWorkflowRuns(ctx)
	p.pollHistory(ctx)
	p.pollStaleRuns(ctx)
	p.pollRunners(ctx)
}

// Run blocks, polling active workflow runs, stale-run reconciliation, and
// runner capacity on their configured intervals until ctx is cancelled.
// Historic workflow run polling is intentionally manual-only via
// RefreshNow because it is much more expensive than the live active-run
// sweep.
func (p *Poller) Run(ctx context.Context) {
	workflowTicker := time.NewTicker(p.intervalOrDefault(p.WorkflowInterval, 5*time.Minute))
	defer workflowTicker.Stop()
	runnerTicker := time.NewTicker(p.intervalOrDefault(p.RunnerInterval, 10*time.Minute))
	defer runnerTicker.Stop()
	staleRunTicker := time.NewTicker(p.intervalOrDefault(p.WorkflowInterval, 5*time.Minute))
	defer staleRunTicker.Stop()

	// Run once immediately so the dashboard has data without waiting a full
	// interval after startup.
	p.pollWorkflowRuns(ctx)
	p.pollRunners(ctx)
	p.pollStaleRuns(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-workflowTicker.C:
			p.pollWorkflowRuns(ctx)
		case <-runnerTicker.C:
			p.pollRunners(ctx)
		case <-staleRunTicker.C:
			p.pollStaleRuns(ctx)
		}
	}
}

func (p *Poller) intervalOrDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// skipForRateLimit reports whether a sweep should be skipped because the
// shared GitHub budget is exhausted. Skipping avoids emitting one failed
// request (and one error log) per repo while the quota is spent.
func (p *Poller) skipForRateLimit(sweep string) bool {
	if p.RateLimiter == nil {
		return false
	}
	limited, resetAt := p.RateLimiter.RateLimited()
	if !limited {
		return false
	}
	p.logger().Info("poll: skipping sweep, GitHub rate limit exhausted",
		"sweep", sweep, "reset_at", resetAt)
	return true
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

// pollWorkflowRuns spot-checks workflow runs for repos that webhooks say
// have had recent activity (any run recorded within SpotCheckWindow), and
// records their current active-run state. This backstops the webhook feed
// for repos that are actually in use: even if a completion delivery is
// lost, the next sweep picks up the run's true state and closes it out
// instead of it going stale forever. Repos with no recent webhook activity
// are not polled — if webhooks are flowing, there is nothing there for a
// REST call to catch, and skipping them is what keeps this scaling with
// active job volume rather than total repo count.
func (p *Poller) pollWorkflowRuns(ctx context.Context) {
	if p.skipForRateLimit("workflow_runs") {
		return
	}
	since := p.now().Add(-p.intervalOrDefault(p.SpotCheckWindow, 24*time.Hour))
	repos, err := p.Store.RecentlyActiveRepos(ctx, since)
	if err != nil {
		p.logger().Error("poll: failed to list recently active repos", "error", err)
		return
	}

	if len(repos) == 0 && p.bootstrapped.CompareAndSwap(false, true) {
		// Nothing recently active yet — either a fresh install with an
		// empty store, or a quiet window with no webhook deliveries. Seed
		// the store with one full-org history sweep so the spot-check
		// window has something to work with going forward. Only ever done
		// once per process lifetime to avoid degrading into a full sweep
		// every cycle.
		p.logger().Info("poll: no recently active repos found, running one-time history bootstrap")
		p.pollHistory(ctx)
		repos, err = p.Store.RecentlyActiveRepos(ctx, since)
		if err != nil {
			p.logger().Error("poll: failed to list recently active repos after bootstrap", "error", err)
			return
		}
	}

	activeRunIDs := map[int64]struct{}{}
	var scannedRepos []string
	for _, fullRepo := range repos {
		repo := strings.TrimPrefix(fullRepo, p.Org+"/")
		runs, err := p.Client.ListActiveWorkflowRuns(ctx, p.Org, repo)
		if err != nil {
			p.logger().Error("poll: failed to list workflow runs", "repo", repo, "error", err)
			continue
		}
		scannedRepos = append(scannedRepos, fullRepo)
		for _, r := range runs {
			activeRunIDs[r.GetID()] = struct{}{}
			if err := p.Store.UpsertWorkflowRun(ctx, toStoreRun(p.Org, repo, r, p.now())); err != nil {
				p.logger().Error("poll: failed to store workflow run", "repo", repo, "run_id", r.GetID(), "error", err)
			}
		}
	}
	if err := p.Store.CloseStaleActiveRuns(ctx, scannedRepos, activeRunIDs, p.now()); err != nil {
		p.logger().Error("poll: failed to close stale active runs", "error", err)
	}

	// Repair any runs whose latest state still reads queued/in_progress but
	// already carries a terminal conclusion (e.g. skipped). These are
	// provably not active — GitHub already gave us the conclusion — so we
	// close them out from local data with no extra API call, keeping them
	// out of the queue-depth snapshot recorded just below.
	if n, err := p.Store.CloseConcludedActiveRuns(ctx); err != nil {
		p.logger().Error("poll: failed to close concluded active runs", "error", err)
	} else if n > 0 {
		p.logger().Info("poll: closed concluded runs still marked active", "count", n)
	}

	p.recordQueueDepthSnapshot(ctx)
}

// recordQueueDepthSnapshot reads the current org-wide queue depth and
// appends a point-in-time reading to the time series, so the dashboard can
// chart queued vs in-progress jobs over time.
func (p *Poller) recordQueueDepthSnapshot(ctx context.Context) {
	depth, err := p.Store.QueueDepth(ctx)
	if err != nil {
		p.logger().Error("poll: failed to compute queue depth for snapshot", "error", err)
		return
	}
	snap := store.QueueDepthSnapshot{
		Queued:     depth.Queued,
		InProgress: depth.InProgress,
		CapturedAt: p.now(),
	}
	if err := p.Store.RecordQueueDepthSnapshot(ctx, snap); err != nil {
		p.logger().Error("poll: failed to record queue depth snapshot", "error", err)
	}
}

// pollRunners records a capacity snapshot for every runner group in the
// org.
func (p *Poller) pollRunners(ctx context.Context) {
	if p.skipForRateLimit("runners") {
		return
	}
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

// pollHistory sweeps every repo in the org for its recent (last 24 hours)
// workflow runs regardless of status, so completed/historic runs show up
// in the dashboard even without a webhook delivering them live. It is
// invoked by manual refresh, and automatically once by pollWorkflowRuns as a
// one-time bootstrap if the spot-check window ever finds no recently active
// repos at all; otherwise it is heavier than the active-run sweep and is
// not scheduled on its own.
func (p *Poller) pollHistory(ctx context.Context) {
	if p.skipForRateLimit("history") {
		return
	}
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

// pollStaleRuns reconciles runs whose latest recorded state is still
// queued/in_progress but hasn't been updated in over
// StaleRunReconcileAfter, by fetching each one directly by run ID. This
// exists because the other sweeps can each leave a run permanently stuck:
// pollWorkflowRuns only rechecks repos with recent webhook activity
// (RecentlyActiveRepos), and pollHistory/ListRecentWorkflowRuns only looks
// back a fixed, date-bounded window. A run in a repo that's gone quiet, or
// older than that window, is never revisited by either sweep — so without
// this, a single missed completion webhook can leave a run showing as
// queued indefinitely, long after GitHub itself resolved it.
func (p *Poller) pollStaleRuns(ctx context.Context) {
	if p.skipForRateLimit("stale_runs") {
		return
	}
	staleAfter := p.intervalOrDefault(p.StaleRunReconcileAfter, 3*time.Hour)
	stale, err := p.Store.ZombieRuns(ctx, staleAfter, p.now())
	if err != nil {
		p.logger().Error("poll: failed to list stale runs for reconciliation", "error", err)
		return
	}

	for _, r := range stale {
		owner, repo, ok := strings.Cut(r.Repo, "/")
		if !ok {
			p.logger().Error("poll: skipping stale run with malformed repo", "repo", r.Repo, "run_id", r.RunID)
			continue
		}
		run, err := p.Client.GetWorkflowRun(ctx, owner, repo, r.RunID)
		if err != nil {
			p.logger().Error("poll: failed to reconcile stale run", "repo", r.Repo, "run_id", r.RunID, "error", err)
			continue
		}
		if run == nil {
			// GitHub no longer has this run (e.g. deleted): its true state
			// can't be recovered, so record it as unknown rather than
			// leaving it stuck as queued/in_progress forever.
			if err := p.Store.UpsertWorkflowRun(ctx, store.WorkflowRun{
				RunID:      r.RunID,
				Repo:       r.Repo,
				Name:       r.Name,
				Status:     "unknown",
				Event:      r.Event,
				HeadBranch: r.HeadBranch,
				Source:     "poll",
				UpdatedAt:  p.now(),
			}); err != nil {
				p.logger().Error("poll: failed to store unknown state for missing stale run", "repo", r.Repo, "run_id", r.RunID, "error", err)
			}
			continue
		}
		if err := p.Store.UpsertWorkflowRun(ctx, toStoreRun(owner, repo, run, p.now())); err != nil {
			p.logger().Error("poll: failed to store reconciled stale run", "repo", r.Repo, "run_id", r.RunID, "error", err)
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
