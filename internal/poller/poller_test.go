package poller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v66/github"

	"github.com/vlussenburg-org/ghes-actions-monitor/internal/store"
)

type fakeGitHubClient struct {
	mu sync.Mutex

	repos            []string
	reposErr         error
	activeRuns       map[string][]*github.WorkflowRun // keyed by repo
	activeRunsErr    map[string]error
	recentRuns       map[string][]*github.WorkflowRun // keyed by repo
	recentRunsErr    map[string]error
	runnerGroups     []*github.RunnerGroup
	runnerGroupsErr  error
	groupRunners     map[int64][]*github.Runner
	groupRunnersErr  map[int64]error
	getRun           map[int64]*github.WorkflowRun // keyed by run ID
	getRunErr        map[int64]error
	listReposCalls   int
	listRunsCalls    int
	listRecentCalls  int
	listGroupsCalls  int
	listRunnersCalls int
	getRunCalls      []int64
}

func (f *fakeGitHubClient) ListRepos(ctx context.Context, org string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listReposCalls++
	if f.reposErr != nil {
		return nil, f.reposErr
	}
	return f.repos, nil
}

func (f *fakeGitHubClient) ListActiveWorkflowRuns(ctx context.Context, owner, repo string) ([]*github.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listRunsCalls++
	if err, ok := f.activeRunsErr[repo]; ok {
		return nil, err
	}
	return f.activeRuns[repo], nil
}

func (f *fakeGitHubClient) ListRecentWorkflowRuns(ctx context.Context, owner, repo string) ([]*github.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listRecentCalls++
	if err, ok := f.recentRunsErr[repo]; ok {
		return nil, err
	}
	return f.recentRuns[repo], nil
}

func (f *fakeGitHubClient) ListRunnerGroups(ctx context.Context, org string) ([]*github.RunnerGroup, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listGroupsCalls++
	if f.runnerGroupsErr != nil {
		return nil, f.runnerGroupsErr
	}
	return f.runnerGroups, nil
}

func (f *fakeGitHubClient) GetWorkflowRun(ctx context.Context, owner, repo string, runID int64) (*github.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getRunCalls = append(f.getRunCalls, runID)
	if err, ok := f.getRunErr[runID]; ok {
		return nil, err
	}
	return f.getRun[runID], nil
}

func (f *fakeGitHubClient) ListRunnerGroupRunners(ctx context.Context, org string, groupID int64) ([]*github.Runner, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listRunnersCalls++
	if err, ok := f.groupRunnersErr[groupID]; ok {
		return nil, err
	}
	return f.groupRunners[groupID], nil
}

type fakeStore struct {
	mu                    sync.Mutex
	runs                  []store.WorkflowRun
	runSnapshots          []store.RunnerSnapshot
	queueDepthSnaps       []store.QueueDepthSnapshot
	runErr                error
	snapErr               error
	queueDepthErr         error
	queueDepthSnapErr     error
	closeStaleErr         error
	queueDepth            store.QueueDepth
	closedRepos           []string
	closedActiveIDs       map[int64]struct{}
	activeRepos           []string
	activeReposErr        error
	activeReposCalls      int
	activeReposAfterFirst []string
	zombieRuns            []store.WorkflowRun
	zombieRunsErr         error
	zombieRunsCalls       int
}

func (f *fakeStore) UpsertWorkflowRun(ctx context.Context, r store.WorkflowRun) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.runErr != nil {
		return f.runErr
	}
	f.runs = append(f.runs, r)
	return nil
}

func (f *fakeStore) CloseStaleActiveRuns(ctx context.Context, repos []string, activeRunIDs map[int64]struct{}, completedAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closeStaleErr != nil {
		return f.closeStaleErr
	}
	f.closedRepos = append([]string(nil), repos...)
	f.closedActiveIDs = activeRunIDs
	return nil
}

func (f *fakeStore) RecordRunnerSnapshot(ctx context.Context, r store.RunnerSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snapErr != nil {
		return f.snapErr
	}
	f.runSnapshots = append(f.runSnapshots, r)
	return nil
}

func (f *fakeStore) QueueDepth(ctx context.Context) (store.QueueDepth, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.queueDepthErr != nil {
		return store.QueueDepth{}, f.queueDepthErr
	}
	return f.queueDepth, nil
}

func (f *fakeStore) RecordQueueDepthSnapshot(ctx context.Context, snap store.QueueDepthSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.queueDepthSnapErr != nil {
		return f.queueDepthSnapErr
	}
	f.queueDepthSnaps = append(f.queueDepthSnaps, snap)
	return nil
}

func (f *fakeStore) RecentlyActiveRepos(ctx context.Context, since time.Time) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.activeReposErr != nil {
		return nil, f.activeReposErr
	}
	f.activeReposCalls++
	if f.activeReposCalls > 1 && f.activeReposAfterFirst != nil {
		return f.activeReposAfterFirst, nil
	}
	return f.activeRepos, nil
}

func (f *fakeStore) ZombieRuns(ctx context.Context, staleAfter time.Duration, now time.Time) ([]store.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.zombieRunsCalls++
	if f.zombieRunsErr != nil {
		return nil, f.zombieRunsErr
	}
	return f.zombieRuns, nil
}

func strPtr(s string) *string { return &s }

func TestPollWorkflowRuns_HappyPath(t *testing.T) {
	fixedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	client := &fakeGitHubClient{
		activeRuns: map[string][]*github.WorkflowRun{
			"widgets": {
				{ID: github.Int64(1), Name: strPtr("CI"), Status: strPtr("queued"), Event: strPtr("push"), HeadBranch: strPtr("main")},
			},
			"gadgets": {
				{ID: github.Int64(2), Name: strPtr("CI"), Status: strPtr("in_progress")},
			},
		},
	}
	st := &fakeStore{activeRepos: []string{"acme/widgets", "acme/gadgets"}}
	p := &Poller{Client: client, Store: st, Org: "acme", Now: func() time.Time { return fixedTime }}

	p.pollWorkflowRuns(context.Background())

	if len(st.runs) != 2 {
		t.Fatalf("expected 2 runs stored, got %d", len(st.runs))
	}
	if st.runs[0].Repo != "acme/widgets" || st.runs[0].RunID != 1 || st.runs[0].Source != "poll" {
		t.Errorf("unexpected first run: %+v", st.runs[0])
	}
	if !st.runs[0].UpdatedAt.Equal(fixedTime) {
		t.Errorf("expected fallback time used, got %v", st.runs[0].UpdatedAt)
	}
}

func TestPollWorkflowRuns_BootstrapsHistoryWhenNoRecentlyActiveRepos(t *testing.T) {
	client := &fakeGitHubClient{
		repos: []string{"widgets"},
		recentRuns: map[string][]*github.WorkflowRun{
			"widgets": {
				{ID: github.Int64(101), Name: strPtr("CI"), Status: strPtr("completed"), Conclusion: strPtr("success")},
			},
		},
	}
	st := &fakeStore{
		activeRepos:           nil,
		activeReposAfterFirst: []string{"acme/widgets"},
	}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background())

	if client.listReposCalls != 1 {
		t.Errorf("expected pollHistory bootstrap to call ListRepos once, got %d", client.listReposCalls)
	}
	if len(st.runs) != 1 || st.runs[0].RunID != 101 {
		t.Fatalf("expected bootstrap to store the historic run, got %+v", st.runs)
	}
	if !p.bootstrapped.Load() {
		t.Error("expected bootstrapped flag to be set")
	}
}

func TestPollWorkflowRuns_DoesNotRebootstrapOnSubsequentEmptyCycles(t *testing.T) {
	client := &fakeGitHubClient{repos: []string{"widgets"}}
	st := &fakeStore{activeRepos: nil}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background())
	p.pollWorkflowRuns(context.Background())

	if client.listReposCalls != 1 {
		t.Errorf("expected bootstrap to run at most once across cycles, got %d ListRepos calls", client.listReposCalls)
	}
}

func TestPollWorkflowRuns_ReposError(t *testing.T) {
	client := &fakeGitHubClient{}
	st := &fakeStore{activeReposErr: errors.New("boom")}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background()) // should not panic

	if len(st.runs) != 0 {
		t.Errorf("expected no runs stored when repo listing fails")
	}
}

func TestPollWorkflowRuns_PerRepoErrorContinues(t *testing.T) {
	client := &fakeGitHubClient{
		activeRunsErr: map[string]error{
			"bad-repo": errors.New("rate limited"),
		},
		activeRuns: map[string][]*github.WorkflowRun{
			"good-repo": {
				{ID: github.Int64(9), Status: strPtr("queued")},
			},
		},
	}
	st := &fakeStore{activeRepos: []string{"acme/bad-repo", "acme/good-repo"}}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background())

	if len(st.runs) != 1 || st.runs[0].RunID != 9 {
		t.Fatalf("expected good-repo's run to still be stored, got %+v", st.runs)
	}
}

func TestPollWorkflowRuns_StoreErrorContinues(t *testing.T) {
	client := &fakeGitHubClient{
		activeRuns: map[string][]*github.WorkflowRun{
			"widgets": {{ID: github.Int64(1), Status: strPtr("queued")}},
		},
	}
	st := &fakeStore{runErr: errors.New("db error"), activeRepos: []string{"acme/widgets"}}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background()) // should not panic
}

func TestPollWorkflowRuns_RecordsQueueDepthSnapshot(t *testing.T) {
	client := &fakeGitHubClient{}
	fixedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	st := &fakeStore{queueDepth: store.QueueDepth{Queued: 4, InProgress: 2}, activeRepos: []string{"acme/widgets"}}
	p := &Poller{Client: client, Store: st, Org: "acme", Now: func() time.Time { return fixedTime }}

	p.pollWorkflowRuns(context.Background())

	if len(st.queueDepthSnaps) != 1 {
		t.Fatalf("expected 1 queue depth snapshot recorded, got %d", len(st.queueDepthSnaps))
	}
	snap := st.queueDepthSnaps[0]
	if snap.Queued != 4 || snap.InProgress != 2 || !snap.CapturedAt.Equal(fixedTime) {
		t.Errorf("unexpected snapshot: %+v", snap)
	}
}

func TestPollWorkflowRuns_ClosesStaleActiveRunsForSuccessfullyScannedRepos(t *testing.T) {
	client := &fakeGitHubClient{
		activeRunsErr: map[string]error{
			"bad-repo": errors.New("rate limited"),
		},
		activeRuns: map[string][]*github.WorkflowRun{
			"good-repo": {
				{ID: github.Int64(9), Status: strPtr("queued")},
			},
		},
	}
	st := &fakeStore{activeRepos: []string{"acme/bad-repo", "acme/good-repo"}}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background())

	if len(st.closedRepos) != 1 || st.closedRepos[0] != "acme/good-repo" {
		t.Fatalf("expected only successfully scanned repo to be reconciled, got %+v", st.closedRepos)
	}
	if _, ok := st.closedActiveIDs[9]; !ok {
		t.Fatalf("expected active run 9 to be preserved during stale reconciliation, got %+v", st.closedActiveIDs)
	}
}

func TestPollWorkflowRuns_CloseStaleErrorDoesNotSkipSnapshot(t *testing.T) {
	client := &fakeGitHubClient{}
	st := &fakeStore{
		closeStaleErr: errors.New("db down"),
		queueDepth:    store.QueueDepth{Queued: 1},
		activeRepos:   []string{"acme/widgets"},
	}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background())

	if len(st.queueDepthSnaps) != 1 {
		t.Fatalf("expected queue depth snapshot even when stale close fails, got %d", len(st.queueDepthSnaps))
	}
}

func TestPollWorkflowRuns_QueueDepthErrorDoesNotPanic(t *testing.T) {
	client := &fakeGitHubClient{}
	st := &fakeStore{queueDepthErr: errors.New("db down"), activeRepos: []string{"acme/widgets"}}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background()) // should not panic

	if len(st.queueDepthSnaps) != 0 {
		t.Errorf("expected no snapshot recorded when QueueDepth fails")
	}
}

func TestPollWorkflowRuns_QueueDepthSnapshotErrorDoesNotPanic(t *testing.T) {
	client := &fakeGitHubClient{}
	st := &fakeStore{queueDepthSnapErr: errors.New("db down"), activeRepos: []string{"acme/widgets"}}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background()) // should not panic
}

func TestPollRunners_HappyPath(t *testing.T) {
	client := &fakeGitHubClient{
		runnerGroups: []*github.RunnerGroup{
			{ID: github.Int64(1), Name: strPtr("default")},
			{ID: github.Int64(2), Name: strPtr("gpu")},
		},
		groupRunners: map[int64][]*github.Runner{
			1: {
				{Busy: github.Bool(true)},
				{Busy: github.Bool(false)},
				{Busy: github.Bool(true)},
			},
			2: {
				{Busy: github.Bool(false)},
			},
		},
	}
	st := &fakeStore{}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollRunners(context.Background())

	if len(st.runSnapshots) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(st.runSnapshots))
	}
	byGroup := map[string]store.RunnerSnapshot{}
	for _, s := range st.runSnapshots {
		byGroup[s.GroupName] = s
	}
	if byGroup["default"].Total != 3 || byGroup["default"].Busy != 2 || byGroup["default"].Idle != 1 {
		t.Errorf("unexpected default group snapshot: %+v", byGroup["default"])
	}
	if byGroup["gpu"].Total != 1 || byGroup["gpu"].Busy != 0 {
		t.Errorf("unexpected gpu group snapshot: %+v", byGroup["gpu"])
	}
}

func TestPollRunners_GroupsError(t *testing.T) {
	client := &fakeGitHubClient{runnerGroupsErr: errors.New("boom")}
	st := &fakeStore{}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollRunners(context.Background()) // should not panic
	if len(st.runSnapshots) != 0 {
		t.Errorf("expected no snapshots when group listing fails")
	}
}

func TestPollRunners_PerGroupErrorContinues(t *testing.T) {
	client := &fakeGitHubClient{
		runnerGroups: []*github.RunnerGroup{
			{ID: github.Int64(1), Name: strPtr("broken")},
			{ID: github.Int64(2), Name: strPtr("ok")},
		},
		groupRunnersErr: map[int64]error{1: errors.New("nope")},
		groupRunners: map[int64][]*github.Runner{
			2: {{Busy: github.Bool(false)}},
		},
	}
	st := &fakeStore{}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollRunners(context.Background())

	if len(st.runSnapshots) != 1 || st.runSnapshots[0].GroupName != "ok" {
		t.Fatalf("expected only 'ok' group snapshot recorded, got %+v", st.runSnapshots)
	}
}

func TestPollRunners_StoreErrorContinues(t *testing.T) {
	client := &fakeGitHubClient{
		runnerGroups: []*github.RunnerGroup{{ID: github.Int64(1), Name: strPtr("default")}},
		groupRunners: map[int64][]*github.Runner{1: {{Busy: github.Bool(false)}}},
	}
	st := &fakeStore{snapErr: errors.New("db error")}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollRunners(context.Background()) // should not panic
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	client := &fakeGitHubClient{repos: []string{}, runnerGroups: []*github.RunnerGroup{}}
	st := &fakeStore{}
	p := &Poller{
		Client:           client,
		Store:            st,
		Org:              "acme",
		WorkflowInterval: time.Millisecond,
		RunnerInterval:   time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}

	client.mu.Lock()
	groupCalls := client.listGroupsCalls
	recentCalls := client.listRecentCalls
	client.mu.Unlock()
	if groupCalls == 0 {
		t.Error("expected at least one immediate poll on startup")
	}
	if recentCalls != 0 {
		t.Errorf("expected scheduled polling to skip manual-only history sweep, got %d recent run calls", recentCalls)
	}
}

func TestPollHistory_HappyPath(t *testing.T) {
	client := &fakeGitHubClient{
		repos: []string{"widgets", "gadgets"},
		recentRuns: map[string][]*github.WorkflowRun{
			"widgets": {
				{ID: github.Int64(101), Name: strPtr("CI"), Status: strPtr("completed"), Conclusion: strPtr("success")},
			},
			"gadgets": {
				{ID: github.Int64(102), Name: strPtr("CI"), Status: strPtr("completed"), Conclusion: strPtr("failure")},
			},
		},
	}
	st := &fakeStore{}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollHistory(context.Background())

	if len(st.runs) != 2 {
		t.Fatalf("expected 2 historic runs stored, got %d", len(st.runs))
	}
}

func TestPollHistory_ReposError(t *testing.T) {
	client := &fakeGitHubClient{reposErr: errors.New("boom")}
	st := &fakeStore{}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollHistory(context.Background()) // should not panic

	if len(st.runs) != 0 {
		t.Errorf("expected no runs stored when repo listing fails")
	}
}

func TestPollHistory_PerRepoErrorContinues(t *testing.T) {
	client := &fakeGitHubClient{
		repos: []string{"bad-repo", "good-repo"},
		recentRunsErr: map[string]error{
			"bad-repo": errors.New("rate limited"),
		},
		recentRuns: map[string][]*github.WorkflowRun{
			"good-repo": {
				{ID: github.Int64(9), Status: strPtr("completed")},
			},
		},
	}
	st := &fakeStore{}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollHistory(context.Background())

	if len(st.runs) != 1 || st.runs[0].RunID != 9 {
		t.Fatalf("expected good-repo's run to still be stored, got %+v", st.runs)
	}
}

func TestPollHistory_StoreErrorContinues(t *testing.T) {
	client := &fakeGitHubClient{
		repos: []string{"widgets"},
		recentRuns: map[string][]*github.WorkflowRun{
			"widgets": {{ID: github.Int64(1), Status: strPtr("completed")}},
		},
	}
	st := &fakeStore{runErr: errors.New("db error")}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollHistory(context.Background()) // should not panic
}

func TestRefreshNow_RunsAllSweeps(t *testing.T) {
	client := &fakeGitHubClient{
		repos: []string{"widgets"},
		activeRuns: map[string][]*github.WorkflowRun{
			"widgets": {{ID: github.Int64(1), Status: strPtr("in_progress")}},
		},
		recentRuns: map[string][]*github.WorkflowRun{
			"widgets": {{ID: github.Int64(2), Status: strPtr("completed"), Conclusion: strPtr("success")}},
		},
		runnerGroups: []*github.RunnerGroup{{ID: github.Int64(1), Name: strPtr("default")}},
		groupRunners: map[int64][]*github.Runner{1: {{Busy: github.Bool(true)}}},
	}
	st := &fakeStore{activeRepos: []string{"acme/widgets"}}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.RefreshNow(context.Background())

	if client.listRunsCalls == 0 || client.listRecentCalls == 0 || client.listGroupsCalls == 0 {
		t.Fatalf("expected RefreshNow to invoke all three sweeps, got %+v", client)
	}
	if len(st.runs) != 2 {
		t.Errorf("expected 2 runs stored (active + historic), got %d", len(st.runs))
	}
	if len(st.runSnapshots) != 1 {
		t.Errorf("expected 1 runner snapshot stored, got %d", len(st.runSnapshots))
	}
}

func TestToStoreRun_UsesRunUpdatedAtWhenPresent(t *testing.T) {
	updated := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	run := &github.WorkflowRun{
		ID:        github.Int64(42),
		Status:    strPtr("completed"),
		UpdatedAt: &github.Timestamp{Time: updated},
	}
	got := toStoreRun("acme", "widgets", run, time.Now())
	if !got.UpdatedAt.Equal(updated) {
		t.Errorf("expected UpdatedAt from run, got %v want %v", got.UpdatedAt, updated)
	}
	if got.Repo != "acme/widgets" {
		t.Errorf("unexpected repo: %s", got.Repo)
	}
}

type fakeRateLimiter struct {
	limited bool
	resetAt time.Time
}

func (f fakeRateLimiter) RateLimited() (bool, time.Time) { return f.limited, f.resetAt }

func TestPoller_SkipsSweepsWhenRateLimited(t *testing.T) {
	client := &fakeGitHubClient{repos: []string{"repo-a"}}
	p := &Poller{
		Client:      client,
		Store:       &fakeStore{},
		Org:         "acme",
		RateLimiter: fakeRateLimiter{limited: true, resetAt: time.Now().Add(time.Hour)},
	}

	p.RefreshNow(context.Background())

	if client.listReposCalls != 0 || client.listGroupsCalls != 0 {
		t.Fatalf("expected no GitHub calls while rate limited, got repos=%d groups=%d",
			client.listReposCalls, client.listGroupsCalls)
	}
}

func TestPoller_PollsWhenNotRateLimited(t *testing.T) {
	client := &fakeGitHubClient{repos: []string{"repo-a"}}
	p := &Poller{
		Client:      client,
		Store:       &fakeStore{},
		Org:         "acme",
		RateLimiter: fakeRateLimiter{limited: false},
	}

	p.RefreshNow(context.Background())

	if client.listReposCalls == 0 {
		t.Fatal("expected GitHub calls when not rate limited")
	}
}

func TestPollStaleRuns_ReconcilesCompletedRun(t *testing.T) {
	fixedTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	client := &fakeGitHubClient{
		getRun: map[int64]*github.WorkflowRun{
			42: {ID: github.Int64(42), Name: strPtr("CI"), Status: strPtr("completed"), Conclusion: strPtr("success"), Event: strPtr("push"), HeadBranch: strPtr("main")},
		},
	}
	st := &fakeStore{
		zombieRuns: []store.WorkflowRun{
			{RunID: 42, Repo: "acme/widgets", Name: "CI", Status: "queued", Event: "push", HeadBranch: "main"},
		},
	}
	p := &Poller{Client: client, Store: st, Org: "acme", Now: func() time.Time { return fixedTime }}

	p.pollStaleRuns(context.Background())

	if len(client.getRunCalls) != 1 || client.getRunCalls[0] != 42 {
		t.Fatalf("expected GetWorkflowRun called once for run 42, got %v", client.getRunCalls)
	}
	if len(st.runs) != 1 {
		t.Fatalf("expected 1 reconciled run stored, got %d", len(st.runs))
	}
	got := st.runs[0]
	if got.RunID != 42 || got.Repo != "acme/widgets" || got.Status != "completed" || got.Conclusion != "success" || got.Source != "poll" {
		t.Errorf("unexpected reconciled run: %+v", got)
	}
	if !got.UpdatedAt.Equal(fixedTime) {
		t.Errorf("expected fallback time used, got %v", got.UpdatedAt)
	}
}

func TestPollStaleRuns_MarksMissingRunAsUnknown(t *testing.T) {
	fixedTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	client := &fakeGitHubClient{
		getRun: map[int64]*github.WorkflowRun{}, // GetWorkflowRun returns (nil, nil): run no longer exists
	}
	st := &fakeStore{
		zombieRuns: []store.WorkflowRun{
			{RunID: 7, Repo: "acme/widgets", Name: "CI", Status: "in_progress", Event: "push", HeadBranch: "main"},
		},
	}
	p := &Poller{Client: client, Store: st, Org: "acme", Now: func() time.Time { return fixedTime }}

	p.pollStaleRuns(context.Background())

	if len(st.runs) != 1 {
		t.Fatalf("expected 1 run stored, got %d", len(st.runs))
	}
	got := st.runs[0]
	if got.RunID != 7 || got.Status != "unknown" || got.Source != "poll" {
		t.Errorf("expected missing run marked unknown, got %+v", got)
	}
	if !got.UpdatedAt.Equal(fixedTime) {
		t.Errorf("expected fallback time used, got %v", got.UpdatedAt)
	}
}

func TestPollStaleRuns_SkipsMalformedRepo(t *testing.T) {
	client := &fakeGitHubClient{}
	st := &fakeStore{
		zombieRuns: []store.WorkflowRun{
			{RunID: 1, Repo: "no-slash-here", Status: "queued"},
		},
	}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollStaleRuns(context.Background()) // should not panic

	if len(client.getRunCalls) != 0 {
		t.Errorf("expected GetWorkflowRun not called for malformed repo, got %v", client.getRunCalls)
	}
	if len(st.runs) != 0 {
		t.Errorf("expected no runs stored for malformed repo, got %d", len(st.runs))
	}
}

func TestPollStaleRuns_PerRunErrorContinues(t *testing.T) {
	fixedTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	client := &fakeGitHubClient{
		getRunErr: map[int64]error{
			1: errors.New("rate limited"),
		},
		getRun: map[int64]*github.WorkflowRun{
			2: {ID: github.Int64(2), Status: strPtr("completed"), Conclusion: strPtr("failure")},
		},
	}
	st := &fakeStore{
		zombieRuns: []store.WorkflowRun{
			{RunID: 1, Repo: "acme/bad-repo", Status: "queued"},
			{RunID: 2, Repo: "acme/good-repo", Status: "queued"},
		},
	}
	p := &Poller{Client: client, Store: st, Org: "acme", Now: func() time.Time { return fixedTime }}

	p.pollStaleRuns(context.Background())

	if len(st.runs) != 1 || st.runs[0].RunID != 2 {
		t.Fatalf("expected only run 2 stored after run 1 errored, got %+v", st.runs)
	}
}

func TestPollStaleRuns_ZombieRunsError(t *testing.T) {
	client := &fakeGitHubClient{}
	st := &fakeStore{zombieRunsErr: errors.New("db down")}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollStaleRuns(context.Background()) // should not panic

	if len(client.getRunCalls) != 0 {
		t.Errorf("expected no GetWorkflowRun calls when ZombieRuns errors")
	}
}

func TestPollStaleRuns_UsesConfiguredStaleAfter(t *testing.T) {
	client := &fakeGitHubClient{}
	st := &fakeStore{}
	p := &Poller{Client: client, Store: st, Org: "acme", StaleRunReconcileAfter: 90 * time.Minute}

	p.pollStaleRuns(context.Background())

	if st.zombieRunsCalls != 1 {
		t.Fatalf("expected ZombieRuns called once, got %d", st.zombieRunsCalls)
	}
}
