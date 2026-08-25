package poller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v66/github"

	"github.com/vlussenburg/ghes-actions-monitor/internal/store"
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
	listReposCalls   int
	listRunsCalls    int
	listRecentCalls  int
	listGroupsCalls  int
	listRunnersCalls int
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
	mu                sync.Mutex
	runs              []store.WorkflowRun
	runSnapshots      []store.RunnerSnapshot
	queueDepthSnaps   []store.QueueDepthSnapshot
	runErr            error
	snapErr           error
	queueDepthErr     error
	queueDepthSnapErr error
	closeStaleErr     error
	queueDepth        store.QueueDepth
	closedRepos       []string
	closedActiveIDs   map[int64]struct{}
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

func strPtr(s string) *string { return &s }

func TestPollWorkflowRuns_HappyPath(t *testing.T) {
	fixedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	client := &fakeGitHubClient{
		repos: []string{"widgets", "gadgets"},
		activeRuns: map[string][]*github.WorkflowRun{
			"widgets": {
				{ID: github.Int64(1), Name: strPtr("CI"), Status: strPtr("queued"), Event: strPtr("push"), HeadBranch: strPtr("main")},
			},
			"gadgets": {
				{ID: github.Int64(2), Name: strPtr("CI"), Status: strPtr("in_progress")},
			},
		},
	}
	st := &fakeStore{}
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

func TestPollWorkflowRuns_ReposError(t *testing.T) {
	client := &fakeGitHubClient{reposErr: errors.New("boom")}
	st := &fakeStore{}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background()) // should not panic

	if len(st.runs) != 0 {
		t.Errorf("expected no runs stored when repo listing fails")
	}
}

func TestPollWorkflowRuns_PerRepoErrorContinues(t *testing.T) {
	client := &fakeGitHubClient{
		repos: []string{"bad-repo", "good-repo"},
		activeRunsErr: map[string]error{
			"bad-repo": errors.New("rate limited"),
		},
		activeRuns: map[string][]*github.WorkflowRun{
			"good-repo": {
				{ID: github.Int64(9), Status: strPtr("queued")},
			},
		},
	}
	st := &fakeStore{}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background())

	if len(st.runs) != 1 || st.runs[0].RunID != 9 {
		t.Fatalf("expected good-repo's run to still be stored, got %+v", st.runs)
	}
}

func TestPollWorkflowRuns_StoreErrorContinues(t *testing.T) {
	client := &fakeGitHubClient{
		repos: []string{"widgets"},
		activeRuns: map[string][]*github.WorkflowRun{
			"widgets": {{ID: github.Int64(1), Status: strPtr("queued")}},
		},
	}
	st := &fakeStore{runErr: errors.New("db error")}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background()) // should not panic
}

func TestPollWorkflowRuns_RecordsQueueDepthSnapshot(t *testing.T) {
	client := &fakeGitHubClient{repos: []string{"widgets"}}
	fixedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	st := &fakeStore{queueDepth: store.QueueDepth{Queued: 4, InProgress: 2}}
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
		repos: []string{"bad-repo", "good-repo"},
		activeRunsErr: map[string]error{
			"bad-repo": errors.New("rate limited"),
		},
		activeRuns: map[string][]*github.WorkflowRun{
			"good-repo": {
				{ID: github.Int64(9), Status: strPtr("queued")},
			},
		},
	}
	st := &fakeStore{}
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
	client := &fakeGitHubClient{repos: []string{"widgets"}}
	st := &fakeStore{
		closeStaleErr: errors.New("db down"),
		queueDepth:    store.QueueDepth{Queued: 1},
	}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background())

	if len(st.queueDepthSnaps) != 1 {
		t.Fatalf("expected queue depth snapshot even when stale close fails, got %d", len(st.queueDepthSnaps))
	}
}

func TestPollWorkflowRuns_QueueDepthErrorDoesNotPanic(t *testing.T) {
	client := &fakeGitHubClient{repos: []string{"widgets"}}
	st := &fakeStore{queueDepthErr: errors.New("db down")}
	p := &Poller{Client: client, Store: st, Org: "acme"}

	p.pollWorkflowRuns(context.Background()) // should not panic

	if len(st.queueDepthSnaps) != 0 {
		t.Errorf("expected no snapshot recorded when QueueDepth fails")
	}
}

func TestPollWorkflowRuns_QueueDepthSnapshotErrorDoesNotPanic(t *testing.T) {
	client := &fakeGitHubClient{repos: []string{"widgets"}}
	st := &fakeStore{queueDepthSnapErr: errors.New("db down")}
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
	calls := client.listReposCalls
	recentCalls := client.listRecentCalls
	client.mu.Unlock()
	if calls == 0 {
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
	st := &fakeStore{}
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
