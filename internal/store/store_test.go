package store

import (
	"context"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:", 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestOpen_Migrates(t *testing.T) {
	s := newTestStore(t)
	if s.db == nil {
		t.Fatal("expected non-nil db handle")
	}
}

func TestQueueDepth(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	runs := []WorkflowRun{
		{RunID: 1, Repo: "org/a", Status: "queued", UpdatedAt: now},
		{RunID: 2, Repo: "org/a", Status: "queued", UpdatedAt: now},
		{RunID: 3, Repo: "org/a", Status: "in_progress", UpdatedAt: now},
		{RunID: 4, Repo: "org/a", Status: "completed", Conclusion: "success", UpdatedAt: now},
	}
	for _, r := range runs {
		if err := s.UpsertWorkflowRun(ctx, r); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	if err := s.RecordQueueDepthSnapshot(ctx, QueueDepthSnapshot{
		Queued: 99, InProgress: 99, CapturedAt: now,
	}); err != nil {
		t.Fatalf("record queue depth snapshot: %v", err)
	}

	d, err := s.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth: %v", err)
	}
	if d.Queued != 2 {
		t.Errorf("expected 2 queued, got %d", d.Queued)
	}
	if d.InProgress != 1 {
		t.Errorf("expected 1 in progress, got %d", d.InProgress)
	}

	// Run 1 transitions from queued -> in_progress: queue depth should shift.
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/a", Status: "in_progress", UpdatedAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	d, err = s.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth: %v", err)
	}
	if d.Queued != 1 || d.InProgress != 2 {
		t.Errorf("expected queued=1 in_progress=2, got %+v", d)
	}
}

func TestCloseStaleActiveRuns(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	for _, r := range []WorkflowRun{
		{RunID: 1, Repo: "org/a", Name: "CI", Status: "in_progress", UpdatedAt: now.Add(-time.Minute)},
		{RunID: 2, Repo: "org/a", Name: "Deploy", Status: "queued", UpdatedAt: now.Add(-time.Minute)},
		{RunID: 3, Repo: "org/b", Name: "CI", Status: "in_progress", UpdatedAt: now.Add(-time.Minute)},
	} {
		if err := s.UpsertWorkflowRun(ctx, r); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	completedAt := now.Add(time.Minute)
	if err := s.CloseStaleActiveRuns(ctx, []string{"org/a"}, map[int64]struct{}{2: {}}, completedAt); err != nil {
		t.Fatalf("CloseStaleActiveRuns: %v", err)
	}

	runs, total, err := s.RecentRuns(ctx, RecentRunsOptions{Limit: 10, SortBy: "updated_at", Desc: true})
	if err != nil {
		t.Fatalf("RecentRuns: %v", err)
	}
	if total != 3 {
		t.Fatalf("expected 3 latest runs, got %d", total)
	}
	byID := map[int64]WorkflowRun{}
	for _, r := range runs {
		byID[r.RunID] = r
	}
	if byID[1].Status != "unknown" || !byID[1].UpdatedAt.Equal(completedAt) {
		t.Fatalf("expected org/a run 1 to be marked unknown at %v, got %+v", completedAt, byID[1])
	}
	if byID[2].Status != "queued" {
		t.Fatalf("expected still-active run 2 to stay queued, got %+v", byID[2])
	}
	if byID[3].Status != "in_progress" {
		t.Fatalf("expected unscanned org/b run 3 to stay in_progress, got %+v", byID[3])
	}
}

func TestCloseStaleActiveRuns_NoReposIsNoop(t *testing.T) {
	s := newTestStore(t)
	if err := s.CloseStaleActiveRuns(context.Background(), nil, nil, time.Now()); err != nil {
		t.Fatalf("CloseStaleActiveRuns: %v", err)
	}
}

func TestCloseStaleActiveRun_DoesNotOverwriteConcurrentCompletion(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/a", Name: "CI", Status: "in_progress", UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert active: %v", err)
	}
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/a", Name: "CI", Status: "completed", Conclusion: "success", UpdatedAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("upsert completion: %v", err)
	}

	if err := s.closeStaleActiveRun(ctx, 1, now.Add(2*time.Second)); err != nil {
		t.Fatalf("closeStaleActiveRun: %v", err)
	}

	runs, total, err := s.RecentRuns(ctx, RecentRunsOptions{Limit: 10, SortBy: "updated_at", Desc: true})
	if err != nil {
		t.Fatalf("RecentRuns: %v", err)
	}
	if total != 1 || len(runs) != 1 {
		t.Fatalf("expected one latest run, got total=%d runs=%+v", total, runs)
	}
	if runs[0].Status != "completed" || runs[0].Conclusion != "success" {
		t.Fatalf("completion should remain latest state, got %+v", runs[0])
	}
}

func TestQueueDepth_SkippedWithStaleStatusNotCountedActive(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// GitHub's ?status=queued / ?status=in_progress list filters can return
	// runs whose status field still reads queued/in_progress while a terminal
	// conclusion (e.g. skipped) is already set. These must not be counted as
	// active.
	runs := []WorkflowRun{
		{RunID: 1, Repo: "org/a", Status: "queued", Conclusion: "skipped", Source: "poll", UpdatedAt: now},
		{RunID: 2, Repo: "org/a", Status: "in_progress", Conclusion: "skipped", Source: "poll", UpdatedAt: now},
		{RunID: 3, Repo: "org/a", Status: "queued", UpdatedAt: now},
	}
	for _, r := range runs {
		if err := s.UpsertWorkflowRun(ctx, r); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	d, err := s.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth: %v", err)
	}
	if d.Queued != 1 {
		t.Errorf("expected 1 queued (only the genuinely-queued run), got %d", d.Queued)
	}
	if d.InProgress != 0 {
		t.Errorf("expected 0 in progress, got %d", d.InProgress)
	}
}

func TestQueueDepth_Empty(t *testing.T) {
	s := newTestStore(t)
	d, err := s.QueueDepth(context.Background())
	if err != nil {
		t.Fatalf("QueueDepth: %v", err)
	}
	if d.Queued != 0 || d.InProgress != 0 {
		t.Errorf("expected zero queue depth on empty store, got %+v", d)
	}
}

func TestRecentOutcomes(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	since := now.Add(-time.Hour)

	runs := []WorkflowRun{
		{RunID: 1, Repo: "org/a", Status: "completed", Conclusion: "success", UpdatedAt: now},
		{RunID: 2, Repo: "org/a", Status: "completed", Conclusion: "success", UpdatedAt: now},
		{RunID: 3, Repo: "org/a", Status: "completed", Conclusion: "failure", UpdatedAt: now},
		{RunID: 4, Repo: "org/a", Status: "queued", UpdatedAt: now},                                               // not completed, excluded
		{RunID: 5, Repo: "org/a", Status: "completed", Conclusion: "success", UpdatedAt: now.Add(-2 * time.Hour)}, // too old
		{RunID: 6, Repo: "org/a", Status: "completed", UpdatedAt: now},                                            // unknown conclusion, excluded
	}
	for _, r := range runs {
		if err := s.UpsertWorkflowRun(ctx, r); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	out, err := s.RecentOutcomes(ctx, since)
	if err != nil {
		t.Fatalf("RecentOutcomes: %v", err)
	}
	if out["success"] != 2 {
		t.Errorf("expected 2 successes, got %d", out["success"])
	}
	if out["failure"] != 1 {
		t.Errorf("expected 1 failure, got %d", out["failure"])
	}
}

func TestRecentOutcomes_DedupesRepeatedPollsOfSameRun(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	since := now.Add(-time.Hour)

	// Simulate the same run being re-recorded across multiple poll/refresh
	// sweeps (e.g. force-refresh clicked repeatedly): each sweep inserts a
	// new row for the same run_id, but it must only be counted once.
	for i := 0; i < 3; i++ {
		if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
			RunID: 1, Repo: "org/a", Status: "completed", Conclusion: "failure",
			UpdatedAt: now.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	out, err := s.RecentOutcomes(ctx, since)
	if err != nil {
		t.Fatalf("RecentOutcomes: %v", err)
	}
	if out["failure"] != 1 {
		t.Errorf("expected exactly 1 failure despite 3 repeated polls of the same run, got %d", out["failure"])
	}
}

func TestCompletedRunOutcomes(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	since := now.Add(-time.Hour)

	runs := []WorkflowRun{
		{RunID: 1, Repo: "org/a", Status: "completed", Conclusion: "success", UpdatedAt: now.Add(-30 * time.Minute)},
		{RunID: 2, Repo: "org/a", Status: "completed", Conclusion: "failure", UpdatedAt: now},
		{RunID: 3, Repo: "org/a", Status: "queued", UpdatedAt: now},                                               // not completed, excluded
		{RunID: 4, Repo: "org/a", Status: "completed", Conclusion: "success", UpdatedAt: now.Add(-2 * time.Hour)}, // too old
	}
	for _, r := range runs {
		if err := s.UpsertWorkflowRun(ctx, r); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	out, err := s.CompletedRunOutcomes(ctx, since)
	if err != nil {
		t.Fatalf("CompletedRunOutcomes: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 outcomes, got %d: %+v", len(out), out)
	}
	if out[0].Conclusion != "success" || out[1].Conclusion != "failure" {
		t.Errorf("expected [success, failure] ordered oldest to newest, got %+v", out)
	}
}

func TestCompletedRunOutcomes_DedupesRepeatedPollsOfSameRun(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	since := now.Add(-time.Hour)

	for i := 0; i < 3; i++ {
		if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
			RunID: 1, Repo: "org/a", Status: "completed", Conclusion: "failure",
			UpdatedAt: now.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	out, err := s.CompletedRunOutcomes(ctx, since)
	if err != nil {
		t.Fatalf("CompletedRunOutcomes: %v", err)
	}
	if len(out) != 1 {
		t.Errorf("expected exactly 1 outcome despite 3 repeated polls of the same run, got %d", len(out))
	}
}

func TestRecentRuns(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	for i := int64(1); i <= 5; i++ {
		if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
			RunID: i, Repo: "org/a", Name: "CI", Status: "completed", Conclusion: "success",
			UpdatedAt: now.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	runs, total, err := s.RecentRuns(ctx, RecentRunsOptions{Limit: 3, Desc: true})
	if err != nil {
		t.Fatalf("RecentRuns: %v", err)
	}
	if total != 5 {
		t.Errorf("expected total=5, got %d", total)
	}
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(runs))
	}
	// Most recently updated (run_id 5) should come first.
	if runs[0].RunID != 5 {
		t.Errorf("expected most recent run first (run_id=5), got %d", runs[0].RunID)
	}
}

func TestRecentRuns_DedupesRepeatedPollsOfSameRun(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Same run re-recorded across multiple poll sweeps must appear once,
	// ordered by its own updated_at, not insertion order.
	for i := 0; i < 3; i++ {
		if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
			RunID: 1, Repo: "org/a", Status: "completed", Conclusion: "success",
			UpdatedAt: now,
		}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 2, Repo: "org/b", Status: "completed", Conclusion: "success",
		UpdatedAt: now.Add(-time.Hour), // older, but inserted last
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	runs, total, err := s.RecentRuns(ctx, RecentRunsOptions{Limit: 10, Desc: true})
	if err != nil {
		t.Fatalf("RecentRuns: %v", err)
	}
	if total != 2 {
		t.Fatalf("expected 2 distinct runs, got %d", total)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs returned, got %d", len(runs))
	}
	if runs[0].RunID != 1 {
		t.Errorf("expected run 1 (more recently updated) first, got %+v", runs)
	}
}

func TestRecentRuns_Pagination(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	for i := int64(1); i <= 5; i++ {
		if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
			RunID: i, Repo: "org/a", Status: "completed", Conclusion: "success",
			UpdatedAt: now.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	page1, total, err := s.RecentRuns(ctx, RecentRunsOptions{Limit: 2, Offset: 0, Desc: true})
	if err != nil {
		t.Fatalf("RecentRuns page1: %v", err)
	}
	if total != 5 {
		t.Errorf("expected total=5, got %d", total)
	}
	if len(page1) != 2 || page1[0].RunID != 5 || page1[1].RunID != 4 {
		t.Fatalf("unexpected page1: %+v", page1)
	}

	page2, _, err := s.RecentRuns(ctx, RecentRunsOptions{Limit: 2, Offset: 2, Desc: true})
	if err != nil {
		t.Fatalf("RecentRuns page2: %v", err)
	}
	if len(page2) != 2 || page2[0].RunID != 3 || page2[1].RunID != 2 {
		t.Fatalf("unexpected page2: %+v", page2)
	}
}

func TestRecentRuns_SortByRepoAscending(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	repos := []string{"zeta", "alpha", "mu"}
	for i, repo := range repos {
		if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
			RunID: int64(i + 1), Repo: repo, Status: "completed", Conclusion: "success", UpdatedAt: now,
		}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	runs, _, err := s.RecentRuns(ctx, RecentRunsOptions{Limit: 10, SortBy: "repo", Desc: false})
	if err != nil {
		t.Fatalf("RecentRuns: %v", err)
	}
	if len(runs) != 3 || runs[0].Repo != "alpha" || runs[1].Repo != "mu" || runs[2].Repo != "zeta" {
		t.Fatalf("expected alphabetical repo order, got %+v", runs)
	}
}

func TestRecentRuns_StatusFilter(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	cases := []WorkflowRun{
		{RunID: 1, Repo: "org/a", Status: "queued", UpdatedAt: now.Add(time.Minute)},
		{RunID: 2, Repo: "org/b", Status: "in_progress", UpdatedAt: now.Add(2 * time.Minute)},
		{RunID: 3, Repo: "org/c", Status: "completed", Conclusion: "success", UpdatedAt: now.Add(3 * time.Minute)},
	}
	for _, r := range cases {
		if err := s.UpsertWorkflowRun(ctx, r); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	runs, total, err := s.RecentRuns(ctx, RecentRunsOptions{Limit: 10, Status: "queued"})
	if err != nil {
		t.Fatalf("RecentRuns: %v", err)
	}
	if total != 1 || len(runs) != 1 || runs[0].Status != "queued" {
		t.Fatalf("expected only queued run, total=%d runs=%+v", total, runs)
	}
}

func TestRecentRuns_UnknownSortFallsBackToUpdatedAt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{RunID: 1, Repo: "org/a", Status: "completed", UpdatedAt: now}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	if _, _, err := s.RecentRuns(ctx, RecentRunsOptions{Limit: 10, SortBy: "'; DROP TABLE workflow_runs; --"}); err != nil {
		t.Fatalf("expected unknown sort column to fall back safely, got error: %v", err)
	}
}

func TestRunnerSnapshots(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := s.RecordRunnerSnapshot(ctx, RunnerSnapshot{GroupName: "default", Total: 10, Busy: 3, Idle: 7, CapturedAt: now}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := s.RecordRunnerSnapshot(ctx, RunnerSnapshot{GroupName: "default", Total: 10, Busy: 5, Idle: 5, CapturedAt: now.Add(time.Minute)}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := s.RecordRunnerSnapshot(ctx, RunnerSnapshot{GroupName: "gpu", Total: 2, Busy: 0, Idle: 2, CapturedAt: now}); err != nil {
		t.Fatalf("record: %v", err)
	}

	snaps, err := s.LatestRunnerSnapshots(ctx)
	if err != nil {
		t.Fatalf("LatestRunnerSnapshots: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(snaps))
	}
	byGroup := map[string]RunnerSnapshot{}
	for _, s := range snaps {
		byGroup[s.GroupName] = s
	}
	if byGroup["default"].Busy != 5 {
		t.Errorf("expected latest 'default' snapshot busy=5, got %d", byGroup["default"].Busy)
	}
	if byGroup["gpu"].Total != 2 {
		t.Errorf("expected 'gpu' total=2, got %d", byGroup["gpu"].Total)
	}
}

func TestQueueDepthHistory_ReturnsOrderedSnapshotsSinceCutoff(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := s.RecordQueueDepthSnapshot(ctx, QueueDepthSnapshot{Queued: 5, InProgress: 2, CapturedAt: now.Add(-3 * time.Hour)}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := s.RecordQueueDepthSnapshot(ctx, QueueDepthSnapshot{Queued: 3, InProgress: 4, CapturedAt: now.Add(-time.Hour)}); err != nil {
		t.Fatalf("record: %v", err)
	}
	// Older than the lookback window; should be excluded.
	if err := s.RecordQueueDepthSnapshot(ctx, QueueDepthSnapshot{Queued: 99, InProgress: 99, CapturedAt: now.Add(-48 * time.Hour)}); err != nil {
		t.Fatalf("record: %v", err)
	}

	history, err := s.QueueDepthHistory(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("QueueDepthHistory: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 snapshots within window, got %d: %+v", len(history), history)
	}
	// Oldest first.
	if history[0].Queued != 5 || history[1].Queued != 3 {
		t.Errorf("expected snapshots ordered oldest-first, got %+v", history)
	}
}

func TestRecordQueueDepthSnapshot_CoalescesWithinMinute(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2024, 1, 1, 12, 0, 10, 0, time.UTC)

	for _, snap := range []QueueDepthSnapshot{
		{Queued: 5, InProgress: 2, CapturedAt: base},
		{Queued: 1, InProgress: 4, CapturedAt: base.Add(40 * time.Second)},
	} {
		if err := s.RecordQueueDepthSnapshot(ctx, snap); err != nil {
			t.Fatalf("record: %v", err)
		}
	}

	history, err := s.QueueDepthHistory(ctx, base.Add(-time.Minute))
	if err != nil {
		t.Fatalf("QueueDepthHistory: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("expected one coalesced point, got %+v", history)
	}
	if history[0].Queued != 1 || history[0].InProgress != 4 {
		t.Fatalf("expected latest bucket values, got %+v", history[0])
	}
	if !history[0].CapturedAt.Equal(base.Truncate(time.Minute)) {
		t.Fatalf("expected minute bucket timestamp, got %v", history[0].CapturedAt)
	}
}

func TestQueueDepthHistory_EmptyWhenNoSnapshots(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	history, err := s.QueueDepthHistory(ctx, time.Now().UTC().Add(-time.Hour))
	if err != nil {
		t.Fatalf("QueueDepthHistory: %v", err)
	}
	if len(history) != 0 {
		t.Errorf("expected no snapshots, got %+v", history)
	}
}

func TestZombieRuns_ReturnsStaleQueuedAndInProgress(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Stale queued run: last updated 2 hours ago.
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/a", Name: "CI", Status: "queued",
		UpdatedAt: now.Add(-2 * time.Hour),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// Stale in_progress run: last updated 3 hours ago.
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 2, Repo: "org/b", Name: "CI", Status: "in_progress",
		UpdatedAt: now.Add(-3 * time.Hour),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// Fresh queued run: updated a minute ago, should NOT be a zombie.
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 3, Repo: "org/c", Name: "CI", Status: "queued",
		UpdatedAt: now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// Stale but completed run: should NOT be a zombie regardless of age.
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 4, Repo: "org/d", Name: "CI", Status: "completed", Conclusion: "success",
		UpdatedAt: now.Add(-24 * time.Hour),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	runs, err := s.ZombieRuns(ctx, 60*time.Minute, now)
	if err != nil {
		t.Fatalf("ZombieRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 zombie runs, got %d: %+v", len(runs), runs)
	}
	// Oldest (most stale) first.
	if runs[0].RunID != 2 || runs[1].RunID != 1 {
		t.Errorf("expected zombie runs ordered oldest-first (run 2, then run 1), got %+v", runs)
	}
}

func TestZombieRuns_EmptyWhenNoneStale(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/a", Name: "CI", Status: "queued",
		UpdatedAt: now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	runs, err := s.ZombieRuns(ctx, 60*time.Minute, now)
	if err != nil {
		t.Fatalf("ZombieRuns: %v", err)
	}
	if len(runs) != 0 {
		t.Errorf("expected no zombie runs, got %+v", runs)
	}
}

func TestZombieRuns_UsesLatestStatePerRun(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Run started queued 2 hours ago (stale), but has since completed.
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/a", Name: "CI", Status: "queued",
		UpdatedAt: now.Add(-2 * time.Hour),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/a", Name: "CI", Status: "completed", Conclusion: "success",
		UpdatedAt: now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	runs, err := s.ZombieRuns(ctx, 60*time.Minute, now)
	if err != nil {
		t.Fatalf("ZombieRuns: %v", err)
	}
	if len(runs) != 0 {
		t.Errorf("expected no zombie runs (run completed since), got %+v", runs)
	}
}

func TestRecentlyActiveRepos_ReturnsReposUpdatedSinceCutoff(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/recent", Name: "CI", Status: "completed", Conclusion: "success",
		UpdatedAt: now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 2, Repo: "org/stale", Name: "CI", Status: "completed", Conclusion: "success",
		UpdatedAt: now.Add(-48 * time.Hour),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	repos, err := s.RecentlyActiveRepos(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("RecentlyActiveRepos: %v", err)
	}
	if len(repos) != 1 || repos[0] != "org/recent" {
		t.Fatalf("expected only org/recent, got %+v", repos)
	}
}

func TestRecentlyActiveRepos_DeduplicatesMultipleRunsPerRepo(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/busy", Name: "CI", Status: "queued",
		UpdatedAt: now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 2, Repo: "org/busy", Name: "CI", Status: "in_progress",
		UpdatedAt: now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	repos, err := s.RecentlyActiveRepos(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("RecentlyActiveRepos: %v", err)
	}
	if len(repos) != 1 || repos[0] != "org/busy" {
		t.Fatalf("expected repo listed once despite multiple runs, got %+v", repos)
	}
}

func TestRecentlyActiveRepos_EmptyWhenNoneRecent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/stale", Name: "CI", Status: "completed", Conclusion: "success",
		UpdatedAt: now.Add(-48 * time.Hour),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	repos, err := s.RecentlyActiveRepos(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("RecentlyActiveRepos: %v", err)
	}
	if len(repos) != 0 {
		t.Errorf("expected no recently active repos, got %+v", repos)
	}
}
