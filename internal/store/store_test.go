package store

import (
	"context"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
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

func TestWorkflowRun_UpsertAndInFlight(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/a", Name: "CI", Status: "queued", UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 2, Repo: "org/b", Name: "CI", Status: "in_progress", UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	n, err := s.InFlightCount(ctx)
	if err != nil {
		t.Fatalf("InFlightCount: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 in-flight runs, got %d", n)
	}

	// Transition run 1 to completed; in-flight should drop to 1.
	if err := s.UpsertWorkflowRun(ctx, WorkflowRun{
		RunID: 1, Repo: "org/a", Name: "CI", Status: "completed", Conclusion: "success", UpdatedAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	n, err = s.InFlightCount(ctx)
	if err != nil {
		t.Fatalf("InFlightCount: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 in-flight run after completion, got %d", n)
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
