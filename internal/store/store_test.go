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

	runs, err := s.RecentRuns(ctx, 3)
	if err != nil {
		t.Fatalf("RecentRuns: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(runs))
	}
	// Most recently inserted (run_id 5) should come first.
	if runs[0].RunID != 5 {
		t.Errorf("expected most recent run first (run_id=5), got %d", runs[0].RunID)
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
