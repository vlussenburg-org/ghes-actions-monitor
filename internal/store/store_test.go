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

func TestAppInventory(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := s.RecordAppInventory(ctx, AppInventoryEntry{
		AppID: 1, AppSlug: "dependabot", AppName: "Dependabot", InstalledBy: "alice",
		RepoSelection: "all", PermissionsJSON: `{"contents":"read"}`, RepoCount: 42, CapturedAt: now,
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := s.RecordAppInventory(ctx, AppInventoryEntry{
		AppID: 1, AppSlug: "dependabot", AppName: "Dependabot", InstalledBy: "alice",
		RepoSelection: "all", PermissionsJSON: `{"contents":"read"}`, RepoCount: 50, CapturedAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := s.RecordAppInventory(ctx, AppInventoryEntry{
		AppID: 2, AppSlug: "codeql", AppName: "CodeQL", InstalledBy: "bob",
		RepoSelection: "selected", PermissionsJSON: `{"security_events":"write"}`, RepoCount: 3, CapturedAt: now,
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	entries, err := s.LatestAppInventory(ctx)
	if err != nil {
		t.Fatalf("LatestAppInventory: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 distinct apps, got %d", len(entries))
	}
	byID := map[int64]AppInventoryEntry{}
	for _, e := range entries {
		byID[e.AppID] = e
	}
	if byID[1].RepoCount != 50 {
		t.Errorf("expected dependabot latest repo_count=50, got %d", byID[1].RepoCount)
	}
}

func TestHealthChecks(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := s.RecordHealthCheck(ctx, HealthCheck{Probe: "status_page", Healthy: true, CheckedAt: now}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := s.RecordHealthCheck(ctx, HealthCheck{Probe: "status_page", Healthy: false, Detail: "degraded", CheckedAt: now.Add(time.Minute)}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := s.RecordHealthCheck(ctx, HealthCheck{Probe: "git_transport", Healthy: true, CheckedAt: now}); err != nil {
		t.Fatalf("record: %v", err)
	}

	checks, err := s.LatestHealthChecks(ctx)
	if err != nil {
		t.Fatalf("LatestHealthChecks: %v", err)
	}
	if len(checks) != 2 {
		t.Fatalf("expected 2 distinct probes, got %d", len(checks))
	}
	byProbe := map[string]HealthCheck{}
	for _, c := range checks {
		byProbe[c.Probe] = c
	}
	if byProbe["status_page"].Healthy {
		t.Errorf("expected latest status_page check to be unhealthy")
	}
	if byProbe["status_page"].Detail != "degraded" {
		t.Errorf("expected detail 'degraded', got %q", byProbe["status_page"].Detail)
	}
}

func TestIncidentsLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	id, err := s.OpenIncident(ctx, "job_throughput", "warning", "throughput collapsed", now)
	if err != nil {
		t.Fatalf("OpenIncident: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero incident id")
	}

	open, err := s.OpenIncidentsByKind(ctx, "job_throughput")
	if err != nil {
		t.Fatalf("OpenIncidentsByKind: %v", err)
	}
	if len(open) != 1 {
		t.Fatalf("expected 1 open incident, got %d", len(open))
	}
	if open[0].EscalationLevel != 0 {
		t.Errorf("expected initial escalation level 0, got %d", open[0].EscalationLevel)
	}

	if err := s.EscalateIncident(ctx, id, 1, now.Add(5*time.Minute)); err != nil {
		t.Fatalf("EscalateIncident: %v", err)
	}
	open, err = s.OpenIncidentsByKind(ctx, "job_throughput")
	if err != nil {
		t.Fatalf("OpenIncidentsByKind: %v", err)
	}
	if open[0].EscalationLevel != 1 {
		t.Errorf("expected escalation level 1, got %d", open[0].EscalationLevel)
	}
	if !open[0].LastEscalatedAt.Valid {
		t.Errorf("expected LastEscalatedAt to be set")
	}

	all, err := s.AllOpenIncidents(ctx)
	if err != nil {
		t.Fatalf("AllOpenIncidents: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 open incident overall, got %d", len(all))
	}

	if err := s.CloseIncident(ctx, id, now.Add(10*time.Minute)); err != nil {
		t.Fatalf("CloseIncident: %v", err)
	}
	open, err = s.OpenIncidentsByKind(ctx, "job_throughput")
	if err != nil {
		t.Fatalf("OpenIncidentsByKind: %v", err)
	}
	if len(open) != 0 {
		t.Fatalf("expected 0 open incidents after close, got %d", len(open))
	}
}
