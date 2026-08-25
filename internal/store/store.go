// Package store provides a lightweight SQLite-backed persistence layer for
// the GitHub Actions Monitor MVP. It records workflow run/job state (from
// the webhook feed and/or API polling) and periodic runner capacity
// snapshots, so the dashboard can show current job queue depth and
// historical trends across restarts/redeploys.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure-Go sqlite driver, no cgo required
)

// Store wraps a SQLite database handle with the monitor's schema and typed
// accessors.
type Store struct {
	db *sql.DB
}

// Open opens (creating if necessary) the SQLite database at path and applies
// the schema migrations. Use ":memory:" for ephemeral/test databases.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite via database/sql performs best with a single writer connection;
	// this avoids "database is locked" errors under concurrent pollers.
	db.SetMaxOpenConns(1)

	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the underlying database handle.
func (s *Store) Close() error {
	return s.db.Close()
}

const schema = `
CREATE TABLE IF NOT EXISTS workflow_runs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id INTEGER NOT NULL,
	repo TEXT NOT NULL,
	name TEXT NOT NULL,
	status TEXT NOT NULL,
	conclusion TEXT NOT NULL DEFAULT '',
	event TEXT NOT NULL DEFAULT '',
	head_branch TEXT NOT NULL DEFAULT '',
	source TEXT NOT NULL DEFAULT 'webhook',
	updated_at DATETIME NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_run_id ON workflow_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_updated_at ON workflow_runs(updated_at);

CREATE TABLE IF NOT EXISTS runner_snapshots (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	group_name TEXT NOT NULL,
	total INTEGER NOT NULL,
	busy INTEGER NOT NULL,
	idle INTEGER NOT NULL,
	captured_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runner_snapshots_captured_at ON runner_snapshots(captured_at);
`

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schema)
	if err != nil {
		return fmt.Errorf("migrate schema: %w", err)
	}
	return nil
}

// WorkflowRun represents a single workflow_run event/state as recorded from
// the webhook feed or (as a fallback) API polling.
type WorkflowRun struct {
	RunID      int64
	Repo       string
	Name       string
	Status     string
	Conclusion string
	Event      string
	HeadBranch string
	Source     string
	UpdatedAt  time.Time
}

// UpsertWorkflowRun records a new state for a workflow run. It always inserts
// a new row so history is preserved; callers query the latest row per run_id
// for "current" state.
func (s *Store) UpsertWorkflowRun(ctx context.Context, r WorkflowRun) error {
	if r.Source == "" {
		r.Source = "webhook"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO workflow_runs (run_id, repo, name, status, conclusion, event, head_branch, source, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.RunID, r.Repo, r.Name, r.Status, r.Conclusion, r.Event, r.HeadBranch, r.Source, r.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert workflow run: %w", err)
	}
	return nil
}

// InFlightCount returns the number of distinct runs currently in a
// non-terminal status ("queued" or "in_progress"), based on each run's most
// recent recorded state.
func (s *Store) InFlightCount(ctx context.Context) (int, error) {
	const q = `
		SELECT COUNT(*) FROM (
			SELECT run_id, status FROM workflow_runs w1
			WHERE w1.id = (
				SELECT MAX(w2.id) FROM workflow_runs w2 WHERE w2.run_id = w1.run_id
			)
		) latest
		WHERE latest.status IN ('queued', 'in_progress')`
	var n int
	if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("count in-flight runs: %w", err)
	}
	return n, nil
}

// QueueDepth reports how many distinct runs are currently queued (waiting
// for a runner) versus already in progress, based on each run's most recent
// recorded state. This is the primary "queue depth" signal for the
// dashboard.
type QueueDepth struct {
	Queued     int
	InProgress int
}

// QueueDepth computes the current queue depth across the org.
func (s *Store) QueueDepth(ctx context.Context) (QueueDepth, error) {
	const q = `
		SELECT latest.status, COUNT(*) FROM (
			SELECT run_id, status FROM workflow_runs w1
			WHERE w1.id = (
				SELECT MAX(w2.id) FROM workflow_runs w2 WHERE w2.run_id = w1.run_id
			)
		) latest
		WHERE latest.status IN ('queued', 'in_progress')
		GROUP BY latest.status`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return QueueDepth{}, fmt.Errorf("query queue depth: %w", err)
	}
	defer rows.Close()

	var d QueueDepth
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return QueueDepth{}, fmt.Errorf("scan queue depth row: %w", err)
		}
		switch status {
		case "queued":
			d.Queued = n
		case "in_progress":
			d.InProgress = n
		}
	}
	return d, rows.Err()
}

// RecentOutcomes returns conclusion counts for runs completed within the
// given window, used to compute success/failure trend rates. Counts
// distinct runs (by run_id, using each run's most recently recorded state)
// rather than raw rows, since polling re-records unchanged runs on every
// sweep and would otherwise inflate the count with duplicates.
func (s *Store) RecentOutcomes(ctx context.Context, since time.Time) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT conclusion, COUNT(*) FROM (
			SELECT run_id, conclusion, status, updated_at FROM workflow_runs w1
			WHERE w1.id = (
				SELECT MAX(w2.id) FROM workflow_runs w2 WHERE w2.run_id = w1.run_id
			)
		) latest
		WHERE latest.status = 'completed' AND latest.updated_at >= ?
		GROUP BY conclusion`, since)
	if err != nil {
		return nil, fmt.Errorf("query recent outcomes: %w", err)
	}
	defer rows.Close()

	out := map[string]int{}
	for rows.Next() {
		var concl string
		var n int
		if err := rows.Scan(&concl, &n); err != nil {
			return nil, fmt.Errorf("scan outcome row: %w", err)
		}
		out[concl] = n
	}
	return out, rows.Err()
}

// RecentRuns returns the most recently updated workflow run states, newest
// first, capped at limit rows. Used to populate a "recent activity" list on
// the dashboard.
func (s *Store) RecentRuns(ctx context.Context, limit int) ([]WorkflowRun, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT run_id, repo, name, status, conclusion, event, head_branch, source, updated_at
		FROM workflow_runs ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("query recent runs: %w", err)
	}
	defer rows.Close()

	var out []WorkflowRun
	for rows.Next() {
		var r WorkflowRun
		if err := rows.Scan(&r.RunID, &r.Repo, &r.Name, &r.Status, &r.Conclusion, &r.Event, &r.HeadBranch, &r.Source, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan recent run row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RunnerSnapshot is a point-in-time reading of a runner group's capacity.
type RunnerSnapshot struct {
	GroupName  string
	Total      int
	Busy       int
	Idle       int
	CapturedAt time.Time
}

// RecordRunnerSnapshot stores a runner capacity reading.
func (s *Store) RecordRunnerSnapshot(ctx context.Context, r RunnerSnapshot) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO runner_snapshots (group_name, total, busy, idle, captured_at)
		VALUES (?, ?, ?, ?, ?)`,
		r.GroupName, r.Total, r.Busy, r.Idle, r.CapturedAt)
	if err != nil {
		return fmt.Errorf("record runner snapshot: %w", err)
	}
	return nil
}

// LatestRunnerSnapshots returns the most recent snapshot for every runner
// group.
func (s *Store) LatestRunnerSnapshots(ctx context.Context) ([]RunnerSnapshot, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT group_name, total, busy, idle, captured_at FROM runner_snapshots r1
		WHERE r1.id = (
			SELECT MAX(r2.id) FROM runner_snapshots r2 WHERE r2.group_name = r1.group_name
		)
		ORDER BY group_name`)
	if err != nil {
		return nil, fmt.Errorf("query latest runner snapshots: %w", err)
	}
	defer rows.Close()

	var out []RunnerSnapshot
	for rows.Next() {
		var r RunnerSnapshot
		if err := rows.Scan(&r.GroupName, &r.Total, &r.Busy, &r.Idle, &r.CapturedAt); err != nil {
			return nil, fmt.Errorf("scan runner snapshot: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
