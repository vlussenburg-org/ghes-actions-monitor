// Package store provides a lightweight SQLite-backed persistence layer for
// the GitHub Actions Monitor. It records workflow run/job events (from the
// webhook feed), point-in-time snapshots (runner capacity, cache usage, app
// inventory), health probe results, and incidents, so the dashboard can show
// both "now" and historical trends across restarts/redeploys.
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

CREATE TABLE IF NOT EXISTS app_inventory_snapshots (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	app_id INTEGER NOT NULL,
	app_slug TEXT NOT NULL,
	app_name TEXT NOT NULL,
	installed_by TEXT NOT NULL DEFAULT '',
	repo_selection TEXT NOT NULL DEFAULT '',
	permissions_json TEXT NOT NULL DEFAULT '{}',
	repo_count INTEGER NOT NULL DEFAULT 0,
	captured_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_app_inventory_captured_at ON app_inventory_snapshots(captured_at);

CREATE TABLE IF NOT EXISTS health_checks (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	probe TEXT NOT NULL,
	healthy INTEGER NOT NULL,
	detail TEXT NOT NULL DEFAULT '',
	latency_ms INTEGER NOT NULL DEFAULT 0,
	checked_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_health_checks_checked_at ON health_checks(checked_at);

CREATE TABLE IF NOT EXISTS incidents (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	kind TEXT NOT NULL,
	severity TEXT NOT NULL,
	summary TEXT NOT NULL,
	opened_at DATETIME NOT NULL,
	closed_at DATETIME,
	last_escalated_at DATETIME,
	escalation_level INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_incidents_opened_at ON incidents(opened_at);
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

// RecentOutcomes returns conclusion counts for runs completed within the
// given window, used to compute success/failure trend rates.
func (s *Store) RecentOutcomes(ctx context.Context, since time.Time) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT conclusion, COUNT(*) FROM workflow_runs
		WHERE status = 'completed' AND updated_at >= ?
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

// AppInventoryEntry is a point-in-time reading of an installed GitHub App.
type AppInventoryEntry struct {
	AppID           int64
	AppSlug         string
	AppName         string
	InstalledBy     string
	RepoSelection   string
	PermissionsJSON string
	RepoCount       int
	CapturedAt      time.Time
}

// RecordAppInventory stores a snapshot row for one installed app.
func (s *Store) RecordAppInventory(ctx context.Context, e AppInventoryEntry) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO app_inventory_snapshots
			(app_id, app_slug, app_name, installed_by, repo_selection, permissions_json, repo_count, captured_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		e.AppID, e.AppSlug, e.AppName, e.InstalledBy, e.RepoSelection, e.PermissionsJSON, e.RepoCount, e.CapturedAt)
	if err != nil {
		return fmt.Errorf("record app inventory: %w", err)
	}
	return nil
}

// LatestAppInventory returns the most recent snapshot for every distinct
// installed app.
func (s *Store) LatestAppInventory(ctx context.Context) ([]AppInventoryEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT app_id, app_slug, app_name, installed_by, repo_selection, permissions_json, repo_count, captured_at
		FROM app_inventory_snapshots a1
		WHERE a1.id = (
			SELECT MAX(a2.id) FROM app_inventory_snapshots a2 WHERE a2.app_id = a1.app_id
		)
		ORDER BY app_slug`)
	if err != nil {
		return nil, fmt.Errorf("query latest app inventory: %w", err)
	}
	defer rows.Close()

	var out []AppInventoryEntry
	for rows.Next() {
		var e AppInventoryEntry
		if err := rows.Scan(&e.AppID, &e.AppSlug, &e.AppName, &e.InstalledBy, &e.RepoSelection, &e.PermissionsJSON, &e.RepoCount, &e.CapturedAt); err != nil {
			return nil, fmt.Errorf("scan app inventory row: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// HealthCheck is the result of a single health probe execution.
type HealthCheck struct {
	Probe     string
	Healthy   bool
	Detail    string
	LatencyMS int64
	CheckedAt time.Time
}

// RecordHealthCheck stores a probe result.
func (s *Store) RecordHealthCheck(ctx context.Context, h HealthCheck) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO health_checks (probe, healthy, detail, latency_ms, checked_at)
		VALUES (?, ?, ?, ?, ?)`,
		h.Probe, h.Healthy, h.Detail, h.LatencyMS, h.CheckedAt)
	if err != nil {
		return fmt.Errorf("record health check: %w", err)
	}
	return nil
}

// LatestHealthChecks returns the most recent result for every distinct
// probe name.
func (s *Store) LatestHealthChecks(ctx context.Context) ([]HealthCheck, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT probe, healthy, detail, latency_ms, checked_at FROM health_checks h1
		WHERE h1.id = (
			SELECT MAX(h2.id) FROM health_checks h2 WHERE h2.probe = h1.probe
		)
		ORDER BY probe`)
	if err != nil {
		return nil, fmt.Errorf("query latest health checks: %w", err)
	}
	defer rows.Close()

	var out []HealthCheck
	for rows.Next() {
		var h HealthCheck
		if err := rows.Scan(&h.Probe, &h.Healthy, &h.Detail, &h.LatencyMS, &h.CheckedAt); err != nil {
			return nil, fmt.Errorf("scan health check row: %w", err)
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// Incident represents an open or historical incident record.
type Incident struct {
	ID              int64
	Kind            string
	Severity        string
	Summary         string
	OpenedAt        time.Time
	ClosedAt        sql.NullTime
	LastEscalatedAt sql.NullTime
	EscalationLevel int
}

// OpenIncident creates a new incident record and returns its ID.
func (s *Store) OpenIncident(ctx context.Context, kind, severity, summary string, openedAt time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO incidents (kind, severity, summary, opened_at)
		VALUES (?, ?, ?, ?)`,
		kind, severity, summary, openedAt)
	if err != nil {
		return 0, fmt.Errorf("open incident: %w", err)
	}
	return res.LastInsertId()
}

// CloseIncident marks an incident as resolved.
func (s *Store) CloseIncident(ctx context.Context, id int64, closedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE incidents SET closed_at = ? WHERE id = ?`, closedAt, id)
	if err != nil {
		return fmt.Errorf("close incident: %w", err)
	}
	return nil
}

// EscalateIncident bumps an incident's escalation level and timestamp.
func (s *Store) EscalateIncident(ctx context.Context, id int64, level int, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE incidents SET escalation_level = ?, last_escalated_at = ? WHERE id = ?`,
		level, at, id)
	if err != nil {
		return fmt.Errorf("escalate incident: %w", err)
	}
	return nil
}

// OpenIncidentsByKind returns all currently-open (closed_at IS NULL)
// incidents of the given kind, most recent first.
func (s *Store) OpenIncidentsByKind(ctx context.Context, kind string) ([]Incident, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, kind, severity, summary, opened_at, closed_at, last_escalated_at, escalation_level
		FROM incidents WHERE kind = ? AND closed_at IS NULL ORDER BY opened_at DESC`, kind)
	if err != nil {
		return nil, fmt.Errorf("query open incidents: %w", err)
	}
	defer rows.Close()
	return scanIncidents(rows)
}

// AllOpenIncidents returns every currently-open incident, most recent first.
func (s *Store) AllOpenIncidents(ctx context.Context) ([]Incident, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, kind, severity, summary, opened_at, closed_at, last_escalated_at, escalation_level
		FROM incidents WHERE closed_at IS NULL ORDER BY opened_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("query all open incidents: %w", err)
	}
	defer rows.Close()
	return scanIncidents(rows)
}

func scanIncidents(rows *sql.Rows) ([]Incident, error) {
	var out []Incident
	for rows.Next() {
		var inc Incident
		if err := rows.Scan(&inc.ID, &inc.Kind, &inc.Severity, &inc.Summary, &inc.OpenedAt, &inc.ClosedAt, &inc.LastEscalatedAt, &inc.EscalationLevel); err != nil {
			return nil, fmt.Errorf("scan incident row: %w", err)
		}
		out = append(out, inc)
	}
	return out, rows.Err()
}
