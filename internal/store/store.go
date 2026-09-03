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
	"strings"
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
// maxOpenConns bounds the connection pool for file-backed databases (ignored
// for ":memory:", which always uses exactly one connection); pass <= 0 to
// use the default of 8.
func Open(path string, maxOpenConns int) (*Store, error) {
	db, err := sql.Open("sqlite", path+
		"?_pragma=busy_timeout(5000)"+
		"&_pragma=journal_mode(WAL)"+
		// NORMAL is safe (and standard practice) in WAL mode: only a
		// checkpoint can lose the last few commits on power loss, not
		// corrupt the database, and it avoids an fsync on every commit.
		"&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if path == ":memory:" {
		// An in-memory database only exists within the connection that
		// created it, so a pool of more than one connection would each
		// see an independent, empty database. Ephemeral/test use stays
		// on a single connection; real (file-backed) use gets the pool
		// below.
		db.SetMaxOpenConns(1)
	} else {
		if maxOpenConns <= 0 {
			maxOpenConns = 8
		}
		// WAL mode allows any number of concurrent readers alongside a
		// single writer, so — unlike the previous single-connection
		// pool — read-heavy dashboard queries no longer need to queue
		// behind (or block) webhook/poller writes. SQLite itself still
		// serializes the one writer at a time; busy_timeout above
		// handles that contention instead of failing with "database is
		// locked".
		db.SetMaxOpenConns(maxOpenConns)
	}

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
-- Covers "latest row per run_id" lookups (SELECT MAX(id) ... WHERE run_id = ?),
-- used by nearly every query in this package. Without it, that correlated
-- subquery falls back to a full table scan per run_id as workflow_runs grows
-- (it's append-only), which serializes badly behind the single writer
-- connection and can starve webhook upserts until they hit their context
-- deadline.
CREATE INDEX IF NOT EXISTS idx_workflow_runs_run_id_id ON workflow_runs(run_id, id);

CREATE TABLE IF NOT EXISTS runner_snapshots (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	group_name TEXT NOT NULL,
	total INTEGER NOT NULL,
	busy INTEGER NOT NULL,
	idle INTEGER NOT NULL,
	captured_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runner_snapshots_captured_at ON runner_snapshots(captured_at);

CREATE TABLE IF NOT EXISTS queue_depth_snapshots (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	queued INTEGER NOT NULL,
	in_progress INTEGER NOT NULL,
	captured_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_queue_depth_snapshots_captured_at ON queue_depth_snapshots(captured_at);
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

// CloseStaleActiveRuns marks latest queued/in_progress runs with an
// "unknown" status when a successful active-run sweep no longer sees them.
// This keeps queue depth accurate when a webhook completion delivery was
// missed, without fetching every completed run on a schedule. The run is
// definitely no longer active on GitHub's side, but its true conclusion is
// unknown because the completion webhook was never received and this
// package intentionally avoids extra API calls to look it up.
func (s *Store) CloseStaleActiveRuns(ctx context.Context, repos []string, activeRunIDs map[int64]struct{}, completedAt time.Time) error {
	if len(repos) == 0 {
		return nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(repos)), ",")
	args := make([]any, 0, len(repos))
	for _, repo := range repos {
		args = append(args, repo)
	}
	query := fmt.Sprintf(`
		SELECT run_id, repo, name, event, head_branch
		FROM workflow_runs w1
		WHERE w1.id = (
			SELECT MAX(w2.id) FROM workflow_runs w2 WHERE w2.run_id = w1.run_id
		)
		AND w1.status IN ('queued', 'in_progress')
		AND w1.repo IN (%s)`, placeholders)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query stale active runs: %w", err)
	}
	defer rows.Close()

	var stale []WorkflowRun
	for rows.Next() {
		var r WorkflowRun
		if err := rows.Scan(&r.RunID, &r.Repo, &r.Name, &r.Event, &r.HeadBranch); err != nil {
			return fmt.Errorf("scan stale active run: %w", err)
		}
		if _, stillActive := activeRunIDs[r.RunID]; stillActive {
			continue
		}
		r.Status = "unknown"
		r.Source = "poll"
		r.UpdatedAt = completedAt
		stale = append(stale, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate stale active runs: %w", err)
	}
	for _, r := range stale {
		if err := s.closeStaleActiveRun(ctx, r.RunID, completedAt); err != nil {
			return fmt.Errorf("close stale active run %d: %w", r.RunID, err)
		}
	}
	return nil
}

func (s *Store) closeStaleActiveRun(ctx context.Context, runID int64, completedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO workflow_runs (run_id, repo, name, status, conclusion, event, head_branch, source, updated_at)
		SELECT run_id, repo, name, 'unknown', '', event, head_branch, 'poll', ?
		FROM workflow_runs w1
		WHERE w1.run_id = ?
		AND w1.id = (
			SELECT MAX(w2.id) FROM workflow_runs w2 WHERE w2.run_id = w1.run_id
		)
		AND w1.status IN ('queued', 'in_progress')`,
		completedAt, runID)
	if err != nil {
		return fmt.Errorf("insert unknown-status workflow run: %w", err)
	}
	return nil
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

// RecentlyActiveRepos returns the distinct repos (in "org/repo" form) that
// have had at least one workflow_run recorded (of any status — the webhook
// handler writes a row for every state transition) with an updated_at at or
// after since. The poller uses this to target its per-cycle REST spot-check
// sweep at only the repos webhooks say have had recent job activity, instead
// of every repo in the org, since webhooks are the primary source of truth
// and REST polling exists only as a backstop for what they might have
// missed (e.g. a dropped completion event turning a run into a zombie).
func (s *Store) RecentlyActiveRepos(ctx context.Context, since time.Time) ([]string, error) {
	const q = `SELECT DISTINCT repo FROM workflow_runs WHERE updated_at >= ?`
	rows, err := s.db.QueryContext(ctx, q, since)
	if err != nil {
		return nil, fmt.Errorf("query recently active repos: %w", err)
	}
	defer rows.Close()

	var repos []string
	for rows.Next() {
		var repo string
		if err := rows.Scan(&repo); err != nil {
			return nil, fmt.Errorf("scan recently active repo: %w", err)
		}
		repos = append(repos, repo)
	}
	return repos, rows.Err()
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
		WHERE latest.status = 'completed' AND latest.conclusion <> '' AND latest.updated_at >= ?
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

// CompletedRunOutcome is a single completed workflow run's terminal
// conclusion and completion time, used to build a failure-rate trend over
// time (bucketed client-side to match the selected dashboard range).
type CompletedRunOutcome struct {
	Conclusion  string
	CompletedAt time.Time
}

// CompletedRunOutcomes returns each distinct run's terminal conclusion and
// completion time for runs completed since the given time, ordered oldest
// to newest. Like RecentOutcomes, this dedupes by run_id (using each run's
// most recently recorded state) so repeated polls of the same run aren't
// double-counted.
func (s *Store) CompletedRunOutcomes(ctx context.Context, since time.Time) ([]CompletedRunOutcome, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT conclusion, updated_at FROM (
			SELECT run_id, conclusion, status, updated_at FROM workflow_runs w1
			WHERE w1.id = (
				SELECT MAX(w2.id) FROM workflow_runs w2 WHERE w2.run_id = w1.run_id
			)
		) latest
		WHERE latest.status = 'completed' AND latest.conclusion <> '' AND latest.updated_at >= ?
		ORDER BY latest.updated_at ASC`, since)
	if err != nil {
		return nil, fmt.Errorf("query completed run outcomes: %w", err)
	}
	defer rows.Close()

	var out []CompletedRunOutcome
	for rows.Next() {
		var o CompletedRunOutcome
		if err := rows.Scan(&o.Conclusion, &o.CompletedAt); err != nil {
			return nil, fmt.Errorf("scan completed run outcome: %w", err)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// RecentRunsOptions controls pagination and sorting for RecentRuns.
type RecentRunsOptions struct {
	Limit  int    // max rows to return (defaults applied by caller)
	Offset int    // rows to skip, for pagination
	SortBy string // one of: updated_at, repo, name, status, conclusion (defaults to updated_at)
	Desc   bool   // sort direction; defaults to true (newest/Z-A first)
	Status string // optional exact status filter: queued, in_progress, completed
}

// recentRunsSortColumns whitelists the columns RecentRuns may sort by, to
// avoid building SQL from unvalidated user input.
var recentRunsSortColumns = map[string]string{
	"updated_at": "updated_at",
	"repo":       "repo",
	"name":       "name",
	"status":     "status",
	"conclusion": "conclusion",
}

var recentRunsStatuses = map[string]bool{
	"queued":      true,
	"in_progress": true,
	"completed":   true,
}

func statusFilterSQL(status string) string {
	if status == "" {
		return ""
	}
	return " AND w1.status = ?"
}

// RecentRuns returns the most recently updated workflow run states, paginated
// and sorted per opts, plus the total distinct-run count (for computing
// page counts). Dedupes to each run's latest recorded state (by run_id) —
// otherwise repeated polls of the same run would appear multiple times.
func (s *Store) RecentRuns(ctx context.Context, opts RecentRunsOptions) ([]WorkflowRun, int, error) {
	col, ok := recentRunsSortColumns[opts.SortBy]
	if !ok {
		col = "updated_at"
	}
	dir := "DESC"
	if !opts.Desc {
		dir = "ASC"
	}
	status := ""
	if recentRunsStatuses[opts.Status] {
		status = opts.Status
	}

	var total int
	totalQuery := `
		SELECT COUNT(*) FROM (
			SELECT run_id, status FROM workflow_runs w1
			WHERE w1.id = (
				SELECT MAX(w2.id) FROM workflow_runs w2 WHERE w2.run_id = w1.run_id
			)
		) latest`
	var totalArgs []any
	if status != "" {
		totalQuery += ` WHERE latest.status = ?`
		totalArgs = append(totalArgs, status)
	}
	if err := s.db.QueryRowContext(ctx, totalQuery, totalArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count recent runs: %w", err)
	}

	// col/dir are whitelisted above, safe to interpolate.
	query := fmt.Sprintf(`
		SELECT run_id, repo, name, status, conclusion, event, head_branch, source, updated_at
		FROM workflow_runs w1
		WHERE w1.id = (
			SELECT MAX(w2.id) FROM workflow_runs w2 WHERE w2.run_id = w1.run_id
		)%s
		ORDER BY %s %s, id DESC
		LIMIT ? OFFSET ?`, statusFilterSQL(status), col, dir)
	args := []any{}
	if status != "" {
		args = append(args, status)
	}
	args = append(args, opts.Limit, opts.Offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query recent runs: %w", err)
	}
	defer rows.Close()

	var out []WorkflowRun
	for rows.Next() {
		var r WorkflowRun
		if err := rows.Scan(&r.RunID, &r.Repo, &r.Name, &r.Status, &r.Conclusion, &r.Event, &r.HeadBranch, &r.Source, &r.UpdatedAt); err != nil {
			return nil, 0, fmt.Errorf("scan recent run row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

// ZombieRuns returns workflow runs whose most recent recorded state is
// still "queued" or "in_progress" but hasn't been updated in over
// staleAfter — i.e. runs that are likely stuck (no runner picked them up,
// or a job hung and GitHub never sent a completion event). Ordered oldest
// (most stale) first.
func (s *Store) ZombieRuns(ctx context.Context, staleAfter time.Duration, now time.Time) ([]WorkflowRun, error) {
	cutoff := now.Add(-staleAfter)
	const q = `
		SELECT run_id, repo, name, status, conclusion, event, head_branch, source, updated_at
		FROM workflow_runs w1
		WHERE w1.id = (
			SELECT MAX(w2.id) FROM workflow_runs w2 WHERE w2.run_id = w1.run_id
		)
		AND status IN ('queued', 'in_progress')
		AND updated_at <= ?
		ORDER BY updated_at ASC`
	rows, err := s.db.QueryContext(ctx, q, cutoff)
	if err != nil {
		return nil, fmt.Errorf("query zombie runs: %w", err)
	}
	defer rows.Close()

	var out []WorkflowRun
	for rows.Next() {
		var r WorkflowRun
		if err := rows.Scan(&r.RunID, &r.Repo, &r.Name, &r.Status, &r.Conclusion, &r.Event, &r.HeadBranch, &r.Source, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan zombie run row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// QueueDepthSnapshot is a point-in-time reading of org-wide queued vs
// in-progress workflow run counts, recorded periodically so the dashboard
// can render a queued-vs-running time series.
type QueueDepthSnapshot struct {
	Queued     int
	InProgress int
	CapturedAt time.Time
}

// RecordQueueDepthSnapshot stores a queue depth reading for the time series.
// Multiple readings in the same UTC minute replace one another so webhook
// bursts do not create noisy duplicate points.
func (s *Store) RecordQueueDepthSnapshot(ctx context.Context, snap QueueDepthSnapshot) error {
	bucket := snap.CapturedAt.UTC().Truncate(time.Minute)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin queue depth snapshot transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx, `
		UPDATE queue_depth_snapshots
		SET queued = ?, in_progress = ?
		WHERE captured_at = ?`,
		snap.Queued, snap.InProgress, bucket)
	if err != nil {
		return fmt.Errorf("update queue depth snapshot: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count updated queue depth snapshots: %w", err)
	}
	if updated == 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO queue_depth_snapshots (queued, in_progress, captured_at)
			VALUES (?, ?, ?)`,
			snap.Queued, snap.InProgress, bucket); err != nil {
			return fmt.Errorf("insert queue depth snapshot: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit queue depth snapshot: %w", err)
	}
	return nil
}

// QueueDepthHistory returns queue depth snapshots recorded since the given
// time, ordered oldest to newest, for rendering a time series chart.
func (s *Store) QueueDepthHistory(ctx context.Context, since time.Time) ([]QueueDepthSnapshot, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT queued, in_progress, captured_at FROM queue_depth_snapshots
		WHERE captured_at >= ?
		ORDER BY captured_at ASC`, since)
	if err != nil {
		return nil, fmt.Errorf("query queue depth history: %w", err)
	}
	defer rows.Close()

	var out []QueueDepthSnapshot
	for rows.Next() {
		var snap QueueDepthSnapshot
		if err := rows.Scan(&snap.Queued, &snap.InProgress, &snap.CapturedAt); err != nil {
			return nil, fmt.Errorf("scan queue depth snapshot: %w", err)
		}
		out = append(out, snap)
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
