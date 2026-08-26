# Copilot instructions

## Project overview

This repository is a Go 1.25 service that monitors GitHub Actions for one
organization. `cmd/server/main.go` is the composition root: it loads
environment configuration, creates GHES/GHEC GitHub clients, opens the SQLite
store, starts the background poller, and mounts the API, static dashboard, and
webhook handler on one HTTP server.

The runtime data flow has two complementary inputs:

- `internal/webhook` handles `POST /webhook/github` workflow-run deliveries and
  is the primary near-real-time signal. When configured, it verifies
  `X-Hub-Signature-256` with HMAC-SHA256.
- `internal/poller` periodically spot-checks active workflow runs and
  runner-group capacity through the GitHub REST API — but only for repos
  webhooks have recorded as recently active (`RecentlyActiveRepos`), so the
  scheduled sweep depends on webhook deliveries to know where to look. A
  one-time full-org history bootstrap runs automatically if the store has
  no recently-active repos at all. Historic/full-org workflow-run backfill
  is otherwise only triggered by the synchronous `/api/refresh` action,
  which is webhook-independent and can run the monitor without a webhook
  configured at all (at the cost of needing manual/scheduled triggering).

`internal/store` persists append-only workflow-run states plus queue-depth and
runner-capacity snapshots in SQLite using the pure-Go `modernc.org/sqlite`
driver. Queries derive current state from the latest row for each `run_id`;
do not assume workflow rows are updated in place. SQLite is configured with
WAL mode and one open connection to avoid writer-lock contention.

`internal/api` exposes the JSON endpoints consumed by
`web/static/index.html`. The dashboard is plain HTML/CSS/JavaScript and
polls the API every 10 seconds. API routes use Go `net/http` method/path
patterns and include guarded refresh and workflow cancellation actions.
Dashboard/API authentication is optional: `AUTH_USERNAME`/`AUTH_TOKEN`
(both required together) enable a built-in HTTP Basic Auth check in
`cmd/server`'s `basicAuth` middleware, wrapped around everything except the
webhook receiver (which is authenticated separately via
`GITHUB_WEBHOOK_SECRET`). If unset, there is no auth at all and deployments
must put the service behind an authenticating proxy and restrict direct
network access.

## Build, test, and lint

Run the same checks used by CI:

```sh
gofmt -l .
go build ./...
go vet ./...
go test ./... -cover
```

CI additionally writes an atomic coverage profile, reports coverage, enforces
at least 80% total coverage, and builds the Docker image:

```sh
go test ./... -coverprofile=coverage.out -covermode=atomic
go tool cover -func=coverage.out
docker build -t ghes-actions-monitor:ci .
```

Run one package or one test directly:

```sh
go test ./internal/poller
go test ./internal/poller -run '^TestName$'
go test ./internal/poller -run 'TestName/Subcase'
```

For local runtime testing, load the ignored `.env` symlink without printing or
committing its credentials, and use port 8091:

```sh
set -a; source .env; set +a; PORT=8091 go run ./cmd/server
```

To persist credentials across ephemeral worktrees, keep the real file outside
the repository at `~/.config/ghes-actions-monitor/.env`, set it to mode 600,
and create `.env` in each worktree as a symlink to that file. Never copy
credential values into tracked files; use `.env.example` only as a schema.

The normal application default is port 8080. Docker builds a CGO-free static
binary and runs it as the nonroot user; persist `/data` when using the
default `DB_PATH=/data/monitor.db`.

## Repository-specific conventions

- Keep runtime configuration in environment variables and update
  `internal/config` when adding one. `GITHUB_ORG` and either the admin PAT or
  complete GitHub App credentials are required.
- Treat an unset `GHES_BASE_URL` (or `github.com`) as GHEC. GHES API clients
  must use `/api/v3/` and `/api/uploads/`; preserve this distinction through
  `config.Config` rather than hardcoding URLs in feature packages.
- Prefer the GitHub App client for polling when available. The admin PAT is
  the fallback and is the administrative credential used for cancellation.
  Keep rate-limit tracking attached to the relevant transport and skip entire
  poll sweeps when the shared budget is exhausted.
- Keep package boundaries interface-driven. `api`, `poller`, and `webhook`
  declare the small store/client interfaces they need so tests can use fakes
  without an HTTP server or real GitHub credentials. Preserve injectable
  `Now`/clock hooks when adding time-dependent behavior.
- New webhook event types should be acknowledged with HTTP 200 unless they
  are malformed or fail processing; only `workflow_run` currently changes
  persisted state. Keep the webhook payload limited to fields the monitor
  actually uses.
- Poller failures are logged per sweep/repository/group and should not stop
  later scheduled work. The active-run sweep must close stale active records
  after a successful repository scan so missed completion webhooks do not
  inflate queue depth.
- Validate and constrain API input at the handler boundary. In particular,
  preserve the organization-owner check for cancellation, positive integer
  parsing/caps for pagination and history windows, and SQL sort-column
  whitelisting in the store.
- Add dashboard-facing data through typed store methods and JSON API handlers;
  keep the static UI in `web/static/index.html` unless a frontend build
  system is intentionally introduced.
- Vendor third-party frontend JS under `web/static/vendor/` (served by the
  same static file handler) instead of loading it from a CDN at runtime, so
  the dashboard keeps working without outbound internet access.
