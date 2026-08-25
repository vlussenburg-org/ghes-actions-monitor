# GitHub Actions Monitor

A small always-on web app that gives you a single pane of glass over
GitHub Actions for one organization: jobs in flight, queue depth (queued
vs. in-progress), recent workflow run outcomes, and runner group capacity.

Built for **GHES** (GitHub Enterprise Server) first, and works against
**GitHub.com / GHEC** with configuration only — see [GHEC_TODO.md](GHEC_TODO.md)
for instance-targeting details and known gaps.

## Current scope (MVP)

- Live workflow run feed via an inbound org webhook (`workflow_run` events).
- Periodic API polling as a backstop: sweeps all org repos for active runs
  and polls runner group capacity (busy/idle/total).
- A minimal read-only dashboard and JSON API showing the above.

Explicitly **out of scope** for this MVP (see `GHEC_TODO.md` for notes on
reintroducing them later): health probes, incident detection, Slack
alerting, GitHub App inventory, Okta-gated auth. Auth for the initial
version is a single admin PAT (no OAuth yet).

## How it works

1. **Webhook (push)** — configure an org webhook pointing at
   `POST /webhook/github` for the `workflow_run` event. This is the primary,
   most reliable signal.
2. **Poller (pull)** — on a schedule, calls the GitHub REST API to sweep for
   active workflow runs (backstop for missed webhook deliveries) and to
   snapshot runner group capacity.
3. **Store** — everything is persisted to a local SQLite database
   (pure Go driver, no CGO) so history survives restarts/redeploys. Point
   `DB_PATH` at a mounted volume in production.
4. **Dashboard/API** — a static HTML dashboard (`web/static/index.html`)
   polls the JSON API every 10s to show current status.

## Configuration

All configuration is via environment variables (see `internal/config`):

| Variable | Required | Default | Notes |
|---|---|---|---|
| `GITHUB_ORG` | yes | — | Org to monitor. |
| `GHES_BASE_URL` | no | unset (→ GHEC) | e.g. `https://ghes.example.com`. Leave unset or set to `https://github.com` to target GHEC. |
| `GITHUB_ADMIN_TOKEN` | one of this or App creds | — | Admin PAT, used for all API calls in the MVP. |
| `GITHUB_APP_ID` / `GITHUB_APP_INSTALLATION_ID` / `GITHUB_APP_PRIVATE_KEY` | one of this or admin token | — | GitHub App identity, used instead of the admin PAT for least-privilege polling. |
| `GITHUB_WEBHOOK_SECRET` | no | — | HMAC secret for verifying inbound webhook deliveries. Strongly recommended in production. |
| `DB_PATH` | no | `data/monitor.db` | SQLite database file path. |
| `PORT` | no | `8080` | HTTP listen port. |
| `WORKFLOW_POLL_INTERVAL` | no | `30s` | Any Go duration string. |
| `RUNNER_POLL_INTERVAL` | no | `60s` | Any Go duration string. |

## Running locally

```sh
export GITHUB_ORG=my-org
export GITHUB_ADMIN_TOKEN=ghp_xxx
export GHES_BASE_URL=https://ghes.example.com   # omit for GHEC
go run ./cmd/server
```

Then open http://localhost:8080.

## Running with Docker

```sh
docker build -t ghes-actions-monitor .
docker run -p 8080:8080 \
  -e GITHUB_ORG=my-org \
  -e GITHUB_ADMIN_TOKEN=ghp_xxx \
  -e GHES_BASE_URL=https://ghes.example.com \
  -v monitor-data:/data \
  ghes-actions-monitor
```

The image is a distroless, CGO-free static binary — no OS package
dependencies at runtime. It's designed to also run well as a managed
container (e.g. Google Cloud Run): respects `PORT`, and expects `DB_PATH`
on a persistent volume/mount so history survives redeploys.

## API endpoints

- `GET /healthz` — liveness probe.
- `GET /api/status` — org queue depth, in-flight count, recent (1h) outcomes.
- `GET /api/runs/recent?limit=N` — most recent workflow run states.
- `GET /api/runners` — latest runner group capacity snapshots.
- `POST /webhook/github` — inbound org webhook receiver (`workflow_run`).

## Development

```sh
go build ./...
go vet ./...
gofmt -l .            # should print nothing
go test ./... -cover
```

CI (`.github/workflows/ci.yml`) runs the above plus a coverage gate
(≥80%) and a Docker build on every push/PR.