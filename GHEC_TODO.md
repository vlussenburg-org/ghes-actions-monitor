# GHEC Support TODO

The GitHub Actions Monitor MVP targets **GHES** (GitHub Enterprise Server)
first, scoped to: workflow run/job feed, job queue depth, and runner
capacity. It's built so it also works against **GitHub.com / GHEC** with
only configuration changes — no code branching required for the current
feature set.

## How instance targeting works

- `GHES_BASE_URL` unset, or set to `https://github.com` → app targets GHEC
  (`api.github.com` / `uploads.github.com`).
- `GHES_BASE_URL` set to an appliance URL (e.g. `https://ghes.example.com`)
  → app targets that GHES instance's `/api/v3` and `/api/uploads` paths.
- `Config.IsGHEC` / `Config.RESTBaseURL()` / `Config.UploadBaseURL()` are the
  single source of truth other packages should use — never hardcode
  `api.github.com` or a GHES path elsewhere.

## Status by feature (current MVP scope)

| Area | GHES | GHEC | Notes |
|---|---|---|---|
| REST API base URL resolution | ✅ | ✅ | `internal/config` |
| GitHub App JWT + installation token auth | ✅ | ✅ | Same REST shape on both; uses resolved base URL. |
| Admin PAT auth (MVP default, no OAuth) | ✅ | ✅ | Same endpoints on both. |
| Org webhook delivery format (`workflow_run`) | ✅ | ✅ | Payload shape and `X-Hub-Signature-256` header identical. |
| Workflow runs / jobs API polling | ✅ | ✅ | Same shape; used to backfill state the webhook feed missed. |
| Runner groups / self-hosted runners API | ✅ | ✅ | Same shape. |
| Hosted runners API (`/orgs/{org}/actions/hosted-runners`) | ⚠️ | ✅ | Most GHES appliances don't offer GitHub-hosted runners. **TODO: feature-detect (404 → fall back to self-hosted runner groups only) rather than assuming hosted runners exist.** |

## Deferred / out of scope for this MVP

The original design also covered health probes (status page, git
transport, SSH), incident detection/escalation, Slack alerting, GitHub App
inventory via audit log, and Okta-gated dashboard auth. These were removed
from the current MVP scope to focus on the core signal: **jobs in flight,
queue depth, and runner capacity**. If/when they're reintroduced, apply the
same GHES/GHEC-aware approach:

- Status page probe (`githubstatus.com`) reflects GHEC/github.com health,
  not a GHES appliance's own health — would need a GHES-specific
  complementary probe (e.g. the appliance's own `/api/v3` reachability).
- Git transport probe (`info/refs?service=git-upload-pack`) and SSH banner
  probe target `github.com`/`ssh.github.com` today; for GHES they'd need to
  target the appliance's own git/SSH endpoints instead.
- OAuth device flow (for CoCo/dashboard auth beyond the admin PAT) is
  available at `/login/device/code` on both GHES (relative to
  `GHES_BASE_URL`) and GHEC (`github.com`) and should generalize cleanly.

## Legend
- ✅ Implemented and instance-agnostic (works for both today).
- ⚠️ Implemented for one target only; needs follow-up work for full parity.

Update this table as packages are built out or scope expands.
