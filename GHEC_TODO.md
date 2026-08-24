# GHEC Support TODO

The GitHub Actions Monitor is designed primarily for **GHES** (GitHub
Enterprise Server) but is built to also work against **GitHub.com / GHEC**
with minimal configuration changes. This file tracks what already works,
what's stubbed, and what still needs dedicated GHEC-specific handling.

## How instance targeting works

- `GHES_BASE_URL` unset, or set to `https://github.com` → app targets GHEC
  (`api.github.com` / `uploads.github.com`).
- `GHES_BASE_URL` set to an appliance URL (e.g. `https://ghes.example.com`)
  → app targets that GHES instance's `/api/v3` and `/api/uploads` paths.
- `Config.IsGHEC` / `Config.RESTBaseURL()` / `Config.UploadBaseURL()` are the
  single source of truth other packages should use — never hardcode
  `api.github.com` or a GHES path elsewhere.

## Status by feature

| Area | GHES | GHEC | Notes |
|---|---|---|---|
| REST API base URL resolution | ✅ | ✅ | `internal/config` |
| GitHub App JWT + installation token auth | ✅ | ✅ | Same REST shape on both; uses resolved base URL. |
| Admin PAT auth (org app inventory / audit log) | ✅ | ✅ | Same endpoints; GHES supports `/orgs/{org}/audit-log` since 3.4+. |
| Hosted runners API (`/orgs/{org}/actions/hosted-runners`) | ⚠️ | ✅ | GHES **does not support GitHub-hosted runners** for self-hosted-only appliances in most configs — needs a feature-detect/fallback to self-hosted runner groups only (`/orgs/{org}/actions/runners`) when hosted-runners API 404s. **TODO: implement fallback + config flag.** |
| Runner groups API | ✅ | ✅ | Same shape. |
| Actions cache usage API | ✅ | ✅ | Same shape; confirm GHES version supports (3.5+). |
| Workflow runs / jobs API | ✅ | ✅ | Same shape. |
| Org webhook delivery format | ✅ | ✅ | `workflow_run` payload identical; signature header identical (`X-Hub-Signature-256`). |
| GitHub status page probe | ✅ | ✅ | Always hits public `githubstatus.com` — this reflects GHEC/github.com status, **not** the customer's own GHES appliance health. For GHES, this probe should be reinterpreted as "is github.com reachable" rather than "is our instance down"; consider adding a GHES-specific probe (e.g. hitting the appliance's own `/api/v3` health/rate_limit endpoint) as a complementary signal. **TODO.** |
| Git transport probe (`info/refs?service=git-upload-pack`) | ⚠️ | ✅ | Currently only implemented against `github.com/{org}/{repo}.git`. **TODO: for GHES, probe against the appliance's own git remote (`{GHES_BASE_URL}/{org}/{repo}.git`) instead/in addition.** |
| SSH banner probe (`ssh.github.com:443`, `github.com:22`) | ⚠️ | ✅ | GHES appliances typically expose their own SSH endpoint on the appliance hostname, not `ssh.github.com`. **TODO: make SSH probe target configurable/derived from `GHES_BASE_URL` host for GHES deployments, while keeping the github.com probe for GHEC.** |
| OAuth device flow (future, for CoCo/dashboard auth beyond admin PAT) | ❌ | ❌ | Not yet implemented for either; GHES and GHEC device-flow endpoints both live at `/login/device/code` relative to `GHES_BASE_URL` (GHES) or `github.com` (GHEC) — should generalize cleanly once added. |
| Okta login gate | N/A | N/A | Independent of GHES/GHEC; no GitHub-instance-specific behavior. |

## Legend
- ✅ Implemented and instance-agnostic (works for both today).
- ⚠️ Implemented for one target only; needs follow-up work for full parity.
- ❌ Not yet implemented for either target.

Update this table as packages are built out.
