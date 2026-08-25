# Feature Backlog

Ideas explicitly deferred from the current MVP (jobs feed, queue depth,
runner capacity). Not scheduled — capturing so they aren't lost.

## Cancel workflow runs

Add `POST /api/runs/{run_id}/cancel` → `POST /repos/{org}/{repo}/actions/runs/{run_id}/cancel`.

- Changes the app from strictly read-only to read/write. Needs explicit
  user-facing confirmation in the dashboard before wiring up a button.
- Token/App permission requirement: repository **Actions: Read and write**
  (currently the MVP only needs read-only).
- Should have basic auth/access control in front of it before enabling in
  any shared deployment (today there's no auth gating at all — fine for a
  single-admin-PAT local/internal MVP, not fine once this can mutate state).

## Auto-provision the org webhook

Instead of requiring an operator to manually add the org webhook in GitHub
settings, have the app create/update it on startup:

- List org webhooks (`GET /orgs/{org}/hooks`), match by target URL, create
  (`POST /orgs/{org}/hooks`) if missing or update (`PATCH .../hooks/{id}`)
  if the secret/events need to change.
- Needs a `PUBLIC_URL` config value (the app's externally reachable base
  URL) and token/App permission: organization **Webhooks: Read and write**.
- Only useful once the app is deployed somewhere with a stable public URL
  (e.g. Cloud Run) — not needed for local dev, where polling alone is
  sufficient. This is why it was deferred rather than built now: local
  integration testing has no public endpoint for GitHub to call anyway.
- A prior draft implementation (`internal/webhookprovision`) was written
  and then removed to keep the MVP scope minimal; revisit that approach
  when this is picked up.
