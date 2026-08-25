# Feature Backlog

Ideas explicitly deferred from the current MVP (jobs feed, queue depth,
runner capacity). Not scheduled — capturing so they aren't lost.

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

## Add outbound GitHub API observability

Use standard Go OpenTelemetry HTTP instrumentation (for example,
`otelhttp.NewTransport`) around the GitHub client transport, exporting spans
and metrics to the deployment's configured backend. Add GitHub-specific
attributes and counters for normalized endpoint, HTTP status, latency, rate
limit headers, poller source (`workflow_sweep`, `runner_sweep`,
`manual_refresh`), and sweeps skipped because of rate limiting. Keep endpoint
labels bounded by avoiding raw repository names and run IDs.
