# Local development

- Load runtime configuration from the repository `.env` file before starting the
  monitor. It contains local credentials and must never be committed or printed.
- Start the dashboard on port 8091 when testing locally:
  `set -a; source .env; set +a; PORT=8091 go run ./cmd/server`
