# Local development

- Keep runtime configuration in `~/.config/ghes-actions-monitor/.env` with mode
  600, and create an ignored `.env` symlink in each worktree:
  `ln -s "$HOME/.config/ghes-actions-monitor/.env" .env`
- Load runtime configuration from `.env` before starting the monitor. It
  contains local credentials and must never be committed or printed.
- Start the dashboard on port 8091 when testing locally:
  `set -a; source .env; set +a; PORT=8091 go run ./cmd/server`
