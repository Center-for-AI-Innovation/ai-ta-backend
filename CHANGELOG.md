# Changelog

## [Unreleased]

### Fixed
- Railway deploys no longer fail silently. `run.sh` now uses `set -euo pipefail` and calls `gunicorn` directly (the build installs deps into system Python via `uv pip install --system`, so the runtime `uv: not found` error is gone). Added a `/health` healthcheck to `railway.json` so a failed start is marked as a crashed deployment instead of leaving the previous deploy serving traffic unnoticed.
