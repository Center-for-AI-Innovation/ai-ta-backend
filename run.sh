#!/bin/bash
set -euo pipefail

# Docs https://docs.gunicorn.org/en/stable/settings.html#workers

# 200 MB object store memory.. necessary to statically allocate or will crash in Railway env restrictions.
# ray start --head --num-cpus 6 --object-store-memory 300000000

export PYTHONPATH=${PYTHONPATH:-}:$(pwd)/ai_ta_backend
# Dependencies are installed into the system Python at build time
# (`uv pip install --system`), so call gunicorn directly. Using `uv run`
# here fails at runtime because the curl-installed uv is not on PATH.
exec gunicorn --workers=3 --threads=100 --worker-class=gthread ai_ta_backend.main:app --timeout 1800
