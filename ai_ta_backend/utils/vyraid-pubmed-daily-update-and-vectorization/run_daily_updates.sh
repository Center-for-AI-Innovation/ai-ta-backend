#!/bin/bash
# Two-stage daily updatefiles pipeline — Stage 1 (download-only).
# main.py never embeds; Stage 2 (batch_vectorize.py) handles all Qdrant work.
#
# I/O-bound: MAX_WORKERS=16 saturates the OA API 10 req/s window plus parallel
# PDF downloads. No Ollama dependency in this script.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source ~/miniforge3/bin/activate

# Env resolution: $ENV_FILE override, else .env / .env.production next to this script
ENV_FILE="${ENV_FILE:-}"
if [[ -z "$ENV_FILE" ]]; then
    for candidate in "$SCRIPT_DIR/.env" "$SCRIPT_DIR/.env.production"; do
        if [[ -e "$candidate" ]]; then ENV_FILE="$candidate"; break; fi
    done
fi
if [[ -n "$ENV_FILE" && -e "$ENV_FILE" ]]; then
    set -a; source "$ENV_FILE"; set +a
else
    echo "WARNING: no env file resolved (ENV_FILE unset, no .env or .env.production in $SCRIPT_DIR)" >&2
fi

export MAX_WORKERS=16

# Guard against concurrent runs: if a previous Stage 1 is still downloading,
# exit cleanly so an overlapping cron tick can't double-process update files.
# Distinct lockfile from Stage 2 (/tmp/pubmed_vectorize.lock) so the two stages
# may run concurrently.
LOCKFILE="/tmp/pubmed_stage1.lock"
exec 200>"$LOCKFILE"
flock -n 200 || { echo "$(date '+%Y-%m-%d %H:%M:%S') - Another Stage 1 run is already in progress. Exiting."; exit 0; }

echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting Stage 1: download new articles (MAX_WORKERS=16, download-only)"

python3 main.py --source updatefiles

echo "$(date '+%Y-%m-%d %H:%M:%S') - Stage 1 complete. Articles downloaded to MinIO; rows recorded in Postgres with vector_indexed=false."
echo "$(date '+%Y-%m-%d %H:%M:%S') - Stage 2 (batch_vectorize.py / vectorize_pg_driven.py) drains the backlog separately."
