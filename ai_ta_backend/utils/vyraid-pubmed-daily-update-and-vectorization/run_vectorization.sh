#!/bin/bash
# Stage 2: Continuous vectorization for backlog and daily articles
# Run this as a background job or cron entry (e.g., every 30 minutes)
#
# Purpose: Process articles with PDFs but vector_indexed=false
# Optimized for throughput: 4 workers, aggressive Qdrant settings, 5000-chunk batches

# Resolve script directory so this script can be called from anywhere (e.g. cron)
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

timestamp=$(date '+%Y-%m-%d %H:%M:%S')
echo "$timestamp - Starting vectorization (Stage 2)..."

# Guard against concurrent runs: if another instance is already running, exit cleanly
LOCKFILE="/tmp/pubmed_vectorize.lock"
exec 200>"$LOCKFILE"
flock -n 200 || { echo "$timestamp - Another vectorization run is already in progress. Exiting."; exit 0; }

# Walk the embedding-endpoint waterfall (remote -> existing local -> spawn local).
# Always exits 0; shared/vectorize.py:get_embedding has in-loop retries that
# pick up endpoints as they come back, so the vectorizer launches regardless.
"$SCRIPT_DIR/ensure_embedding_endpoint.sh" || true

# Run batch vectorization with aggressive mode
# --aggressive-mode: Fast retry (5 attempts), low backoff (0.1s), 5000-chunk batching
# --workers: Embedding/IO-bound. Default 4 keeps Ollama loaded without thrashing;
#   override with VECTORIZE_WORKERS for a faster backlog drain (e.g. 8).
# --limit 0: Process unlimited articles (removes when backlog clears)
python3 batch_vectorize.py \
  --workers "${VECTORIZE_WORKERS:-4}" \
  --aggressive-mode \
  --limit 0 \
  2>&1 | tee -a vectorization_stage2.log

echo "$timestamp - Vectorization run complete"
