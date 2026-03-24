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
set -a
source /home/dadams/pub-med-daily/.env.production
set +a

timestamp=$(date '+%Y-%m-%d %H:%M:%S')
echo "$timestamp - Starting vectorization (Stage 2)..."

# Run batch vectorization with aggressive mode
# --aggressive-mode: Fast retry (5 attempts), low backoff (0.1s), 5000-chunk batching
# --workers 4: Embedding-bound, 4 workers keep Ollama endpoint loaded without thrashing
# --limit 0: Process unlimited articles (removes when backlog clears)
python batch_vectorize.py \
  --workers 4 \
  --aggressive-mode \
  --limit 0 \
  2>&1 | tee -a vectorization_stage2.log

echo "$timestamp - Vectorization run complete"
