#!/bin/bash
# Two-stage daily updatefiles pipeline
# Stage 1 (this script): Download new articles with high parallelism, skip vectorization
# Stage 2 (batch_vectorize.py): Vectorize in background, keeps backlog clear
#
# Strategy: Decouple I/O-bound download (can use 16 workers) from CPU-bound embedding (4 workers).
# New articles download in 0.5-3 hours, vectorize in 0.5-2 hours independently.

# Resolve script directory so this script can be called from anywhere (e.g. cron)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source ~/miniforge3/bin/activate
set -a; source /home/dadams/pub-med-daily/.env.production; set +a

# Stage 1: Download with aggressive worker count (I/O bound - OA API + PDF downloads)
# MAX_WORKERS = 16: Justified by OA API 10 req/s limit + network I/O parallelism
# --skip-vectorization: Let Stage 2 handle embedding (decoupled, can run independently)
export MAX_WORKERS=16
echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting Stage 1: download new articles (MAX_WORKERS=16, skip vectorization)"

# Ensure Ollama is running (needed for Stage 1 vectorization path if skip-vectorization is removed)
if ! curl -s --max-time 3 http://localhost:11434/ > /dev/null 2>&1; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Ollama not running, starting it..."
    OLLAMA_HOST=0.0.0.0:11434 nohup ~/bin/ollama serve >> /tmp/ollama.log 2>&1 &
    sleep 8
fi

python3 main.py --source updatefiles --skip-vectorization

echo "$(date '+%Y-%m-%d %H:%M:%S') - Stage 1 complete. Articles downloaded to MinIO."
echo "$(date '+%Y-%m-%d %H:%M:%S') - Stage 2 (vectorization) will run as background job."
