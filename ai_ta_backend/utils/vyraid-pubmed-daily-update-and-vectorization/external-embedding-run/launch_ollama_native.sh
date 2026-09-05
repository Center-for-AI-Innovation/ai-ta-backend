#!/usr/bin/env bash
#SBATCH --job-name=ollama-embed
#SBATCH --output=ollama-%j.log
#SBATCH --error=ollama-%j.err
#SBATCH --time=48:00:00
#SBATCH --gres=gpu:4
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --partition=gpu
# ^^^ Adjust partition, account, QOS as needed for your cluster.
# Add:  #SBATCH --account=<your-account>
#       #SBATCH --qos=<your-qos>

set -euo pipefail

###############################################################################
# Configuration
###############################################################################
OLLAMA_PORT="${OLLAMA_PORT:-11434}"
MODEL="nomic-embed-text:v1.5"
GPU_IDS="0,1,2,3"
HEALTH_TIMEOUT=120

###############################################################################
# Check for Ollama
###############################################################################
if ! command -v ollama &>/dev/null; then
    echo "[$(date)] ERROR: ollama not found in PATH"
    echo "  Install: curl -fsSL https://ollama.com/install.sh | sh"
    exit 1
fi

echo "[$(date)] Ollama: $(ollama --version 2>/dev/null || echo 'unknown version')"

###############################################################################
# Launch Ollama
###############################################################################
export CUDA_VISIBLE_DEVICES="${GPU_IDS}"
export OLLAMA_HOST="0.0.0.0:${OLLAMA_PORT}"
export OLLAMA_SCHED_SPREAD=1
export OLLAMA_KEEP_ALIVE="24h"

echo "[$(date)] Launching Ollama on port ${OLLAMA_PORT} with GPUs: ${GPU_IDS}"
ollama serve &
OLLAMA_PID=$!
echo "[$(date)] Ollama PID: ${OLLAMA_PID}"

###############################################################################
# Wait for health
###############################################################################
echo "[$(date)] Waiting for Ollama to start (timeout: ${HEALTH_TIMEOUT}s) ..."
elapsed=0
while ! curl -sf "http://localhost:${OLLAMA_PORT}/api/version" &>/dev/null; do
    if [[ ${elapsed} -ge ${HEALTH_TIMEOUT} ]]; then
        echo "[$(date)] ERROR: Ollama did not start within ${HEALTH_TIMEOUT}s"
        kill ${OLLAMA_PID} 2>/dev/null || true
        exit 1
    fi
    sleep 2
    elapsed=$((elapsed + 2))
done

echo "[$(date)] Ollama is running"

###############################################################################
# Pull model
###############################################################################
echo "[$(date)] Pulling model: ${MODEL} ..."
ollama pull "${MODEL}"

echo "[$(date)] Verifying model ..."
TEST_RESP=$(curl -sf -X POST "http://localhost:${OLLAMA_PORT}/api/embeddings" \
    -d "{\"model\": \"${MODEL}\", \"prompt\": \"test\"}" \
    --max-time 30)
EMB_DIM=$(echo "${TEST_RESP}" | python3 -c "import sys,json;print(len(json.load(sys.stdin)['embedding']))" 2>/dev/null || echo "FAIL")
echo "[$(date)] Embedding test: ${EMB_DIM} dimensions"

###############################################################################
# Connection info
###############################################################################
HOSTNAME=$(hostname)
echo ""
echo "============================================================"
echo " Ollama is ready!"
echo " Host:  ${HOSTNAME}"
echo " Port:  ${OLLAMA_PORT}"
echo " Model: ${MODEL}"
echo " GPUs:  ${GPU_IDS}"
echo ""
echo " Connect from this node:"
echo "   export EMBEDDING_BASE_URL=http://localhost:${OLLAMA_PORT}/api/embeddings"
echo ""
echo " Connect from another node:"
echo "   export EMBEDDING_BASE_URL=http://${HOSTNAME}:${OLLAMA_PORT}/api/embeddings"
echo ""
echo " Run the vectorizer:"
echo "   python revectorize.py --source both --workers 8 --aggressive-mode --skip-dedup"
echo "============================================================"
echo ""

###############################################################################
# Keep alive
###############################################################################
echo "[$(date)] Keeping Ollama alive. Kill this job to stop."
wait ${OLLAMA_PID}
