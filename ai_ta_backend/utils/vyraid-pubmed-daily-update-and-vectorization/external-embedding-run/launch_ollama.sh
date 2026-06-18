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
# Use `discover_system.sh` output to determine correct values.
# Add:  #SBATCH --account=<your-account>
#       #SBATCH --qos=<your-qos>

set -euo pipefail

###############################################################################
# Configuration
###############################################################################
OLLAMA_PORT="${OLLAMA_PORT:-11434}"
MODEL="nomic-embed-text:v1.5"
GPU_IDS="0,1,2,3"
HEALTH_TIMEOUT=120    # seconds to wait for Ollama to start
SIF_SEARCH_DIRS=("${HOME}/.llmflux" "${HOME}" "/tmp" "$(pwd)")
CONTAINER_DEF=""      # set to path of container.def if you need to build

###############################################################################
# Find or build the SIF container
###############################################################################
echo "[$(date)] Looking for llm_processor.sif ..."
SIF_PATH=""
for dir in "${SIF_SEARCH_DIRS[@]}"; do
    found=$(find "${dir}" -maxdepth 3 -name "llm_processor.sif" 2>/dev/null | head -1)
    if [[ -n "${found}" ]]; then
        SIF_PATH="${found}"
        break
    fi
done

if [[ -z "${SIF_PATH}" ]]; then
    echo "[$(date)] No pre-built SIF found. Attempting to build ..."

    if [[ -n "${CONTAINER_DEF}" && -f "${CONTAINER_DEF}" ]]; then
        SIF_PATH="$(pwd)/llm_processor.sif"
        apptainer build "${SIF_PATH}" "${CONTAINER_DEF}"
    elif command -v llmflux &>/dev/null; then
        # LLMFlux can build the container
        echo "[$(date)] Trying 'llmflux' to locate container def ..."
        LLMFLUX_PKG=$(python3 -c "import llmflux; print(llmflux.__path__[0])" 2>/dev/null || true)
        if [[ -n "${LLMFLUX_PKG}" && -f "${LLMFLUX_PKG}/container/container.def" ]]; then
            SIF_PATH="$(pwd)/llm_processor.sif"
            apptainer build "${SIF_PATH}" "${LLMFLUX_PKG}/container/container.def"
        fi
    fi

    if [[ -z "${SIF_PATH}" || ! -f "${SIF_PATH}" ]]; then
        echo "[$(date)] ERROR: Cannot find or build SIF container."
        echo "  Options:"
        echo "    1. Set CONTAINER_DEF= to path of container.def and resubmit"
        echo "    2. Build manually: apptainer build llm_processor.sif container.def"
        echo "    3. Use launch_ollama_native.sh instead"
        exit 1
    fi
fi

echo "[$(date)] Using SIF: ${SIF_PATH}"

###############################################################################
# Prepare directories
###############################################################################
OLLAMA_HOME="${HOME}/.ollama_slurm_${SLURM_JOB_ID:-$$}"
mkdir -p "${OLLAMA_HOME}"

BIND_DIRS="${OLLAMA_HOME}:${OLLAMA_HOME}"
# Add /tmp binding if needed
if [[ -d /tmp ]]; then
    BIND_DIRS="${BIND_DIRS},/tmp:/tmp"
fi

###############################################################################
# Launch Ollama via Apptainer
###############################################################################
echo "[$(date)] Launching Ollama on port ${OLLAMA_PORT} with GPUs: ${GPU_IDS}"

export APPTAINERENV_CUDA_VISIBLE_DEVICES="${GPU_IDS}"
export APPTAINERENV_OLLAMA_SCHED_SPREAD=1
export APPTAINERENV_OLLAMA_HOST="0.0.0.0:${OLLAMA_PORT}"
export APPTAINERENV_OLLAMA_MODELS="${OLLAMA_HOME}/models"
export APPTAINERENV_OLLAMA_KEEP_ALIVE="24h"

apptainer exec --nv --cleanenv \
    --bind "${BIND_DIRS}" \
    --env "CUDA_VISIBLE_DEVICES=${GPU_IDS}" \
    --env "OLLAMA_SCHED_SPREAD=1" \
    --env "OLLAMA_HOST=0.0.0.0:${OLLAMA_PORT}" \
    --env "OLLAMA_MODELS=${OLLAMA_HOME}/models" \
    --env "OLLAMA_KEEP_ALIVE=24h" \
    "${SIF_PATH}" \
    ollama serve &

OLLAMA_PID=$!
echo "[$(date)] Ollama PID: ${OLLAMA_PID}"

###############################################################################
# Wait for Ollama to become healthy
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

OLLAMA_VER=$(curl -sf "http://localhost:${OLLAMA_PORT}/api/version" | python3 -c "import sys,json;print(json.load(sys.stdin).get('version','unknown'))" 2>/dev/null || echo "unknown")
echo "[$(date)] Ollama is running (version: ${OLLAMA_VER})"

###############################################################################
# Pull the embedding model
###############################################################################
echo "[$(date)] Pulling model: ${MODEL} ..."
curl -sf -X POST "http://localhost:${OLLAMA_PORT}/api/pull" \
    -d "{\"name\": \"${MODEL}\"}" \
    --max-time 600 | tail -1

echo "[$(date)] Verifying model ..."
TEST_RESP=$(curl -sf -X POST "http://localhost:${OLLAMA_PORT}/api/embeddings" \
    -d "{\"model\": \"${MODEL}\", \"prompt\": \"test\"}" \
    --max-time 30)
EMB_DIM=$(echo "${TEST_RESP}" | python3 -c "import sys,json;print(len(json.load(sys.stdin)['embedding']))" 2>/dev/null || echo "FAIL")
echo "[$(date)] Embedding test: ${EMB_DIM} dimensions"

if [[ "${EMB_DIM}" != "768" ]]; then
    echo "[$(date)] WARNING: Expected 768 dimensions, got ${EMB_DIM}"
fi

###############################################################################
# Print connection info
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
# Keep alive — wait for Ollama process (or SLURM job timeout)
###############################################################################
echo "[$(date)] Keeping Ollama alive. Kill this job to stop."
wait ${OLLAMA_PID}
