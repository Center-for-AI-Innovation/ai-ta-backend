#!/usr/bin/env bash
# Three-tier preflight supervisor for the Stage 2 vectorizer.
#
#   Tier 1: remote secret-ollama.ncsa.ai (intermittent; may NXDOMAIN)
#   Tier 2: existing local Ollama on 127.0.0.1:11434
#   Tier 3: spawn a fresh local Ollama (GPU auto-detected; CPU if no GPU)
#
# Always exits 0. shared/vectorize.py:get_embedding has its own multi-URL
# retry loop, so the vectorizer is allowed to run even if every tier here
# is unhealthy at startup — it'll pick up endpoints as they come back.

# No `set -e` on purpose: tier probes are expected to fail in normal operation.
set -u

REMOTE_URL="${REMOTE_EMBEDDING_URL:-https://secret-ollama.ncsa.ai/api/embeddings}"
LOCAL_PORT="${OLLAMA_PORT:-11434}"
LOCAL_BASE="http://127.0.0.1:${LOCAL_PORT}"
LOCAL_URL="${LOCAL_BASE}/api/embeddings"
MODEL="nomic-embed-text:v1.5"
SPAWN_TIMEOUT="${SPAWN_TIMEOUT:-60}"
OLLAMA_LOG="${OLLAMA_LOG:-/home/dadams/pub-med-daily/logs/ollama.log}"
OLLAMA_BIN="${OLLAMA_BIN:-$HOME/bin/ollama}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] preflight: $*"; }

# 200 + embedding length 768 == healthy.
probe_embeddings() {
    local url="$1" timeout="${2:-10}"
    local body
    body=$(curl -sS --max-time "$timeout" -X POST "$url" \
        -H 'Content-Type: application/json' \
        -d "{\"model\":\"${MODEL}\",\"prompt\":\"hi\"}" 2>/dev/null) || return 1
    [[ -z "$body" ]] && return 1
    local dim
    dim=$(echo "$body" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(len(d.get("embedding",[])))' 2>/dev/null) || return 1
    [[ "$dim" == "768" ]]
}

ensure_model_present() {
    local base="$1"
    if curl -sS --max-time 5 "${base}/api/tags" 2>/dev/null | grep -q "\"${MODEL}\""; then
        return 0
    fi
    log "model ${MODEL} not present at ${base}; pulling"
    curl -sS --max-time 600 -X POST "${base}/api/pull" \
        -H 'Content-Type: application/json' \
        -d "{\"name\":\"${MODEL}\"}" >/dev/null 2>&1 || {
        log "pull via API failed; trying CLI"
        "$OLLAMA_BIN" pull "$MODEL" >>"$OLLAMA_LOG" 2>&1 || return 1
    }
}

#-----------------------------------------------------------------------------
# Tier 1: remote
#-----------------------------------------------------------------------------
log "tier 1: probing ${REMOTE_URL}"
if probe_embeddings "$REMOTE_URL" 5; then
    log "tier 1 healthy — remote secret-ollama responding"
    # No early exit: localhost may also be useful as the second URL in
    # EMBEDDING_URLS. We've established that *something* in the waterfall
    # works.
else
    log "tier 1 unavailable (NXDOMAIN / timeout / non-200)"
fi

#-----------------------------------------------------------------------------
# Tier 2: existing local
#-----------------------------------------------------------------------------
log "tier 2: probing ${LOCAL_BASE}/api/version"
if curl -sS --max-time 3 "${LOCAL_BASE}/api/version" >/dev/null 2>&1; then
    log "tier 2 local Ollama is up; checking model"
    if ensure_model_present "$LOCAL_BASE" && probe_embeddings "$LOCAL_URL" 10; then
        log "tier 2 healthy — local Ollama serving ${MODEL}"
        exit 0
    fi
    log "tier 2 local Ollama present but model/embeddings unhealthy"
fi

#-----------------------------------------------------------------------------
# Tier 3: spawn local
#-----------------------------------------------------------------------------
if pgrep -f "ollama serve" >/dev/null 2>&1; then
    log "tier 3: ollama serve process already running but unhealthy; not respawning"
else
    if [[ ! -x "$OLLAMA_BIN" ]]; then
        log "tier 3 FAILED: ollama binary not found at $OLLAMA_BIN"
        exit 0
    fi

    # GPU auto-detect: if nvidia-smi lists at least one GPU, let Ollama use
    # all visible devices (unset CUDA_VISIBLE_DEVICES). Otherwise force CPU.
    gpu_count=0
    if command -v nvidia-smi >/dev/null 2>&1; then
        gpu_count=$(nvidia-smi -L 2>/dev/null | wc -l)
    fi

    if [[ "$gpu_count" -gt 0 ]]; then
        log "tier 3: detected ${gpu_count} GPU(s); launching Ollama with GPU"
        unset CUDA_VISIBLE_DEVICES
    else
        log "tier 3: no GPU detected; launching Ollama in CPU-only mode"
        export CUDA_VISIBLE_DEVICES=""
    fi

    mkdir -p "$(dirname "$OLLAMA_LOG")"
    OLLAMA_HOST="127.0.0.1:${LOCAL_PORT}" \
    OLLAMA_KEEP_ALIVE="24h" \
        nohup "$OLLAMA_BIN" serve >>"$OLLAMA_LOG" 2>&1 &
    log "tier 3: spawned ollama serve (pid=$!), log=${OLLAMA_LOG}"

    # Wait for /api/version
    elapsed=0
    while ! curl -sS --max-time 2 "${LOCAL_BASE}/api/version" >/dev/null 2>&1; do
        if [[ $elapsed -ge $SPAWN_TIMEOUT ]]; then
            log "tier 3 FAILED: ollama did not come up within ${SPAWN_TIMEOUT}s — vectorizer will retry inline"
            exit 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    log "tier 3: ollama responding to /api/version after ${elapsed}s"
fi

# Verify model + embeddings on whatever ollama serve is now running.
if ensure_model_present "$LOCAL_BASE" && probe_embeddings "$LOCAL_URL" 30; then
    log "tier 3 healthy — local Ollama serving ${MODEL}"
else
    log "tier 3 partial: ollama up but ${MODEL} unhealthy — vectorizer will retry inline"
fi

exit 0
