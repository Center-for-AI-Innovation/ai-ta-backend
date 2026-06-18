#!/usr/bin/env bash
# discover_system.sh — Discover infrastructure on an external SLURM cluster
# Run this first after copying external-embedding-run/ to the cluster.
set -euo pipefail

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

section() { echo -e "\n${BOLD}=== $1 ===${NC}"; }
ok()      { echo -e "  ${GREEN}✓${NC} $1"; }
warn()    { echo -e "  ${YELLOW}!${NC} $1"; }
fail()    { echo -e "  ${RED}✗${NC} $1"; }

REPORT=""
note() { REPORT="${REPORT}\n$1"; }

# ── GPUs ──────────────────────────────────────────────────────────────────
section "GPU Hardware"
if command -v nvidia-smi &>/dev/null; then
    GPU_COUNT=$(nvidia-smi -L 2>/dev/null | wc -l)
    ok "nvidia-smi found — ${GPU_COUNT} GPU(s) detected"
    nvidia-smi -L 2>/dev/null | sed 's/^/    /'
    nvidia-smi --query-gpu=name,memory.total --format=csv,noheader 2>/dev/null | sed 's/^/    /'
    note "GPUs: ${GPU_COUNT}"
else
    fail "nvidia-smi not found"
    note "GPUs: NONE"
fi

# ── CUDA ──────────────────────────────────────────────────────────────────
section "CUDA"
if command -v nvcc &>/dev/null; then
    CUDA_VER=$(nvcc --version 2>/dev/null | grep "release" | awk '{print $NF}')
    ok "nvcc: ${CUDA_VER}"
elif [[ -d /usr/local/cuda ]]; then
    ok "/usr/local/cuda exists"
else
    warn "No nvcc or /usr/local/cuda found (may be available via module load)"
fi

# ── LLMFlux ───────────────────────────────────────────────────────────────
section "LLMFlux"
if command -v llmflux &>/dev/null; then
    LLMFLUX_VER=$(llmflux --version 2>/dev/null || echo "unknown")
    ok "llmflux installed: ${LLMFLUX_VER}"
    note "LLMFlux: installed"

    # Look for pre-built SIF container
    LLMFLUX_DIR="${HOME}/.llmflux"
    SIF_FOUND=""
    for search_dir in "${LLMFLUX_DIR}" "${HOME}" "/tmp" "$(pwd)"; do
        found=$(find "${search_dir}" -maxdepth 3 -name "llm_processor.sif" 2>/dev/null | head -1)
        if [[ -n "${found}" ]]; then
            SIF_FOUND="${found}"
            break
        fi
    done

    if [[ -n "${SIF_FOUND}" ]]; then
        ok "SIF container found: ${SIF_FOUND}"
        note "SIF: ${SIF_FOUND}"
    else
        warn "No llm_processor.sif found — will need to build"
        note "SIF: not found (build needed)"
    fi
else
    warn "llmflux not installed"
    note "LLMFlux: not installed"
fi

# ── Apptainer / Singularity ──────────────────────────────────────────────
section "Apptainer / Singularity"
if command -v apptainer &>/dev/null; then
    APT_VER=$(apptainer --version 2>/dev/null)
    ok "apptainer: ${APT_VER}"
    note "Apptainer: ${APT_VER}"
elif command -v singularity &>/dev/null; then
    SING_VER=$(singularity --version 2>/dev/null)
    ok "singularity: ${SING_VER}"
    note "Apptainer: singularity ${SING_VER}"
else
    warn "Neither apptainer nor singularity found"
    note "Apptainer: not available"
fi

# ── Ollama (native) ──────────────────────────────────────────────────────
section "Ollama (native)"
if command -v ollama &>/dev/null; then
    OLLAMA_VER=$(ollama --version 2>/dev/null || echo "unknown")
    ok "ollama installed: ${OLLAMA_VER}"
    note "Ollama native: ${OLLAMA_VER}"
    # Check if running
    if curl -sf http://localhost:11434/api/version &>/dev/null; then
        ok "Ollama server responding on port 11434"
    else
        warn "Ollama not currently running on port 11434"
    fi
else
    warn "ollama not found in PATH"
    note "Ollama native: not installed"
fi

# ── Module system ─────────────────────────────────────────────────────────
section "Module System"
if command -v module &>/dev/null; then
    ok "module command available"
    echo "    Available GPU/CUDA/Ollama modules:"
    module avail 2>&1 | grep -iE "cuda|gpu|ollama|apptainer|singularity" | head -10 | sed 's/^/    /' || warn "No matching modules found"
else
    warn "module command not available"
fi

# ── Network Connectivity ─────────────────────────────────────────────────
section "Network Connectivity"

# MinIO
if curl -sf --connect-timeout 10 "https://minio-api.ncsa.ai/" &>/dev/null; then
    ok "MinIO (minio-api.ncsa.ai) reachable"
    note "MinIO: reachable"
else
    fail "MinIO (minio-api.ncsa.ai) not reachable"
    note "MinIO: NOT reachable"
fi

# Qdrant Cloud
QDRANT_HOST="${QDRANT_URL:-https://qdrant.ncsa.ai}"
if curl -sf --connect-timeout 10 "${QDRANT_HOST}" &>/dev/null; then
    ok "Qdrant (${QDRANT_HOST}) reachable"
    note "Qdrant: reachable"
else
    # Try with /collections endpoint
    if curl -sf --connect-timeout 10 -H "api-key: ${QDRANT_API_KEY:-none}" "${QDRANT_HOST}/collections" &>/dev/null; then
        ok "Qdrant (${QDRANT_HOST}) reachable (via /collections)"
        note "Qdrant: reachable"
    else
        fail "Qdrant (${QDRANT_HOST}) not reachable"
        note "Qdrant: NOT reachable"
    fi
fi

# Supabase
SUPA_HOST="${SUPABASE_URL:-}"
if [[ -n "${SUPA_HOST}" ]]; then
    if curl -sf --connect-timeout 10 "${SUPA_HOST}" &>/dev/null; then
        ok "Supabase (${SUPA_HOST}) reachable"
    else
        warn "Supabase (${SUPA_HOST}) not reachable"
    fi
fi

# ── SLURM ─────────────────────────────────────────────────────────────────
section "SLURM"
if command -v sinfo &>/dev/null; then
    ok "SLURM available"
    echo "    GPU partitions:"
    sinfo -o "%P %G %D %N" 2>/dev/null | grep -i gpu | head -10 | sed 's/^/    /' || warn "No GPU partitions found"
    echo "    Account info:"
    sacctmgr show assoc where user="$USER" format=Account%20,Partition%20,QOS%30 2>/dev/null | head -10 | sed 's/^/    /' || true
    note "SLURM: available"
else
    warn "SLURM not available"
    note "SLURM: not available"
fi

# ── Python ────────────────────────────────────────────────────────────────
section "Python Environment"
if command -v python3 &>/dev/null; then
    PY_VER=$(python3 --version 2>&1)
    ok "${PY_VER}"

    echo "    Package check:"
    for pkg in requests pymupdf minio qdrant-client langchain-text-splitters psycopg2 python-dotenv; do
        if python3 -c "import importlib; importlib.import_module('${pkg//-/_}')" 2>/dev/null; then
            echo -e "    ${GREEN}✓${NC} ${pkg}"
        else
            echo -e "    ${RED}✗${NC} ${pkg} — install with: pip install ${pkg}"
        fi
    done
else
    fail "python3 not found"
fi

# ── Disk Space ────────────────────────────────────────────────────────────
section "Disk Space"
df -h . 2>/dev/null | tail -1 | awk '{print "    Available: " $4 " (on " $1 ")"}'
df -h /tmp 2>/dev/null | tail -1 | awk '{print "    /tmp: " $4 " available"}'

# ── Summary & Recommendation ─────────────────────────────────────────────
section "SUMMARY"
echo -e "${REPORT}"

echo ""
echo -e "${BOLD}Recommended launch strategy:${NC}"
if command -v llmflux &>/dev/null && [[ -n "${SIF_FOUND:-}" ]]; then
    echo -e "  ${GREEN}→ Use launch_ollama.sh (LLMFlux container at ${SIF_FOUND})${NC}"
elif command -v apptainer &>/dev/null || command -v singularity &>/dev/null; then
    echo -e "  ${YELLOW}→ Use launch_ollama.sh (build SIF first, or install LLMFlux)${NC}"
elif command -v ollama &>/dev/null; then
    echo -e "  ${YELLOW}→ Use launch_ollama_native.sh (native Ollama)${NC}"
else
    echo -e "  ${RED}→ Install Ollama or LLMFlux, then use launch_ollama_native.sh${NC}"
fi
echo ""
