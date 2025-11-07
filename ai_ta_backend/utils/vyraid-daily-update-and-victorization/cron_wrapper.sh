#!/bin/bash
# Cron wrapper script for the pipeline
# This script checks if the pipeline is already running before starting a new instance
# Automatically recovers from FAILED states and runs every 12 hours
# 
# Example crontab entry (run every 12 hours):
# 0 */12 * * * /projects/uiucchat/vyraid_pubmed_daily_update/vyraid-ftp/daily_update_and_vectorization/cron_wrapper.sh >> /var/log/pipeline_cron.log 2>&1

# Change to script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Try to activate the conda environment (vyraid-daily) for cron runs
CONDA_ACTIVATED=0

# Attempt to find and source conda.sh via conda info --base (if conda is on PATH)
CONDA_BASE="$(__env_con=1 /usr/bin/env conda info --base 2>/dev/null || true)"
if [ -n "$CONDA_BASE" ] && [ -f "$CONDA_BASE/etc/profile.d/conda.sh" ]; then
    # shellcheck disable=SC1090
    source "$CONDA_BASE/etc/profile.d/conda.sh"
    conda activate vyraid-daily && CONDA_ACTIVATED=1
fi

# Fallback to common install locations if conda isn't on PATH in cron
if [ "$CONDA_ACTIVATED" -ne 1 ]; then
    for CB in "$HOME/miniconda3" "$HOME/anaconda3"; do
        if [ -f "$CB/etc/profile.d/conda.sh" ]; then
            # shellcheck disable=SC1090
            source "$CB/etc/profile.d/conda.sh"
            conda activate vyraid-daily && { CONDA_ACTIVATED=1; break; }
        fi
    done
fi

if [ "$CONDA_ACTIVATED" -eq 1 ]; then
    PYTHON="python"
    echo "Using conda env 'vyraid-daily' python: $(command -v python)"
else
    # Prefer the conda environment's python by absolute path as a fallback
    PYTHON="/home/tkyaw2/.conda/envs/vyraid-daily/bin/python"
    if [ ! -x "$PYTHON" ]; then
        echo "Warning: conda env python not found or not executable: $PYTHON" >&2
        echo "Falling back to system python3. If this is unintended, update PYTHON in this script." >&2
        PYTHON="python3"
    fi
fi

# Load environment variables (properly handle inline comments)
if [ -f .env ]; then
    set -a
    source <(grep -v '^#' .env | sed 's/#.*$//' | sed '/^[[:space:]]*$/d')
    set +a
fi

echo "=== Pipeline Cron Job Started at $(date) ==="

# Check pipeline state
$PYTHON check_pipeline_state.py
EXIT_CODE=$?

case $EXIT_CODE in
    0)
    echo "Pipeline is ready to run. Starting..."
    $PYTHON main.py
        PIPELINE_EXIT=$?
        echo "Pipeline finished with exit code: $PIPELINE_EXIT"
        ;;
    1)
        echo "Pipeline is already running. Skipping this run."
        ;;
    2)
        echo "WARNING: Pipeline is in FAILED state."
        echo "Automatically clearing failed state and retrying..."
        
        # Backup the failed state for debugging in pipeline_state directory
        if [ -f pipeline_state/pipeline_state.json ]; then
            BACKUP_FILE="pipeline_state/pipeline_state.failed.$(date +%Y%m%d_%H%M%S).json"
            cp pipeline_state/pipeline_state.json "$BACKUP_FILE"
            echo "Failed state backed up to: $BACKUP_FILE"
            rm pipeline_state/pipeline_state.json
        fi
        
    echo "Starting pipeline after failed state recovery..."
    $PYTHON main.py
        PIPELINE_EXIT=$?
        echo "Pipeline finished with exit code: $PIPELINE_EXIT"
        ;;
    3)
        echo "ERROR: Could not read pipeline state."
    echo "Attempting to run anyway..."
    $PYTHON main.py
        ;;
    *)
        echo "Unexpected exit code from state check: $EXIT_CODE"
        ;;
esac

echo "=== Pipeline Cron Job Ended at $(date) ==="
echo ""
