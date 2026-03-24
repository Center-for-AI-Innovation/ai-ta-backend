#!/bin/bash
# FTP Reconciliation Workflow: Full end-to-end OA PDF recovery

set -e

# Resolve to the parent directory (vyraid-pubmed-daily-update-and-vectorization/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."
source ~/miniforge3/bin/activate
set -a; source /home/dadams/pub-med-daily/.env.production; set +a

echo "========================================================================"
echo "FTP OA PDF RECONCILIATION WORKFLOW"
echo "========================================================================"

# Phase 1: Reconciliation (crawl + gap analysis)
echo ""
echo "[Phase 1/3] Reconciliation: Crawl FTP and identify gaps..."
python ftp/ftp_reconciliation.py

if [ ! -f ftp_reconciliation_result.json ]; then
    echo "❌ Reconciliation failed"
    exit 1
fi

# Parse results
TOTAL=$(jq -r '.report.ftp_total' ftp_reconciliation_result.json)
DB=$(jq -r '.report.db_total' ftp_reconciliation_result.json)
MINIO=$(jq -r '.report.minio_total' ftp_reconciliation_result.json)
COVERAGE=$(jq -r '.report.coverage_percent' ftp_reconciliation_result.json)
GAP=$(jq -r '.report.ftp_in_db_not_in_minio' ftp_reconciliation_result.json)

echo "✓ Reconciliation complete"
echo "  FTP: $TOTAL PDFs available"
echo "  DB: $DB articles indexed"
echo "  MinIO: $MINIO PDFs downloaded"
echo "  Coverage: $COVERAGE%"
echo "  Fillable gap: $GAP PDFs (FTP+DB, not MinIO)"

# Phase 2: Download missing (if significant gap)
if [ "$GAP" -gt 0 ]; then
    echo ""
    echo "[Phase 2/3] Download: Retrieving $GAP missing PDFs..."
    
    WORKERS=${1:-8}  # Allow override: script.sh [workers]
    python ftp/ftp_download_missing.py --workers $WORKERS
    
    if [ ! -f ftp_download_checkpoint.json ]; then
        echo "❌ Download failed"
        exit 1
    fi
    
    SUCCESS=$(jq -r '.stats.success' ftp_download_checkpoint.json)
    FAILED=$(jq -r '.stats.failed' ftp_download_checkpoint.json)
    
    echo "✓ Downloads complete: $SUCCESS succeeded, $FAILED failed"
else
    echo ""
    echo "[Phase 2/3] Download: No gaps to fill (coverage already $COVERAGE%)"
fi

# Phase 3: Vectorization (optional - only if Ollama available)
echo ""
echo "[Phase 3/3] Vectorization: Processing new PDFs..."

# Check Ollama health
if curl -s https://secret-ollama.ncsa.ai/api/embeddings \
    -H "Content-Type: application/json" \
    -d '{"model":"nomic-embed-text:v1.5","prompt":"test"}' > /dev/null 2>&1; then
    
    echo "✓ Ollama healthy, starting vectorization..."
    python batch_vectorize.py --limit 100 --aggressive-mode
    
    echo "✓ Vectorization started"
    echo "  For full backlog, continue with: python batch_vectorize.py --aggressive-mode"
else
    echo "⚠️  Ollama unavailable (HTTP 530), skipping vectorization for now"
    echo "  Retry later with: python batch_vectorize.py --aggressive-mode"
fi

echo ""
echo "========================================================================"
echo "WORKFLOW COMPLETE"
echo "========================================================================"
echo "Summary:"
echo "  ✓ FTP reconciliation: $TOTAL available vs $MINIO in MinIO"
echo "  ✓ Gap identified: $GAP articles"
if [ "$GAP" -gt 0 ]; then
    echo "  ✓ Downloaded & ingested: $SUCCESS new PDFs"
fi
echo ""
echo "Results saved:"
echo "  - ftp_reconciliation_result.json (gap analysis)"
if [ "$GAP" -gt 0 ]; then
    echo "  - ftp_download_checkpoint.json (download progress)"
fi
echo "  - batch_vectorize_checkpoint.json (vectorization progress)"
echo ""
