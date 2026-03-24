#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="/home/dadams/pub-med-daily"
ENV_FILE="${ROOT_DIR}/.env.production"
STAGE_DIR="${STAGE_DIR:-/scratch/oa_pdf}"
FTP_HOST="${FTP_HOST:-ftp.ncbi.nlm.nih.gov}"
FTP_REMOTE_DIR="${FTP_REMOTE_DIR:-/pub/pmc/oa_pdf}"
FTP_PARALLEL="${FTP_PARALLEL:-6}"
MC_ALIAS="${MC_ALIAS:-pubmed-remote}"
MC_BIN="${MC_BIN:-mcli}"
DO_DELETE="${DO_DELETE:-0}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [stage|sync|audit|cleanup|all]

Phases:
  audit    - generate .pdf.gz manifest (read-only)
  stage    - mirror FTP oa_pdf tree to local ${STAGE_DIR}
  sync     - mirror local ${STAGE_DIR} to MinIO bucket root
  cleanup  - delete .pdf.gz objects (requires DO_DELETE=1)
  all      - run audit -> stage -> sync -> audit (and cleanup if DO_DELETE=1)

Environment overrides:
  STAGE_DIR=/scratch/oa_pdf
  FTP_PARALLEL=6
  DO_DELETE=1            # only for cleanup phase
EOF
}

phase="${1:-all}"
case "$phase" in
  stage|sync|audit|cleanup|all) ;;
  -h|--help) usage; exit 0 ;;
  *) usage; echo "Invalid phase: $phase"; exit 1 ;;
esac

cd "$ROOT_DIR"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

require_cmd lftp
require_cmd "$MC_BIN"
require_cmd python3

if [[ -z "${MINIO_ENDPOINT:-}" || -z "${MINIO_ACCESS_KEY:-}" || -z "${MINIO_SECRET_KEY:-}" ]]; then
  echo "Missing MinIO env vars in ${ENV_FILE}: MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY"
  exit 1
fi

MINIO_BUCKET="${MINIO_BUCKET:-pubmed}"
MINIO_SECURE_FLAG="${MINIO_SECURE:-true}"
MINIO_SCHEME="https"
if [[ "${MINIO_SECURE_FLAG,,}" == "false" || "${MINIO_SECURE_FLAG}" == "0" ]]; then
  MINIO_SCHEME="http"
fi

MINIO_ENDPOINT_NO_SCHEME="${MINIO_ENDPOINT#http://}"
MINIO_ENDPOINT_NO_SCHEME="${MINIO_ENDPOINT_NO_SCHEME#https://}"
MINIO_URL="${MINIO_SCHEME}://${MINIO_ENDPOINT_NO_SCHEME}"

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

run_audit() {
  mkdir -p "$ROOT_DIR/reports"
  local report_file="$ROOT_DIR/reports/pdfgz_manifest_$(date -u +%Y%m%dT%H%M%SZ).csv"
  echo "[$(timestamp)] AUDIT: generating .pdf.gz manifest -> $report_file"
  python3 "$ROOT_DIR/scripts/minio_pdfgz_manifest.py" --bucket "$MINIO_BUCKET" --output "$report_file"
}

run_stage() {
  mkdir -p "$STAGE_DIR"
  echo "[$(timestamp)] STAGE: mirroring ftp://${FTP_HOST}${FTP_REMOTE_DIR} -> ${STAGE_DIR}"
  lftp -e "set net:max-retries 8; set net:timeout 30; set net:reconnect-interval-base 5; set xfer:clobber on; mirror --continue --parallel=${FTP_PARALLEL} --verbose ${FTP_REMOTE_DIR} ${STAGE_DIR}; bye" "https://${FTP_HOST}"
}

run_sync() {
  echo "[$(timestamp)] SYNC: configuring mc alias ${MC_ALIAS} (${MINIO_URL})"
  "$MC_BIN" alias set "$MC_ALIAS" "$MINIO_URL" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null

  echo "[$(timestamp)] SYNC: mirroring ${STAGE_DIR}/ -> ${MC_ALIAS}/${MINIO_BUCKET}/"
  "$MC_BIN" mirror --overwrite --remove --preserve "${STAGE_DIR}/" "${MC_ALIAS}/${MINIO_BUCKET}/"

  echo "[$(timestamp)] SYNC: convergence pass"
  "$MC_BIN" mirror --overwrite --preserve "${STAGE_DIR}/" "${MC_ALIAS}/${MINIO_BUCKET}/"
}

run_cleanup() {
  if [[ "$DO_DELETE" != "1" ]]; then
    echo "Cleanup requested but DO_DELETE is not 1. Refusing delete."
    exit 2
  fi
  echo "[$(timestamp)] CLEANUP: deleting .pdf.gz objects from bucket ${MINIO_BUCKET}"
  python3 "$ROOT_DIR/scripts/minio_pdfgz_manifest.py" --bucket "$MINIO_BUCKET" --delete --yes
}

case "$phase" in
  audit)
    run_audit
    ;;
  stage)
    run_stage
    ;;
  sync)
    run_sync
    ;;
  cleanup)
    run_cleanup
    ;;
  all)
    run_audit
    run_stage
    run_sync
    run_audit
    if [[ "$DO_DELETE" == "1" ]]; then
      run_cleanup
      run_audit
    fi
    ;;
esac

echo "[$(timestamp)] COMPLETE phase=${phase}"
