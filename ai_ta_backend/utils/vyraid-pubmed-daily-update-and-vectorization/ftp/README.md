# FTP OA PDF Reconciliation

Reconciles Open Access PDFs between three sources — NCBI FTP, our Postgres DB, and MinIO — then downloads and vectorizes any missing PDFs.

Run this occasionally (monthly or when investigating coverage gaps), not as part of the daily pipeline.

---

## Three-Phase Workflow

```
Phase 1: Reconciliation (ftp_reconciliation.py)
  ├─ Download NCBI's oa_file_list.csv (~900 MB)
  ├─ Extract all available PMCIDs
  ├─ Query Postgres (vyraid.articles) for DB inventory
  ├─ Query MinIO for already-downloaded PDFs
  └─ Output: ftp_reconciliation_result.json (gap analysis)

Phase 2: Download Missing (ftp_download_missing.py)
  ├─ Load gaps from Phase 1
  ├─ Priority: articles in FTP+DB but not in MinIO
  ├─ Parallel HTTP download (8 workers default)
  ├─ Upload to MinIO + update Postgres pdf_location
  └─ Checkpoint: ftp_download_checkpoint.json

Phase 2b: Ingest FTP-only articles (ftp_ingest_missing_db.py)
  ├─ Resolve PMCIDs found on FTP but not in DB
  ├─ Query NCBI idconv API: PMCID → PMID
  └─ Insert/update vyraid.articles in Postgres

Phase 3: Vectorization (../batch_vectorize.py)
  ├─ Process articles with pdf_location but vector_indexed=false
  └─ Standard Stage 2 batch vectorization
```

---

## Quick Start

### Full workflow (recommended)

From the `vyraid-pubmed-daily-update-and-vectorization/` directory:

```bash
# Run all three phases with 8 download workers
bash ftp/ftp_reconciliation_workflow.sh 8

# Or with more workers if bandwidth allows
bash ftp/ftp_reconciliation_workflow.sh 16
```

### Individual phases

**Phase 1 — Reconciliation only:**
```bash
cd ftp/
python ftp_reconciliation.py
# Output: ../ftp_reconciliation_result.json
```

**Phase 2 — Download missing PDFs:**
```bash
# Requires Phase 1 output (ftp_reconciliation_result.json)
python ftp/ftp_download_missing.py --workers 8

# Resume an interrupted run
python ftp/ftp_download_missing.py --workers 8 --resume

# Test with a small batch
python ftp/ftp_download_missing.py --workers 4 --limit 100
```

**Phase 2b — Ingest FTP-only articles into DB:**
```bash
python ftp/ftp_ingest_missing_db.py
```

**Phase 3 — Vectorize new PDFs:**
```bash
python batch_vectorize.py --aggressive-mode

# Preview only (no writes)
python batch_vectorize.py --dry-run --limit 100
```

---

## Expected Output

### Phase 1 report
```
RECONCILIATION REPORT
  FTP availability:  ~3,000,000 unique PMCIDs
  DB coverage:       ~500,000 articles
  MinIO inventory:   ~450,000 PDFs
  Coverage:          ~15% of FTP

  Gaps:
    DB articles not in MinIO: 50,000   ← download + vectorize
    FTP articles not in DB:   2.5M+    ← out of scope (not yet ingested)
    FTP+DB but not MinIO:     ~50,000  ← priority downloadable gap
```

### Phase 2 progress
```
✓ PMC11721717 (2.3 MB)
✓ PMC11700456 (5.1 MB)
...
Download complete: 50,000 succeeded, 120 failed
```

---

## Configuration

All services read from `.env` (copy from `.env-example` in parent dir):

```bash
# MinIO
MINIO_ENDPOINT=minio-api.ncsa.ai
MINIO_BUCKET=pubmed
MINIO_SECURE=true

# Postgres
POSTGRES_DSN=postgresql://...

# Ollama (Phase 3 only)
EMBEDDING_BASE_URL=https://ollama.example.com/api/embeddings
```

---

## Checkpoint / Resume

All phases support checkpoint-based resume:

```bash
# Phase 1 cache (FTP crawl, ~1.5 MB)
cat ftp_crawl_checkpoint.json | head -5

# Phase 2 progress
cat ftp_download_checkpoint.json | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('stats', d))"

# Phase 3 progress
cat batch_vectorize_checkpoint.json | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('processed_count'), 'processed')"
```

---

## Performance

| Phase | Typical duration | Bottleneck | Tuning |
|-------|-----------------|------------|--------|
| Phase 1 | 30–60 min | FTP server + CSV download | Use cached checkpoint with `--no-crawl` |
| Phase 2 | 4–12 hours | Network bandwidth | `--workers 16` if bandwidth allows |
| Phase 3 | 8–24 hours | Ollama embedding throughput | 4 workers, `--aggressive-mode` |

---

## Notes

- Phase 1 uses NCBI's `oa_file_list.csv` (CSV-based enumeration), not recursive FTP crawl — much faster and more reliable.
- The checkpoint files (`ftp_reconciliation_result.json`, `ftp_download_checkpoint.json`) are runtime artifacts and are not committed to Git.
- `ftp_ingest_missing_db.py` resolves PMCIDs not yet in the DB by calling the NCBI idconv API. ~25% will not have a PMID (conference papers, books, etc.) — this is expected.
