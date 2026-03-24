# PubMed Daily Update & Vectorization Pipeline

Automates continuous ingestion and vectorization of Open Access PubMed Central (PMC) articles for use in UIUC.chat RAG retrieval.

Each daily run:
- Fetches PubMed XML update files from NCBI
- Queries the PMC OA Web Service to discover freely downloadable PDFs
- Uploads PDFs to MinIO preserving the `oa_pdf/` directory structure
- Embeds text chunks via Ollama (`nomic-embed-text:v1.5`) and upserts to Qdrant
- Records article metadata and progress in Supabase Postgres

---

## Architecture

### Two-Stage Daily Pipeline (primary workflow)

```
Stage 1 — Download (I/O bound, 16 workers)       Stage 2 — Vectorize (CPU/GPU bound, 4 workers)
  run_daily_updates.sh → main.py                    run_vectorization.sh → batch_vectorize.py
  ┌──────────────────────────────┐                  ┌──────────────────────────────────────┐
  │ 1. Pull NCBI XML updates     │                  │ 1. Query Postgres for articles with  │
  │ 2. OA API → PDF download URL │                  │    pdf_location but vector_indexed=F │
  │ 3. Fetch PDF → MinIO upload  │    ──────────→   │ 2. Download PDF from MinIO           │
  │ 4. Upsert Postgres row       │                  │ 3. Chunk (7000 chars, 200 overlap)   │
  │    (skip vectorization)      │                  │ 4. Embed via Ollama, upsert Qdrant   │
  └──────────────────────────────┘                  │ 5. Mark vector_indexed=true          │
                                                    └──────────────────────────────────────┘
```

Stage 1 runs once daily; Stage 2 runs continuously (every 30 min via cron) to drain the backlog.

### Supporting Workflows

| Script | Purpose | When to use |
|--------|---------|-------------|
| `vectorize_minio_direct.py` | Bulk vectorize existing MinIO PDFs without Postgres dependency | Large catch-up runs |
| `ftp/ftp_reconciliation_workflow.sh` | Full FTP→MinIO gap recovery (3-phase) | Periodic reconciliation |
| `check_pipeline_state.py` | Prevent duplicate `main.py` runs | Called by `cron_wrapper.sh` |

---

## Setup

### 1. Clone & Install

```bash
cd /home/dadams/pub-med-daily/ai-ta-backend
pip install -r ai_ta_backend/utils/vyraid-pubmed-daily-update-and-vectorization/requirements.txt
```

### 2. Configure Environment

```bash
cp .env-example .env
# Edit .env with your values (see below)
```

Key environment variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `POSTGRES_DSN` | Supabase Postgres connection string | `postgresql://user:pass@host:5432/db` |
| `DB_SCHEMA` | Postgres schema | `vyraid` |
| `MINIO_ENDPOINT` | MinIO API endpoint | `minio-api.ncsa.ai` |
| `MINIO_BUCKET` | Bucket name | `pubmed` |
| `MINIO_SECURE` | Use HTTPS | `true` |
| `EMBEDDING_BASE_URL` | Ollama embeddings endpoint | `http://localhost:11434/api/embeddings` |
| `QDRANT_URL` | Qdrant host | `http://localhost` |
| `QDRANT_PORT` | Qdrant port | `6333` |
| `QDRANT_COLLECTION` | Collection name | `ncbi_pdfs` |
| `VECTOR_SIZE` | Embedding dimensions | `768` |
| `CHUNK_SIZE` | Characters per chunk | `7000` |
| `CHUNK_OVERLAP` | Overlap between chunks | `200` |
| `MAX_WORKERS` | Parallel download threads | `16` (Stage 1), `4` (Stage 2) |

### 3. Database Tables (if not yet created)

```sql
CREATE TABLE vyraid.xml_processing_log (
  xml_filename          VARCHAR(20) UNIQUE PRIMARY KEY,
  last_processed_pmcid  VARCHAR(20),
  total_processing_time INTERVAL,
  total_pmcid_processed INTEGER DEFAULT 0,
  processed_all_pmcid   BOOLEAN DEFAULT FALSE
);

CREATE TABLE vyraid.xml_failed_downloads (
  xml_filename      VARCHAR(20) NOT NULL,
  failed_pmcid      VARCHAR(20) NOT NULL,
  failure_timestamp TIMESTAMPTZ DEFAULT NOW(),
  failure_reason    TEXT,
  PRIMARY KEY (xml_filename, failed_pmcid),
  CONSTRAINT fk_processing_state
    FOREIGN KEY (xml_filename) REFERENCES vyraid.xml_processing_log(xml_filename)
    ON DELETE CASCADE
);
```

---

## Running Manually

### Stage 1 — Download new articles

```bash
cd ai-ta-backend/ai_ta_backend/utils/vyraid-pubmed-daily-update-and-vectorization
export $(grep -v '^#' /home/dadams/pub-med-daily/.env.production | xargs)

# Daily update files (primary use)
python main.py --source updatefiles --skip-vectorization

# Test with a limit
python main.py --source updatefiles --skip-vectorization --limit 10
```

### Stage 2 — Vectorize the backlog

```bash
# Process all articles with pdf_location but vector_indexed=false
python batch_vectorize.py --workers 4 --aggressive-mode

# Dry run (no writes)
python batch_vectorize.py --dry-run --limit 5
```

### Bulk MinIO vectorization (catch-up only)

```bash
# Vectorize PDFs already in MinIO without going through Postgres backlog
python vectorize_minio_direct.py --local-qdrant --collection pubmed_v2 --workers 4
```

---

## Cron Automation

### Recommended crontab

```crontab
# Stage 1: Download new NCBI update files daily at 2:00 AM
0 2 * * * /home/dadams/pub-med-daily/ai-ta-backend/ai_ta_backend/utils/vyraid-pubmed-daily-update-and-vectorization/run_daily_updates.sh >> /var/log/pubmed_daily.log 2>&1

# Stage 2: Drain vectorization backlog every 30 minutes
*/30 * * * * /home/dadams/pub-med-daily/ai-ta-backend/ai_ta_backend/utils/vyraid-pubmed-daily-update-and-vectorization/run_vectorization.sh >> /var/log/pubmed_vectorize.log 2>&1
```

### Using cron_wrapper.sh (safe overlap prevention)

`cron_wrapper.sh` wraps `main.py` with:
- Conda env activation
- Duplicate run prevention (checks `pipeline_state.json` + live PID)
- Auto-recovery from FAILED state (backs up and retries)

```crontab
# Alternative: run_daily with state protection
0 2 * * * /home/dadams/pub-med-daily/ai-ta-backend/ai_ta_backend/utils/vyraid-pubmed-daily-update-and-vectorization/cron_wrapper.sh >> /var/log/pubmed_cron.log 2>&1
```

Make executable first:
```bash
chmod +x run_daily_updates.sh run_vectorization.sh cron_wrapper.sh
```

---

## FTP Reconciliation (Periodic)

For recovering gaps between NCBI FTP, Postgres DB, and MinIO — see [ftp/README.md](ftp/README.md).

```bash
# Full end-to-end (8-worker download)
bash ftp/ftp_reconciliation_workflow.sh 8
```

---

## Key Parameters

| Parameter | Value | Location |
|-----------|-------|----------|
| Chunk size | 7,000 chars | `CHUNK_SIZE` in `.env` |
| Chunk overlap | 200 chars | `CHUNK_OVERLAP` in `.env` |
| Embedding batch upsert | 125 chunks | `EMBED_BATCH_UPSERT` in `.env` |
| Vector dimensions | 768 | `VECTOR_SIZE` in `.env` (nomic-embed-text:v1.5) |
| Qdrant batch (Stage 2) | 5,000 chunks | `batch_vectorize.py --aggressive-mode` |

---

## Monitoring

### Quick status

```bash
# Infrastructure health
python scripts/inspect_qdrant.py
python scripts/vectorize_status.py

# Production summary (Postgres + MinIO + Qdrant)
python scripts/quick_prod_summary.py

# Recent MinIO activity
python scripts/list_recent_minio.py

# Live pipeline monitor (DB-backed)
python scripts/monitor_pipeline.py
```

### Pipeline state

```bash
# Check if main.py is running
python check_pipeline_state.py

# Tail state file
tail -f pipeline_state/pipeline_state.json
```

### Useful SQL (run against Supabase)

```sql
-- Articles by PDF availability
SELECT
  COUNT(*) FILTER (WHERE pdf_location IS NOT NULL) AS has_pdf,
  COUNT(*) FILTER (WHERE vector_indexed = true)     AS vectorized,
  COUNT(*)                                           AS total
FROM vyraid.articles;

-- Recent additions (last 24h)
SELECT COUNT(*) FROM vyraid.articles
WHERE created_at > NOW() - INTERVAL '24 hours';

-- Vectorization backlog
SELECT COUNT(*) FROM vyraid.articles
WHERE pdf_location IS NOT NULL AND vector_indexed = false;
```

See `scripts/failure_analysis.sql`, `scripts/quick_prod_summary.sql`, and `scripts/prod_index_setup.sql` for more queries.

---

## Scripts Reference

| Script | Purpose |
|--------|---------|
| `scripts/quick_prod_summary.py` | Snapshot: article counts, recent XML files, MinIO/Qdrant stats |
| `scripts/quick_test_summary.py` | Validate pipeline completeness from pipeline_state.json |
| `scripts/vectorize_status.py` | Vectorization backlog: vectorized vs. pending PDFs |
| `scripts/inspect_qdrant.py` | List Qdrant collections, point counts, sample payloads |
| `scripts/list_recent_minio.py` | Show 25 most recently modified MinIO objects |
| `scripts/minio_pdfgz_manifest.py` | Generate timestamped CSV manifests of MinIO PDFs; supports `--delete --yes` |
| `scripts/oa_ftp_minio_mirror.sh` | Multi-phase FTP→MinIO ingestion orchestrator (`audit`, `stage`, `sync`, `cleanup`) |
| `scripts/monitor_pipeline.py` | Real-time pipeline progress monitor (article stats by date) |
| `scripts/failure_analysis.sql` | SQL queries for PDF download failure diagnostics |
| `scripts/quick_prod_summary.sql` | Fast indexed queries: counts, recent additions, date ranges |
| `scripts/prod_index_setup.sql` | Create performance indexes on Supabase (run once) |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `pipeline_state.json` shows FAILED | Previous crash | `cron_wrapper.sh` auto-recovers; or manually delete the file |
| MinIO 530/403 errors | Credentials or HTTPS mismatch | Check `MINIO_SECURE=true` for HTTPS endpoints |
| Embedding timeout | Ollama overloaded | Reduce `--workers` or check Ollama health |
| Qdrant upsert errors | Collection missing | Verify `QDRANT_COLLECTION` exists with `scripts/inspect_qdrant.py` |
| `psycopg2 OperationalError` | Network / auth | Connection retries are built in; check DSN and Supabase status |
| Stage 2 processes same article repeatedly | `vector_indexed` not updating | Check Postgres write permissions for the DSN user |

For failed PMCIDs: `SELECT * FROM vyraid.xml_failed_downloads ORDER BY failure_timestamp DESC LIMIT 50;`

---

## File Layout

```
vyraid-pubmed-daily-update-and-vectorization/
├── main.py                    # Core Stage 1 pipeline
├── batch_vectorize.py         # Stage 2: backlog vectorizer
├── vectorize_minio_direct.py  # Bulk MinIO vectorizer (catch-up)
├── check_pipeline_state.py    # Readiness check (used by cron_wrapper)
├── run_daily_updates.sh       # Stage 1 cron entry point
├── run_vectorization.sh       # Stage 2 cron entry point
├── cron_wrapper.sh            # Safe wrapper with state management
├── requirements.txt
├── .env-example               # Copy to .env and fill in values
├── shared/                    # Shared modules (config, DB, models, state)
├── ftp/                       # FTP reconciliation suite (periodic use)
│   ├── README.md
│   ├── ftp_reconciliation.py
│   ├── ftp_download_missing.py
│   ├── ftp_ingest_missing_db.py
│   └── ftp_reconciliation_workflow.sh
├── scripts/                   # Diagnostic & operational tools
│   └── (see Scripts Reference above)
└── pipeline_state/            # Runtime state files (not committed)
```
