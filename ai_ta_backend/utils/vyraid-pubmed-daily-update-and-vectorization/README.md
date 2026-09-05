# PubMed Daily Update & Vectorization Pipeline

Continuously ingests and vectorizes Open Access PubMed Central (PMC) articles for
UIUC.chat RAG retrieval.

Each daily run:
- Fetches PubMed XML update files from NCBI
- Queries the PMC OA Web Service to find freely downloadable PDFs
- Uploads PDFs to MinIO, preserving the `oa_pdf/` layout
- Records article metadata in Supabase Postgres with `vector_indexed=false`
- (Separately) embeds text chunks via Ollama `nomic-embed-text:v1.5` and upserts to Qdrant

---

## Architecture — two stages

```
Stage 1 — Download (I/O bound)                  Stage 2 — Vectorize (GPU bound)
  run_daily_updates.sh → main.py                  run_vectorization.sh → batch_vectorize.py
  ┌──────────────────────────────┐                ┌──────────────────────────────────────┐
  │ 1. Pull NCBI XML updates     │                │ 1. Query Postgres: pdf_location set, │
  │ 2. OA API → PDF download URL │                │    vector_indexed=false (keyset page)│
  │ 3. Fetch PDF → MinIO upload  │   ─────────→   │ 2. Download PDF from MinIO           │
  │ 4. Upsert Postgres row,      │                │ 3. Chunk (1000 chars, 200 overlap)   │
  │    vector_indexed=false      │                │ 4. Embed via Ollama, upsert Qdrant   │
  └──────────────────────────────┘                │ 5. Mark vector_indexed=true          │
                                                  └──────────────────────────────────────┘
```

Stage 1 (`main.py`) is **download-only** — it never touches Qdrant or any embedding
endpoint. Stage 2 (`batch_vectorize.py`) drains whatever Stage 1 leaves behind.
Stage 1 runs once daily; Stage 2 runs every 30 min to keep the backlog drained.

### Catch-up / backfill (preserved alongside the daily cron)

These tools re-vectorize or backfill outside the incremental daily flow:

| Tool | Use |
|------|-----|
| `batch_vectorize.py --limit 0` | Drain an arbitrarily large Postgres backlog (also the Stage 2 cron) |
| `vectorize_pg_driven.py` | **Primary catch-up** — Postgres-driven, O(1) memory, no bucket scan, shardable via `--from-pmid/--to-pmid` |
| `vectorize_minio_direct.py` | **Deprecated** — legacy full-MinIO-scan, kept as the vetting baseline; remove once `vectorize_pg_driven.py` is vetted |
| `scripts/postgres_vs_minio_audit.py` | Read-only two-way audit: is Postgres a faithful index of MinIO? Run before retiring the legacy tool |
| `external-embedding-run/` | HPC/SLURM multi-GPU **bulk** re-embed of the whole corpus (see its README) |

### Embedding endpoint configuration

Stage 2 reads `EMBEDDING_URLS` (comma-separated, linear fallback); if unset it
falls back to a single `EMBEDDING_BASE_URL`. Stage 1 ignores both.

```bash
# localhost first (reliable); secret-ollama only as last resort.
EMBEDDING_URLS=http://localhost:11434/api/embeddings,https://secret-ollama.ncsa.ai/api/embeddings
```

> `secret-ollama.ncsa.ai` is unreliable — it can return HTTP 200 with an empty
> embedding. `shared/vectorize.py:get_embedding` rejects any vector whose length
> isn't `VECTOR_SIZE` (768) and falls through to the next URL, so keep localhost first.

Verify any endpoint:
```bash
curl -X POST $URL -H 'Content-Type: application/json' \
  -d '{"model":"nomic-embed-text:v1.5","prompt":"hi"}' | jq '.embedding | length'   # expect 768
```

---

## Setup

### 1. Install
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env-example .env     # then edit values
```

Both the shell wrappers and the Python entrypoints resolve the env file the same
way (`shared/env.py:load_project_env`, zero-dep, idempotent):

1. `$ENV_FILE` if set (absolute/relative, honors `~`)
2. `.env` next to the script
3. `.env.production` next to the script

If none resolve, the env is assumed pre-exported. On the production host,
`.env.production` is a gitignored symlink to `/home/dadams/pub-med-daily/.env.production`.

Key variables (see `.env-example` for the full list):

| Variable | Description | Example |
|----------|-------------|---------|
| `POSTGRES_DSN` | Supabase Postgres connection string | `postgresql://user:pass@host:5432/db` |
| `DB_SCHEMA` | Postgres schema | `vyraid` |
| `MINIO_ENDPOINT` / `MINIO_SECURE` | MinIO API host / use HTTPS | `minio-api.ncsa.ai` / `true` |
| `MINIO_BUCKET` | Bucket | `pubmed` |
| `EMBEDDING_URLS` | Stage 2 embedding endpoints (fallback order) | see above |
| `QDRANT_URL` / `QDRANT_COLLECTION` | Qdrant host / collection | `https://qdrant.ncsa.ai` / `pubmed_v2`* |
| `VECTOR_SIZE` | Embedding dims | `768` |
| `CHUNK_SIZE` / `CHUNK_OVERLAP` | Chunking | `1000` / `200` |
| `MAX_WORKERS` | Parallel workers | `16` (Stage 1) / `4` (Stage 2) |

\* `.env-example` defaults the collection to `ncbi_pdfs`; production uses `pubmed_v2`.
`CHUNK_SIZE=1000` is required by `nomic-embed-text:v1.5` — changing it means re-vectorizing.

### 3. Database tables (if not already present)
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

## Running manually

```bash
# Stage 1 — download new articles (no embedding dependency)
python main.py --source updatefiles            # daily update files (primary)
python main.py --source updatefiles --limit 10 # quick test

# Stage 2 — vectorize the backlog
python batch_vectorize.py --workers 4 --aggressive-mode
python batch_vectorize.py --dry-run --limit 5  # no writes

# Catch-up — Postgres-driven (primary)
python vectorize_pg_driven.py --workers 4
#   A/B into a temp collection:
python vectorize_pg_driven.py --workers 2 --limit 100 --collection pubmed_v2_vet_pgdriven
```

Stage 1 records each article in `vyraid.articles` with `vector_indexed=false` so
Stage 2 picks it up. Stage 2 pages the backlog with **keyset** pagination
(`pmid < cursor`), not OFFSET, so a shrinking result set never skips rows.

---

## Cron automation

The canonical schedule is checked in as **`crontab_pubmed.txt`**. Install it:

```bash
crontab crontab_pubmed.txt          # replaces the crontab
crontab -l                          # verify
```

It runs Stage 1 daily at **16:00 CDT** (safely after NCBI's ~14:03 ET posting; a
missed day self-heals since each run catches up all missing files) and Stage 2
every 30 minutes. Both runners are `flock`-guarded with distinct locks
(`/tmp/pubmed_stage1.lock`, `/tmp/pubmed_vectorize.lock`), so ticks never overlap
and the two stages may run concurrently.

`run_vectorization.sh` first calls **`ensure_embedding_endpoint.sh`**, a three-tier
preflight: probe remote `secret-ollama` → probe local Ollama on
`127.0.0.1:11434` (pull `nomic-embed-text:v1.5` if missing) → spawn a fresh
`~/bin/ollama serve` (GPU auto-detected via `nvidia-smi -L`, else CPU). It always
exits 0; `get_embedding` has its own per-URL retry/backoff for endpoints that
recover mid-run.

**Optional state-guard alternative:** `cron_wrapper.sh` wraps Stage 1 with conda
activation, duplicate-run prevention (`pipeline_state.json` + live PID via
`check_pipeline_state.py`), and auto-recovery from a FAILED state. Swap it in for
`run_daily_updates.sh` in the crontab if you want that extra protection.

---

## Monitoring

```bash
python scripts/quick_prod_summary.py        # Postgres + MinIO + Qdrant snapshot
python scripts/vectorize_status.py          # vectorized vs. pending backlog
python scripts/monitor_pipeline.py          # live progress (article stats by date)
python scripts/postgres_vs_minio_audit.py   # read-only Postgres↔MinIO audit
python check_pipeline_state.py              # is Stage 1 running?
```

```sql
-- PDF availability & backlog
SELECT COUNT(*) FILTER (WHERE pdf_location IS NOT NULL) AS has_pdf,
       COUNT(*) FILTER (WHERE vector_indexed) AS vectorized,
       COUNT(*) AS total
FROM vyraid.articles;

-- Vectorization backlog
SELECT COUNT(*) FROM vyraid.articles WHERE pdf_location IS NOT NULL AND NOT vector_indexed;

-- Recent failed downloads
SELECT * FROM vyraid.xml_failed_downloads ORDER BY failure_timestamp DESC LIMIT 50;
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `pipeline_state.json` shows FAILED | Previous crash | `cron_wrapper.sh` auto-recovers, or delete the file |
| MinIO 530/403 errors | Credentials / HTTPS mismatch | Set `MINIO_SECURE=true` for HTTPS endpoints |
| Embeddings empty or 768-len rejects | `secret-ollama` returning empty vectors | Ensure localhost Ollama is up and first in `EMBEDDING_URLS` |
| Embedding timeout | Ollama overloaded | Lower `--workers`; check `nvidia-smi` |
| Qdrant upsert errors | Collection missing | Verify `QDRANT_COLLECTION` exists / dims = 768 |
| `psycopg2 OperationalError` | Network / auth | Retries are built in; check DSN + Supabase status |
| Stage 2 reprocesses same article | `vector_indexed` not updating | Check Postgres write perms for the DSN user |

---

## Known caveats

- **NCBI FTP deprecation (deadline Aug 2026):** `main.py` fetches PDFs from NCBI's
  `/pub/pmc/deprecated/oa_pdf/` HTTPS path, which NCBI removes in **August 2026**.
  This is a stopgap — migrate Stage 1 to the `pmc-oa-opendata` AWS S3 bucket before then.
- **`secret-ollama` empty embeddings:** see the embedding-endpoint note above; keep
  localhost first in `EMBEDDING_URLS`.

---

## File layout

```
vyraid-pubmed-daily-update-and-vectorization/
├── main.py                     # Stage 1: download (NCBI → OA API → MinIO → Postgres)
├── batch_vectorize.py          # Stage 2: incremental backlog vectorizer
├── vectorize_pg_driven.py      # Catch-up: Postgres-driven (primary)
├── vectorize_minio_direct.py   # Catch-up: legacy MinIO-scan (deprecated baseline)
├── check_pipeline_state.py     # Readiness check (used by cron_wrapper.sh)
├── run_daily_updates.sh        # Stage 1 cron entry point (flock-guarded)
├── run_vectorization.sh        # Stage 2 cron entry point (flock-guarded)
├── ensure_embedding_endpoint.sh# Stage 2 embedding-endpoint preflight
├── cron_wrapper.sh             # Optional state-guard wrapper for Stage 1
├── crontab_pubmed.txt          # Canonical, installable crontab
├── .env-example                # Copy to .env
├── requirements.txt
├── shared/                     # config, database, vectorize, env, state_logger, models
├── scripts/                    # Diagnostics: quick_prod_summary, vectorize_status,
│                               #   monitor_pipeline, postgres_vs_minio_audit
├── external-embedding-run/     # Separate HPC/SLURM bulk re-vectorization toolkit
└── pipeline_state/             # Runtime state files (gitignored)
```
