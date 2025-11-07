# Daily PubMed OA Pipeline

Automates ingestion and vectorization of Open Access PubMed Central (PMC) articles. Each run:
- Fetches PubMed daily update XML files from NCBI.
- Discovers OA PDFs via the PMC OA Web Service.
- Uploads PDFs to MinIO using the original `oa_pdf` directory structure.
- Vectorizes PDFs via an Ollama-compatible embeddings API and upserts chunks to Qdrant.
- Upserts article metadata into Supabase Postgres (`pdf_location`, `vector_indexed = true`, etc.).

Progress and retries are tracked via Postgres tables (`vyraid.xml_processing_log`, `vyraid.xml_failed_downloads`) and a local state file (`pipeline_state.json`).

---

## Prerequisites

- Python 3.11 (recommended via a `conda` env named `vyraid-daily`).
- Access to Supabase Postgres, MinIO/S3 endpoint, Qdrant, and an embeddings endpoint (Ollama-style `/embeddings`).
- System libs required by PyMuPDF (`pymupdf`) installed on the host.

---

## Setup

1. Clone and enter the repo
   ```bash
   git clone <repo-url>
   cd daily_update_and_vectorization
   ```

2. Create and activate environment
   ```bash
   conda create -n vyraid-daily python=3.11 -y
   conda activate vyraid-daily
   ```

3. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

4. Configure environment
   - Edit `.env` with your values:
     - `POSTGRES_DSN`, `DB_SCHEMA`
     - `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`, `MINIO_SECURE`
     - `EMBEDDING_BASE_URL`
     - `QDRANT_URL`, `QDRANT_API_KEY`, `QDRANT_COLLECTION`
     - Optional: `MAX_WORKERS`, `XML_LOG_EVERY_N`, `TEST_LIMIT`, `FILE_LOCATION`, `PIPELINE_STATE_LOG`
   - Ensure `PIPELINE_STATE_LOG` points to your state file path (default `pipeline_state.json`).

5. Prepare database tables (if not present)
   ```sql
   CREATE TABLE vyraid.xml_processing_log (
     xml_filename VARCHAR(20) UNIQUE PRIMARY KEY,
     last_processed_pmcid VARCHAR(20),
     total_processing_time INTERVAL,
     total_pmcid_processed INTEGER DEFAULT 0,
     processed_all_pmcid BOOLEAN DEFAULT FALSE
   );

   CREATE TABLE vyraid.xml_failed_downloads (
     xml_filename VARCHAR(20) NOT NULL,
     failed_pmcid VARCHAR(20) NOT NULL,
     failure_timestamp TIMESTAMPTZ DEFAULT NOW(),
     failure_reason TEXT,
     PRIMARY KEY (xml_filename, failed_pmcid),
     CONSTRAINT fk_processing_state
       FOREIGN KEY (xml_filename)
       REFERENCES vyraid.xml_processing_log(xml_filename)
       ON DELETE CASCADE
   );
   ```

---

## Running Manually

- Full run:
  ```bash
  python main.py
  ```
- Test with limit:
  ```bash
  python main.py --limit 5
  ```

`main.py` updates `pipeline_state.json` with `RUNNING`, `COMPLETED`, or `FAILED`. `check_pipeline_state.py` reads that file and verifies the PID to prevent overlapping runs.

---

## Cron Automation

Use `cron_wrapper.sh` to manage runs and auto-recover from failures.

- Behavior:
  - Activates `vyraid-daily` conda env (with fallbacks).
  - Loads `.env`.
  - Runs `python check_pipeline_state.py` and branches on exit code:
    - `0`: Ready → runs `python main.py`.
    - `1`: Already running → skips.
    - `2`: Failed → backs up `pipeline_state.json` to `pipeline_state.failed.<timestamp>.json`, deletes the original, then runs.
    - `3`: State unreadable → logs warning, runs anyway.

- Example crontab (every 12 hours):
  ```
  0 */12 * * * /projects/uiucchat/vyraid_pubmed_daily_update/vyraid-ftp/daily_update_and_vectorization/cron_wrapper.sh >> /var/log/pipeline_cron.log 2>&1
  ```

- Before adding cron:
  ```bash
  chmod +x cron_wrapper.sh
  ```
  Ensure the cron user can read/write the repo directory, access the conda env, and reach NCBI/MinIO/Qdrant/embedding endpoints.

---

## Operational Notes

- State backups: On failure (exit code 2), the wrapper saves a timestamped copy of `pipeline_state.json`.
- Failed PMCIDs: Inspect `vyraid.xml_failed_downloads` (e.g., `pdf_not_found_http_404`, network errors, vectorization failures).
- Resuming: The pipeline resumes within an XML using `last_processed_pmcid` from `xml_processing_log`.
- Qdrant indexing threshold: Temporarily set to `0` during bulk ingest, restored to `10,000` afterward (even on exceptions).
- Testing: Use `TEST_LIMIT` in `.env` or `--limit` to cap successful items for dry runs.

---

## Troubleshooting

- MinIO 530/403/404: Verify credentials, bucket, and `MINIO_SECURE` (must be `true` for HTTPS endpoints behind Cloudflare). A stored `pdf_not_found_http_404` means the NCBI PDF URL returned 404 and was skipped.
- psycopg2 OperationalError: Automatic retries are in place; repeated failures usually indicate network or auth issues.
- Embedding timeouts: Check logs under `pipeline.vectorize`. The PDF is in MinIO under the recorded key for manual inspection.
- Stuck state: If a process was killed, clear or fix `pipeline_state.json` before restarting.