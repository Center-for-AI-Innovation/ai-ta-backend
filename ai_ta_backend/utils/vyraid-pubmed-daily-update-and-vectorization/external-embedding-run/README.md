# PubMed Re-vectorization — External Embedding Run

A self-contained toolkit for **bulk / catch-up** re-vectorization of the PubMed
PDF corpus on an HPC SLURM cluster (H100 GPUs). It is **not** part of the daily
cron — use it for one-off full re-embeds or large backfills that benefit from
multi-GPU throughput. The daily pipeline's incremental Stage 2 lives one level up
(`batch_vectorize.py`); the Postgres-driven catch-up tool is `vectorize_pg_driven.py`.

Reads PDFs from MinIO, extracts text with PyMuPDF, chunks with
`RecursiveCharacterTextSplitter` (chunk_size=1000, overlap=200), embeds with
Ollama `nomic-embed-text:v1.5` (768-dim), and upserts to Qdrant `pubmed_v2`
(Cosine, on-disk).

## Quick Start

```bash
bash discover_system.sh                 # 1. inspect GPUs / LLMFlux / Apptainer / network
cp .env.template .env && $EDITOR .env   # 2. fill in credentials
pip install -r requirements.txt         # 3. install deps
sbatch launch_ollama.sh                 # 4. Ollama via LLMFlux Apptainer (preferred)
#   or: sbatch launch_ollama_native.sh  #      native Ollama fallback
python test_connectivity.py             # 5. verify MinIO / Qdrant / Ollama / Supabase
python revectorize.py --source both --workers 8 --aggressive-mode --skip-dedup 2>&1 | tee revectorize.log
```

## Files

| File | Purpose |
|------|---------|
| `revectorize.py` | Main vectorization script (MinIO → chunk → embed → Qdrant) |
| `discover_system.sh` | System discovery: GPUs, LLMFlux, Apptainer, network |
| `launch_ollama.sh` | SLURM job: Ollama via LLMFlux Apptainer container (preferred) |
| `launch_ollama_native.sh` | SLURM job: native Ollama (fallback) |
| `test_connectivity.py` | Verify MinIO, Qdrant, Ollama, Supabase reachability |
| `.env.template` | Environment variable template |
| `requirements.txt` | Python dependencies |

## Parameters

| Parameter | Value |
|-----------|-------|
| Embedding model | `nomic-embed-text:v1.5` (137M, F16, nomic-bert) |
| Vector dimensions | 768 |
| Chunk size / overlap | 1000 / 200 characters |
| Distance metric | Cosine (on-disk vectors + payload) |
| Collection | `pubmed_v2` |

> Chunk size 1000 is required by the model; changing it means re-vectorizing.

## Architecture

```
MinIO (2M+ PDFs)  ──→  PyMuPDF extract  ──→  RecursiveCharacterTextSplitter
                                                    │  (1000 / 200)
Supabase articles ──→  pdf_location keys ──────────┘
                                                    ▼
                                   Ollama  nomic-embed-text:v1.5  (768-dim)
                                                    ▼
                                   Qdrant  pubmed_v2  (Cosine, on_disk)
```

**Sources** (`--source`): `minio` lists all `.pdf` objects in the `pubmed`
bucket (~2M, keyed `{hex2}/{hex2}/{name}.PMC{id}.pdf`); `supabase` queries
`vyraid.articles WHERE pdf_location IS NOT NULL`; `both` is the deduped union.

**Per-point payload:**
```json
{
  "page_content": "chunk text ...",
  "s3_path": "46/1d/pnas.202319160.PMC10998587.pdf",
  "readable_filename": "pnas.202319160.PMC10998587.pdf",
  "pagenumber": 3,
  "chunk_index": 7,
  "total_chunks": 42,
  "pmcid": "PMC10998587"
}
```

During bulk import the collection runs with `indexing_threshold=0`, restored to
`10000` on completion.

## CLI Reference

```bash
python revectorize.py --dry-run --source minio --limit 10     # dry run, no writes
python revectorize.py --source minio --limit 100 --workers 4  # small test
python revectorize.py --source both --workers 8 --aggressive-mode --skip-dedup  # full run
python revectorize.py --source both --workers 8 --aggressive-mode --resume      # resume
python revectorize.py --drop-collection --confirm-drop        # DESTRUCTIVE
python revectorize.py --reset-supabase                        # reset vector_indexed tracking
```

`--manifest <csv>` skips the multi-minute MinIO scan by reading a pre-built key list.

## SLURM configuration

Edit the `#SBATCH` headers in `launch_ollama.sh` / `launch_ollama_native.sh`
(run `discover_system.sh` to see available partitions/accounts):

```bash
#SBATCH --partition=gpu       # GPU partition
#SBATCH --account=<account>   # SLURM account
#SBATCH --qos=<qos>           # QOS
#SBATCH --gres=gpu:4          # GPUs
#SBATCH --time=48:00:00       # wall time
```

LLMFlux (https://github.com/Center-for-AI-Innovation/LLMFlux) ships an Apptainer
SIF (`llm_processor.sif`) with Ollama + CUDA preconfigured; `launch_ollama.sh`
reuses it to run `ollama serve` persistently. Find the SIF under `~/.llmflux/`,
via `python3 -c "import llmflux; print(llmflux.__path__[0])"`, or build it with
`apptainer build llm_processor.sif container.def`.

## Multi-GPU throughput

Single Ollama with `OLLAMA_SCHED_SPREAD=1` spreads work across all GPUs. For more
throughput, run one Ollama per GPU on consecutive ports and round-robin over them:

```bash
for i in 0 1 2 3; do CUDA_VISIBLE_DEVICES=$i OLLAMA_HOST=0.0.0.0:$((11434+i)) ollama serve & done

python revectorize.py --embedding-urls \
  http://localhost:11434/api/embeddings http://localhost:11435/api/embeddings \
  http://localhost:11436/api/embeddings http://localhost:11437/api/embeddings \
  --workers 16 --aggressive-mode --skip-dedup
```

## Checkpoint / resume

State is written to `revectorize_checkpoint.json` every 25 articles
(`completed_keys`, `failed`, `stats`, `last_key`, `timestamp`). `--resume` loads
`completed_keys` so finished PDFs are skipped. If the checkpoint is corrupt,
delete it and restart with `--skip-dedup` (skips the Qdrant scroll).

## Monitoring

```bash
tail -f revectorize.log
curl -s -H "api-key: $QDRANT_API_KEY" "$QDRANT_URL/collections/pubmed_v2" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['result']['points_count'])"
watch -n 2 nvidia-smi
```

## Scale & network

~2M PDFs × ~50 chunks ≈ 50–100M+ points; size the Qdrant tier accordingly.
All connections are outbound from the SLURM node: MinIO `minio-api.ncsa.ai:443`
(HTTPS), Qdrant `$QDRANT_URL:443` (HTTPS), Supabase `db.*.supabase.co:6543`
(Postgres), Ollama `localhost:11434` (HTTP).

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Ollama unreachable | Check SLURM logs (`cat ollama-*.log`); `curl http://localhost:11434/api/tags`; verify GPUs with `nvidia-smi` |
| MinIO unreachable | Firewall; test `curl -I https://minio-api.ncsa.ai/` |
| Qdrant unreachable | Verify key: `curl -H "api-key: $QDRANT_API_KEY" "$QDRANT_URL/collections"` |
| Embedding slow | Check GPU util; use multi-instance Ollama with `--embedding-urls` |
| Out of memory | Lower `--workers`; check `/tmp` space for PDF temp files |
| Empty/corrupt PDFs | Handled gracefully (`EmptyFileError`); logged and skipped |
| Checkpoint corrupt | Delete `revectorize_checkpoint.json`, restart with `--skip-dedup` |

## Key functions (`revectorize.py`)

`load_config` (env + CLI, aggressive mode) · `QdrantWriter`
(ensure_collection / set_bulk_mode / upsert_batch / get_vectorized_paths) ·
`get_embedding` (round-robin multi-endpoint) · `chunk_text` ·
`vectorize_object` (one PDF) · `gather_work_list` (minio/supabase/both) ·
`run` (health check → gather → dedup → ThreadPoolExecutor → checkpoint) ·
`drop_and_recreate_collection` · `reset_supabase_vector_indexed` ·
`ProgressTracker` (thread-safe ETA/throughput).
