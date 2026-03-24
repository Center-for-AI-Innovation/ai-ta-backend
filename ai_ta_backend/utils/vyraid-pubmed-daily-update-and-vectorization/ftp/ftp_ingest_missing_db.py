#!/usr/bin/env python3
"""
Ingest FTP-only PMCIDs into the DB by resolving PMCIDs -> PMIDs via NCBI idconv.

Inputs:
- ftp_reconciliation_result.json (uses gaps['ftp_not_in_db'])

Outputs:
- Inserts/updates vyraid.articles with pmid+pmcid
- Checkpoint file for resume
"""

import os
import json
import time
import logging
from typing import Dict, List

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv('.env.production')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

IDCONV_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
CHECKPOINT_FILE = "ftp_ingest_db_checkpoint.json"


def load_pmcids_from_reconciliation() -> List[str]:
    if not os.path.exists('ftp_reconciliation_result.json'):
        raise FileNotFoundError("ftp_reconciliation_result.json not found. Run ftp_reconciliation.py first.")
    with open('ftp_reconciliation_result.json') as f:
        data = json.load(f)
    pmcids = data.get('gaps', {}).get('ftp_not_in_db', [])
    return sorted(pmcids)


def load_checkpoint() -> Dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {
        'last_index': 0,
        'processed': 0,
        'inserted': 0,
        'updated': 0,
        'missing_pmid': 0,
        'errors': 0,
        'timestamp': None,
    }


def save_checkpoint(state: Dict) -> None:
    state['timestamp'] = time.time()
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def idconv_lookup(pmcids: List[str], tool: str, email: str) -> Dict[str, int]:
    """Return mapping of PMCID -> PMID for given PMCIDs."""
    params = {
        'ids': ','.join(pmcids),
        'format': 'json',
        'tool': tool,
        'email': email,
    }
    resp = requests.get(IDCONV_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    records = data.get('records', [])
    mapping = {}
    for rec in records:
        pmcid = rec.get('pmcid')
        pmid = rec.get('pmid')
        if pmcid and pmid:
            mapping[pmcid] = int(pmid)
    return mapping


def upsert_articles(conn, mapping: Dict[str, int]) -> Dict[str, int]:
    """Insert/update articles with pmid+pmcid. Returns counts."""
    if not mapping:
        return {'inserted': 0, 'updated': 0}
    
    rows = [(pmid, pmcid) for pmcid, pmid in mapping.items()]
    inserted = 0
    updated = 0
    
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        cur.executemany(
            """
            INSERT INTO vyraid.articles (pmid, pmcid, vector_indexed, last_updated)
            VALUES (%s, %s, false, NOW())
            ON CONFLICT (pmid) DO UPDATE SET
                pmcid = EXCLUDED.pmcid,
                last_updated = NOW()
            """,
            rows,
        )
        # executemany does not give per-row affected info; approximate as inserted
        inserted = len(rows)
    
    conn.commit()
    return {'inserted': inserted, 'updated': updated}


def main():
    import argparse

    ap = argparse.ArgumentParser(description="Ingest FTP-only PMCIDs into DB")
    ap.add_argument("--batch-size", type=int, default=200, help="PMCIDs per idconv request")
    ap.add_argument("--sleep", type=float, default=0.34, help="Seconds to sleep between requests")
    ap.add_argument("--limit", type=int, default=0, help="Limit total PMCIDs to process (0=all)")
    ap.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    ap.add_argument("--tool", type=str, default="pubmed-oa-backfill", help="NCBI tool name")
    ap.add_argument("--email", type=str, default=os.getenv('NCBI_EMAIL', 'anonymous@example.com'), help="NCBI contact email")
    args = ap.parse_args()

    pmcids = load_pmcids_from_reconciliation()
    if args.limit > 0:
        pmcids = pmcids[:args.limit]

    state = load_checkpoint() if args.resume else {
        'last_index': 0,
        'processed': 0,
        'inserted': 0,
        'updated': 0,
        'missing_pmid': 0,
        'errors': 0,
        'timestamp': None,
    }

    start_index = state['last_index']
    total = len(pmcids)

    log.info("=" * 80)
    log.info("FTP DB INGEST - Add FTP-only PMCIDs to DB")
    log.info("=" * 80)
    log.info(f"Total PMCIDs to process: {total:,}")
    log.info(f"Starting at index: {start_index:,}")
    log.info(f"Batch size: {args.batch_size}")
    log.info(f"Sleep between requests: {args.sleep}s")

    dsn = os.getenv('POSTGRES_DSN')
    conn = psycopg2.connect(dsn)

    try:
        for i in range(start_index, total, args.batch_size):
            batch = pmcids[i:i + args.batch_size]
            if not batch:
                break

            try:
                mapping = idconv_lookup(batch, tool=args.tool, email=args.email)
                missing = len(batch) - len(mapping)
                counts = upsert_articles(conn, mapping)

                state['processed'] += len(batch)
                state['inserted'] += counts['inserted']
                state['updated'] += counts['updated']
                state['missing_pmid'] += missing
                state['last_index'] = i + len(batch)

                if state['processed'] % 2000 == 0:
                    save_checkpoint(state)
                    log.info(f"Processed {state['processed']:,}/{total:,} PMCIDs (inserted {state['inserted']:,}, missing PMID {state['missing_pmid']:,})")

            except Exception as e:
                state['errors'] += len(batch)
                state['last_index'] = i + len(batch)
                save_checkpoint(state)
                log.warning(f"Batch failed at index {i}: {e}")

            time.sleep(args.sleep)

    finally:
        conn.close()
        save_checkpoint(state)

    log.info("\n" + "=" * 80)
    log.info("DB INGEST COMPLETE")
    log.info("=" * 80)
    log.info(f"Processed: {state['processed']:,}")
    log.info(f"Inserted/updated: {state['inserted']:,}")
    log.info(f"Missing PMID: {state['missing_pmid']:,}")
    log.info(f"Errors: {state['errors']:,}")
    log.info(f"Checkpoint: {CHECKPOINT_FILE}")
    log.info("=" * 80)


if __name__ == "__main__":
    main()
