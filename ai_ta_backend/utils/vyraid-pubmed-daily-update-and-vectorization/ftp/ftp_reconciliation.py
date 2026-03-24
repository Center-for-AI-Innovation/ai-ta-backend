#!/usr/bin/env python3
"""
Full FTP Reconciliation: Crawl https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_pdf/
and reconcile with our DB/MinIO to identify missing OA PDFs.

Process:
1. List all PMCIDs available on FTP
2. Compare with DB (articles table) + MinIO
3. Report gaps
4. Download missing PDFs to MinIO
5. Queue for vectorization
"""

import os
import sys
import re
import logging
import requests
from urllib.parse import urljoin
from typing import Set, Tuple, Dict
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import csv
import io

import psycopg2
from minio import Minio
from dotenv import load_dotenv

load_dotenv('.env.production')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

FTP_BASE = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/"
FTP_TIMEOUT = 60
CSV_CHECKPOINT = "ftp_oa_file_list.csv"

# ============================================================================
# FTP CSV Listing (efficient alternative to crawling)
# ============================================================================

def parse_pmcid_from_path(path: str) -> str:
    """Extract PMCID from path like 'PMC1234567.pdf.gz' or 'oa_file_list/00/PMC1234567'."""
    match = re.search(r'(PMC\d+)', path)
    return match.group(1) if match else None


def download_oa_file_list() -> Tuple[Set[str], Dict[str, str]]:
    """
    Download NCBI's official oa_file_list.csv and extract all PMCIDs + their paths.
    
    File format: File,Article Citation,Accession ID,Last Updated (YYYY-MM-DD HH:MM:SS),PMID,License
    Column 0 = File path (e.g., oa_pdf/00/00/10.1177_xxx.PMC12361176.pdf)
    Column 2 = Accession ID (PMCID)
    
    Returns: 
        - Set of PMCIDs available on FTP (for backward compatibility)
        - Dict mapping PMCID → full FTP path (for oa_pdf/ entries only)
    """
    url = FTP_BASE + "oa_file_list.csv"
    log.info(f"Downloading {url} (~900MB, ~15 min)...")
    
    try:
        resp = requests.get(url, timeout=FTP_TIMEOUT, stream=True)
        resp.raise_for_status()
        
        pmcids = set()
        pmcid_to_path = {}  # PMCID → FTP path mapping
        oa_pdf_count = 0
        line_count = 0
        
        # Use csv.reader to properly handle quoted fields and timestamps
        # Stream by reading text line by line and parsing with csv module
        csv_buffer = ""
        for chunk in resp.iter_content(decode_unicode=True, chunk_size=8192):
            csv_buffer += chunk
            # Process complete lines from buffer
            lines = csv_buffer.split('\n')
            csv_buffer = lines[-1]  # Keep incomplete line for next iteration
            
            for line in lines[:-1]:
                line_count += 1
                if line_count == 1:  # Skip header
                    log.debug(f"CSV header: {line[:80]}...")
                    continue
                
                # Use csv module to parse row properly
                try:
                    reader = csv.reader([line])
                    parts = next(reader)
                    if len(parts) >= 3:
                        file_path = parts[0].strip()  # Column 0: File path
                        pmcid = parts[2].strip()  # Column 2: Accession ID (PMCID)
                        
                        if pmcid and pmcid.startswith('PMC'):
                            pmcids.add(pmcid)
                            
                            # Only map oa_pdf/ entries (individual PDFs), not oa_package/ (tar.gz)
                            if file_path.startswith('oa_pdf/'):
                                pmcid_to_path[pmcid] = file_path
                                oa_pdf_count += 1
                except Exception as e:
                    log.debug(f"Could not parse line {line_count}: {e}")
                    continue
                
                if line_count % 100000 == 0:
                    log.info(f"  Parsed {line_count:,} lines, {len(pmcids):,} PMCIDs ({oa_pdf_count:,} oa_pdf/)...")
        
        # Process final line if buffer not empty
        if csv_buffer.strip():
            line_count += 1
            try:
                reader = csv.reader([csv_buffer])
                parts = next(reader)
                if len(parts) >= 3:
                    file_path = parts[0].strip()
                    pmcid = parts[2].strip()  # Fixed: was parts[1]
                    if pmcid and pmcid.startswith('PMC'):
                        pmcids.add(pmcid)
                        if file_path.startswith('oa_pdf/'):
                            pmcid_to_path[pmcid] = file_path
                            oa_pdf_count += 1
            except Exception as e:
                log.debug(f"Could not parse final line: {e}")
        
        log.info(f"✓ Parsed {line_count:,} total lines")
        log.info(f"  Total PMCIDs: {len(pmcids):,}")
        log.info(f"  Individual PDFs (oa_pdf/): {len(pmcid_to_path):,}")
        log.info(f"  Packages (oa_package/): {len(pmcids) - len(pmcid_to_path):,}")
        return pmcids, pmcid_to_path
        
    except requests.exceptions.Timeout:
        log.error(f"Timeout downloading {url}")
        raise
    except Exception as e:
        log.error(f"Error downloading {url}: {e}")
        raise


# ============================================================================
# Database & MinIO
# ============================================================================

def get_db_pmcids_with_pdf() -> Set[str]:
    """Get all PMCIDs in DB that should have a PDF."""
    dsn = os.getenv('POSTGRES_DSN')
    try:
        conn = psycopg2.connect(dsn)
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute(r"""
                SELECT DISTINCT pmcid 
                FROM vyraid.articles 
                WHERE pmcid IS NOT NULL AND pmcid ~ '^PMC\d+$'
            """)
            pmcids = {row[0] for row in cur.fetchall() if row[0]}
            log.info(f"DB has {len(pmcids)} valid PMCIDs")
        conn.close()
        return pmcids
    except Exception as e:
        log.error(f"Database error: {e}")
        return set()


def get_minio_pmcids() -> Set[str]:
    """Get PMCIDs already in MinIO."""
    endpoint = os.getenv('MINIO_ENDPOINT', 'minio-api.ncsa.ai').rstrip('/')
    if '://' in endpoint:
        from urllib.parse import urlparse
        parsed = urlparse(f"http://{endpoint}" if '://' not in endpoint else endpoint)
        endpoint = f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname
    
    secure = os.getenv('MINIO_SECURE', 'true').lower() in ('1', 'true', 'yes')
    
    client = Minio(
        endpoint,
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=secure
    )
    
    bucket = os.getenv('MINIO_BUCKET', 'pubmed')
    pmcids = set()
    
    try:
        objects = client.list_objects(bucket, recursive=True)
        for obj in objects:
            pmcid = parse_pmcid_from_path(obj.object_name)
            if pmcid:
                pmcids.add(pmcid)
        log.info(f"MinIO has {len(pmcids)} PMCIDs with PDFs")
    except Exception as e:
        log.error(f"MinIO error: {e}")
    
    return pmcids


# ============================================================================
# Reconciliation
# ============================================================================

def reconcile(ftp_pmcids: Set[str], db_pmcids: Set[str], minio_pmcids: Set[str]) -> Tuple[Dict, Dict]:
    """Analyze gaps between FTP, DB, and MinIO."""
    
    # Articles in DB but not downloaded to MinIO
    db_not_in_minio = db_pmcids - minio_pmcids
    
    # On FTP but not in our DB
    ftp_not_in_db = ftp_pmcids - db_pmcids
    
    # On FTP and in DB but not downloaded (gap we can fill)
    ftp_in_db_not_in_minio = (ftp_pmcids & db_pmcids) - minio_pmcids
    
    report = {
        'ftp_total': len(ftp_pmcids),
        'db_total': len(db_pmcids),
        'minio_total': len(minio_pmcids),
        'db_not_in_minio': len(db_not_in_minio),
        'ftp_not_in_db': len(ftp_not_in_db),
        'ftp_in_db_not_in_minio': len(ftp_in_db_not_in_minio),
        'coverage_percent': 100 * len(minio_pmcids) / max(1, len(ftp_pmcids)),
    }
    
    gaps = {
        'db_not_in_minio': db_not_in_minio,
        'ftp_not_in_db': ftp_not_in_db,
        'ftp_in_db_not_in_minio': ftp_in_db_not_in_minio,
    }
    
    return report, gaps


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    ap = argparse.ArgumentParser(description="Reconcile FTP OA PDFs with DB/MinIO")
    args = ap.parse_args()
    
    log.info("=" * 80)
    log.info("FTP RECONCILIATION - PubMed OA PDFs")
    log.info("=" * 80)
    
    # Step 1: Get FTP file list (or load from cache)
    log.info("\n[1/3] Fetching FTP inventory...")
    PATH_MAPPING_FILE = "ftp_pmcid_paths.json"
    
    if os.path.exists(PATH_MAPPING_FILE):
        # Prefer crawl-based mapping: authoritative list of files that exist
        log.info(f"Loading crawl-based FTP inventory from {PATH_MAPPING_FILE}...")
        with open(PATH_MAPPING_FILE) as f:
            pmcid_to_path = json.load(f)
        ftp_pmcids = set(pmcid_to_path.keys())
        log.info(f"✓ Loaded {len(ftp_pmcids):,} PMCIDs with paths from crawl")
    elif os.path.exists(CSV_CHECKPOINT):
        log.info(f"Loading cached CSV from {CSV_CHECKPOINT} (delete to re-fetch)...")
        with open(CSV_CHECKPOINT) as f:
            ftp_pmcids = set(line.strip() for line in f if line.strip())
        pmcid_to_path = {}
        log.info(f"✓ Loaded {len(ftp_pmcids):,} PMCIDs from CSV cache (no paths)")
    else:
        log.info(f"Downloading oa_file_list.csv from {FTP_BASE}...")
        ftp_pmcids, pmcid_to_path = download_oa_file_list()
        
        # Save checkpoints
        with open(CSV_CHECKPOINT, 'w') as f:
            for pmcid in sorted(ftp_pmcids):
                f.write(pmcid + '\n')
        with open(PATH_MAPPING_FILE, 'w') as f:
            json.dump(pmcid_to_path, f, indent=2)
        log.info(f"✓ Saved {len(ftp_pmcids):,} PMCIDs to {CSV_CHECKPOINT}")
        log.info(f"✓ Saved {len(pmcid_to_path):,} path mappings to {PATH_MAPPING_FILE}")
    
    log.info(f"✓ FTP inventory: {len(ftp_pmcids):,} PMCIDs ({len(pmcid_to_path):,} downloadable PDFs)")
    
    # Step 2: Get DB + MinIO inventory
    log.info("\n[2/3] Querying database and MinIO...")
    db_pmcids = get_db_pmcids_with_pdf()
    minio_pmcids = get_minio_pmcids()
    
    # Step 3: Reconcile
    log.info("\n[3/3] Reconciling...")
    report, gaps = reconcile(ftp_pmcids, db_pmcids, minio_pmcids)
    
    # Report
    log.info("\n" + "=" * 80)
    log.info("RECONCILIATION REPORT")
    log.info("=" * 80)
    log.info(f"FTP availability: {report['ftp_total']:,} unique PMCIDs")
    log.info(f"DB coverage: {report['db_total']:,} PMCIDs")
    log.info(f"MinIO inventory: {report['minio_total']:,} PDFs downloaded")
    log.info(f"\nCurrent coverage: {report['coverage_percent']:.1f}% of available OA")
    log.info("\nGaps identified:")
    log.info(f"  [1] DB articles not in MinIO: {report['db_not_in_minio']:,}")
    log.info(f"      → Should download + vectorize")
    log.info(f"  [2] FTP articles not in DB: {report['ftp_not_in_db']:,}")
    log.info(f"      → Need to add to DB first")
    log.info(f"  [3] FTP+DB but not MinIO: {report['ftp_in_db_not_in_minio']:,}")
    log.info(f"      → Priority: downloadable gap")
    log.info("=" * 80)
    
    # Save results
    results = {
        'report': report,
        'gaps': {k: list(v) for k, v in gaps.items()},
        'timestamp': time.time(),
        'note': f'Path mappings for {len(pmcid_to_path):,} oa_pdf/ entries in ftp_pmcid_paths.json'
    }
    
    with open('ftp_reconciliation_result.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    log.info(f"\nResults saved to: ftp_reconciliation_result.json")
    log.info(f"Path mappings saved to: ftp_pmcid_paths.json ({len(pmcid_to_path):,} entries)")
    log.info("Next: Run ftp_download_missing.py to fill gaps")
