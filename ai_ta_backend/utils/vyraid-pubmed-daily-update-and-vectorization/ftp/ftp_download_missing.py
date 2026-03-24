#!/usr/bin/env python3
"""
Download missing OA PDFs from FTP and ingest into MinIO + DB.

Uses gaps from ftp_reconciliation.py to prioritize downloads.
Parallel download with checkpoint/resume capability.
"""

import os
import sys
import re
import logging
import requests
import json
import time
from typing import Set, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

import psycopg2
from minio import Minio
from dotenv import load_dotenv

load_dotenv('.env.production')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

DOWNLOAD_CHECKPOINT = "ftp_download_checkpoint.json"
FTP_TIMEOUT = 60

# ============================================================================
# Config
# ============================================================================

def get_minio_client():
    """Initialize MinIO client."""
    endpoint = os.getenv('MINIO_ENDPOINT', 'minio-api.ncsa.ai').rstrip('/')
    if '://' in endpoint:
        from urllib.parse import urlparse
        parsed = urlparse(f"http://{endpoint}" if '://' not in endpoint else endpoint)
        endpoint = f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname
    
    secure = os.getenv('MINIO_SECURE', 'true').lower() in ('1', 'true', 'yes')
    
    return Minio(
        endpoint,
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=secure
    )


def get_db_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(os.getenv('POSTGRES_DSN'))


# ============================================================================
# Download Logic
# ============================================================================

def parse_pmcid_from_path(path: str) -> Optional[str]:
    """Extract PMCID from path."""
    match = re.search(r'(PMC\d+)', path)
    return match.group(1) if match else None


def download_pdf_from_ftp(url: str) -> Optional[bytes]:
    """Download PDF from FTP URL. Use exact path from CSV mapping."""
    try:
        resp = requests.get(url, timeout=FTP_TIMEOUT, stream=True)
        if resp.status_code == 200:
            log.debug(f"Downloaded from {url}")
            return resp.content
        else:
            log.debug(f"Failed {url}: HTTP {resp.status_code}")
            return None
    except Exception as e:
        log.debug(f"Failed {url}: {e}")
        return None


def upload_to_minio(client: Minio, pmcid: str, pdf_data: bytes) -> bool:
    """Upload PDF to MinIO using OA FTP structure."""
    bucket = os.getenv('MINIO_BUCKET', 'pubmed')
    
    # Extract 2-char prefix from PMCID (e.g., PMC123456 → 12)
    pmcid_num = pmcid.replace('PMC', '')
    prefix = pmcid_num[:2] if len(pmcid_num) > 2 else pmcid_num.zfill(2)
    
    # Object path mirrors FTP: aa/PMCnnnnnnn.pdf.gz
    object_path = f"{prefix}/{pmcid}.pdf.gz"
    
    try:
        client.put_object(
            bucket,
            object_path,
            BytesIO(pdf_data),
            length=len(pdf_data),
            content_type='application/gzip'
        )
        log.debug(f"Uploaded {pmcid} to {object_path}")
        return True
    except Exception as e:
        log.warning(f"MinIO upload failed for {pmcid}: {e}")
        return False


def update_db_pdf_location(pmcid: str, object_path: str) -> bool:
    """Update DB with pdf_location for article."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE vyraid.articles
                SET pdf_location = %s, last_updated = NOW()
                WHERE pmcid = %s AND (pdf_location IS NULL OR pdf_location = '')
            """, (f"s3://pubmed/{object_path}", pmcid))
            conn.commit()
        conn.close()
        return True
    except Exception as e:
        log.warning(f"DB update failed for {pmcid}: {e}")
        return False


def process_pmcid(pmcid: str, ftp_url: str) -> Dict:
    """Download, upload, and update DB for single PMCID."""
    result = {
        'pmcid': pmcid,
        'ftp_url': ftp_url,
        'status': 'pending',
        'bytes_downloaded': 0,
    }
    
    try:
        # Download using exact path from CSV mapping
        pdf_data = download_pdf_from_ftp(ftp_url)
        if not pdf_data:
            result['status'] = 'download_failed'
            return result
        
        result['bytes_downloaded'] = len(pdf_data)
        
        # Upload to MinIO
        minio = get_minio_client()
        prefix = pmcid.replace('PMC', '')[:2]
        object_path = f"{prefix}/{pmcid}.pdf.gz"
        
        if not upload_to_minio(minio, pmcid, pdf_data):
            result['status'] = 'upload_failed'
            return result
        
        # Update DB
        if not update_db_pdf_location(pmcid, object_path):
            result['status'] = 'db_update_failed'
            return result
        
        result['status'] = 'success'
        result['minio_path'] = object_path
        
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
    
    return result


# ============================================================================
# Checkpoint/Resume
# ============================================================================

def load_checkpoint() -> Dict:
    """Load download checkpoint."""
    if os.path.exists(DOWNLOAD_CHECKPOINT):
        with open(DOWNLOAD_CHECKPOINT) as f:
            return json.load(f)
    return {'processed': {}, 'stats': {'success': 0, 'failed': 0}}


def save_checkpoint(results: Dict):
    """Save progress checkpoint."""
    checkpoint = load_checkpoint()
    for pmcid, result in results.items():
        checkpoint['processed'][pmcid] = result
    
    # Update stats
    checkpoint['stats']['success'] = sum(1 for r in checkpoint['processed'].values() if r['status'] == 'success')
    checkpoint['stats']['failed'] = sum(1 for r in checkpoint['processed'].values() if r['status'] != 'success')
    checkpoint['timestamp'] = time.time()
    
    with open(DOWNLOAD_CHECKPOINT, 'w') as f:
        json.dump(checkpoint, f, indent=2)


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    ap = argparse.ArgumentParser(description="Download missing OA PDFs from FTP")
    ap.add_argument("--workers", type=int, default=8, help="Parallel workers (default: 8)")
    ap.add_argument("--limit", type=int, default=0, help="Max PDFs to download (0=unlimited)")
    ap.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    args = ap.parse_args()
    
    log.info("=" * 80)
    log.info("FTP PDF DOWNLOAD - Missing OA PDFs")
    log.info("=" * 80)
    
    # Load reconciliation results
    if not os.path.exists('ftp_reconciliation_result.json'):
        log.error("ftp_reconciliation_result.json not found. Run ftp_reconciliation.py first.")
        sys.exit(1)
    
    # Load PMCID→path mapping
    if not os.path.exists('ftp_pmcid_paths.json'):
        log.error("ftp_pmcid_paths.json not found. Run ftp_reconciliation.py first.")
        sys.exit(1)
    
    with open('ftp_reconciliation_result.json') as f:
        recon_data = json.load(f)
    
    with open('ftp_pmcid_paths.json') as f:
        pmcid_to_path = json.load(f)
    
    log.info(f"Loaded {len(pmcid_to_path):,} PMCID→path mappings from CSV")
    
    report = recon_data['report']
    gaps = recon_data['gaps']
    
    # Priority 1: FTP+DB but not MinIO (highest value, already indexed)
    # BUT: Only include PMCIDs that have path mappings (are in oa_pdf/, not oa_package/)
    priority_pmcids_raw = set(gaps['ftp_in_db_not_in_minio'])  # Convert list to set
    priority_pmcids = priority_pmcids_raw & set(pmcid_to_path.keys())  # Only PMCIDs with paths
    
    skipped = len(priority_pmcids_raw) - len(priority_pmcids)
    log.info(f"Priority: {len(priority_pmcids):,} PMCIDs to download (FTP+DB, not MinIO, with paths)")
    if skipped > 0:
        log.info(f"  Skipping: {skipped:,} PMCIDs without oa_pdf/ paths (likely in oa_package/ tar.gz)")
    log.info(f"  (Will skip: {report['ftp_not_in_db']:,} new articles, {report['db_not_in_minio']:,} non-FTP articles)")
    
    if args.resume:
        checkpoint = load_checkpoint()
        already_done = set(checkpoint['processed'].keys())
        priority_pmcids = priority_pmcids - already_done
        log.info(f"Resuming: {len(already_done)} already done, {len(priority_pmcids)} remaining")
    
    if args.limit > 0:
        priority_pmcids = set(list(priority_pmcids)[:args.limit])
    
    log.info(f"\n[Starting download of {len(priority_pmcids):,} PDFs with {args.workers} workers...]")
    log.info(f"Using actual FTP paths from CSV (column 0), no pattern guessing\n")
    
    # Note: CSV contains actual FTP paths like oa_pdf/00/00/10.1177_xxx.PMC123.pdf
    # We use these exact paths instead of guessing directory structure
    
    total_bytes = 0
    stats = {'success': 0, 'failed': 0, 'no_path': 0}
    batch_results = {}
    
    FTP_BASE = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/"
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for pmcid in priority_pmcids:
            # Get actual path from CSV mapping
            if pmcid not in pmcid_to_path:
                log.warning(f"No path mapping for {pmcid}, skipping")
                stats['no_path'] += 1
                continue
            
            ftp_path = pmcid_to_path[pmcid]
            ftp_url = f"{FTP_BASE}{ftp_path}"
            
            future = executor.submit(process_pmcid, pmcid, ftp_url)
            futures[future] = pmcid
        
        for future in as_completed(futures):
            pmcid = futures[future]
            try:
                result = future.result()
                batch_results[pmcid] = result
                
                if result['status'] == 'success':
                    stats['success'] += 1
                    total_bytes += result.get('bytes_downloaded', 0)
                    log.info(f"✓ {pmcid} ({result['bytes_downloaded'] / 1024 / 1024:.1f} MB)")
                else:
                    stats['failed'] += 1
                    log.warning(f"✗ {pmcid}: {result['status']}")
                
            except Exception as e:
                stats['failed'] += 1
                log.error(f"✗ {pmcid}: {e}")
    
    # Save checkpoint
    save_checkpoint(batch_results)
    
    # Report
    log.info("\n" + "=" * 80)
    log.info("DOWNLOAD COMPLETE")
    log.info("=" * 80)
    log.info(f"Success: {stats['success']:,}")
    log.info(f"Failed: {stats['failed']:,}")
    if stats.get('no_path', 0) > 0:
        log.info(f"No path mapping: {stats['no_path']:,}")
    log.info(f"Total downloaded: {total_bytes / 1024 / 1024 / 1024:.2f} GB")
    log.info(f"Checkpoint saved: {DOWNLOAD_CHECKPOINT}")
    log.info("=" * 80)
