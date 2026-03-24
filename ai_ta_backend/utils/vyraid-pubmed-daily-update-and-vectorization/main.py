# oa_minio_qdrant_pipeline.py
# Unified pipeline: OA PDF -> MinIO -> Qdrant -> Postgres (Supabase)
from __future__ import annotations

import argparse
import functools
import logging
import os
import re
import time
import tempfile
import uuid
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from datetime import datetime

import requests
import pymupdf  # PyMuPDF
import psycopg2
from psycopg2 import sql, OperationalError as PgOperationalError
import gzip
from datetime import date
from lxml import etree
from minio import Minio
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError, ChunkedEncodingError, HTTPError
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()

from qdrant_client import QdrantClient, models

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
SEASONS = {
    "spring": 3, "summer": 6, "fall": 9, "autumn": 9, "winter": 12,
}

def _parser_to_int(s: Optional[str]) -> Optional[int]:
    try:
        return int(s) if s is not None else None
    except Exception:
        return None

def _parser_parse_pub_date(pubdate_el: etree._Element) -> Optional[date]:
    year_el = pubdate_el.find("Year")
    if year_el is not None and year_el.text:
        y = _parser_to_int(year_el.text.strip())
        m = None
        d = None
        month_el = pubdate_el.find("Month")
        if month_el is not None and month_el.text:
            mtxt = month_el.text.strip().lower()
            m = _parser_to_int(mtxt)
            if m is None:
                m = MONTHS.get(mtxt)
        day_el = pubdate_el.find("Day")
        if day_el is not None and day_el.text:
            d = _parser_to_int(day_el.text.strip())
        if y:
            if not m:
                m = 1
            if not d:
                d = 1
            try:
                return date(y, m, d)
            except Exception:
                return None
    season_el = pubdate_el.find("Season")
    if season_el is not None and season_el.text and year_el is not None and year_el.text:
        y = _parser_to_int(year_el.text.strip())
        m = SEASONS.get(season_el.text.strip().lower())
        if y and m:
            return date(y, m, 1)
    medline_el = pubdate_el.find("MedlineDate")
    if medline_el is not None and medline_el.text:
        txt = medline_el.text.strip().lower()
        import re
        ymatch = re.search(r"(19|20)\d{2}", txt)
        y = int(ymatch.group(0)) if ymatch else None
        m = None
        for token in txt.replace(".", " ").replace("-", " ").split():
            if token in MONTHS:
                m = MONTHS[token]
                break
        if y:
            return date(y, m or 1, 1)
    return None

def _parser_pub_history_date(pubmed_data: etree._Element) -> Optional[date]:
    if pubmed_data is None:
        return None
    hist = pubmed_data.find("History")
    if hist is None:
        return None
    order = ["ppublish", "epublish", "ecollection", "pubmed", "entrez"]
    for status in order:
        el = hist.find(f"PubMedPubDate[@PubStatus='{status}']")
        if el is not None:
            y = el.findtext("Year")
            m = el.findtext("Month")
            d = el.findtext("Day")
            yi, mi, di = _parser_to_int(y), _parser_to_int(m), _parser_to_int(d)
            if yi:
                return date(yi, mi or 1, di or 1)
    return None

def _parser_join_text(el: Optional[etree._Element]) -> Optional[str]:
    if el is None:
        return None
    txt = "".join(el.itertext())
    txt = " ".join(txt.split())
    return txt or None

def _parser_abstract_text(abstract_el: etree._Element) -> Optional[str]:
    parts = []
    for ab in abstract_el.findall("AbstractText"):
        label = ab.get("Label")
        body = " ".join("".join(ab.itertext()).split())
        if not body:
            continue
        if label:
            parts.append(f"{label}: {body}")
        else:
            parts.append(body)
    if not parts:
        return None
    return "\n".join(parts)

@dataclass
class ArticleRow:
    pmid: int
    pmcid: Optional[str]
    doi: Optional[str]
    title: Optional[str]
    abstract: Optional[str]
    publication_date: Optional[date]

def iter_pubmed_articles(gz_path) -> Generator[ArticleRow, None, None]:
    with gzip.open(gz_path, "rb") as f:
        context = etree.iterparse(f, events=("end",), tag=("PubmedArticle",))
        for _ev, article_el in context:
            try:
                medline = article_el.find("MedlineCitation")
                pubdata = article_el.find("PubmedData")
                pmid_txt = medline.findtext("PMID") if medline is not None else None
                pmid = int(pmid_txt.strip()) if pmid_txt else None
                title_el = medline.find("Article/ArticleTitle") if medline is not None else None
                title = _parser_join_text(title_el)
                abstract_el = medline.find("Article/Abstract") if medline is not None else None
                abstract = _parser_abstract_text(abstract_el) if abstract_el is not None else None
                pubdate_el = medline.find("Article/Journal/JournalIssue/PubDate") if medline is not None else None
                pdate = _parser_parse_pub_date(pubdate_el) if pubdate_el is not None else None
                if pdate is None:
                    pdate = _parser_pub_history_date(pubdata)
                pmcid = None
                doi = None
                if pubdata is not None:
                    idlist = pubdata.find("ArticleIdList")
                    if idlist is not None:
                        for idel in idlist.findall("ArticleId"):
                            idtype = (idel.get("IdType") or "").lower()
                            val = (idel.text or "").strip()
                            if not val:
                                continue
                            if idtype in ("pmc", "pmcid", "pmcbook"):
                                pmcid = val
                            elif idtype == "doi":
                                doi = val.lower()
                if pmid is not None:
                    yield ArticleRow(
                        pmid=pmid,
                        pmcid=pmcid,
                        doi=doi,
                        title=title,
                        abstract=abstract,
                        publication_date=pdate,
                    )
            finally:
                article_el.clear()
                while article_el.getprevious() is not None:
                    del article_el.getparent()[0]
        del context

# -----------------------------
# Constants / External services
# -----------------------------

UPDATE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
BASELINE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"

# PMC OA API docs + record examples show <link format="pdf" href="ftp://.../oa_pdf/...pdf">
# We'll use this endpoint to discover PDF links by PMCID.
OA_API = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi"

# -----------------------------
# Settings
# -----------------------------

def _to_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on"}

def _to_int(name: str, default: int = 0) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default

def _normalize_minio_endpoint(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return s
    if "://" in s:
        u = urlparse(s)
        host = u.hostname or ""
        port = u.port
        return f"{host}:{port}" if port else host
    s = s.split("/", 1)[0]
    if s.startswith("http:") or s.startswith("https:"):
        s = s.split(":", 1)[1].lstrip("/")
    return s

@dataclass(frozen=True)
class Settings:
    # Logging / behavior
    log_level: int
    max_workers: int
    xml_log_every_n: int  # how many successful pmcids to aggregate before logging progress time
    test_limit: int       # optional safety limiter while testing (0 = unlimited)

    # Postgres / schema
    db_dsn: str
    db_schema: Optional[str]

    # MinIO
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_secure: bool
    file_location_prefix: Optional[str]  # optional "folder" prefix inside the bucket

    # Embeddings / Qdrant
    embedding_base_url: str
    qdrant_url: str
    qdrant_port: int
    qdrant_api_key: Optional[str]
    qdrant_collection: str
    vector_size: int
    chunk_size: int
    chunk_overlap: int
    embed_batch_upsert: int

def get_settings() -> Settings:
    return Settings(
        log_level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        max_workers=_to_int("MAX_WORKERS", 8),
        xml_log_every_n=_to_int("XML_LOG_EVERY_N", 100),
        test_limit=_to_int("TEST_LIMIT", 0),

        db_dsn=os.getenv("POSTGRES_DSN") or os.getenv("DB_DSN", ""),  # support both names
        db_schema=os.getenv("DB_SCHEMA") or "vyraid",

        minio_endpoint=_normalize_minio_endpoint(os.getenv("MINIO_ENDPOINT", "")),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", ""),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", ""),
        minio_bucket=os.getenv("MINIO_BUCKET", "pubmed"),
        minio_secure=_to_bool("MINIO_SECURE", False),
        file_location_prefix=os.getenv("FILE_LOCATION", "").strip() or None,

        embedding_base_url=os.environ["EMBEDDING_BASE_URL"],

        qdrant_url=os.environ["QDRANT_URL"],
        qdrant_port=_to_int("QDRANT_PORT", 6333),
        qdrant_api_key=os.getenv("QDRANT_API_KEY") or None,
        qdrant_collection=os.getenv("QDRANT_COLLECTION", "ncbi_pdfs"),
        vector_size=_to_int("VECTOR_SIZE", 768),
        chunk_size=_to_int("CHUNK_SIZE", 7000),
        chunk_overlap=_to_int("CHUNK_OVERLAP", 200),
        embed_batch_upsert=_to_int("EMBED_BATCH_UPSERT", 1000),
    )

# -----------------------------
# Utilities
# -----------------------------

def list_update_files(session: requests.Session) -> List[str]:
    r = session.get(UPDATE_URL, timeout=60)
    r.raise_for_status()
    files = re.findall(r"pubmed\d+n\d+\.xml\.gz", r.text)
    return sorted(set(files))

def list_baseline_files(session: requests.Session, start: Optional[str] = None, end: Optional[str] = None) -> List[str]:
    """List baseline XML files from NCBI FTP baseline directory.
    
    Note: As of 2026, baseline files use the pubmed26n prefix (PubMed 2026 DTD version).
    These files contain the full historical catalog from 2003 onwards.
    
    Args:
        session: requests.Session for HTTP requests.
        start: Optional 4-digit file number to start from (e.g., '0001'), inclusive.
        end: Optional 4-digit file number to end at (e.g., '0100'), inclusive.
    
    Returns:
        Sorted list of baseline filenames.
    """
    r = session.get(BASELINE_URL, timeout=60)
    r.raise_for_status()
    # Match baseline files - currently pubmed26n as of 2026
    # This will need updating when NCBI moves to pubmed27n, etc.
    files = re.findall(r"pubmed26n\d+\.xml\.gz", r.text)
    files = sorted(set(files))
    
    # Filter by start/end if specified
    if start is not None or end is not None:
        def get_file_number(fname: str) -> str:
            """Extract the NNNN part from pubmedXXnNNNN.xml.gz."""
            match = re.search(r"n(\d+)\.xml\.gz", fname)
            return match.group(1) if match else ""
        
        if start:
            files = [f for f in files if get_file_number(f) >= start]
        if end:
            files = [f for f in files if get_file_number(f) <= end]
    
    return files

def normalize_pmcid(pmcid: str) -> str:
    pmcid = (pmcid or "").strip().upper()
    return pmcid if pmcid.startswith("PMC") else f"PMC{pmcid}"

def ftp_to_https(url: str) -> str:
    p = urlparse(url)
    if p.scheme == "ftp" and p.netloc == "ftp.ncbi.nlm.nih.gov":
        return "https://" + url[len("ftp://") :]
    return url

def ftp_relative_path(url: str) -> str:
    """
    Return the path portion after /oa_pdf for MinIO storage. If not present, returns p.path.
    """
    p = urlparse(url)
    path = p.path
    marker = "/oa_pdf"
    idx = path.lower().find(marker)
    if idx != -1:
        rel = path[idx + len(marker) :]
        if not rel:
            return Path(path).name
        return rel
    return path

def parse_pdf_location_for_key(pdf_location: str, default_bucket: str) -> Tuple[str, str]:
    """
    Given pdf_location (e.g., 'minio://bucket/key' or 's3://bucket/key' or 'key'),
    return (bucket, key). If only key given, use default_bucket.
    """
    if "://" in pdf_location:
        parsed = urlparse(pdf_location)
        bucket = parsed.netloc or default_bucket
        key = parsed.path.lstrip("/")
        return bucket, key
    return default_bucket, pdf_location.lstrip("/")

# -----------------------------
# PostgreSQL Retry Logic
# -----------------------------

def pg_retry(max_attempts=4, initial_delay=1.0, max_delay=10.0, backoff_factor=2.0):
    """
    Decorator to retry PostgreSQL operations on connection/network failures.
    Handles psycopg2.OperationalError which covers connection timeouts and network issues.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(self, *args, **kwargs)
                except PgOperationalError as e:
                    last_exception = e
                    error_msg = str(e).lower()

                    # Leave the connection usable for the next attempt
                    try:
                        self._safe_rollback()
                    except Exception:
                        pass
                    
                    # Only retry on connection/network related errors
                    if any(keyword in error_msg for keyword in [
                        'connection', 'timeout', 'network', 'server closed',
                        'connection refused', 'connection timed out',
                        'could not connect', 'connection lost'
                    ]):
                        if attempt < max_attempts:
                            self._log.warning(
                                "PostgreSQL connection error (attempt %d/%d): %s. Retrying in %.1fs...",
                                attempt, max_attempts, e, delay
                            )
                            time.sleep(delay)
                            delay = min(delay * backoff_factor, max_delay)
                            
                            # Try to reconnect
                            try:
                                self._reconnect()
                            except Exception as reconnect_err:
                                self._log.warning("Reconnection failed: %s", reconnect_err)
                            continue
                    
                    # Re-raise if not a retryable error or max attempts reached
                    raise
                except Exception:
                    # Make sure we are not stuck in an aborted transaction before bubbling up
                    try:
                        self._safe_rollback()
                    except Exception:
                        pass
                    # Re-raise non-PostgreSQL exceptions immediately
                    raise
            
            # If we get here, all retries failed
            self._log.error("PostgreSQL operation failed after %d attempts", max_attempts)
            raise last_exception
        
        return wrapper
    return decorator

# -----------------------------
# DB Access Layer (Supabase PG)
# -----------------------------

class DB:
    def __init__(self, dsn: str, schema: Optional[str]):
        self._dsn = dsn
        self._schema = schema
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False
        self._log = logging.getLogger("pipeline.db")

    def _safe_rollback(self) -> None:
        """Rollback if the connection is in an aborted state; ignore rollback errors."""
        try:
            if self._conn:
                self._conn.rollback()
        except Exception:
            pass

    def _reconnect(self) -> None:
        """Attempt to reconnect to the database."""
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass
        
        self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = False
        self._log.info("Successfully reconnected to PostgreSQL")
    
    def _with_reconnect(self, operation_func):
        """Execute database operation with automatic reconnection on connection errors."""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                return operation_func()
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                self._log.warning("DB operation failed (attempt %d/%d): %s", attempt, max_attempts, e)
                if attempt < max_attempts:
                    try:
                        self._reconnect()
                    except Exception as reconnect_err:
                        self._log.error("Reconnection failed: %s", reconnect_err)
                        if attempt == max_attempts - 1:
                            raise
                else:
                    raise
        raise RuntimeError("DB operation failed after all reconnection attempts")

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def _tbl(self, name: str) -> sql.Identifier:
        return sql.Identifier(self._schema, name) if self._schema else sql.Identifier(name)

    # ------------- Articles -------------

    @pg_retry(max_attempts=3)
    def get_article_status(self, pmid: int) -> Tuple[Optional[str], bool]:
        tbl = self._tbl("articles")
        q = sql.SQL("SELECT pdf_location, COALESCE(vector_indexed,false) FROM {tbl} WHERE pmid = %s").format(tbl=tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (pmid,))
            row = cur.fetchone()
            if not row:
                return None, False
            return (row[0], bool(row[1]))

    @pg_retry(max_attempts=3)
    def upsert_article_after_vectorize(self, row: ArticleRow, pdf_location: str) -> None:
        """
        Upsert basic fields AND set vector_indexed = true after successful vectorization.
        """
        tbl = self._tbl("articles")
        q = sql.SQL(
            """
            INSERT INTO {tbl} (pmid, pmcid, doi, title, abstract, publication_date, pdf_location, vector_indexed, last_updated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,true,NOW())
            ON CONFLICT (pmid) DO UPDATE SET
                pmcid = EXCLUDED.pmcid,
                doi = EXCLUDED.doi,
                title = EXCLUDED.title,
                abstract = EXCLUDED.abstract,
                publication_date = EXCLUDED.publication_date,
                pdf_location = EXCLUDED.pdf_location,
                vector_indexed = true,
                last_updated = NOW()
            """
        ).format(tbl=tbl)
        with self._conn.cursor() as cur:
            cur.execute(
                q.as_string(self._conn),
                (
                    row.pmid,
                    row.pmcid,
                    row.doi,
                    row.title,
                    row.abstract,
                    row.publication_date,
                    pdf_location,
                ),
            )
        self._conn.commit()

    # ------------- XML Logging -------------

    @pg_retry(max_attempts=3)
    def ensure_xml_log_row(self, xml_filename: str) -> None:
        log_tbl = self._tbl("xml_processing_log")
        q = sql.SQL(
            """
            INSERT INTO {log_tbl} (xml_filename, last_processed_pmcid, total_processing_time, total_pmcid_processed, processed_all_pmcid)
            VALUES (%s, NULL, INTERVAL '0 seconds', 0, false)
            ON CONFLICT (xml_filename) DO NOTHING
            """
        ).format(log_tbl=log_tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (xml_filename,))
        self._conn.commit()

    @pg_retry(max_attempts=3)
    def get_xml_log_progress(self, xml_filename: str) -> Tuple[Optional[str], bool]:
        """Return (last_processed_pmcid, processed_all_pmcid)."""
        log_tbl = self._tbl("xml_processing_log")
        q = sql.SQL("SELECT last_processed_pmcid, COALESCE(processed_all_pmcid,false) FROM {t} WHERE xml_filename=%s").format(t=log_tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (xml_filename,))
            row = cur.fetchone()
            if not row:
                return None, False
            return row[0], bool(row[1])

    @pg_retry(max_attempts=3)
    def update_xml_log_progress(self, xml_filename: str, last_processed_pmcid: str, delta_seconds: float, newly_processed_count: int) -> None:
        """Update xml_processing_log progress metrics.

        Adds the elapsed seconds to total_processing_time and increments
        total_pmcid_processed by newly_processed_count, and refreshes last_updated.
        """
        log_tbl = self._tbl("xml_processing_log")
        q = sql.SQL(
            """
            UPDATE {t}
               SET last_processed_pmcid = %s,
                   total_processing_time = COALESCE(total_processing_time, INTERVAL '0 seconds') + make_interval(secs => %s)
                  ,total_pmcid_processed = COALESCE(total_pmcid_processed, 0) + %s
                  ,last_updated = NOW()
             WHERE xml_filename = %s
            """
        ).format(t=log_tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (last_processed_pmcid, int(delta_seconds), newly_processed_count, xml_filename))
        self._conn.commit()

    @pg_retry(max_attempts=3)
    def record_xml_failure(self, xml_filename: str, pmcid: str, reason: Optional[str]) -> None:
        fail_tbl = self._tbl("xml_failed_downloads")
        q = sql.SQL(
            """
            INSERT INTO {t} (xml_filename, failed_pmcid, failure_reason)
            VALUES (%s,%s,%s)
            ON CONFLICT (xml_filename, failed_pmcid) DO UPDATE SET
                failure_timestamp = NOW(),
                failure_reason = EXCLUDED.failure_reason
            """
        ).format(t=fail_tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (xml_filename, pmcid, reason))
        self._conn.commit()

    @pg_retry(max_attempts=3)
    def mark_xml_completed(self, xml_filename: str, last_processed_pmcid: Optional[str]) -> None:
        """Mark an xml file as fully processed (processed_all_pmcid = true)."""
        log_tbl = self._tbl("xml_processing_log")
        q = sql.SQL(
            """
            UPDATE {t}
               SET processed_all_pmcid = true,
                   last_processed_pmcid = COALESCE(%s, last_processed_pmcid)
             WHERE xml_filename = %s
            """
        ).format(t=log_tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (last_processed_pmcid, xml_filename))
        self._conn.commit()

# -----------------------------
# MinIO helper
# -----------------------------

class MinioClient:
    def __init__(self, cfg: Settings):
        self.bucket = cfg.minio_bucket
        self.client = Minio(
            endpoint=cfg.minio_endpoint,
            access_key=cfg.minio_access_key,
            secret_key=cfg.minio_secret_key,
            secure=cfg.minio_secure,
        )
        self._log = logging.getLogger("pipeline.minio")

    def upload_file(self, object_name: str, local_path: str) -> None:
        self.client.fput_object(self.bucket, object_name, local_path)

    def remove_object_safe(self, object_name: str) -> None:
        try:
            self.client.remove_object(self.bucket, object_name)
        except Exception as e:
            self._log.warning("MinIO delete failed for %s: %s", object_name, e)

# -----------------------------
# Qdrant helper
# -----------------------------

class QdrantHelper:
    def __init__(self, cfg: Settings):
        self.cfg = cfg
        # Be careful not to leak secrets in logs
        masked_key = None
        if cfg.qdrant_api_key:
            masked_key = cfg.qdrant_api_key[:4] + "..." + cfg.qdrant_api_key[-4:]
        print("cfg.qdrant_url:", cfg.qdrant_url)
        print("cfg.qdrant_api_key:", masked_key)

        # If a full URL with scheme is provided, parse it to extract host/port/https explicitly.
        # The qdrant_client library doesn't infer port from URL scheme, so we must do it manually.
        # This avoids forcing port 6333 when the service is reachable only over HTTPS:443 (e.g., behind Cloudflare).
        raw_url = (cfg.qdrant_url or "").rstrip("/")
        if raw_url.startswith("http://") or raw_url.startswith("https://"):
            u = urlparse(raw_url)
            host = u.hostname or "localhost"
            use_https = (u.scheme == "https")
            # Infer port: use URL port if specified, otherwise 443 for https, 6333 for http
            port = u.port or (443 if use_https else 6333)
            self.client = QdrantClient(host=host, port=port, https=use_https, api_key=cfg.qdrant_api_key, timeout=100, check_compatibility=False)
        else:
            # Fallback: treat qdrant_url as host and infer https from port 443
            use_https = (cfg.qdrant_port == 443)
            self.client = QdrantClient(host=raw_url or "localhost", port=cfg.qdrant_port, https=use_https, api_key=cfg.qdrant_api_key, timeout=100, check_compatibility=False)
        self._log = logging.getLogger("pipeline.qdrant")
        # Ensure index on s3_path for fast filtered deletes (idempotent)
        try:
            self.client.create_payload_index(
                collection_name=self.cfg.qdrant_collection,
                field_name="s3_path",
                field_schema=models.KeywordIndexParams(),  # Keyword index for equality filter
            )
        except Exception:
            # Index may already exist; ignore
            pass
    
    def set_indexing_threshold(self, threshold: int) -> None:
        """
        Update the collection's indexing_threshold parameter.
        Use 0 for bulk ingestion, 1000 for normal operation.
        """
        try:
            self.client.update_collection(
                collection_name=self.cfg.qdrant_collection,
                optimizer_config=models.OptimizersConfigDiff(indexing_threshold=threshold)
            )
            self._log.info("Set indexing_threshold to %d for collection %s", threshold, self.cfg.qdrant_collection)
        except Exception as e:
            self._log.warning("Failed to set indexing_threshold to %d: %s", threshold, e)

    def delete_by_s3_path(self, s3_path_key: str) -> None:
        """
        Delete ALL points whose payload has s3_path == s3_path_key in the target collection.
        """
        filt = models.Filter(must=[models.FieldCondition(key="s3_path", match=models.MatchValue(value=s3_path_key))])
        self.client.delete(collection_name=self.cfg.qdrant_collection, points_selector=filt, wait=False)
        # small pause after delete to give Qdrant a short breather for eventual consistency
        time.sleep(5)

    def upsert_chunks(self, points: List[models.PointStruct]) -> None:
        if not points or len(points) < 1:
            return
        # Retry transient failures (network/read timeouts) a few times with backoff.
        max_attempts = 4
        delay = 1.0
        for attempt in range(1, max_attempts + 1):
            try:
                self.client.upsert(collection_name=self.cfg.qdrant_collection, points=points, wait=False)
                # small pause after upsert to reduce pressure on Qdrant for bulk operations
                time.sleep(5)
                return
            except Exception as e:
                self._log.warning("Qdrant upsert attempt %d/%d failed: %s", attempt, max_attempts, e)
                if attempt == max_attempts:
                    self._log.exception("Qdrant upsert failed after %d attempts", max_attempts)
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 10)
    
    def __enter__(self):
        """Context manager entry: set indexing threshold to 0 for bulk operations."""
        self.set_indexing_threshold(0)
        time.sleep(2)  # Give Qdrant time to apply setting
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: reset indexing threshold to normal operation value."""
        try:
            self.set_indexing_threshold(10_000)
        except Exception as e:
            self._log.warning("Failed to reset indexing_threshold on exit: %s", e)
        return False  # Don't suppress exceptions

# -----------------------------
# Embedding & Chunking
# -----------------------------

# Use the same splitter as your vector_ingestion
from langchain_text_splitters.character import RecursiveCharacterTextSplitter

def chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> List[str]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n\n", "\n", " ", ""],
    )
    return splitter.split_text(text)

def get_embedding(embedding_base_url: str, text: str) -> List[float]:
    """
    Call your Ollama embedding endpoint (nomic-embed-text:v1.5) with retries.
    """
    url = embedding_base_url
    max_retries = 30
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json={"model": "nomic-embed-text:v1.5", "prompt": text}, timeout=300)
            resp.raise_for_status()
            return resp.json()["embedding"]
        except Exception:
            time.sleep(min(0.25 * (attempt + 1), 5))
    raise RuntimeError("Embedding failed after retries")

def extract_text_per_page(local_pdf_path: str) -> List[Tuple[int, str]]:
    """
    Returns list of (page_number, text).
    """
    doc = pymupdf.open(local_pdf_path)
    out: List[Tuple[int, str]] = []
    for idx, page in enumerate(doc, start=1):
        raw = page.get_text().encode("utf8", errors="ignore").decode("utf8", errors="ignore")
        out.append((idx, raw))
    return out

def vectorize_pdf_to_qdrant(
    cfg: Settings,
    qdrant: QdrantHelper,
    pmid: int,
    bucket: str,
    key: str,
    local_pdf_path: str
) -> int:
    """
    Read PDF pages, chunk, embed, and upsert to Qdrant.
    Returns total chunk count.
    """
    page_chunks: list[tuple[int, str]] = []
    doc = pymupdf.open(local_pdf_path)
    for page_num, page in enumerate(doc, start=1):
        raw = page.get_text().encode("utf8", errors="ignore").decode("utf8", errors="ignore")
        for chunk in chunk_text(raw, cfg.chunk_size, cfg.chunk_overlap):
            page_chunks.append((page_num, chunk))
    total_chunks = len(page_chunks)
    if total_chunks == 0:
        return 0

    # 2) Embed + upsert with explicit UUIDs and original payload keys (+ we keep pmid/s3_bucket as extra context)
    batch: list[models.PointStruct] = []
    for i, (page_num, chunk) in enumerate(page_chunks):
        emb = get_embedding(cfg.embedding_base_url, chunk)
        batch.append(
            models.PointStruct(
                id=str(uuid.uuid4()),  # explicit UUID like the original script
                vector=emb,
                payload={
                    # original keys
                    "page_content":      chunk,
                    "s3_path":           key,
                    "readable_filename": os.path.basename(key),
                    "pagenumber":        page_num,
                    "chunk_index":       i,
                    "total_chunks":      total_chunks,
                    "pmid":              pmid,
                },
            )
        )
        if len(batch) >= cfg.embed_batch_upsert:
            qdrant.upsert_chunks(batch)
            batch = []
    if batch:
        qdrant.upsert_chunks(batch)

    return total_chunks


# -----------------------------
# Open Access API (PMC)
# -----------------------------

def oa_fetch_pdf(session: requests.Session, pmcid: str) -> Optional[Tuple[str, str]]:
    """
    Query the PMC OA API for a single PMCID.
    Returns (https_download_url, relative_path_under_oa_pdf) if a PDF exists; otherwise None.

    Notes:
      - API reference & example record format: see PMC OA Web Service docs.  (cited below)
      - We explicitly ignore API "error" codes like idIsNotOpenAccess / idDoesNotExist.
      - API key increases rate limit from 3 req/s to 10 req/s
    """
    pmcid = normalize_pmcid(pmcid)
    params = {"id": pmcid}
    # Add API key if available (increases rate limit to 10 req/s)
    api_key = os.getenv("NCBI_API_KEY") or os.getenv("OA_API_KEY")
    if api_key:
        params["api_key"] = api_key
    for attempt in range(2):
        try:
            r = session.get(OA_API, params=params, timeout=8)
        except (ConnectionError, ChunkedEncodingError, RemoteDisconnected):
            if attempt == 2:
                raise
            time.sleep(0.5 * (2 ** attempt))
            continue
        if r.status_code == 500:
            return None
        if r.status_code == 429:
            time.sleep(0.5)
            continue
        r.raise_for_status()
        break

    import xml.etree.ElementTree as ET
    root = ET.fromstring(r.text)

    # Skip known OA API "error" responses
    for err in root.findall(".//error"):
        code = err.attrib.get("code", "")
        if code in {"idIsNotOpenAccess", "idDoesNotExist"}:
            return None
        # Other error? treat as None to skip, caller logs explicit failures elsewhere where relevant
        return None

    link = None
    for rec in root.findall(".//record"):
        for l in rec.findall("link"):
            if l.attrib.get("format") == "pdf":
                link = l.attrib.get("href")
                break
        if link:
            break
    if not link:
        return None

    https_link = ftp_to_https(link)
    rel_path = ftp_relative_path(link)  # keep original FTP layout under /oa_pdf
    return https_link, rel_path.lstrip("/")

# -----------------------------
# Process State Logger
# -----------------------------

class ProcessStateLogger:
    """
    Manages a JSON log file to track the current state of the pipeline.
    States: RUNNING, COMPLETED, FAILED
    This allows cron jobs to check if the script should run.
    """
    def __init__(self, log_file_path: str):
        self.log_file_path = Path(log_file_path)
        self._log = logging.getLogger("pipeline.state")
        self.log_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    def _read_state(self) -> dict:
        """Read current state from file, return empty dict if doesn't exist."""
        if not self.log_file_path.exists():
            return {}
        try:
            with open(self.log_file_path, "r") as f:
                return json.load(f)
        except Exception as e:
            self._log.warning("Failed to read state file: %s", e)
            return {}
    
    def _write_state(self, state_data: dict) -> None:
        """Write state to file atomically."""
        try:
            # Write to temp file first, then rename (atomic on POSIX)
            temp_path = self.log_file_path.with_suffix(".tmp")
            with open(temp_path, "w") as f:
                json.dump(state_data, f, indent=2, default=str)
            temp_path.replace(self.log_file_path)
        except Exception as e:
            self._log.error("Failed to write state file: %s", e)
    
    def mark_running(self, pid: int) -> None:
        """Mark pipeline as running."""
        state = {
            "status": "RUNNING",
            "pid": pid,
            "start_time": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "error_message": None
        }
        self._write_state(state)
        self._log.info("Pipeline state: RUNNING (PID: %d)", pid)
    
    def mark_completed(self, processed_count: int, metrics: dict | None = None) -> None:
        """Mark pipeline as completed successfully."""
        state = self._read_state()
        state.update({
            "status": "COMPLETED",
            "end_time": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "processed_count": processed_count,
            "error_message": None
        })
        if metrics is not None:
            state["metrics"] = metrics
        self._write_state(state)
        self._log.info("Pipeline state: COMPLETED (processed: %d)", processed_count)
    
    def mark_failed(self, error_message: str) -> None:
        """Mark pipeline as failed with error message and save backup."""
        state = self._read_state()
        state.update({
            "status": "FAILED",
            "end_time": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "error_message": error_message
        })
        self._write_state(state)
        
        # Save a timestamped backup of the failed state in pipeline_state directory
        try:
            backup_dir = self.log_file_path.parent / "pipeline_state"
            backup_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = backup_dir / f"pipeline_state.failed.{timestamp}.json"
            with open(backup_file, "w") as f:
                json.dump(state, f, indent=2, default=str)
            self._log.info("Failed state backed up to: %s", backup_file)
        except Exception as e:
            self._log.warning("Failed to save backup state file: %s", e)
        
        self._log.error("Pipeline state: FAILED - %s", error_message)
    
    def get_state(self) -> dict:
        """Get current state for external inspection."""
        return self._read_state()
    
    def is_running(self) -> bool:
        """Check if pipeline is currently running."""
        state = self._read_state()
        if state.get("status") != "RUNNING":
            return False
        
        # Check if the PID is still alive
        pid = state.get("pid")
        if pid:
            try:
                # Send signal 0 to check if process exists (doesn't actually send a signal)
                os.kill(pid, 0)
                return True
            except (OSError, ProcessLookupError):
                # Process doesn't exist anymore
                return False
        return False

# -----------------------------
# Orchestrator
# -----------------------------

def run_pipeline(limit: Optional[int] = None, source: str = "updatefiles", baseline_start: Optional[str] = None, baseline_end: Optional[str] = None, skip_vectorization: bool = False) -> None:
    cfg = get_settings()
    logging.basicConfig(level=cfg.log_level, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    if not _to_bool("HTTP_DEBUG", False):
        for noisy in (
            "urllib3",
            "httpx",
            "httpcore",
            "httpcore.connection",
            "httpcore.http11",
        ):
            logging.getLogger(noisy).setLevel(logging.WARNING)
    log = logging.getLogger("pipeline")

    # Optional lightweight metrics to understand bottlenecks
    import threading
    metrics_enabled = _to_bool("METRICS", False)
    metrics_lock = threading.Lock()
    metrics = {
        "count": 0,
        "oa": 0.0,
        "download": 0.0,
        "upload": 0.0,
        "qdrant_delete": 0.0,
        "vectorize": 0.0,
    }
    metrics_total = {
        "count": 0,
        "oa": 0.0,
        "download": 0.0,
        "upload": 0.0,
        "qdrant_delete": 0.0,
        "vectorize": 0.0,
    }

    # Initialize process state logger
    state_log_path = os.getenv("PIPELINE_STATE_LOG", "pipeline_state/pipeline_state.json")
    state_logger = ProcessStateLogger(state_log_path)
    
    # Check if already running
    if state_logger.is_running():
        log.warning("Pipeline is already running (check %s). Exiting.", state_log_path)
        return
    
    # Mark as running
    state_logger.mark_running(os.getpid())

    # Optional test limiter
    if limit is None or limit <= 0:
        limit = cfg.test_limit if cfg.test_limit > 0 else None

    # Validate source parameter and determine file source URL
    source = source.lower() if source else "updatefiles"
    if source not in {"updatefiles", "baseline", "both"}:
        source = "updatefiles"
    
    file_source_url = UPDATE_URL
    is_baseline = source in {"baseline", "both"}
    
    # Service clients
    http_session = requests.Session()
    db = DB(cfg.db_dsn, cfg.db_schema)
    minio_client = MinioClient(cfg)

    # Use Qdrant as context manager to guarantee threshold reset
    with QdrantHelper(cfg) as qdrant:
        try:
            try:
                # List files based on source
                if source == "updatefiles":
                    files = list_update_files(http_session)  # ascending
                    file_source_url = UPDATE_URL
                    log.info("Loading updatefiles (daily pipeline)")
                elif source == "baseline":
                    files = list_baseline_files(http_session, start=baseline_start, end=baseline_end)
                    file_source_url = BASELINE_URL
                    log.info("Loading baseline files (historical catalog, files %s to %s)", baseline_start or "0001", baseline_end or "END")
                else:  # both
                    baseline_files = list_baseline_files(http_session, start=baseline_start, end=baseline_end)
                    update_files = list_update_files(http_session)
                    files = sorted(set(baseline_files + update_files))
                    log.info("Loading both baseline and updatefiles (%d total files)", len(files))
                
                # Main processing logic goes here (all indented inside try block)
            except Exception as e:
                if source == "updatefiles":
                    error_msg = f"Failed to list update files: {e}"
                else:
                    error_msg = f"Failed to list files (source={source}): {e}"
                log.error(error_msg)
                state_logger.mark_failed(error_msg)
                return

            processed_global = 0
            total_files = len(files)

            # Process each XML file (ascending). We always resume within an XML using the last_processed_pmcid.
            for idx, xml_fname in enumerate(files, start=1):
                if limit is not None and processed_global >= limit:
                    break

                log.info("Starting XML %s (%d/%d, processed so far=%d)", xml_fname, idx, total_files, processed_global)

                db.ensure_xml_log_row(xml_fname)
                last_done_pmcid, file_completed = db.get_xml_log_progress(xml_fname)
                if file_completed:
                    log.info("Skipping XML %s (already fully processed)", xml_fname)
                    continue

                # download this XML.gz to a tempdir and keep it alive while processing
                tmpdir_obj = tempfile.TemporaryDirectory()
                try:
                    tmpdir = tmpdir_obj.name
                    gz_path = Path(tmpdir) / xml_fname
                    try:
                        # Determine correct URL source based on source mode and filename
                        # In "both" mode: baseline files are 0001-1274, updatefiles are 1275+
                        current_file_url = file_source_url
                        if source == "both":
                            try:
                                match = re.search(r"n(\d+)\.xml", xml_fname)
                                if match:
                                    file_num = int(match.group(1))
                                    if file_num <= 1274:
                                        current_file_url = BASELINE_URL
                                    else:
                                        current_file_url = UPDATE_URL
                            except Exception:
                                pass  # Fall back to default file_source_url
                        
                        # Download XML file itself
                        for attempt in range(5):
                            try:
                                with http_session.get(current_file_url + xml_fname, stream=True, timeout=60) as r:
                                    r.raise_for_status()
                                    with open(gz_path, "wb") as f:
                                        for chunk in r.iter_content(1024 * 512):
                                            if chunk:
                                                f.write(chunk)
                                break
                            except (ConnectionError, ChunkedEncodingError, RemoteDisconnected):
                                if attempt == 4:
                                    raise
                                time.sleep(0.5 * (2 ** attempt))
                                continue
                    except Exception as e:
                        # This is a meta failure (XML download), log at file-level and continue to next file.
                        db.record_xml_failure(xml_fname, "XML_FILE", f"xml download failed: {e}")
                        log.warning("Failed to download XML %s: %s", xml_fname, e)
                        tmpdir_obj.cleanup()
                        continue

                    # Verify the downloaded file exists and log its size for debugging
                    if not gz_path.exists():
                        db.record_xml_failure(xml_fname, "XML_FILE", "xml download missing on disk")
                        log.error("Downloaded XML missing on disk: %s", gz_path)
                        tmpdir_obj.cleanup()
                        continue
                    try:
                        gz_size = gz_path.stat().st_size
                        log.info("Downloaded XML %s (%0.2f MB)", xml_fname, gz_size / 1024 / 1024)
                    except Exception:
                        pass

                    # Prepare per-file progress aggregation
                    successes_since_log = 0
                    batch_t0 = time.perf_counter()
                    last_success_pmcid = last_done_pmcid

                    # Define processing function for a single article row
                    def process_row(row: ArticleRow):
                        nonlocal last_success_pmcid, skip_vectorization

                        if not row.pmcid:
                            return ("skip", row.pmid, row.pmcid, "no_pmcid")

                        # Respect resume: if we had a last_processed_pmcid, skip until greater (string compare on normalized)
                        if last_done_pmcid:
                            if normalize_pmcid(row.pmcid) <= normalize_pmcid(last_done_pmcid):
                                return ("skip", row.pmid, row.pmcid, "resume_skip")

                        # Wrap entire processing logic in try-except to ensure no exceptions escape and crash the pipeline
                        try:
                            return _process_row_inner(row)
                        except Exception as e:
                            # Final safety net: catch ANY exception that escaped from inner processing
                            pmcid_normalized = normalize_pmcid(row.pmcid) if row.pmcid else "UNKNOWN"
                            logging.getLogger("pipeline").exception(
                                "Unexpected exception processing PMCID %s (pmid=%s): %s",
                                pmcid_normalized, row.pmid, e
                            )
                            try:
                                db.record_xml_failure(xml_fname, pmcid_normalized, f"unexpected_error: {type(e).__name__}: {str(e)}")
                            except Exception:
                                pass  # Even logging the failure failed; just continue
                            return ("error", row.pmid, row.pmcid, "unexpected_exception")

                    def _process_row_inner(row: ArticleRow):
                        nonlocal last_success_pmcid

                        try:
                            t_oa0 = time.perf_counter()
                            oa_result = oa_fetch_pdf(http_session, row.pmcid)
                            t_oa1 = time.perf_counter()
                        except Exception as e:
                            # OA API call-level failure should be logged
                            db.record_xml_failure(xml_fname, normalize_pmcid(row.pmcid), f"oa_api_error: {e}")
                            logging.getLogger("pipeline.oa").warning("OA API error for %s: %s", row.pmcid, e)
                            return ("error", row.pmid, row.pmcid, "oa_api_error")

                        if not oa_result:
                            # No OA PDF for this PMCID, skip quietly
                            return ("skip", row.pmid, row.pmcid, "no_pdf")

                        download_url, rel_key_under_oa_pdf = oa_result
                        # Start with the deterministic OA relative path (optionally prefixed)
                        base_object_key = rel_key_under_oa_pdf
                        if cfg.file_location_prefix:
                            base_object_key = f"{cfg.file_location_prefix.strip('/')}/{base_object_key}"
                        object_key = base_object_key  # may be versioned later if existing pdf

                        # Determine existing article status
                        try:
                            current_pdf_location, already_indexed = db.get_article_status(row.pmid)
                        except Exception as e:
                            db.record_xml_failure(xml_fname, normalize_pmcid(row.pmcid), f"db_read_error: {e}")
                            return ("error", row.pmid, row.pmcid, "db_read_error")

                        # Determine old bucket/key from stored pdf_location (if any)
                        old_bucket, old_key = (None, None)
                        if current_pdf_location:
                            try:
                                old_bucket, old_key = parse_pdf_location_for_key(current_pdf_location, minio_client.bucket)
                            except Exception:
                                pass

                        # Use the OA-relative path (optionally prefixed) as the object key.
                        # Do not generate timestamps or extra suffixes — keep the key "as is".
                        object_key = base_object_key

                        key_changed = (old_key != object_key)

                        # Skip if PDF already exists with same key (no need to re-download)
                        if current_pdf_location and not key_changed and not skip_vectorization:
                            # PDF already stored, just skip unless we need to re-vectorize
                            return ("skip", row.pmid, row.pmcid, "pdf_already_exists")
                        
                        # For skip_vectorization mode, also skip if PDF exists (focus on missing PDFs only)
                        if skip_vectorization and current_pdf_location and not key_changed:
                            return ("skip", row.pmid, row.pmcid, "pdf_already_exists")

                        # Download PDF to local temp, then upload to MinIO
                        with tempfile.TemporaryDirectory() as file_tmp:
                            local_pdf = Path(file_tmp) / Path(object_key).name
                            # Download PDF with retries
                            t_dl0 = time.perf_counter()
                            for attempt in range(5):
                                try:
                                    with http_session.get(download_url, stream=True, timeout=25) as r:
                                        if r.status_code == 404:
                                            reason = "pdf_not_found_http_404"
                                            db.record_xml_failure(xml_fname, normalize_pmcid(row.pmcid), reason)
                                            return ("skip", row.pmid, row.pmcid, reason)
                                        r.raise_for_status()
                                        with open(local_pdf, "wb") as f:
                                            for chunk in r.iter_content(1024 * 512):
                                                if chunk:
                                                    f.write(chunk)
                                    break
                                except (ConnectionError, ChunkedEncodingError, RemoteDisconnected):
                                    if attempt == 4:
                                        db.record_xml_failure(xml_fname, normalize_pmcid(row.pmcid), "pdf_download_fail")
                                        return ("error", row.pmid, row.pmcid, "pdf_download_fail")
                                    time.sleep(0.5 * (2 ** attempt))
                                    continue
                                except HTTPError as e:
                                    status = getattr(e.response, "status_code", None)
                                    if status == 404:
                                        reason = "pdf_not_found_http_404"
                                        db.record_xml_failure(xml_fname, normalize_pmcid(row.pmcid), reason)
                                        return ("skip", row.pmid, row.pmcid, reason)
                                    if attempt == 4:
                                        db.record_xml_failure(
                                            xml_fname,
                                            normalize_pmcid(row.pmcid),
                                            f"pdf_http_error:{status or 'unknown'}",
                                        )
                                        return ("error", row.pmid, row.pmcid, "pdf_http_error")
                                    time.sleep(0.5 * (2 ** attempt))
                                    continue
                            t_dl1 = time.perf_counter()

                            # Upload (new) file into MinIO
                            try:
                                t_ul0 = time.perf_counter()
                                minio_client.upload_file(object_key, str(local_pdf))
                                t_ul1 = time.perf_counter()
                            except Exception as e:
                                db.record_xml_failure(xml_fname, normalize_pmcid(row.pmcid), f"minio_upload_fail: {e}")
                                return ("error", row.pmid, row.pmcid, "minio_upload_fail")

                            # If vector_indexed is True (step #2) OR the key changed (step #3), delete old chunks first
                            try:
                                t_del0 = time.perf_counter()
                                if already_indexed and old_key:
                                    qdrant.delete_by_s3_path(old_key)
                                t_del1 = time.perf_counter()
                            except Exception as e:
                                # don't abort the whole item; record and continue (we’ll overwrite with new vectors)
                                db.record_xml_failure(xml_fname, normalize_pmcid(row.pmcid), f"qdrant_delete_fail: {e}")
                                t_del1 = time.perf_counter()

                            # Vectorize into Qdrant
                            t_vec0 = time.perf_counter()
                            if skip_vectorization:
                                # Skip vectorization, just mark as stored
                                logging.getLogger("pipeline.vectorize").info(
                                    "Skipping vectorization for %s (skip_vectorization=True)", normalize_pmcid(row.pmcid)
                                )
                                t_vec1 = time.perf_counter()
                            else:
                                try:
                                    total_chunks = vectorize_pdf_to_qdrant(cfg, qdrant, row.pmid, minio_client.bucket, object_key, str(local_pdf))
                                    if total_chunks <= 0:
                                        raise RuntimeError("no_chunks_produced")
                                    t_vec1 = time.perf_counter()
                                except pymupdf.EmptyFileError:
                                    db.record_xml_failure(xml_fname, normalize_pmcid(row.pmcid), "empty_pdf")
                                    return ("error", row.pmid, row.pmcid, "empty_pdf")
                                except Exception as e:
                                    # Log full traceback to help identify whether the timeout came from
                                    # embedding generation, Qdrant upsert, or another step inside vectorization.
                                    logging.getLogger("pipeline.vectorize").exception(
                                        "Vectorize failed for %s (pmid=%s)", normalize_pmcid(row.pmcid), row.pmid
                                    )
                                    db.record_xml_failure(xml_fname, normalize_pmcid(row.pmcid), f"vectorize_fail: {e}")
                                    t_vec1 = time.perf_counter()
                                    return ("error", row.pmid, row.pmcid, "vectorize_fail")

                        # Delete old MinIO object if the key changed (step #3)
                        if key_changed and old_key:
                            minio_client.remove_object_safe(old_key)

                        # Upsert DB row with pdf_location & set vector_indexed=true
                        try:
                            db.upsert_article_after_vectorize(row, object_key)
                        except Exception as e:
                            db.record_xml_failure(xml_fname, normalize_pmcid(row.pmcid), f"db_upsert_fail: {e}")
                            return ("error", row.pmid, row.pmcid, "db_upsert_fail")

                        last_success_pmcid = normalize_pmcid(row.pmcid)

                        # Aggregate metrics
                        if metrics_enabled:
                            try:
                                with metrics_lock:
                                    metrics["count"] += 1
                                    metrics["oa"] += (t_oa1 - t_oa0)
                                    metrics["download"] += (t_dl1 - t_dl0)
                                    metrics["upload"] += max(0.0, (t_ul1 - t_ul0))
                                    metrics["qdrant_delete"] += max(0.0, (t_del1 - t_del0))
                                    metrics["vectorize"] += (t_vec1 - t_vec0)
                                    metrics_total["count"] += 1
                                    metrics_total["oa"] += (t_oa1 - t_oa0)
                                    metrics_total["download"] += (t_dl1 - t_dl0)
                                    metrics_total["upload"] += max(0.0, (t_ul1 - t_ul0))
                                    metrics_total["qdrant_delete"] += max(0.0, (t_del1 - t_del0))
                                    metrics_total["vectorize"] += (t_vec1 - t_vec0)
                            except Exception:
                                pass
                        return ("ok", row.pmid, row.pmcid, object_key)

                    # Consume the XML file with a thread pool (efficient parallelism)
                    rows_iter = iter_pubmed_articles(gz_path)
                    futures = []
                    processed_in_file_ok = 0
                    aborted_early = False
                    skip_counts = {
                        "no_pmcid": 0,
                        "no_pdf": 0,
                        "pdf_already_exists": 0,
                        "resume_skip": 0,
                        "other": 0,
                    }

                    with ThreadPoolExecutor(max_workers=max(1, cfg.max_workers)) as ex:
                        try:
                            for row in rows_iter:
                                # Optional overall limit
                                if limit is not None and (processed_global + len(futures)) >= limit:
                                    aborted_early = True
                                    break
                                futures.append(ex.submit(process_row, row))

                            for fut in as_completed(futures):
                                try:
                                    status, pmid, pmcid, info = fut.result()
                                except Exception as e:
                                    # Catch any unexpected exceptions that escaped from process_row
                                    # This ensures individual PDF failures don't crash the entire pipeline
                                    logging.getLogger("pipeline").exception("Unexpected exception in process_row: %s", e)
                                    # Try to extract some context if possible
                                    try:
                                        # The future might have some context, but we can't reliably get pmcid here
                                        # Just log and continue
                                        pass
                                    except Exception:
                                        pass
                                    continue
                            
                                if status == "ok":
                                    processed_global += 1
                                    processed_in_file_ok += 1
                                    successes_since_log += 1

                                    if successes_since_log >= cfg.xml_log_every_n:
                                        delta = time.perf_counter() - batch_t0
                                        try:
                                            db.update_xml_log_progress(xml_fname, last_success_pmcid, delta, successes_since_log)
                                        except Exception as e:
                                            logging.getLogger("pipeline.db").warning("xml_log update failed: %s", e)
                                        log.info(
                                            "Progress %s: +%d ok (file ok=%d, total ok=%d, last pmcid=%s, %.1fs)",
                                            xml_fname,
                                            successes_since_log,
                                            processed_in_file_ok,
                                            processed_global,
                                            last_success_pmcid,
                                            delta,
                                        )
                                        # If metrics are enabled, log rolling averages and reset
                                        if metrics_enabled:
                                            try:
                                                with metrics_lock:
                                                    c = max(1, metrics["count"])
                                                    log.info(
                                                        "Averages (last %d): oa=%.2fs, download=%.2fs, upload=%.2fs, qdrant_delete=%.2fs, vectorize=%.2fs",
                                                        metrics["count"],
                                                        metrics["oa"] / c,
                                                        metrics["download"] / c,
                                                        metrics["upload"] / c,
                                                        metrics["qdrant_delete"] / c,
                                                        metrics["vectorize"] / c,
                                                    )
                                                    # reset
                                                    metrics.update({
                                                        "count": 0,
                                                        "oa": 0.0,
                                                        "download": 0.0,
                                                        "upload": 0.0,
                                                        "qdrant_delete": 0.0,
                                                        "vectorize": 0.0,
                                                    })
                                            except Exception:
                                                pass
                                        successes_since_log = 0
                                        batch_t0 = time.perf_counter()

                                elif status == "skip":
                                    reason = info or "unknown"
                                    if reason in {"no_pdf", "pdf_not_found_http_404"}:
                                        skip_counts["no_pdf"] += 1
                                    elif reason == "no_pmcid":
                                        skip_counts["no_pmcid"] += 1
                                    elif reason == "pdf_already_exists":
                                        skip_counts["pdf_already_exists"] += 1
                                    elif reason == "resume_skip":
                                        skip_counts["resume_skip"] += 1
                                    else:
                                        skip_counts["other"] += 1

                                elif status == "error":
                                    # already logged in xml_failed_downloads
                                    pass
                        finally:
                            # Ensure all futures complete before executor closes
                            # This prevents premature shutdown and resource cleanup
                            for fut in futures:
                                if not fut.done():
                                    try:
                                        fut.result(timeout=30)  # Give stragglers 30s to finish
                                    except Exception:
                                        pass  # Already handled in main loop

                    # Flush remaining partial progress for this XML
                    if successes_since_log > 0 and last_success_pmcid:
                        delta = time.perf_counter() - batch_t0
                        try:
                            db.update_xml_log_progress(xml_fname, last_success_pmcid, delta, successes_since_log)
                        except Exception as e:
                            logging.getLogger("pipeline.db").warning("xml_log update failed (final): %s", e)

                    # If we did not abort early due to global limit, mark XML as fully processed.
                    if not aborted_early:
                        try:
                            db.mark_xml_completed(xml_fname, last_success_pmcid)
                        except Exception as e:
                            logging.getLogger("pipeline.db").warning("failed to mark xml completed: %s", e)

                    log.info(
                        "Finished XML %s: %d ok, skips: no_pmcid=%d, no_pdf=%d, pdf_exists=%d, resume_skip=%d, other=%d%s",
                        xml_fname,
                        processed_in_file_ok,
                        skip_counts["no_pmcid"],
                        skip_counts["no_pdf"],
                        skip_counts["pdf_already_exists"],
                        skip_counts["resume_skip"],
                        skip_counts["other"],
                        " (aborted early)" if aborted_early else "",
                    )
                
                    # Break if limit reached
                    if limit is not None and processed_global >= limit:
                        log.info("Reached global limit of %d items. Stopping.", limit)
                        break
                finally:
                    # Always clean up the temp directory for this XML
                    try:
                        tmpdir_obj.cleanup()
                    except Exception:
                        pass

            db.close()
            
            # Mark as completed and include aggregate metrics if enabled
            if metrics_enabled and metrics_total["count"] > 0:
                c = max(1, metrics_total["count"])
                metrics_payload = {
                    "totals": metrics_total,
                    "averages": {
                        "oa": metrics_total["oa"] / c,
                        "download": metrics_total["download"] / c,
                        "upload": metrics_total["upload"] / c,
                        "qdrant_delete": metrics_total["qdrant_delete"] / c,
                        "vectorize": metrics_total["vectorize"] / c,
                    }
                }
                state_logger.mark_completed(processed_global, metrics=metrics_payload)
            else:
                state_logger.mark_completed(processed_global)
            log.info("All done. processed=%s", processed_global)
        finally:
            # Cleanup HTTP session
            try:
                http_session.close()
            except Exception:
                pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="PMC OA -> MinIO -> Qdrant -> Postgres pipeline")
    ap.add_argument("--limit", type=int, default=None, help="Hard stop after processing this many successful items")
    ap.add_argument("--source", choices=["updatefiles", "baseline", "both"], default="updatefiles", 
                    help="File source: updatefiles (daily), baseline (historical), or both")
    ap.add_argument("--baseline-start", default=None, 
                    help="Baseline file number to start from (e.g., 0001, inclusive)")
    ap.add_argument("--baseline-end", default=None, 
                    help="Baseline file number to end at (e.g., 0100, inclusive)")
    ap.add_argument("--skip-vectorization", action="store_true", default=False,
                    help="Skip vectorization step (focus on MinIO PDF downloads only)")
    args = ap.parse_args()
    
    try:
        run_pipeline(limit=args.limit, source=args.source, baseline_start=args.baseline_start, baseline_end=args.baseline_end, skip_vectorization=args.skip_vectorization)
    except KeyboardInterrupt:
        log = logging.getLogger("pipeline")
        log.warning("Pipeline interrupted by user")
        state_log_path = os.getenv("PIPELINE_STATE_LOG", "pipeline_state/pipeline_state.json")
        state_logger = ProcessStateLogger(state_log_path)
        state_logger.mark_failed("Pipeline interrupted by user (KeyboardInterrupt)")
        # Try to reset Qdrant threshold
        try:
            cfg = get_settings()
            qdrant = QdrantHelper(cfg)
            qdrant.set_indexing_threshold(10_000)
            print("Reset Qdrant indexing threshold to 10,000")
        except Exception:
            pass
        raise
    except Exception as e:
        log = logging.getLogger("pipeline")
        log.exception("Pipeline failed with unexpected error")
        state_log_path = os.getenv("PIPELINE_STATE_LOG", "pipeline_state/pipeline_state.json")
        state_logger = ProcessStateLogger(state_log_path)
        state_logger.mark_failed(f"Unexpected error: {type(e).__name__}: {str(e)}")
        # Try to reset Qdrant threshold
        try:
            cfg = get_settings()
            qdrant = QdrantHelper(cfg)
            qdrant.set_indexing_threshold(10_000)
        except Exception:
            pass
        raise
