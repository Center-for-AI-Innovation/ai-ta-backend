"""Utility functions for pipeline."""
import re
from pathlib import Path
from typing import Tuple
from urllib.parse import urlparse


# Constants
UPDATE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
OA_API = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi"


def normalize_pmcid(pmcid: str) -> str:
    """Normalize PMC ID to PMCxxxx format."""
    pmcid = (pmcid or "").strip().upper()
    return pmcid if pmcid.startswith("PMC") else f"PMC{pmcid}"


def ftp_to_https(url: str) -> str:
    """Convert FTP URLs to HTTPS."""
    p = urlparse(url)
    if p.scheme == "ftp" and p.netloc == "ftp.ncbi.nlm.nih.gov":
        return "https://" + url[len("ftp://"):]
    return url


def ftp_relative_path(url: str) -> str:
    """
    Return the path portion after /oa_pdf for MinIO storage.
    If not present, returns p.path.
    """
    p = urlparse(url)
    path = p.path
    marker = "/oa_pdf"
    idx = path.lower().find(marker)
    if idx != -1:
        rel = path[idx + len(marker):]
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
