"""Configuration and settings for PubMed pipeline."""
import os
import logging
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()


def _to_bool(name: str, default: bool = False) -> bool:
    """Convert environment variable to boolean."""
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on"}


def _to_int(name: str, default: int = 0) -> int:
    """Convert environment variable to integer."""
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _normalize_minio_endpoint(value: str) -> str:
    """Normalize MinIO endpoint to host:port format."""
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
    """Pipeline configuration from environment variables."""
    
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
    """Load and return settings from environment."""
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
