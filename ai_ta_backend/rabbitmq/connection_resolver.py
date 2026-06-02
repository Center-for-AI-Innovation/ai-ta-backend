"""
WorkerConnectionResolver: Lightweight per-project connection resolution for the ingest worker.

Queries project_external_connections to resolve Qdrant and S3 overrides per project.
Projects without external configs get None values (caller uses env-based defaults).

Self-contained within rabbitmq/ -- crypto functions are embedded to avoid
cross-package imports (worker Docker image only includes this directory).
"""

import base64
import hashlib
import json
import logging
import os
import threading
from dataclasses import dataclass
from typing import Literal, Optional

import boto3
from botocore.config import Config
from cachetools import TTLCache
from qdrant_client import QdrantClient
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine as SAEngine

try:
    from vector_store import PgVectorStore
except ModuleNotFoundError:
    from ai_ta_backend.rabbitmq.vector_store import PgVectorStore

try:
    from rmsql import SQLAlchemyIngestDB
except ModuleNotFoundError:
    from ai_ta_backend.rabbitmq.rmsql import SQLAlchemyIngestDB

logger = logging.getLogger(__name__)

# Sentinel indicating "no external config; use defaults"
_NO_EXTERNAL = object()

_CONFIG_TTL = 300       # 5 min for decrypted configs
_CONNECTION_TTL = 1800  # 30 min for live connections


def _parse_allowed_embedding_providers() -> tuple:
    """Parse ``ALLOWED_EMBEDDING_PROVIDERS`` (comma-separated, lowercased).

    Mirrors ``retrieval_service._parse_allowed_embedding_providers`` and the
    frontend ``validation.ts`` parser. Empty / unset → default
    ``('openai', 'ollama')`` — the providers ingest knows how to embed with.
    """
    raw = os.getenv("ALLOWED_EMBEDDING_PROVIDERS")
    if not raw:
        return ("openai", "ollama")
    parsed = tuple(p for p in (s.strip().lower() for s in raw.split(",")) if p)
    if not parsed:
        raise ValueError(
            "ALLOWED_EMBEDDING_PROVIDERS is set but parses to an empty list. "
            "Unset it or supply at least one provider."
        )
    return parsed


ALLOWED_EMBEDDING_PROVIDERS: tuple = _parse_allowed_embedding_providers()


# ── Embedded Decryption (mirrors ai_ta_backend/utils/crypto.py) ──────────

def _decrypt(encrypted_text: str, key: str) -> str:
    """AES-256-GCM decryption. Format: v1.<ciphertext+tag base64>.<iv base64>"""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    version, encrypted_base64, iv_base64 = encrypted_text.split('.')
    if version != 'v1':
        raise ValueError(f'Unsupported encryption version: {version}')

    pw_hash = hashlib.sha256(key.encode('utf-8')).digest()
    iv = base64.b64decode(iv_base64)
    encrypted = base64.b64decode(encrypted_base64)
    tag = encrypted[-16:]
    ciphertext = encrypted[:-16]

    cipher = Cipher(algorithms.AES(pw_hash), modes.GCM(iv, tag), backend=default_backend())
    decryptor = cipher.decryptor()
    return (decryptor.update(ciphertext) + decryptor.finalize()).decode('utf-8')


def _decrypt_config(stored: dict) -> Optional[dict]:
    """Decrypt a {"encrypted": "v1.xxx.yyy"} blob from project_external_connections."""
    if not stored:
        return None
    encrypted_str = stored.get("encrypted")
    if not encrypted_str:
        return None
    key = os.environ.get('ENCRYPTION_MASTER_KEY', '')
    if not key:
        logger.warning("ENCRYPTION_MASTER_KEY not set; cannot decrypt external config")
        return None
    plaintext = _decrypt(encrypted_str, key)
    return json.loads(plaintext)


# ── Resolved Connections Dataclass ────────────────────────────────────────

@dataclass
class ResolvedConnections:
    """Holds per-project overrides. None fields mean 'use defaults'.

    ``engine_kind`` is always populated:
      - 'qdrant'  → external Qdrant; ``qdrant_client`` and
        ``qdrant_collection_name`` are set.
      - 'pgvector' → pgvector engine; ``pgvector_store`` is set (bound to
        the per-project external pg when ``database_config`` is present,
        else None so the worker falls back to its host pgvector store).
    """
    qdrant_client: Optional[QdrantClient] = None
    qdrant_collection_name: Optional[str] = None
    s3_client: Optional[object] = None
    s3_bucket_name: Optional[str] = None
    engine_kind: Literal['qdrant', 'pgvector'] = 'pgvector'
    documents_sql_engine: Optional[SAEngine] = None
    pgvector_store: Optional[PgVectorStore] = None

    # Per-project embedding override. None fields mean "use env defaults".
    # ``query_instruction`` is deliberately absent: it is a query-time prefix
    # and must not be applied to ingested document chunks (commit 1a8c2e9).
    embedding_provider: Optional[str] = None       # 'openai' | 'ollama'
    embedding_model: Optional[str] = None
    embedding_api_key: Optional[str] = None         # openai
    embedding_api_base: Optional[str] = None        # openai (base URL)
    embedding_base_url: Optional[str] = None        # ollama server root

    @property
    def has_overrides(self) -> bool:
        return any(
            [self.qdrant_client, self.s3_client,
             self.documents_sql_engine, self.pgvector_store,
             self.embedding_provider]
        )


# ── WorkerConnectionResolver ─────────────────────────────────────────────

class WorkerConnectionResolver:
    """Resolves per-project infrastructure connections for the ingest worker."""

    def __init__(self, sql_session: SQLAlchemyIngestDB):
        self._sql = sql_session

        self._config_cache: TTLCache = TTLCache(maxsize=256, ttl=_CONFIG_TTL)
        self._qdrant_cache: TTLCache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._s3_cache: TTLCache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._engine_cache: TTLCache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._pgvector_cache: TTLCache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)

        self._locks: dict[str, threading.Lock] = {}
        self._master_lock = threading.Lock()

    def _get_lock(self, key: str) -> threading.Lock:
        with self._master_lock:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]

    # ── Config lookup ──

    def _get_raw_config(self, project_name: str) -> Optional[dict]:
        if project_name in self._config_cache:
            cached = self._config_cache[project_name]
            return None if cached is _NO_EXTERNAL else cached

        row = self._sql.getExternalConnection(project_name)
        if not row:
            self._config_cache[project_name] = _NO_EXTERNAL
            return None

        self._config_cache[project_name] = row
        return row

    def _get_decrypted_field(self, project_name: str, field: str) -> Optional[dict]:
        config = self._get_raw_config(project_name)
        if not config:
            return None
        encrypted_blob = config.get(field)
        if not encrypted_blob:
            return None
        try:
            return _decrypt_config(encrypted_blob)
        except Exception as e:
            logger.error("Failed to decrypt %s for project %s: %s", field, project_name, e)
            return None

    # ── Public API ──

    def resolve(self, project_name: str) -> ResolvedConnections:
        """Resolve per-project Qdrant / pgvector / SQL / S3 overrides.

        Returns a ResolvedConnections with engine_kind set:
          - 'qdrant'  when ``qdrant_config`` is present
          - 'pgvector' otherwise; ``database_config`` (if present) binds
            ``documents_sql_engine`` and ``pgvector_store`` to the external pg.
        """
        if not project_name:
            return ResolvedConnections()

        result = ResolvedConnections()

        qdrant_config = self._get_decrypted_field(project_name, "qdrant_config")
        if qdrant_config:
            result.engine_kind = 'qdrant'
            result.qdrant_client = self._get_or_create_qdrant(project_name, qdrant_config)
            result.qdrant_collection_name = qdrant_config.get("default_collection")

        # database_config can coexist with qdrant_config (SQL stays on the
        # external pg even when vector lives in Qdrant).
        db_config = self._get_decrypted_field(project_name, "database_config")
        if db_config:
            engine = self._get_or_create_engine(project_name, db_config)
            result.documents_sql_engine = engine
            if result.engine_kind == 'pgvector':
                result.pgvector_store = self._get_or_create_pgvector_store(
                    project_name, engine
                )

        s3_config = self._get_decrypted_field(project_name, "s3_config")
        if s3_config:
            result.s3_client = self._get_or_create_s3(project_name, s3_config)
            result.s3_bucket_name = s3_config.get("bucket_name")

        # Embedding override. Mirrors retrieval_service._resolve_embedding_client
        # (ingest-only: no query_instruction). Legacy fallback: an ``embedding``
        # block nested in qdrant_config, used by projects predating the column.
        emb_cfg = self._get_decrypted_field(project_name, "embedding_config")
        if not emb_cfg and qdrant_config:
            emb_cfg = qdrant_config.get("embedding")
        if emb_cfg:
            self._resolve_embedding(result, project_name, emb_cfg)

        if result.has_overrides:
            logger.info(
                "Resolved external connections for project: %s (engine=%s)",
                project_name, result.engine_kind,
            )

        return result

    # ── Embedding ──

    def _resolve_embedding(self, result: ResolvedConnections,
                           project_name: str, emb_cfg: dict) -> None:
        """Populate the embedding fields on ``result`` from a decrypted
        embedding config. On a disallowed/misconfigured provider, log and leave
        the fields unset so ingest falls back to env defaults — one bad project
        must not crash the worker loop (diverges from the request-time reference,
        which raises)."""
        provider = emb_cfg.get("provider", "openai")
        if provider not in ALLOWED_EMBEDDING_PROVIDERS:
            logger.error(
                "Disallowed embedding provider %r for project %s (allowed: %s); "
                "falling back to env default embedding",
                provider, project_name, ALLOWED_EMBEDDING_PROVIDERS,
            )
            return

        if provider == "ollama":
            base_url = emb_cfg.get("base_url") or os.environ.get("OLLAMA_SERVER_URL")
            if not base_url:
                logger.error(
                    "embedding_config provider='ollama' for project %s has no "
                    "base_url and OLLAMA_SERVER_URL is unset; falling back to env "
                    "default embedding", project_name,
                )
                return
            result.embedding_base_url = base_url
        else:  # openai (or any future OpenAI-compatible provider)
            result.embedding_api_key = emb_cfg.get("api_key")
            result.embedding_api_base = emb_cfg.get("api_base")

        result.embedding_provider = provider
        result.embedding_model = emb_cfg.get("model")

    # ── Qdrant ──

    def _get_or_create_qdrant(self, project_name: str, qdrant_config: dict) -> QdrantClient:
        if project_name in self._qdrant_cache:
            return self._qdrant_cache[project_name]

        lock = self._get_lock(f"qdrant:{project_name}")
        with lock:
            if project_name in self._qdrant_cache:
                return self._qdrant_cache[project_name]

            logger.info("Creating external Qdrant client for project: %s", project_name)
            client = QdrantClient(
                url=qdrant_config["url"],
                api_key=qdrant_config["api_key"],
                port=int(qdrant_config["port"]),
                https=qdrant_config.get("https", False),
                timeout=20,
            )
            self._qdrant_cache[project_name] = client
            return client

    # ── Documents SQL engine + per-project PgVectorStore ──

    def _get_or_create_engine(self, project_name: str, db_config: dict) -> SAEngine:
        if project_name in self._engine_cache:
            return self._engine_cache[project_name]

        lock = self._get_lock(f"engine:{project_name}")
        with lock:
            if project_name in self._engine_cache:
                return self._engine_cache[project_name]

            connection_uri = db_config["connection_uri"]
            logger.info(
                "Creating external SQL engine for project: %s", project_name
            )
            engine = create_engine(
                connection_uri,
                pool_size=5,
                max_overflow=10,
                pool_recycle=1800,
                pool_pre_ping=True,
            )
            self._engine_cache[project_name] = engine
            return engine

    def _get_or_create_pgvector_store(
        self, project_name: str, engine: SAEngine
    ) -> PgVectorStore:
        if project_name in self._pgvector_cache:
            return self._pgvector_cache[project_name]

        lock = self._get_lock(f"pgvector:{project_name}")
        with lock:
            if project_name in self._pgvector_cache:
                return self._pgvector_cache[project_name]
            store = PgVectorStore(engine=engine)
            self._pgvector_cache[project_name] = store
            return store

    # ── S3 ──

    def _get_or_create_s3(self, project_name: str, s3_config: dict):
        if project_name in self._s3_cache:
            return self._s3_cache[project_name]

        lock = self._get_lock(f"s3:{project_name}")
        with lock:
            if project_name in self._s3_cache:
                return self._s3_cache[project_name]

            logger.info("Creating external S3 client for project: %s", project_name)
            endpoint_url = s3_config.get("endpoint_url")
            region = s3_config.get("region")
            client_kwargs = dict(
                aws_access_key_id=s3_config["aws_access_key_id"],
                aws_secret_access_key=s3_config["aws_secret_access_key"],
                endpoint_url=endpoint_url,
                config=Config(s3={'addressing_style': 'path'}) if endpoint_url else None,
            )
            if region:
                client_kwargs["region_name"] = region
            client = boto3.client("s3", **client_kwargs)
            self._s3_cache[project_name] = client
            return client

    # ── Cache Invalidation ──

    def invalidate(self, project_name: str):
        """Drop all cached connections/configs for a project."""
        self._config_cache.pop(project_name, None)
        self._qdrant_cache.pop(project_name, None)
        self._s3_cache.pop(project_name, None)
        engine = self._engine_cache.pop(project_name, None)
        if engine is not None:
            try:
                engine.dispose()
            except Exception as e:
                logger.warning("Failed to dispose engine for %s: %s", project_name, e)
        self._pgvector_cache.pop(project_name, None)
        logger.info("Invalidated cached connections for project: %s", project_name)
