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
from typing import Optional

import boto3
from botocore.config import Config
from cachetools import TTLCache
from qdrant_client import QdrantClient

try:
    from rmsql import SQLAlchemyIngestDB
except ModuleNotFoundError:
    from ai_ta_backend.rabbitmq.rmsql import SQLAlchemyIngestDB

logger = logging.getLogger(__name__)

# Sentinel indicating "no external config; use defaults"
_NO_EXTERNAL = object()

_CONFIG_TTL = 300       # 5 min for decrypted configs
_CONNECTION_TTL = 1800  # 30 min for live connections


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
    """Holds per-project overrides. None fields mean 'use defaults'."""
    qdrant_client: Optional[QdrantClient] = None
    qdrant_collection_name: Optional[str] = None
    s3_client: Optional[object] = None
    s3_bucket_name: Optional[str] = None

    @property
    def has_overrides(self) -> bool:
        return any([self.qdrant_client, self.s3_client])


# ── WorkerConnectionResolver ─────────────────────────────────────────────

class WorkerConnectionResolver:
    """Resolves per-project infrastructure connections for the ingest worker."""

    def __init__(self, sql_session: SQLAlchemyIngestDB):
        self._sql = sql_session

        self._config_cache: TTLCache = TTLCache(maxsize=256, ttl=_CONFIG_TTL)
        self._qdrant_cache: TTLCache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._s3_cache: TTLCache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)

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
        """Resolve Qdrant and S3 overrides for a project.
        Returns a ResolvedConnections with None fields for anything that
        should use env-based defaults.
        """
        if not project_name:
            return ResolvedConnections()

        result = ResolvedConnections()

        qdrant_config = self._get_decrypted_field(project_name, "qdrant_config")
        if qdrant_config:
            result.qdrant_client = self._get_or_create_qdrant(project_name, qdrant_config)
            result.qdrant_collection_name = qdrant_config["default_collection"]

        s3_config = self._get_decrypted_field(project_name, "s3_config")
        if s3_config:
            result.s3_client = self._get_or_create_s3(project_name, s3_config)
            result.s3_bucket_name = s3_config.get("bucket_name")

        if result.has_overrides:
            logger.info("Resolved external connections for project: %s", project_name)

        return result

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
        logger.info("Invalidated cached connections for project: %s", project_name)
