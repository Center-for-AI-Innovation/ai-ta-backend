"""
ConnectionManager: Singleton service for dynamic per-project connection resolution.

Resolves S3, Postgres, and Qdrant connections on a per-project basis.
Projects without external configs use the default (env-based) connections.
Connections and decrypted configs are cached with TTL to avoid per-request overhead.
"""

import logging
import os
import threading
from contextlib import contextmanager

import boto3
from cachetools import TTLCache
from injector import inject
from qdrant_client import QdrantClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy.sql import text as sa_text

from ai_ta_backend.database.sql import SQLDatabase
from ai_ta_backend.database.vector import VectorDatabase
from ai_ta_backend.database.aws import AWSStorage
from ai_ta_backend.utils.crypto import decrypt_config

logger = logging.getLogger(__name__)

# Sentinel indicating "no external config; use defaults"
_NO_EXTERNAL = object()

# Cache TTLs in seconds
_CONFIG_TTL = 300  # 5 minutes for decrypted configs
_CONNECTION_TTL = 1800  # 30 minutes for live connections


class ConnectionManager:
    """Resolves per-project infrastructure connections with caching."""

    @inject
    def __init__(self, sql_db: SQLDatabase, vector_db: VectorDatabase, aws: AWSStorage):
        self._sql_db = sql_db
        self._vector_db = vector_db
        self._aws = aws

        # Caches: project_name -> value
        self._config_cache = TTLCache(maxsize=256, ttl=_CONFIG_TTL)
        self._engine_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._sql_db_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._qdrant_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._vdb_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._s3_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)

        # Per-project locks to prevent duplicate connection creation
        self._locks: dict[str, threading.Lock] = {}
        self._master_lock = threading.Lock()

        # Default connection params from env (for projects without external config)
        self._default_qdrant_collection = os.environ.get("QDRANT_COLLECTION_NAME", "")
        self._default_s3_bucket = os.environ.get("S3_BUCKET_NAME", "")

    def _get_lock(self, project_name: str) -> threading.Lock:
        with self._master_lock:
            if project_name not in self._locks:
                self._locks[project_name] = threading.Lock()
            return self._locks[project_name]

    # ── Config Resolution ──

    def _get_config(self, project_name: str) -> dict | None:
        """Get decrypted external config for a project. Returns None if no external config."""
        if project_name in self._config_cache:
            cached = self._config_cache[project_name]
            return None if cached is _NO_EXTERNAL else cached

        row = self._sql_db.getExternalConnection(project_name)
        if not row:
            self._config_cache[project_name] = _NO_EXTERNAL
            return None

        self._config_cache[project_name] = row
        return row

    def _get_decrypted_field(self, project_name: str, field: str) -> dict | None:
        """Get a specific decrypted config field (s3_config, database_config, qdrant_config)."""
        config = self._get_config(project_name)
        if not config:
            return None
        encrypted_blob = config.get(field)
        if not encrypted_blob:
            return None
        return decrypt_config(encrypted_blob)

    # ── Database Resolution ──

    def get_sql_db(self, project_name: str) -> "SQLDatabase":
        """Get a SQLDatabase instance for the given project.
        Returns a project-specific SQLDatabase bound to the external engine,
        or the default SQLDatabase if no external config exists.
        All existing SQLDatabase methods (getDisabledDocGroups, getAllMaterialsForCourse, etc.)
        work unchanged -- they just run against the right database.
        """
        db_config = self._get_decrypted_field(project_name, "database_config")
        if not db_config:
            return self._sql_db

        # Check cache for an existing SQLDatabase wrapper
        if project_name in self._sql_db_cache:
            return self._sql_db_cache[project_name]

        engine = self._get_or_create_engine(project_name, db_config)
        project_sql_db = SQLDatabase(engine=engine)
        self._sql_db_cache[project_name] = project_sql_db
        return project_sql_db

    @contextmanager
    def get_db_session(self, project_name: str):
        """Get a SQLAlchemy session for the given project.
        Uses the project's external DB if configured, otherwise the default.
        """
        sql_db = self.get_sql_db(project_name)
        with sql_db.get_session() as session:
            yield session

    def _get_or_create_engine(self, project_name: str, db_config: dict):
        if project_name in self._engine_cache:
            return self._engine_cache[project_name]

        lock = self._get_lock(f"engine:{project_name}")
        with lock:
            # Double-check after acquiring lock
            if project_name in self._engine_cache:
                return self._engine_cache[project_name]

            connection_uri = db_config["connection_uri"]
            logger.info(f"Creating external DB engine for project: {project_name}")
            engine = create_engine(
                connection_uri,
                pool_size=5,
                max_overflow=10,
                pool_recycle=1800,
                pool_pre_ping=True,
            )
            self._engine_cache[project_name] = engine
            return engine

    # ── Qdrant Client Resolution ──

    def get_vector_db(self, project_name: str) -> "VectorDatabase":
        """Get a VectorDatabase instance for the given project.

        Returns a project-specific VectorDatabase bound to the external Qdrant
        client, or the default VectorDatabase if no external config exists.
        All existing VectorDatabase methods (execute_search, delete, upsert, etc.)
        work unchanged -- they just run against the right Qdrant instance.
        """
        qdrant_config = self._get_decrypted_field(project_name, "qdrant_config")
        if not qdrant_config:
            return self._vector_db

        if project_name in self._vdb_cache:
            return self._vdb_cache[project_name]

        client = self._get_or_create_qdrant(project_name, qdrant_config)
        if "collection_name" not in qdrant_config:
            qdrant_config["collection_name"] = self._default_qdrant_collection
        vdb = VectorDatabase(qdrant_client=client, qdrant_config=qdrant_config)
        self._vdb_cache[project_name] = vdb
        return vdb

    def _get_or_create_qdrant(
        self, project_name: str, qdrant_config: dict
    ) -> QdrantClient:
        if project_name in self._qdrant_cache:
            return self._qdrant_cache[project_name]

        lock = self._get_lock(f"qdrant:{project_name}")
        with lock:
            if project_name in self._qdrant_cache:
                return self._qdrant_cache[project_name]

            logger.info(f"Creating external Qdrant client for project: {project_name}")
            client = QdrantClient(
                url=qdrant_config["url"],
                api_key=qdrant_config.get("api_key"),
                port=int(qdrant_config["port"]) if qdrant_config.get("port") else None,
                https=qdrant_config.get("https", False),
                timeout=20,
            )
            self._qdrant_cache[project_name] = client
            return client

    # ── S3 Client Resolution ──

    def get_s3_client(self, project_name: str) -> tuple["AWSStorage", str]:
        """Returns (AWSStorage, bucket_name) for the given project.
        Returns a project-specific AWSStorage wrapping the external S3 client,
        or the default AWSStorage if no external config exists.
        """
        s3_config = self._get_decrypted_field(project_name, "s3_config")
        if not s3_config:
            return self._aws, self._default_s3_bucket

        aws = self._get_or_create_s3(project_name, s3_config)
        bucket_name = s3_config.get("bucket_name", self._default_s3_bucket)
        return aws, bucket_name

    def _get_or_create_s3(self, project_name: str, s3_config: dict) -> AWSStorage:
        if project_name in self._s3_cache:
            return self._s3_cache[project_name]

        lock = self._get_lock(f"s3:{project_name}")
        with lock:
            if project_name in self._s3_cache:
                return self._s3_cache[project_name]

            logger.info(f"Creating external S3 client for project: {project_name}")
            client = boto3.client(
                "s3",
                region_name=s3_config.get("region"),
                aws_access_key_id=s3_config["aws_access_key_id"],
                aws_secret_access_key=s3_config["aws_secret_access_key"],
            )
            aws = AWSStorage(s3_client=client)
            self._s3_cache[project_name] = aws
            return aws

    # ── Cache Invalidation ───────────────────────────────────────────

    def invalidate(self, project_name: str):
        """Invalidate all cached connections and configs for a project.
        Call this when a project's external connection config is created/updated/deleted.
        """
        self._config_cache.pop(project_name, None)
        self._sql_db_cache.pop(project_name, None)

        # Dispose engine if cached (releases pooled connections)
        engine = self._engine_cache.pop(project_name, None)
        if engine is not None:
            try:
                engine.dispose()
            except Exception as e:
                logger.warning(f"Error disposing engine for {project_name}: {e}")

        self._qdrant_cache.pop(project_name, None)
        self._vdb_cache.pop(project_name, None)
        self._s3_cache.pop(project_name, None)

        logger.info(f"Invalidated all cached connections for project: {project_name}")

    # ── Connection Testing ───────────────────────────────────────────

    @staticmethod
    def test_database_connection(connection_uri: str) -> dict:
        """Test a database connection URI. Returns {"success": True} or {"success": False, "error": str}."""
        try:
            engine = create_engine(connection_uri, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(sa_text("SELECT 1"))
            engine.dispose()
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def test_qdrant_connection(
        url: str, api_key: str = None, port: int = None, https: bool = False
    ) -> dict:
        """Test a Qdrant connection. Returns {"success": True} or {"success": False, "error": str}."""
        try:
            client = QdrantClient(
                url=url,
                api_key=api_key,
                port=port,
                https=https,
                timeout=10,
            )
            client.get_collections()
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def test_s3_connection(
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region: str = None,
        bucket_name: str = None,
    ) -> dict:
        """Test an S3 connection. Returns {"success": True} or {"success": False, "error": str}."""
        try:
            client = boto3.client(
                "s3",
                region_name=region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
            if bucket_name:
                client.head_bucket(Bucket=bucket_name)
            else:
                client.list_buckets()
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}
