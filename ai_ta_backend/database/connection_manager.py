"""
ConnectionManager: Singleton service for dynamic per-project connection resolution.

Resolves S3, Postgres, and Qdrant connections on a per-project basis.
Projects without external configs use the default (env-based) connections.
Connections and decrypted configs are cached with TTL to avoid per-request overhead.

This module is **read-only**. CRUD for `project_external_connections` lives in
the Next.js frontend (`uiuc-chat-frontend`
`src/pages/api/UIUC-api/projectConnections*`). The table schema is owned by
the frontend Drizzle migrations; the SQLAlchemy `ProjectExternalConnection`
model mirrors that table here purely so the read path can use ORM queries.
Do not re-add write helpers to this service.
"""

import logging
import os
import threading
from contextlib import contextmanager

import boto3
from botocore.config import Config
from cachetools import TTLCache
from injector import inject
from qdrant_client import QdrantClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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

        # Caches: project_name -> value. Each resource has its own cache so
        # eviction of one (e.g. an S3 client) never affects another type.
        self._config_cache = TTLCache(maxsize=256, ttl=_CONFIG_TTL)
        self._engine_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._sql_db_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._qdrant_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._vdb_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._s3_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        # Dedicated caches for pgvector handles so they share no key
        # space with the SQLDatabase / Qdrant wrappers above.
        self._pgvector_store_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)
        self._pgvector_vdb_cache = TTLCache(maxsize=64, ttl=_CONNECTION_TTL)

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
    #
    # Routing rule: external SQL connections are scoped to **document-related**
    # data only (documents, doc_groups, documents_doc_groups,
    # documents_in_progress, documents_failed). Conversation, project, stats,
    # auth, and workflow data always live on the host main DB regardless of a
    # project's database_config.
    #
    #   get_documents_sql_db(project_name) -> external if configured, else host
    #   get_sql_db()                        -> always the host main DB
    #
    # Pick the accessor based on which tables the call will touch.

    def get_sql_db(self) -> "SQLDatabase":
        """Return the host main SQLDatabase.

        Use this for all non-document data: conversations, messages, projects,
        project_stats, llm-convo-monitor, pre_authorized_api_keys,
        n8n_workflows, project_external_connections. These tables always live
        on the host platform's database, never on a project's external DB.
        """
        return self._sql_db

    def get_documents_sql_db(self, project_name: str) -> "SQLDatabase":
        """Return the SQLDatabase that owns document-related tables for a project.

        Returns a project-specific SQLDatabase bound to the external engine
        when the project has a `database_config` set in
        `project_external_connections`; otherwise returns the host main DB.
        Only call this for document-scoped operations: documents, doc_groups,
        documents_doc_groups, documents_in_progress, documents_failed.
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
        """Get a SQLAlchemy session bound to the project's documents DB.

        Uses the project's external DB if configured, otherwise the host main
        DB. Intended for document-scoped writes (ingest pipeline). For
        host-only data, use `get_sql_db().get_session()` directly.
        """
        sql_db = self.get_documents_sql_db(project_name)
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

    # ── Vector Engine Resolution ──
    #
    # Engine selection rule (resolved per request):
    #   1. Project has an active non-null qdrant_config  → external Qdrant
    #   2. Else                                          → pgvector
    #      (host pgvector by default; per-project external pg when the
    #       project has a `database_config` — embeddings follow docs.)
    #
    # ``get_vector_db(project_name)`` returns a VectorDatabase that always
    # knows which engine to use: Qdrant-backed when `qdrant_config` is set,
    # pgvector-backed (bound to per-project or host pg) otherwise. Callers
    # do not need to branch on engine kind.

    def get_vector_engine_kind(self, project_name: str) -> str:
        """Return 'qdrant' or 'pgvector' for the given project.

        Pure decision; never builds a client. Use this to branch ingest /
        doc_groups payload writes between Qdrant setPayload and pgvector
        UPDATE.
        """
        if self._get_decrypted_field(project_name, "qdrant_config"):
            return "qdrant"
        return "pgvector"

    def get_pgvector_store(self, project_name: str | None = None):
        """Return the PgVectorStore for the project's documents pg.

        When ``project_name`` has a ``database_config`` set, the store is
        bound to the per-project external Postgres engine; otherwise the
        host singleton is returned. Imported lazily so deployments running
        pure-Qdrant don't pay the psycopg2 import cost.
        """
        if project_name is not None:
            db_config = self._get_decrypted_field(project_name, "database_config")
            if db_config:
                cached = self._pgvector_store_cache.get(project_name)
                if cached is not None:
                    return cached
                # Double-checked locking — matches `_get_or_create_engine`
                # style so concurrent first-time callers don't construct
                # two stores against the same engine.
                lock = self._get_lock(f"pgvector:{project_name}")
                with lock:
                    cached = self._pgvector_store_cache.get(project_name)
                    if cached is not None:
                        return cached
                    from ai_ta_backend.database.vector_store import PgVectorStore

                    engine = self._get_or_create_engine(project_name, db_config)
                    store = PgVectorStore(engine=engine)
                    self._pgvector_store_cache[project_name] = store
                    return store

        # Host pgvector singleton — synchronized to avoid concurrent
        # construction creating two stores on the first hit.
        with self._master_lock:
            if not hasattr(self, "_pgvector_store") or self._pgvector_store is None:
                from ai_ta_backend.database.vector_store import get_vector_store
                self._pgvector_store = get_vector_store()
        return self._pgvector_store

    def get_vector_db(self, project_name: str) -> "VectorDatabase":
        """Return a VectorDatabase for the project.

        - ``qdrant_config`` present → VectorDatabase wired to that Qdrant.
        - Otherwise → VectorDatabase wired to pgvector (per-project pg when
          ``database_config`` is present, else host pg). All existing
          VectorDatabase methods (execute_search, delete, upsert, etc.)
          dispatch internally.
        """
        qdrant_config = self._get_decrypted_field(project_name, "qdrant_config")
        if qdrant_config:
            if project_name in self._vdb_cache:
                return self._vdb_cache[project_name]
            lock = self._get_lock(f"qdrant-vdb:{project_name}")
            with lock:
                if project_name in self._vdb_cache:
                    return self._vdb_cache[project_name]
                client = self._get_or_create_qdrant(project_name, qdrant_config)
                vdb = VectorDatabase(
                    qdrant_client=client, qdrant_config=qdrant_config
                )
                self._vdb_cache[project_name] = vdb
                return vdb

        # pgvector path — bind to per-project pg if database_config is set,
        # otherwise the host singleton. Double-checked locking mirrors the
        # Qdrant path so concurrent first-time callers share one VectorDatabase.
        if project_name in self._pgvector_vdb_cache:
            return self._pgvector_vdb_cache[project_name]
        lock = self._get_lock(f"pgvector-vdb:{project_name}")
        with lock:
            if project_name in self._pgvector_vdb_cache:
                return self._pgvector_vdb_cache[project_name]
            store = self.get_pgvector_store(project_name)
            vdb = VectorDatabase(pgvector_store=store)
            self._pgvector_vdb_cache[project_name] = vdb
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
                api_key=qdrant_config["api_key"],
                port=int(qdrant_config["port"]),
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
            endpoint_url = s3_config.get("endpoint_url")
            region = s3_config.get("region")
            client_kwargs = dict(
                aws_access_key_id=s3_config["aws_access_key_id"],
                aws_secret_access_key=s3_config["aws_secret_access_key"],
                endpoint_url=endpoint_url,
                config=Config(s3={"addressing_style": "path"}) if endpoint_url else None,
            )
            if region:
                client_kwargs["region_name"] = region
            client = boto3.client("s3", **client_kwargs)
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
        self._pgvector_store_cache.pop(project_name, None)

        # Dispose engine if cached (releases pooled connections)
        engine = self._engine_cache.pop(project_name, None)
        if engine is not None:
            try:
                engine.dispose()
            except Exception as e:
                logger.warning(f"Error disposing engine for {project_name}: {e}")

        self._qdrant_cache.pop(project_name, None)
        self._vdb_cache.pop(project_name, None)
        self._pgvector_vdb_cache.pop(project_name, None)
        self._s3_cache.pop(project_name, None)

        logger.info(f"Invalidated all cached connections for project: {project_name}")

    # NOTE: Connection-testing endpoints live in the Next.js frontend
    # (uiuc-chat-frontend src/utils/projectConnections/tester.ts). The
    # backend never probes third-party endpoints on behalf of the UI; it
    # only resolves cached configs for runtime dispatch.
