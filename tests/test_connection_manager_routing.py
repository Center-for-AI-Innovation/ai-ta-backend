"""
Routing tests for ConnectionManager. Pure unit tests — no live Postgres / Qdrant.

We bypass the heavy default constructor by allocating ConnectionManager with
``object.__new__`` and wiring only the fields the routing decisions actually
touch. Decryption is short-circuited by patching ``decrypt_config`` to be an
identity function so the cached rows can carry plain dicts directly.

Cases (driven by the table in §13 of the dual-engine plan):

  no override                  -> pgvector, host pg engine
  qdrant_config only           -> qdrant
  database_config only         -> pgvector, per-project pg engine
  qdrant_config + database     -> qdrant (vector); per-project pg engine
                                  still used for documents SQL
"""

from __future__ import annotations

import threading
from unittest.mock import patch

import pytest
from cachetools import TTLCache

from ai_ta_backend.database import connection_manager as cm


PROJECT = "demo"


def _build_manager(rows: dict[str, dict | None]):
    """Return a ConnectionManager wired with mock rows and identity decryption.

    ``rows`` maps project_name → external-connection row (or None for no row).
    """
    mgr = object.__new__(cm.ConnectionManager)

    # Mock sql_db with a getExternalConnection that hits the rows table.
    class _SqlStub:
        def getExternalConnection(self, project_name):
            return rows.get(project_name)

    mgr._sql_db = _SqlStub()
    mgr._vector_db = object()  # placeholder — replaced by per-project resolution
    mgr._aws = object()

    mgr._config_cache = TTLCache(maxsize=8, ttl=60)
    mgr._engine_cache = TTLCache(maxsize=8, ttl=60)
    mgr._sql_db_cache = TTLCache(maxsize=8, ttl=60)
    mgr._qdrant_cache = TTLCache(maxsize=8, ttl=60)
    mgr._vdb_cache = TTLCache(maxsize=8, ttl=60)
    mgr._s3_cache = TTLCache(maxsize=8, ttl=60)
    mgr._pgvector_store_cache = TTLCache(maxsize=8, ttl=60)
    mgr._pgvector_vdb_cache = TTLCache(maxsize=8, ttl=60)

    mgr._locks = {}
    mgr._master_lock = threading.Lock()
    mgr._default_qdrant_collection = "default-coll"
    mgr._default_s3_bucket = "default-bucket"
    return mgr


@pytest.fixture(autouse=True)
def identity_decrypt(monkeypatch):
    """Treat the encrypted blob's `encrypted` field as the plaintext dict."""

    def _identity(blob):
        if isinstance(blob, dict) and "encrypted" in blob:
            return blob["encrypted"]
        return blob

    monkeypatch.setattr(cm, "decrypt_config", _identity)


def test_no_override_routes_to_pgvector_host():
    mgr = _build_manager({PROJECT: None})
    assert mgr.get_vector_engine_kind(PROJECT) == "pgvector"


def test_qdrant_config_only_routes_to_qdrant():
    mgr = _build_manager(
        {
            PROJECT: {
                "qdrant_config": {
                    "encrypted": {
                        "url": "https://q.example.com",
                        "api_key": "k",
                        "port": 6333,
                        "default_collection": "c",
                    }
                },
                "database_config": None,
                "s3_config": None,
                "embedding_config": None,
            }
        }
    )
    assert mgr.get_vector_engine_kind(PROJECT) == "qdrant"


def test_database_config_only_routes_to_pgvector():
    mgr = _build_manager(
        {
            PROJECT: {
                "qdrant_config": None,
                "database_config": {
                    "encrypted": {"connection_uri": "postgres://u:p@h/db"}
                },
                "s3_config": None,
                "embedding_config": None,
            }
        }
    )
    assert mgr.get_vector_engine_kind(PROJECT) == "pgvector"

    # get_pgvector_store(project_name) should not return the host singleton
    # — it builds a per-project store bound to the connection_uri engine.
    with patch.object(cm, "create_engine") as create_engine_mock:
        engine_sentinel = object()
        create_engine_mock.return_value = engine_sentinel
        with patch(
            "ai_ta_backend.database.vector_store.PgVectorStore"
        ) as PgVecCtor:
            PgVecCtor.return_value = "per-project-store"
            got = mgr.get_pgvector_store(PROJECT)
    assert got == "per-project-store"
    create_engine_mock.assert_called_once()
    # The PgVectorStore should be constructed with the engine kwarg.
    _, kwargs = PgVecCtor.call_args
    assert kwargs.get("engine") is engine_sentinel


def test_qdrant_plus_database_routes_qdrant_for_vector():
    mgr = _build_manager(
        {
            PROJECT: {
                "qdrant_config": {
                    "encrypted": {
                        "url": "https://q.example.com",
                        "api_key": "k",
                        "port": 6333,
                        "default_collection": "c",
                    }
                },
                "database_config": {
                    "encrypted": {"connection_uri": "postgres://u:p@h/db"}
                },
                "s3_config": None,
                "embedding_config": None,
            }
        }
    )
    assert mgr.get_vector_engine_kind(PROJECT) == "qdrant"


def test_no_vector_engine_env_branch():
    """Even with VECTOR_ENGINE=qdrant set, no row → pgvector. Env switch is dead."""
    import os
    os.environ["VECTOR_ENGINE"] = "qdrant"
    try:
        mgr = _build_manager({PROJECT: None})
        assert mgr.get_vector_engine_kind(PROJECT) == "pgvector"
    finally:
        os.environ.pop("VECTOR_ENGINE", None)
