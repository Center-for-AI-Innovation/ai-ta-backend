"""
Embedding-resolution tests for the worker's WorkerConnectionResolver.

Pure unit tests — no live Postgres / Qdrant / Ollama. We stub the SQL session's
``getExternalConnection`` and short-circuit decryption with an identity function
(the encrypted blob's ``encrypted`` field is treated as the plaintext dict), then
assert ``resolve()`` populates only the embedding fields when only an
``embedding_config`` is present.

Mirrors the routing-table rule that ingest embeds with the project's configured
model (provider gated by ALLOWED_EMBEDDING_PROVIDERS), falling back to env
defaults when no/invalid config is present.
"""

from __future__ import annotations

import pytest

from ai_ta_backend.rabbitmq import connection_resolver as cr


PROJECT = "demo"


class _SqlStub:
    def __init__(self, rows):
        self._rows = rows

    def getExternalConnection(self, project_name):
        return self._rows.get(project_name)


def _build_resolver(rows):
    return cr.WorkerConnectionResolver(_SqlStub(rows))


@pytest.fixture(autouse=True)
def identity_decrypt(monkeypatch):
    """Treat the blob's `encrypted` field as the already-decrypted dict."""

    def _identity(blob):
        if isinstance(blob, dict) and "encrypted" in blob:
            return blob["encrypted"]
        return blob

    monkeypatch.setattr(cr, "_decrypt_config", _identity)


def test_no_embedding_config_leaves_fields_unset():
    resolver = _build_resolver({PROJECT: None})
    result = resolver.resolve(PROJECT)
    assert result.embedding_provider is None
    assert result.embedding_model is None
    assert result.has_overrides is False


def test_openai_embedding_config_resolved():
    resolver = _build_resolver(
        {
            PROJECT: {
                "qdrant_config": None,
                "database_config": None,
                "s3_config": None,
                "embedding_config": {
                    "encrypted": {
                        "provider": "openai",
                        "model": "text-embedding-3-large",
                        "api_key": "sk-test",
                        "api_base": "https://api.openai.com/v1",
                    }
                },
            }
        }
    )
    result = resolver.resolve(PROJECT)
    assert result.embedding_provider == "openai"
    assert result.embedding_model == "text-embedding-3-large"
    assert result.embedding_api_key == "sk-test"
    assert result.embedding_api_base == "https://api.openai.com/v1"
    assert result.embedding_base_url is None
    assert result.has_overrides is True


def test_ollama_embedding_config_resolved():
    resolver = _build_resolver(
        {
            PROJECT: {
                "qdrant_config": None,
                "database_config": None,
                "s3_config": None,
                "embedding_config": {
                    "encrypted": {
                        "provider": "ollama",
                        "model": "Qwen/Qwen3-Embedding-8B",
                        "base_url": "https://ollama.example.com",
                    }
                },
            }
        }
    )
    result = resolver.resolve(PROJECT)
    assert result.embedding_provider == "ollama"
    assert result.embedding_model == "Qwen/Qwen3-Embedding-8B"
    assert result.embedding_base_url == "https://ollama.example.com"
    assert result.embedding_api_key is None


def test_ollama_falls_back_to_env_server_url(monkeypatch):
    monkeypatch.setenv("OLLAMA_SERVER_URL", "https://env-ollama.example.com")
    resolver = _build_resolver(
        {
            PROJECT: {
                "qdrant_config": None,
                "database_config": None,
                "s3_config": None,
                "embedding_config": {
                    "encrypted": {"provider": "ollama", "model": "m"}
                },
            }
        }
    )
    result = resolver.resolve(PROJECT)
    assert result.embedding_provider == "ollama"
    assert result.embedding_base_url == "https://env-ollama.example.com"


def test_ollama_without_base_url_falls_back_to_default(monkeypatch):
    monkeypatch.delenv("OLLAMA_SERVER_URL", raising=False)
    resolver = _build_resolver(
        {
            PROJECT: {
                "qdrant_config": None,
                "database_config": None,
                "s3_config": None,
                "embedding_config": {
                    "encrypted": {"provider": "ollama", "model": "m"}
                },
            }
        }
    )
    result = resolver.resolve(PROJECT)
    # No base_url and no env → treat as no override (env default embedding).
    assert result.embedding_provider is None
    assert result.embedding_base_url is None


def test_disallowed_provider_falls_back_to_default(monkeypatch):
    monkeypatch.setattr(cr, "ALLOWED_EMBEDDING_PROVIDERS", ("openai", "ollama"))
    resolver = _build_resolver(
        {
            PROJECT: {
                "qdrant_config": None,
                "database_config": None,
                "s3_config": None,
                "embedding_config": {
                    "encrypted": {"provider": "cohere", "model": "embed-v3"}
                },
            }
        }
    )
    result = resolver.resolve(PROJECT)
    assert result.embedding_provider is None
    assert result.embedding_model is None


def test_legacy_embedding_nested_in_qdrant_config(monkeypatch):
    # Avoid building a real Qdrant client for the legacy-fallback path.
    monkeypatch.setattr(
        cr.WorkerConnectionResolver,
        "_get_or_create_qdrant",
        lambda self, project_name, cfg: object(),
    )
    resolver = _build_resolver(
        {
            PROJECT: {
                "qdrant_config": {
                    "encrypted": {
                        "url": "https://q.example.com",
                        "api_key": "k",
                        "port": 6333,
                        "default_collection": "c",
                        "embedding": {
                            "provider": "openai",
                            "model": "legacy-model",
                            "api_key": "sk-legacy",
                            "api_base": "https://legacy.example.com/v1",
                        },
                    }
                },
                "database_config": None,
                "s3_config": None,
                "embedding_config": None,
            }
        }
    )
    result = resolver.resolve(PROJECT)
    assert result.embedding_provider == "openai"
    assert result.embedding_model == "legacy-model"
    assert result.embedding_api_base == "https://legacy.example.com/v1"
