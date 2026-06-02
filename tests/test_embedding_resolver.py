"""
Unit tests for RetrievalService embedding paths.

These tests avoid the heavy ``RetrievalService.__init__`` by allocating the
instance with ``object.__new__`` and wiring only the fields the methods under
test touch.

Coverage:
  - ``_store_conversation_content`` routes through ``embed_documents`` and never
    prepends the Qwen ``Instruct:`` query prefix to document chunks.
  - ``_embed_document`` calls ``embed_documents`` once with the raw text.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from ai_ta_backend.service import retrieval_service
from ai_ta_backend.service.retrieval_service import (
    ALLOWED_EMBEDDING_PROVIDERS,
    RetrievalService,
    _parse_allowed_embedding_providers,
)


def _build_service(embedding_client) -> RetrievalService:
    svc = object.__new__(RetrievalService)
    svc.sentry = MagicMock()
    svc.posthog = MagicMock()

    conn_manager = MagicMock()
    vdb = MagicMock()
    conn_manager.get_vector_db.return_value = vdb
    svc.conn_manager = conn_manager
    svc._test_vdb = vdb

    # Bypass _resolve_embedding_client — it requires a real conn_manager state.
    svc._resolve_embedding_client = MagicMock(
        return_value=(embedding_client, "Given a query, retrieve relevant passages")
    )
    return svc


def test_store_conversation_content_uses_embed_documents_not_embed_query():
    """Ingest must call embed_documents (no Qwen prefix), never embed_query."""
    client = MagicMock()
    client.embed_documents.side_effect = lambda texts: [[0.1, 0.2, 0.3] for _ in texts]
    client.embed_query.side_effect = AssertionError(
        "embed_query must not be called on the ingest path"
    )

    svc = _build_service(client)

    result = svc._store_conversation_content(
        text_content="hello world " * 200,
        conversation_id="conv-1",
        course_name="demo",
        readable_filename="chat.txt",
        s3_path="s3://b/k",
    )

    assert result["success"] is True
    assert result["chunks_created"] >= 1
    assert client.embed_documents.called
    assert not client.embed_query.called

    # No chunk passed in was prefixed with the Qwen instruction.
    for call in client.embed_documents.call_args_list:
        (texts,) = call.args if call.args else (call.kwargs["texts"],)
        for text in texts:
            assert not text.startswith("Instruct:"), (
                f"Document chunk was prefixed with Qwen query instruction: {text!r}"
            )

    svc._test_vdb.upsert_main_collection.assert_called_once()


def test_embed_document_helper_passes_text_through_unchanged():
    client = MagicMock()
    client.embed_documents.return_value = [[0.42]]

    svc = object.__new__(RetrievalService)
    out = svc._embed_document("raw document text", client)

    assert out == [0.42]
    client.embed_documents.assert_called_once_with(["raw document text"])
    assert not client.embed_query.called


# ---------------------------------------------------------------------------
# Provider whitelist (ALLOWED_EMBEDDING_PROVIDERS)
# ---------------------------------------------------------------------------


def test_default_allowed_providers_contains_openai_and_ollama():
    # Unset env at module load → default tuple.
    assert "openai" in ALLOWED_EMBEDDING_PROVIDERS
    assert "ollama" in ALLOWED_EMBEDDING_PROVIDERS


def test_parser_respects_comma_separated_env(monkeypatch):
    monkeypatch.setenv("ALLOWED_EMBEDDING_PROVIDERS", "openai, ollama, custom")
    assert _parse_allowed_embedding_providers() == ("openai", "ollama", "custom")


def test_parser_rejects_empty_env(monkeypatch):
    monkeypatch.setenv("ALLOWED_EMBEDDING_PROVIDERS", " , ,, ")
    with pytest.raises(ValueError, match="empty list"):
        _parse_allowed_embedding_providers()


def test_resolve_embedding_client_rejects_unknown_provider(monkeypatch):
    # Patch the module-level constant to simulate a tightened whitelist
    # without having to reload the module.
    monkeypatch.setattr(
        retrieval_service, "ALLOWED_EMBEDDING_PROVIDERS", ("openai",)
    )

    svc = object.__new__(RetrievalService)
    svc.embedding_model = "text-embedding-3-small"
    svc.openai_api_key = "sk-test"
    svc.openai_api_base = "https://api.openai.com/v1"
    svc.qwen_query_instruction = "test"

    conn_manager = MagicMock()
    # Simulate a row with a disallowed provider in embedding_config.
    def _get_field(project, field):
        if field == "embedding_config":
            return {"provider": "anthropic", "model": "voyage-3"}
        return None

    conn_manager._get_decrypted_field.side_effect = _get_field
    svc.conn_manager = conn_manager

    with pytest.raises(ValueError, match="Unsupported embedding provider"):
        svc._resolve_embedding_client("demo")


def test_resolve_embedding_client_accepts_allowed_provider(monkeypatch):
    """Whitelist enforcement does not reject in-list providers."""
    monkeypatch.setattr(
        retrieval_service, "ALLOWED_EMBEDDING_PROVIDERS", ("openai", "ollama")
    )

    svc = object.__new__(RetrievalService)
    svc.embedding_model = "text-embedding-3-small"
    svc.openai_api_key = "sk-test"
    svc.openai_api_base = "https://api.openai.com/v1"
    svc.qwen_query_instruction = "test"

    conn_manager = MagicMock()
    conn_manager._get_decrypted_field.side_effect = lambda p, f: (
        {"provider": "openai", "model": "text-embedding-3-small"}
        if f == "embedding_config"
        else None
    )
    svc.conn_manager = conn_manager

    client, instr = svc._resolve_embedding_client("demo")
    assert client is not None
    assert instr == "test"
