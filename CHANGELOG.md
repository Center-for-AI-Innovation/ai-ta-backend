# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `ALLOWED_EMBEDDING_PROVIDERS` env var (default `openai,ollama`) — comma-separated allow-list enforced by `_resolve_embedding_client`. Disallowed providers now raise `ValueError("Unsupported embedding provider …")` instead of silently routing to the OpenAI client. Mirrored on the frontend by `validation.ts`, which uses the env var to narrow the Zod `provider` enum.
- Dual-engine vector dispatch in `ConnectionManager` and `VectorDatabase`: pgvector is the default; Qdrant is used only when a project has an active `qdrant_config`.
- `embedding_config` column on `project_external_connections` for per-project embedding-provider overrides (works for both Qdrant and pgvector projects). Top-level location replaces the legacy nested `qdrant_config.embedding` (still honored as fallback).
- `PgVectorStore.search()` — vector search with the same filter semantics as the frontend's `vectorSearchWithDrizzle` (cosine `<=>`, JSONB `@>` doc-group filters, conversation-id handling).
- `PgVectorStore` accepts an explicit `engine=` / `connection_uri=` so the host singleton, the worker resolver, and the main `ConnectionManager` can each bind it to the right Postgres.
- Worker `ResolvedConnections` now carries `engine_kind`, `documents_sql_engine`, and `pgvector_store`; the ingest worker uses the per-project SQL engine and pgvector store when `database_config` is set.
- Backend routing unit tests covering all four override permutations (no override / qdrant only / database only / qdrant + database).

### Changed
- `_resolve_embedding_client` now takes a `project_name` and reads `embedding_config` via the read-only `ConnectionManager`.
- `VectorDatabase.execute_search` dispatches on engine; pgvector results are wrapped as `_PgvectorScoredPoint` so downstream `_process_search_results` works uniformly.
- `SQLAlchemyIngestDB.__init__` accepts an optional `engine=` to bind to a per-project external Postgres.
- Frontend `vectorSearchWithDrizzle(projectName, params)` resolves the documents Drizzle client through `ConnectionManager` per project.
- Documentation: external-connections docs now describe the engine routing precedence and the deprecated nested `embedding` location.

### Removed
- `VECTOR_ENGINE` environment switch — engine is decided per project by the row in `project_external_connections`. Removes the `os.environ['VECTOR_ENGINE']` KeyError that previously broke imports when the env var was unset.
- Env-driven shared-Qdrant fallback in the frontend `ConnectionManager` and in the backend `VectorDatabase` default constructor.

### Fixed
- Qwen `Instruct:` query prefix is no longer mis-applied to document chunks in `_store_conversation_content`. Chat-file ingest now goes through a new `_embed_document` helper that calls `embedding_client.embed_documents([text])[0]`, so Qwen-model projects no longer pollute stored vectors with the query-only instruction prefix.
- `getPublicDocGroups` no longer branches on `VECTOR_ENGINE`.
- Ingest worker Qdrant upsert flush is no longer nested inside the per-context loop — the residual final batch is now flushed after iteration completes.
- `PgVectorStore.delete_by_filter` raises `ValueError` on unsupported keys instead of silently rewriting them to `s3_path`.
- `0010_add_embedding_config.sql` ships hand-written without a `meta/_journal.json` entry, matching the existing convention for `0001` / `0006` / `0007` / `0009`. Operators apply these via `psql`, not `drizzle-kit migrate`. See `uiuc-chat-frontend/docs/EXTERNAL_CONNECTIONS.md` ("Migration journal — important caveat").
