---
description: >-
  Connect your own S3, PostgreSQL, or Qdrant infrastructure to any project.
  Projects without external connections use the shared default infrastructure.
layout:
  title:
    visible: true
  description:
    visible: true
  tableOfContents:
    visible: true
  outline:
    visible: true
  pagination:
    visible: true
---

# External Connections

## Overview

External connections let you point any project at **your own infrastructure** -- a private S3 bucket, a dedicated PostgreSQL database, or a self-hosted Qdrant instance -- instead of relying on the shared platform resources.

{% hint style="info" %}
Projects without an external connection config require **zero setup**. They automatically use the shared platform infrastructure (default S3 bucket, PostgreSQL database, and Qdrant collection).
{% endhint %}

This is useful when you need:

* **Data isolation** -- keep documents and embeddings in your own cloud accounts.
* **Self-hosted vector search** -- run your own Qdrant cluster with custom collections.
* **Multi-collection search** -- query multiple Qdrant collections (e.g., PubMed, Patents, NCBI Books) in parallel and merge results.
* **Custom embedding models** -- use a different embedding provider (OpenAI-compatible or Ollama) per project.

## Where CRUD lives

{% hint style="warning" %}
**CRUD for external connections is owned by the Next.js frontend, not this backend.** The endpoints documented in earlier revisions of this page (`POST/GET/DELETE/PATCH /api/project-connections*`) have been removed. The frontend is the sole writer to the `project_external_connections` table; this backend is a **read-only consumer**.

* Frontend repo: `uiuc-chat-frontend`
* Source: `src/pages/api/UIUC-api/projectConnections*`
* Operator docs: `uiuc-chat-frontend/docs/EXTERNAL_CONNECTIONS.md`
* Authorization: super-admin-only (see frontend docs).
{% endhint %}

## Supported Connection Types

### S3 / MinIO

Controls where uploaded documents and exported files are stored. Configure this when you have a private S3 bucket or a self-hosted MinIO instance.

### PostgreSQL Database

Controls where **document metadata + embeddings** are stored: the `documents`, `documents_in_progress`, `documents_failed`, `doc_groups`, `documents_doc_groups`, **and `embeddings`** tables. Configure this when you want a project's document inventory and its vectors to live in your own Postgres.

{% hint style="info" %}
**Embeddings follow the documents DB.** When `database_config` is set and `qdrant_config` is NOT, the same external Postgres holds both documents and embeddings — the platform's pgvector extension is used. Operators must install `pgvector` and apply the platform's migrations on the external pg before activating the row. See the developer guide for the exact migrations.
{% endhint %}

{% hint style="info" %}
**The external SQL connection is document-scoped.** Conversations, messages, project metadata, analytics, API keys, and workflow state always remain on the host platform's main database — they are never written to a project's external Postgres, even when `database_config` is set. See [Scope of the External SQL Connection](../developers/external-connections-config.md#scope-of-the-external-sql-connection) for the full table-level breakdown.
{% endhint %}

### Qdrant Vector Database

Controls where vector embeddings live and how retrieval works. When `qdrant_config` is set, embeddings live in Qdrant — overriding the pgvector default. Every Qdrant config must set `default_collection` -- this is the project's primary collection, where all ingest writes go and which is always included in search.

Optionally, add a `collections` array to fan out searches across additional collections in parallel. Each entry can apply a post-processor that normalizes results from specialized data sources (PubMed, Patents, NCBI Books, Clinical Trials). `default_collection` is searched alongside the listed collections automatically -- you don't need to list it twice.

### Embedding Model (per project)

`embedding_config` is a top-level column for per-project embedding-model overrides. It works regardless of which vector engine the project uses (Qdrant or pgvector). When omitted, the platform falls back to the env-driven default. See the developer guide for the plaintext shape.

### Routing precedence (resolved per request)

1. `qdrant_config` present → vector lives in external Qdrant.
2. Else → vector lives in pgvector. If `database_config` is present, the **same external Postgres** stores both documents and embeddings.
3. No overrides → host Postgres (with pgvector) for both.

There is no `VECTOR_ENGINE` environment switch — the row alone decides.

## How It Works (read path)

1. **Configs are created and updated by the frontend** (see "Where CRUD lives" above). Each config block is encrypted with AES-256-GCM before being stored in JSONB.
2. **This backend resolves connections at runtime.** On every query or ingest job, `ConnectionManager` reads the project's row from `project_external_connections` and routes traffic to the right infrastructure. Only rows where `is_active = true` are honored -- this is enforced in both the web backend and the ingest worker, so toggling `is_active` flips behavior everywhere. Configs are cached (5 min TTL) and connections are cached (30 min TTL).
3. **Decryption** uses the same `ENCRYPTION_MASTER_KEY` env var the frontend used to encrypt. The two services MUST share this key.
4. **Cache invalidation across services** is the frontend's responsibility for its own caches; this backend's `ConnectionManager` currently relies on its 5-minute TTL for changes the frontend writes. A cross-service Redis pub/sub channel is on the roadmap (frontend issue).

## Security

* All config values (API keys, access keys, connection URIs) are **encrypted at rest** with AES-256-GCM.
* This backend requires the `ENCRYPTION_MASTER_KEY` environment variable to be set in order to decrypt configs at runtime.
* The frontend GET endpoint returns **masked values** (`****MPLE`) so secrets are never exposed through any API.

## Next Steps

* [Frontend API reference](../../../uiuc-chat-frontend/docs/EXTERNAL_CONNECTIONS.md) -- where to actually call CRUD.
* [Configuration Reference](../developers/external-connections-config.md) -- complete field-by-field config schemas, post-processors, embedding providers, and environment variables.
