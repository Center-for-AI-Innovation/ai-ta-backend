---
description: >-
  Complete configuration reference for per-project external connections:
  S3, PostgreSQL, Qdrant, multi-collection search, embedding providers, and
  vector search post-processors.
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

# External Connections Configuration Reference

This page documents the exact JSON config format for each connection type. All configs are encrypted at rest and stored as JSONB in the `project_external_connections` table.

{% hint style="info" %}
**Who writes these rows:** the Next.js frontend (`uiuc-chat-frontend` `src/pages/api/UIUC-api/projectConnections*`) is the sole writer. This backend reads the rows at runtime for per-project dispatch. The Zod validation schemas in `uiuc-chat-frontend/src/utils/projectConnections/validation.ts` are the source of truth for required fields and accepted shapes; the descriptions on this page must stay aligned with those schemas.
{% endhint %}

For a higher-level overview, see [External Connections](../features/external-connections.md).

## Bucket Storage Config (S3 / MinIO)

The `s3_config` block supports either AWS S3 or any S3-compatible storage such as MinIO. The presence of `endpoint_url` toggles between the two: omit it for AWS S3, set it for MinIO or other S3-compatible services.

### AWS S3

```json
{
  "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
  "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "bucket_name": "my-project-bucket",
  "region": "us-east-1"
}
```

### MinIO (or other S3-compatible storage)

```json
{
  "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
  "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "bucket_name": "my-project-bucket",
  "endpoint_url": "https://minio.example.com"
}
```

| Field                    | Type   | Required | Description                                                                                         |
| ------------------------ | ------ | -------- | --------------------------------------------------------------------------------------------------- |
| `aws_access_key_id`     | string | **Yes**  | Access key ID (works for both AWS S3 and MinIO)                                                    |
| `aws_secret_access_key` | string | **Yes**  | Secret access key (works for both AWS S3 and MinIO)                                                |
| `bucket_name`            | string | No       | Bucket name. Falls back to the `S3_BUCKET_NAME` environment variable if omitted.                    |
| `endpoint_url`           | string | No       | Custom S3-compatible endpoint URL. **Provide this for MinIO**; omit for AWS S3.                     |
| `region`                 | string | No       | AWS region for the bucket (e.g., `us-east-1`). Required by clients that don't have an automatic region-resolution chain (notably the AWS SDK for JavaScript v3). When omitted, the Flask backend falls back to boto3's resolution chain (`AWS_DEFAULT_REGION`, instance metadata, etc.). |

{% hint style="info" %}
**MinIO users:** Set `endpoint_url` to your MinIO server address (e.g., `https://minio.example.com`). Path-style addressing is automatically enabled when `endpoint_url` is provided. `region` is generally not needed for MinIO but is accepted if your deployment requires it.
{% endhint %}

{% hint style="info" %}
**Frontend access:** When the Next.js frontend resolves S3 connections directly (using AWS SDK JS v3, which has no region-resolution chain), `region` must be present in the stored config. The frontend defaults to `us-east-1` when absent, so existing rows without `region` keep working — but explicitly setting it is recommended for any non-`us-east-1` bucket.
{% endhint %}

## Database Config

```json
{
  "connection_uri": "postgresql://user:password@db.example.com:5432/project_db"
}
```

| Field            | Type   | Required | Description                                                                                                      |
| ---------------- | ------ | -------- | ---------------------------------------------------------------------------------------------------------------- |
| `connection_uri` | string | **Yes**  | Full PostgreSQL connection URI. The engine is created with `pool_size=5`, `max_overflow=10`, `pool_recycle=1800`. |

### Scope of the External SQL Connection

An external `database_config` is **document-scoped**. Only the tables that participate in document ingestion and retrieval-side filtering are routed to the project's external Postgres. Conversation, project, analytics, auth, and workflow data always live on the host platform's main DB regardless of whether `database_config` is set.

When `qdrant_config` is **not** set, embeddings also live on the external DB (the platform's pgvector path uses the documents engine). When `qdrant_config` **is** set, the external DB stores documents only; embeddings go to Qdrant.

| Lives in external DB (when `database_config` is set) | Always lives on host main DB |
| ---------------------------------------------------- | ---------------------------- |
| `documents`                                          | `conversations`              |
| `documents_in_progress`                              | `messages`                   |
| `documents_failed`                                   | `projects`                   |
| `doc_groups`                                         | `project_stats`              |
| `documents_doc_groups`                               | `llm-convo-monitor`          |
| `embeddings` *(when `qdrant_config` is not set)*     | `pre_authorized_api_keys`    |
|                                                      | `n8n_workflows`              |
|                                                      | `project_external_connections` (the routing table itself) |

The external DB schema must therefore provide the six document-side tables (five plus `embeddings` when running on pgvector). Conversation history, project metadata, stats dashboards, API key resolution, and workflow locks all read/write the host DB.

### External Postgres provisioning (pgvector projects)

Before activating a `database_config` row, the operator must apply the platform's migrations on the external Postgres. The required objects are:

* `pgvector` extension (`CREATE EXTENSION IF NOT EXISTS vector;`)
* Tables:
  * `embeddings` — vectorized chunks (4096-dim by default; see migration 0007).
  * `documents`, `documents_in_progress`, `documents_failed`
  * `doc_groups`, `documents_doc_groups`
* Stored procedures: `add_document_to_group`, `add_document_to_group_url` (frontend migrations `0001_custom_functions.sql` / `0007_embeddings_table.sql`).

The frontend ships these as Drizzle migrations under `uiuc-chat-frontend/src/db/migrations/`. Apply migrations 0006 / 0007 (pgvector + embeddings) and the migrations that create the document tables on the external pg before flipping `is_active = true`.

In code, the routing rule is enforced by two `ConnectionManager` accessors:

* `get_documents_sql_db(project_name)` — returns the project's external DB if configured, else the host. Use for document-scoped queries only.
* `get_sql_db()` — always returns the host main DB. Use for everything else.

## Qdrant Config

Every Qdrant config has one required collection (`default_collection`). All ingest writes and single-collection deletes target this collection. Add an optional `collections` array to fan out reads across additional collections in parallel.

### Required: `default_collection`

`default_collection` must be set on every Qdrant config — it is the project's primary collection.

```json
{
  "url": "https://qdrant.example.com",
  "api_key": "your-qdrant-api-key",
  "port": 6333,
  "https": true,
  "default_collection": "my-project-collection",
  "skip_quantization_rescore": true
}
```

| Field                        | Type    | Required | Description                                                                                                     |
| ---------------------------- | ------- | -------- | --------------------------------------------------------------------------------------------------------------- |
| `url`                        | string  | **Yes**  | Qdrant server URL                                                                                               |
| `api_key`                    | string  | **Yes**  | API key for Qdrant authentication                                                                               |
| `port`                       | integer | **Yes**  | Qdrant port (e.g., `6333`)                                                                                      |
| `default_collection`         | string  | **Yes**  | Primary Qdrant collection. All ingest writes and single-collection deletes target this collection. Always searched on read. |
| `https`                      | boolean | No       | Whether to use HTTPS. Default: `false`.                                                                         |
| `collections`                | array   | No       | Additional collections to fan out searches across. See [Optional `collections`](#optional-collections-fan-out-search) below. |
| `skip_quantization_rescore`  | boolean | No       | Skip quantization rescore during search. Default: `true`.                                                       |
| `embedding`                  | object  | No       | **Deprecated.** Use the top-level `embedding_config` column instead. Still honored as a fallback when no top-level config is present. See [Embedding Provider Config](#embedding-provider-config) below. |

### Optional: `collections` (fan-out search)

Add `collections` when your project needs to search across multiple Qdrant collections in parallel — for example, combining results from PubMed, Patents, and your own document collection. When present, every read fans out across `default_collection` plus every entry in `collections`, with results merged and sorted by score.

```json
{
  "url": "https://qdrant.example.com",
  "api_key": "your-qdrant-api-key",
  "port": 6333,
  "https": true,
  "default_collection": "main-documents",
  "collections": [
    {
      "name": "pubmed-articles",
      "top_n": 50,
      "use_filter": false,
      "processor": "pubmed"
    },
    {
      "name": "us-patents",
      "top_n": 30,
      "processor": "patents"
    },
    {
      "name": "ncbi-books",
      "processor": "ncbi_books"
    },
    {
      "name": "clinical-trials",
      "processor": "clinical_trials"
    }
  ],
  "parallel": true,
  "sort_combined": true
}
```

{% hint style="info" %}
**`default_collection` is auto-included in fan-out search.** You do not need to list it inside `collections`. If you do list a collection whose `name` matches `default_collection`, the entry's options (`top_n`, `use_filter`, `processor`) take effect — otherwise the default is searched with no per-collection overrides. **Ingest writes always go to `default_collection` only**, regardless of how many entries are in `collections`.
{% endhint %}

#### Per-Collection Fields

| Field        | Type    | Required | Description                                                                                                                                              |
| ------------ | ------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`       | string  | **Yes**  | Qdrant collection name                                                                                                                                   |
| `top_n`      | integer | No       | Maximum results to retrieve from this collection. Defaults to the request-level `top_n` (typically 100).                                                 |
| `use_filter` | boolean | No       | Whether to apply the course-name filter to this collection. Default: `true`. Set to `false` for shared collections not partitioned by course/project.    |
| `processor`  | string  | No       | Post-processor key for normalizing results. One of: `pubmed`, `patents`, `ncbi_books`, `clinical_trials`. See [Post-Processors](#post-processors-for-vector-search) below. |

#### Top-Level Fan-Out Settings

| Field            | Type    | Default | Description                                                             |
| ---------------- | ------- | ------- | ----------------------------------------------------------------------- |
| `parallel`       | boolean | `true`  | Search all collections in parallel using a thread pool.                 |
| `sort_combined`  | boolean | `true`  | Sort the combined results from all collections by score (descending).   |

## Embedding Provider Config

The platform reads embedding overrides from the top-level **`embedding_config`** column on `project_external_connections`. The shape below applies to both vector engines (Qdrant and pgvector). If omitted, the platform uses the default embedding model from environment variables.

> **Backward compatibility:** an `embedding` key nested inside `qdrant_config` is still honored as a fallback so existing Qdrant projects keep working. New projects should set `embedding_config` directly.

### OpenAI-Compatible Provider

`embedding_config` (new top-level column) plaintext shape:

```json
{
  "provider": "openai",
  "model": "text-embedding-3-small",
  "api_key": "sk-your-openai-key",
  "api_base": "https://api.openai.com/v1",
  "query_instruction": "Represent the query for retrieval:"
}
```

### Ollama Provider

```json
{
  "provider": "ollama",
  "model": "nomic-embed-text",
  "base_url": "http://localhost:11434"
}
```

| Field               | Type   | Required       | Description                                                                                                   |
| ------------------- | ------ | -------------- | ------------------------------------------------------------------------------------------------------------- |
| `provider`          | string | **Yes**        | `"openai"` or `"ollama"`. The Zod validator rejects other values.                                             |
| `model`             | string | No             | Embedding model name. Falls back to env `EMBEDDING_MODEL`.                                                    |
| `api_key`           | string | No             | OpenAI API key. Only for `openai` provider. Falls back to env `OPENAI_API_KEY`.                               |
| `api_base`          | string | No             | OpenAI-compatible API base URL. Only for `openai` provider. Falls back to env `EMBEDDING_API_BASE`.           |
| `base_url`          | string | Required (ollama) | Ollama server URL (e.g., `http://localhost:11434`). Required when `provider` is `"ollama"`.                  |
| `query_instruction` | string | No             | Prefix instruction for Qwen embedding models. Applied as `Instruct: {instruction}\nQuery:{query}` at query time. |

{% hint style="info" %}
The `query_instruction` is only applied during **query embedding** for Qwen models. Documents are embedded without the instruction prefix during ingest.
{% endhint %}

## Post-Processors for Vector Search

Post-processors normalize search results from specialized Qdrant collections into the standard payload format used by the retrieval pipeline. They are invoked automatically during multi-collection search when the `processor` key is set on a collection entry.

### Standard Payload Fields

Every post-processor maps collection-specific fields to these standard fields:

| Field               | Description                                              |
| ------------------- | -------------------------------------------------------- |
| `page_content`      | The main text content of the result                      |
| `readable_filename` | Human-readable source name (prefixed by data source)     |
| `s3_path`           | Normalized storage path                                  |
| `course_name`       | Project/course name (set to the querying project)        |
| `url`               | Link to the original source                              |
| `pagenumber`        | Page or section number within the source document        |

### Available Post-Processors

| Processor Key      | Source Data          | What It Does                                                                                                                           |
| ------------------ | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `pubmed`           | PubMed articles      | Prefixes `"Pubmed: "` to `readable_filename`. Normalizes `s3_path` to `pubmed/` prefix. Maps `pagenumber` from payload.               |
| `patents`          | USPTO patents        | Extracts `text` field as `page_content`. Uses `uspto_url` as `url`. Prefixes `"Patent: "` to filename. Normalizes `s3_path` to `patents/`. |
| `ncbi_books`       | NCBI books           | Prefixes `"NCBI Book: "` to `readable_filename`. Maps `page_number` to `pagenumber`. Normalizes `s3_path` to `ncbi-output/` prefix.   |
| `clinical_trials`  | ClinicalTrials.gov   | Extracts `text` as `page_content`. Prefixes `"Clinical Trial: "` to filename. Normalizes `s3_path` to `clinical-trials/` prefix.      |

{% hint style="info" %}
Collections **without** a `processor` key return results as-is, with no field transformation. The processor only runs on collections that explicitly set the `processor` field in their multi-collection config entry.
{% endhint %}

## Connection Caching and Lifecycle

The platform caches connections to minimize overhead:

| Cache                        | TTL         | What's Cached                                   |
| ---------------------------- | ----------- | ------------------------------------------------ |
| Decrypted config             | 5 minutes   | Decrypted external connection configs            |
| Live connections             | 30 minutes  | SQLAlchemy engines, Qdrant clients, S3 clients   |

* **Creating, updating, or deleting** a connection config **immediately invalidates** all caches for that project.
* DB engine disposal on invalidation releases pooled connections.
* Per-project locking prevents duplicate connection creation during concurrent requests.

## Environment Variables

These environment variables must be set on the backend for external connections to work:

| Variable                  | Description                                                                                              |
| ------------------------- | -------------------------------------------------------------------------------------------------------- |
| `ENCRYPTION_MASTER_KEY`   | Base64-encoded 32-byte key for AES-256-GCM encryption of connection configs. **Required.**               |
| `QDRANT_COLLECTION_NAME`  | Platform-wide default Qdrant collection name. Used as the `default_collection` when a project has no `qdrant_config` (i.e., uses shared infrastructure). Per-project configs must set `default_collection` explicitly. |
| `S3_BUCKET_NAME`          | Default S3 bucket. Used as fallback when `bucket_name` is not set in `s3_config`.                        |

Generate an encryption key:

```bash
python -c "import os, base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```
