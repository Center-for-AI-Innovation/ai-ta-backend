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

## Supported Connection Types

### S3 / MinIO

Controls where uploaded documents and exported files are stored. Configure this when you have a private S3 bucket or a self-hosted MinIO instance.

### PostgreSQL Database

Controls where **document metadata** is stored: the `documents`, `documents_in_progress`, `documents_failed`, `doc_groups`, and `documents_doc_groups` tables. Configure this when you want a project's document inventory to live in your own Postgres alongside your S3 and Qdrant.

{% hint style="info" %}
**The external SQL connection is document-scoped.** Conversations, messages, project metadata, analytics, API keys, and workflow state always remain on the host platform's main database — they are never written to a project's external Postgres, even when `database_config` is set. See [Scope of the External SQL Connection](../developers/external-connections-config.md#scope-of-the-external-sql-connection) for the full table-level breakdown.
{% endhint %}

### Qdrant Vector Database

Controls where vector embeddings live and how retrieval works. Every Qdrant config must set `default_collection` -- this is the project's primary collection, where all ingest writes go and which is always included in search.

Optionally, add a `collections` array to fan out searches across additional collections in parallel. Each entry can apply a post-processor that normalizes results from specialized data sources (PubMed, Patents, NCBI Books, Clinical Trials). `default_collection` is searched alongside the listed collections automatically -- you don't need to list it twice.

## How It Works

1. **Create or update** a connection config via the API (`POST /api/project-connections`). You can configure any combination of S3, PostgreSQL, and Qdrant -- only the configs you provide will override the defaults.
2. **Platform resolves connections at runtime.** On every query or ingest job, the system checks if the project has external configs and routes traffic to the right infrastructure. Only rows where `is_active = true` are loaded -- this is enforced in both the web backend and the ingest worker, so toggling `is_active` flips behavior everywhere. Configs are cached (5 min TTL) and connections are cached (30 min TTL) to avoid per-request overhead.
3. **Secrets are encrypted at rest** using AES-256-GCM before being stored in the database.
4. **Retrieving a config via GET** returns masked secrets -- only the last 4 characters are shown (e.g., `****MPLE`), so credentials are never exposed in API responses.
5. **Deleting individual configs** is supported by passing a `type` query param (`s3`, `database`, or `qdrant`) to `DELETE /api/project-connections`. Omit `type` to delete the entire row as before.
6. **Toggling without deleting** is supported via `PATCH /api/project-connections/active` -- this flips `is_active` so the project temporarily falls back to the shared defaults while keeping the stored configs for later reactivation.
7. **Cache is automatically invalidated** whenever you create, update, delete, or toggle a connection config.

{% hint style="warning" %}
Always **test your connection** using the `POST /api/project-connections/test` endpoint before saving. Invalid configs can prevent retrieval and ingest from working for that project.
{% endhint %}

## Security

* All config values (API keys, access keys, connection URIs) are **encrypted at rest** with AES-256-GCM.
* The `GET` endpoint returns **masked values** (e.g., `****MPLE`) so secrets are never exposed through the API.
* The backend requires the `ENCRYPTION_MASTER_KEY` environment variable to be set for encryption/decryption.
* Cached connections are automatically invalidated when configs change.

## Quick Start

### Step 1: Test the Connection

Before saving, verify that your credentials work:

```python
import requests

url = "https://uiuc.chat/api/project-connections/test"
headers = {"Content-Type": "application/json"}

# Test a Qdrant connection
data = {
    "type": "qdrant",
    "config": {
        "url": "https://your-qdrant-instance.com",
        "api_key": "your-qdrant-api-key",
        "port": 6333
    }
}

response = requests.post(url, headers=headers, json=data)
print(response.json())
# {"success": true}
```

### Step 2: Save the Connection Config

```python
url = "https://uiuc.chat/api/project-connections"

data = {
    "project_name": "my-project",
    "qdrant_config": {
        "url": "https://your-qdrant-instance.com",
        "api_key": "your-qdrant-api-key",
        "port": 6333,
        "https": True,
        "default_collection": "my-collection"
    },
    "s3_config": {
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "bucket_name": "my-project-bucket"
        # Add "endpoint_url" only if using MinIO or an S3-compatible service
    }
}

response = requests.post(url, headers=headers, json=data)
print(response.json())
# {"success": true, "project_name": "my-project", "project_id": 42}
```

### Step 3: Verify It Was Saved

```python
url = "https://uiuc.chat/api/project-connections"
params = {"project_name": "my-project"}

response = requests.get(url, params=params)
print(response.json())
# Secrets are masked -- only last 4 characters shown
# {
#   "found": true,
#   "project_name": "my-project",
#   "is_active": true,
#   "s3_config": {"aws_access_key_id": "****MPLE", "bucket_name": "****cket", ...},
#   "qdrant_config": {"url": "****com", "api_key": "****-key", ...}
# }
```

## Next Steps

* [Full API Reference](../api/endpoints.md#external-connections-api) -- detailed endpoint documentation with all request/response shapes.
* [Configuration Reference](../developers/external-connections-config.md) -- complete field-by-field config schemas, post-processors, embedding providers, and environment variables.
