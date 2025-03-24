# System Architecture

The key priority of this architecture is developer velocity.

* For hosted offerings, Vercel + Railway + Supabase + Beam has been a fantastic combo.
* We also self host much of our stack with Docker.

<figure><img src="../.gitbook/assets/CleanShot 2025-03-04 at 12.59.19.png" alt=""><figcaption><p>Architecture as of March 2025. Every grey line item is a Docker container.</p></figcaption></figure>

### Our entire stack

Everything runs in Docker. Vercel is the one exception, but we also have a docker version.&#x20;

**Full stack frontend: React + Next.js**

**Backend: Python Flask**

* Only used for Python-specific features, like advanced retrieval methods, Nomic document maps.&#x20;
* All other backend operations live in Next.js.

**Databases**&#x20;

* SQL: Postgres&#x20;
* Object storage: S3 / MinIO&#x20;
* Vector DB: Qdrant&#x20;
* Metadata: Redis - required for every page load

**Required stateless services:**&#x20;

* Document ingest queue (to handle spiky workloads without overwhelming our DBs): Python-RQ&#x20;
* User Auth: Keycloak (user data stored in Postgres)

**Optional stateless add-ons:**&#x20;

* LLM Serving: Ollama and vLLM&#x20;
* Web Crawling: Crawlee&#x20;
* Semantic Maps of documents and conversation history: Nomic Atlas

**Optional state-full add-ons:**&#x20;

* Tool use: N8N workflow builder&#x20;
* Error monitoring: Sentry&#x20;
* Google Analytics clone: Posthog

### User-defined Custom Tool Use by LLM

Using N8N for a user-friendly GUI to define custom tools. This way, any user can give their chatbot custom tools that will be automatically invoked when appropriate, as decided by the LLM.

## What happens when you hit send?&#x20;

1. User submits prompt
   1. Determine if tools should be invoked, if so execute them and store the outputs.
2. Embed user prompt with LLM embedding model
3. Retrieve most related documents from vector DB
4. Robust prompt engineering to:
   1. add as many documents as possible to the context window,
   2. retain as much of the conversation history as possible
   3. include tool outputs and images
   4. include our user-configurable prompt engineering features (tutor mode, document references)
5. Send final prompt-engineered message to the final LLM, stream result.
   1. During streaming, replace LLM citations with proper links (using state machine). e.g. \[doc 1, page 3] is replaced with [https://s3.link-to-document.pdf?page=3](https://s3.link-to-document.pdf/?page=3)

## Self-hostable version (coming Q1 2025)

Simplify to a single Docker-compose script.

* PostgreSQL[^1]: Main or "top level" storage, contains pointers to all other DBs and additional metadata.&#x20;
* MinIO: File storage (pdf/docx/mp4)&#x20;
* Redis/[ValKey](https://github.com/valkey-io/valkey): User and project metadata, fast retrieval needed for page load.&#x20;
* Qdrant: Vector DB for document embeddings.

<figure><img src="../.gitbook/assets/CleanShot 2024-05-01 at 09.57.08.png" alt=""><figcaption></figcaption></figure>

[^1]: 
