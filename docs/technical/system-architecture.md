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

## How does it work, in technical detail?

### RAG chatbot, what happens when you hit send?&#x20;

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

### Document Ingest, how does it work?

<figure><img src="../.gitbook/assets/CleanShot 2025-04-07 at 12.00.44.png" alt=""><figcaption><p>Document ingest for uploaded files. Web crawling is very similar.</p></figcaption></figure>

1. User uploads a document via "Dropzone" file upload.&#x20;
   1. Client-side check for supported filetypes.
   2. [Generate pre-signed S3 url](https://github.com/CAII-NCSA/uiuc-chat-frontend/blob/main/src/pages/api/UIUC-api/getPresignedUrl.ts) for direct Clinet --> S3 upload (bypass our servers to save bandwith fees).&#x20;
   3. After upload is complete, send POST to our Beam.cloud `Ingest()` queue.
2. Beam.cloud `Ingest()` queue. [Code is here](https://github.com/Center-for-AI-Innovation/ai-ta-backend/blob/main/ai_ta_backend/beam/ingest.py).
   1. Ingest high level: A ingest function for each filetype -> [Prevent duplicate uploads](../features/duplication-in-ingested-documents.md) -> Chunk & embed -> upload to Qdrant & SQL databases. Done. If any failure occurres, it'll retry a max of 9 times with exponential backoff.
   2. [Read filetype, forward request to proper ingest function](https://github.com/Center-for-AI-Innovation/ai-ta-backend/blob/main/ai_ta_backend/beam/ingest.py#L372) (e.g. pdf/word/excel/etc).&#x20;
   3.  Each ingest function has the same interface.

       1. Input: `s3_filepath, course_name`
          1. Call `self.split_and_upload()` with the extracted text + metadata:&#x20;

       A parallel lists of metadata and text strings, the indexes match so metadata\[0] is for text\[0], so on.  `Metadata dictionaries` (typically 1 per "page") and a list of text strings which is the content.

       ```
           metadatas: List[Dict[str, Any]] = [
               {
                   'course_name': course_name,
                   's3_path': s3_path,
                   'pagenumber': page['page_number'] + 1,
                   'timestamp': '',
                   'readable_filename': kwargs.get('readable_filename', page['readable_filename']),
                   'url': kwargs.get('url', ''),
                   'base_url': kwargs.get('base_url', ''),
               } for page in pdf_pages
           ]
           pdf_texts = [page['text'] for page in pdf_pages]
           
       ```
3. During this time, the frontend is poling the SQL database to update the website GUI with success/failed indicators.&#x20;

#### **Document ingest during web crawling**&#x20;

While web crawling we always link to the source materials, like a search engine. Our citations operate like Perplexity or ChatGPT with Search; crawl the web and link to the original sources.

Compatible "files" are uploaded to S3, including PDFs, Word, PPT, Excel. Even that, that's just a backup - we always link to the original source, and attempt to detect when they're 404 missing and fallback to our local version.&#x20;

Most web pages are not files, they're HTML, and that is _**not**_ uploaded to S3. Instead it's stored directly in SQL, and we link to the original source, just like a search engine.&#x20;

<figure><img src="../.gitbook/assets/image (5).png" alt=""><figcaption><p>Document ingest during web crawling.</p></figcaption></figure>

## Self-hostable version (coming Q1 2025)

Simplify to a single Docker-compose script.

* PostgreSQL[^1]: Main or "top level" storage, contains pointers to all other DBs and additional metadata.&#x20;
* MinIO: File storage (pdf/docx/mp4)&#x20;
* Redis/[ValKey](https://github.com/valkey-io/valkey): User and project metadata, fast retrieval needed for page load.&#x20;
* Qdrant: Vector DB for document embeddings.

<figure><img src="../.gitbook/assets/CleanShot 2024-05-01 at 09.57.08.png" alt=""><figcaption></figcaption></figure>

[^1]: 
