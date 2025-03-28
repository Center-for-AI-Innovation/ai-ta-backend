# Cost of Service

## Core Services

### Language modeling

Our biggest cost by far is OpenAI, especially GPT-4. However, in most cases we pass this cost directly to the consumer with a "BYO API Keys" model. Soon we'll support "BYO `base_url`" option users can self-host or use alternative hosting providers (like Anyscale or Fireworks) to host their LLM.

### Backend

#### Railway

Railway.app hosts our Python Flask backend. You pay per second of CPU and Memory usage. Our cost is dominated by memory usage, not CPU.&#x20;

As of January 2024, our web crawling service is a separate Railway deployment. It costs $1-2/mo during idle periods for background memory usage. Too early to tell the long-term cost of web scraping, but it should be minimal. I deployed it to Railway instead of serverless functions like Lambda because the Chrome browser is too large for Vercel's serverless. It is workable on Lambda, but my Illinois AWS account is blocked from that service.

Recent average $70/mo&#x20;

<figure><img src="../.gitbook/assets/CleanShot 2024-08-07 at 21.08.43.png" alt=""><figcaption></figcaption></figure>

<figure><img src="../.gitbook/assets/CleanShot 2024-03-26 at 17.46.24.png" alt=""><figcaption><p>Railway payment history.</p></figcaption></figure>

#### Supabase

All data is stored in Supabase. It's replicated into other purpose-specific database, like Vectors in Qdrant and metadata in Redis. But Supabase is our "Source of Truth". Supabase is $25/mo for our tier.&#x20;

#### Qdrant Vector Store

Our vector embeddings for RAG are stored in Qdrant, the best of the vector databases (used by OpenAI and Azure). It's "self-hosted" on AWS. It's an EC2 instance with the most memory per dollar, `t4g.xlarge` with 16GB of RAM, and a gp3 disk with increased IOPS for faster retrieval (it really helps). The disk is 60 GiB, `12,000 IOPS` and `250 MB/s` throughput. IOPS are important, throughput is not (because it's small text files).

This is our most expensive service since high-memory EC2 instances are expensive. $100/mo for the EC2 and $50/mo for the faster storage.

<figure><img src="../.gitbook/assets/CleanShot 2024-01-30 at 11.31.34.png" alt=""><figcaption><p>AWS bill for Qdrant hosting.</p></figcaption></figure>

#### S3 document storage

S3 stores user-uploaded content that's not text, like PDFs, Word, PowerPoint, Video, etc. That way, when a user wants to click a citation to see the source document, we can show them the full source as it was given to us.

Currently this cost about $10/mo in storage + data egress fees.&#x20;

#### Beam Serverless functions&#x20;

We run highly scalable jobs, primarily document ingest, on Beam.cloud. It's [wonderfully cheap and reliable](https://x.com/KastanDay/status/1790066477372158196). Highly recommend. Steady-state average of $5/mo so far.&#x20;

<figure><img src="../.gitbook/assets/CleanShot 2024-08-07 at 21.05.32.png" alt=""><figcaption></figcaption></figure>

### Frontend

The frontend is React on Next.js, hosted on Vercel. We're still comfortably inside the free tier. If our usage increases a lot, we could pay $20/mo for everything we need.

### Services

* [Sentry.io](https://sentry.io/) for error and latency monitoring. Free tier.&#x20;
* [Posthog.com](https://posthog.com/) for usage monitoring and custom logs. Free tier... mostly.
* [Nomic](https://www.nomic.ai/) for maps of embedding spaces. Educational free tier.
  * As of August 2024 we started an Educational enterprise tier at $99/mo.
* [GitBook](https://www.gitbook.com/) for public documentation. Free tier.



## Total costs

| Category              | Cost per Month |
| --------------------- | -------------- |
| Frontend              | $0             |
| Backend               | $260           |
| Services              | $99            |
| --------------------- | -------------  |
| TOTAL                 | $359/mo        |

