---
description: >-
  I'm being extremely transparent. It's a core value of mine. Hopefully you can
  learn from our decisions.
hidden: true
noIndex: true
---

# Cost of Service

## Costs to run UIUC.chat as of April 2025

We're becoming an production Illinois service. Let's take a look at our costs before we launch to campus (anticipated full campus advertising campeign in September 2025).&#x20;

### Core Services

LLM inference is the most expensive part of the app, but we pass that onto the user with a "BYO API Keys" model. $0.&#x20;

#### Frontend: $30/mo

Hosted on Vercel, had to upgrade to pro tier for greater usage. We're doing 600k function invokations, that's dominated by our "polling" during document uploads. I'm working to reduce tons of unnecessary polling.

<figure><img src="../.gitbook/assets/CleanShot 2025-04-07 at 12.29.58.png" alt=""><figcaption><p>Typical vercel bill invoice.</p></figcaption></figure>

#### Backend: $67/mo

We host our Python backend and a few supporting services on Railway.&#x20;

<figure><img src="../.gitbook/assets/CleanShot 2025-04-07 at 12.35.03.png" alt=""><figcaption><p>Monthly cost of Railway, broken down by service.</p></figcaption></figure>

<figure><img src="../.gitbook/assets/CleanShot 2025-04-07 at 12.35.43 (1).png" alt=""><figcaption><p>History of Railway payments. Average: $67.59/mo.</p></figcaption></figure>



#### Beam.cloud: $15/mo

Beam.cloud runs our document ingest queue, and a few supporting functions for [AI Tool use](../features/tool-use-in-conversation.md#tools-demo).&#x20;

<figure><img src="../.gitbook/assets/CleanShot 2025-04-07 at 13.01.06.png" alt=""><figcaption></figcaption></figure>



### Databases

#### Postgres on Supabase: $49/mo (trending upwards).

We upgraded from the "Small" to "Medium" instance in Febuaruy 2025. Still, it seems a little under-sized for our needs and occasionally locks up under heavy load.

I’m falling out of love with Supabase. (1) It “locks up” under heavy load, e.g. a user exporting their files while another user adds tons of new file uploads. (2) Using their (optional) SDK creates vendor lock-in. (3) The pricing is good, better than most, but only on-par with AWS Aurora RDS. I’d use managed AWS RDS in the future, or self hosted vanilla Postgres + PGBouncer.&#x20;

<figure><img src="../.gitbook/assets/CleanShot 2025-04-07 at 12.37.57.png" alt=""><figcaption><p>Supabase billing history. Average $49/mo.</p></figcaption></figure>

<figure><img src="../.gitbook/assets/CleanShot 2025-04-07 at 12.41.25.png" alt=""><figcaption><p>Supabase latest bill, broken down by category. The negitive numbers come from "included usage" on the Pro Plan.</p></figcaption></figure>

#### Vector DB: $329 (with credits, trending down)

Hosted on AWS EC2 `i3en.xlarge` with all data stored in-memory - this is not the most cost effective. Using AWS credits supplied to the [Center for AI Innovation](https://ai.ncsa.illinois.edu/).

We're going to move this somewhere else more cost effective.&#x20;

#### Redis

Purchased via Redis Cloud on AWS Marketplace, just a flat rate $5/mo. Using AWS credits supplied to the [Center for AI Innovation](https://ai.ncsa.illinois.edu/).

#### AWS S3 ($25/mo)

This ranges from $10-$30/mo, depending on egress costs.

### Newsletter $16/mo

Mailgun + Ghost (self hosted) powers news.uiuc.chat. Mailgun is the only supported provider for Ghost, we pay Mailgun a base of $15/mo + usage, averaging $16/mo.&#x20;

### Nomic Atlas $100/mo

A fantastic startup creating visualizations of embedding spaces. We use this to (1) visualize all the documents a user has uploaded and (2) visualize all the conversations in each chatbot. Both have great filtering, search, clutering, hierarchical topic labeling. It's pretty great. They give us $100/mo education pricing.

## Total costs

| Service             | $/mo  | Notes                                                                        |
| ------------------- | ----- | ---------------------------------------------------------------------------- |
| Frontend            | $30   |                                                                              |
| Backend             | $82   |                                                                              |
| Databases           | $439  | $329 is Qdrant, which we're moving somewhere cheaper.                        |
| Supporting services | $116  | Mailgun + Nomic Atlas.                                                       |
| **Total**           | $667  | Soon to be $367 w/ cheaper Qdrant. Largest costs are covered by AWS credits. |



<details>

<summary>Costs when we were first starting (as of July 2024): </summary>

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





</details>



