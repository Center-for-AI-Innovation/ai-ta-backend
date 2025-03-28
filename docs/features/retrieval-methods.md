# Retrieval Methods

In RAG (retrieval augmented generation), it's critical to actually retrieve the necessary context to answer a user query. Here's my favorite AI-forward ideas to find the "needle in the haystack" of relevant documents for a user query.

In order of increasing complexity and novelty:

## 1. Standard Vector Retrieval

By default (as of May, 2024) [UIUC.chat](https://www.uiuc.chat/) uses standard vector retrieval lookup. It compares an embedding of the `user query` with embeddings of all the documents in the user's project. The top 80 document chunks are used in final LLM call to answer the user query.

<figure><img src="../.gitbook/assets/how-rag-works (1).png" alt=""><figcaption><p>Generic/simple/standard RAG system.</p></figcaption></figure>

## 2. Parent document retrieval

"Parent document retrieval" refers to _expanding_ the context around the retrieved document chunks. In our case, for each of the top 5 chunks retrieved, we additionally retrieve the 2 preceding and 2 subsequent chunks to the retrieved chunk. In effect, for the closest matches from standard RAG, we grab a little more "above and below" the most relevant chunks/paragraphs we retrieved.&#x20;

Intuitively, we say this solves the "off by one" problem. For example, users as questions like `what is the solution to the Bernoulli equation?`And say we have a textbook in our document corpus. Naively this query embedding matches with the _problem setup_ in the textbook, _**but not the problem solution**_ that follows in the subsequent paragraphs. Therefore, we expand the context to ensure we capture both the problem description and solution in our relevant contexts. This works particularly well for textbook questions.

## 3. Multiple Queries + Filtering

**Key idea:** use an LLM to diversify your queries, then use an LLM to filter out irrelevant passages before we go to big LLM, e.g. GPT-4 (many times more expensive) for final answer generation.

**The retrieval data flow:**&#x20;

1. User query ->&#x20;
2. LLM generates multiple similar queries w/ more keywords ->&#x20;
3. Vector KNN Retrieval & reranking ->&#x20;
4. Filter out truly bad/irrelevant passages w/ small LLM ->&#x20;
5. Get parent docs (see above) for top 5 passages ->&#x20;
6. A set of documents/passages for 'final answer' generation w/ large LLM.

**Challenge:** LLM filtering requires 1-5 seconds at minimum, even fully parallel. Dramatic slowdown to performance. This will get better quickly as small LLMs get smarter & even faster.

## 4. LLM-Guided Retrieval

**Key idea:** It's retrieval with function calling to explore the documents.&#x20;

Mirroring the human research process, we let the LLM decide if the retrieved context is relevant and, more importantly, where to look next. The LLM decides between a set of options like "next page" or "previous page" and similar to explore the document and find the best passages to answer our target question.&#x20;

{% file src="../.gitbook/assets/Final Proposal - LLM Guided Retrieval.pdf" %}

<figure><img src="../.gitbook/assets/CleanShot 2024-05-01 at 15.40.44.png" alt=""><figcaption></figcaption></figure>

LLM Guided retrieval thrives with structured data.&#x20;

### Scientific PDF Parsing

{% hint style="info" %}
TL;DR:

1. Start with **Grobid**. Excellent at parsing `sections`, `references`.&#x20;
2. Re-run with **Unstructured**. Replace all figures and tables from Grobid with Unstructured, we find their `yolox` model is best at parsing tables accurately.
3. For math (LaTeX), use Nougout.
{% endhint %}

***

Based on our empirical testing, and conversations with domain experts from NCSA and Argonne, this is our PDF parsing pipeline for Pumbed, Arxiv, and any typical scientific PDFs.&#x20;

* [**Grobid**](https://github.com/kermitt2/grobid) **is the best at creating outlines from scientific PDFs.** The [Full-Text module](https://grobid.readthedocs.io/en/latest/training/fulltext/) properly segments articles into sections, like `1. introduction, 1.1 background on LLMs, 2. methods... etc.` Precise outlines is crucial to LLM-guided-retrieval, for the LLM to properly request other sections of the paper.
  * We highly recommend the [doc2json wrapper around Grobid](https://github.com/allenai/s2orc-doc2json) to make it easier to use the outputs.
* [**Unstructured**](https://github.com/Unstructured-IO/unstructured) **is the best at parsing tables.** In our experiments with tricky PDFs, YOLOX is slightly superior to Detectron2.&#x20;

<pre class="language-python"><code class="lang-python">from unstructured.partition.auto import partition

<strong>elements = partition(filename="path/to/file.pdf",
</strong>                     strategy="hi_res",
                     hi_res_model_name="yolox")
</code></pre>

<details>

<summary>Example of a complex table parsed with Unstructured vs Grobid</summary>

Here's a tricky table to parse. We want to capture all this info into a markdown-like format.&#x20;

* We find Grobid really struggles with this; often misses the table entirerly.
* Unstructured w/ `yolox` does a near-perfect job. In this case, the only problem is the `+/-` symbols are usually parsed into `+` symbols. Although readability is overall satisfactory.

![](<../.gitbook/assets/CleanShot 2024-07-01 at 11.01.24.png>)



</details>

* [**Nougut**](https://github.com/facebookresearch/nougat) **is the best at parsing mathematical symbols.** Excellent at parsing "rendered LaTeX symbols back raw LaTeX code." This method uses an encoder-decoder Transformer model, so realistically it requires a GPU to run.&#x20;

### Infrastructure & System Architecture

**Storage infra**

1. Store PDFs in an object store, like Minio (a self-hosted S3 alternative).
2. Store processed text in SQLite, a phenomenal database for this purpose.

**Processing Infra**

* Python main process
  * Use a [Queue](https://docs.python.org/3/library/queue.html) of PDFs to process.&#x20;
  * Use a [`ProcessPoolExecutor`](https://docs.python.org/3/library/concurrent.futures.html#processpoolexecutor) to parallelize processing.
  * Use TempFile objects to prevent the machine's disk from saturating.
* Grobid - host an endpoint on a capable server, GPU recommended but not critical.
* Unstructured - create a Flask/FastAPI endpoint on a capable server.&#x20;
* Nougat - create a Flask/FastAPI endpoint on a capable server.

### Data layout in SQLite

Our goal is to parse and store academic PDFs with maximally structured information so that LLM's can adeptly explore the documents by "asking" to view PDF sections on demand.

3 tables to store academic PDFs:

1. Article
2. Section
3. Contexts

Using [FastnanoID](https://github.com/oliverlambson/fastnanoid?tab=readme-ov-file) to quickly generate unique and short random IDs for entries.&#x20;

**Papers**

"A paper has sections and references"

<table><thead><tr><th width="104">NanoID</th><th>Num tokens</th><th width="78">Title</th><th>Date published</th><th>Journal</th><th>Authors</th><th>Sections</th></tr></thead><tbody><tr><td></td><td></td><td></td><td></td><td></td><td></td><td>[array of pointers to section object]</td></tr></tbody></table>

**Sections (including references)**

"A section has contexts"

<table><thead><tr><th>NanoID</th><th width="117">Num tokens</th><th>Section title </th><th>Section number</th><th>Contexts</th></tr></thead><tbody><tr><td></td><td></td><td></td><td>Mark "ref" if it's reference, otherwise, section numbers</td><td>[array of pointers to context object]</td></tr></tbody></table>

**Contexts**

The base unit of text. Each context must fit within an LLM embedding model's context window (typically 8k tokens or more precisely`2^13 = 8,192` tokens).

<table><thead><tr><th width="105">NanoID</th><th>Text</th><th width="127">Section title</th><th width="156">Section number</th><th width="125">Num tokens</th><th width="204">embedding-nomic_1.5</th><th width="142">Page number</th><th>stop reason</th></tr></thead><tbody><tr><td></td><td>&#x3C;Raw text></td><td></td><td></td><td></td><td></td><td></td><td>"Section" or "Token limit" if the section is larger than our embedding model context window.</td></tr></tbody></table>

#### Example SQLite DB of pubmed articles

Here's a full SQLite database you can download to explore the final form of our documents. I recommend using [DB Browser for SQLite](https://sqlitebrowser.org/) to view the tables.

{% file src="../.gitbook/assets/Pubmed_Example_extraction.db" %}
A SQLite database containing an example of a single article in the following format.&#x20;
{% endfile %}

#### SQL implementation details

SQLite, and most SQL implementations, don't allow for a single field to point to an array of foreign keys, so we use the _**Junction table**_ pattern for our one-to-many relationships.

_**Junction tables**_ simply allow one article to have **many** `sections` and one `section` to have **many** `contexts`.

* `Article_Sections` table

```sql
CREATE TABLE IF NOT EXISTS article_sections (
    Article_ID TEXT,
    Section_ID TEXT,
    PRIMARY KEY (Article_ID, Section_ID),
    FOREIGN KEY (Article_ID) REFERENCES articles(ID),
    FOREIGN KEY (Section_ID) REFERENCES sections(ID)
);
```

* `Section_Contexts` table

```sql
CREATE TABLE IF NOT EXISTS sections_contexts (
    Section_ID TEXT,
    Context_ID TEXT,
    PRIMARY KEY (Section_ID, Context_ID),
    FOREIGN KEY (Section_ID) REFERENCES sections(ID),
    FOREIGN KEY (Context_ID) REFERENCES contexts(ID)
);
```

We will publish our SQLite files here for PubMed and other academic datasets when available.
