---
description: Description of the duplication logic used in the document ingest pipeline.
---

# Duplication in Ingested Documents

There are 2 pathways to ingest new documents into your project - direct file upload and web scrape. We have a content-based matching logic in place to check if the incoming document is already present in the system.&#x20;

The check if performed after the text extraction step in the pipeline and is as follows:&#x20;

* First, Supabase is queried based on either `s3_path` (if direct upload) or `url` (if web scrape).
* If the query doesn't return anything, the incoming document is brand new and is ingested into the database.
* If the query returns some documents, we check for an exact filename or URL match among the documents.
  * If there's no exact filename/URL match, the incoming document is new and is ingested.
  * If there is an exact filename/URL match, we compare the contents of the incoming document to the existing document.
    * If the document contents match, the incoming document is considered a duplicate and is **not** ingested.
    * If the contents do not match, the incoming document is treated as an updated version of the existing older document. The older document is removed from the database and the incoming document is ingested.\
      \
