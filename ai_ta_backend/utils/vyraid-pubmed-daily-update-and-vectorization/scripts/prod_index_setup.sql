-- Production index and stats setup for PubMed pipeline (schema: vyraid)
-- Run during low-traffic window. Uses CONCURRENTLY to avoid write blocking.

-- Disable statement timeout for this session (indexes may take 30+ minutes)
SET statement_timeout = 0;

-- 1) Core retrieval/vectorization indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_pdf_location
  ON vyraid.articles (pdf_location);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_vector_indexed
  ON vyraid.articles (vector_indexed);

-- 2) Publication-date status + coverage by date range
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_pub_date_pdf
  ON vyraid.articles (publication_date, pdf_location);

-- 3) Recent ingestion (find new PDFs to vectorize, and gap detection by retrieval time)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_last_updated_pdf
  ON vyraid.articles (last_updated DESC, pdf_location);

-- 4) Failure visibility (recent failures, recurring files)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_xml_failed_downloads_reason_ts
  ON vyraid.xml_failed_downloads (failure_reason, failure_timestamp DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_xml_failed_downloads_file
  ON vyraid.xml_failed_downloads (xml_filename, failed_pmcid);

-- 5) Composite indexes for high-priority queries (added Feb 2026)
-- These significantly improve vectorization queue queries that were timing out
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_vector_pdf_partial
  ON vyraid.articles (vector_indexed, pdf_location)
  WHERE pdf_location IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_pmid_vector
  ON vyraid.articles (pmid, vector_indexed);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_pdf_null
  ON vyraid.articles (pmid, pdf_location)
  WHERE pdf_location IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_articles_pubdate_vector
  ON vyraid.articles (publication_date DESC, vector_indexed)
  WHERE publication_date IS NOT NULL;

-- 6) Refresh planner stats
ANALYZE vyraid.articles;
ANALYZE vyraid.xml_failed_downloads;

-- Optional: lightweight coverage summary views (run after indexes are in place)
-- Daily coverage by publication date
-- CREATE MATERIALIZED VIEW vyraid.article_pubdate_daily AS
-- SELECT publication_date::date AS pub_date, COUNT(*) AS pdfs
-- FROM vyraid.articles
-- WHERE pdf_location IS NOT NULL AND publication_date IS NOT NULL
-- GROUP BY 1;
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_article_pubdate_daily_date
--   ON vyraid.article_pubdate_daily (pub_date);
-- REFRESH MATERIALIZED VIEW CONCURRENTLY vyraid.article_pubdate_daily;

-- Recent ingestion window (last 24h) quick check
-- SELECT COUNT(*) FROM vyraid.articles WHERE last_updated > NOW() - INTERVAL '24 hours';

-- Vectorization queue sample
-- SELECT pmid, pdf_location
-- FROM vyraid.articles
-- WHERE pdf_location IS NOT NULL AND vector_indexed = false
-- ORDER BY last_updated DESC
-- LIMIT 1000;
