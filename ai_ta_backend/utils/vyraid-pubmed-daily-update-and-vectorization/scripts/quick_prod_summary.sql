-- Quick production status queries using indexes
-- Should complete in <30s with proper indexes

SET statement_timeout = 0;

-- Total articles with PDF (should use idx_articles_pdf_location)
SELECT 'Articles with PDF' AS metric, COUNT(*) AS count
FROM vyraid.articles 
WHERE pdf_location IS NOT NULL;

-- Vectorized articles (should use idx_articles_vector_indexed)
SELECT 'Vectorized articles' AS metric, COUNT(*) AS count
FROM vyraid.articles 
WHERE vector_indexed = true;

-- Recent articles (last 24h) - should use idx_articles_last_updated_pdf
SELECT 'Added last 24h' AS metric, COUNT(*) AS count
FROM vyraid.articles 
WHERE last_updated > NOW() - INTERVAL '24 hours';

-- Total failures
SELECT 'Total failures' AS metric, COUNT(*) AS count
FROM vyraid.xml_failed_downloads;

-- Date ranges (using indexed scans)
SELECT 'Date range' AS metric, 
       MIN(publication_date)::text || ' to ' || MAX(publication_date)::text AS count
FROM vyraid.articles 
WHERE publication_date IS NOT NULL;

SELECT 'Retrieval range' AS metric,
       MIN(last_updated)::text || ' to ' || MAX(last_updated)::text AS count
FROM vyraid.articles;
