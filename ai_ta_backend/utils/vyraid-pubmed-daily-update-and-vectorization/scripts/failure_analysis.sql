-- Production failure analysis
-- Understand why PDFs are failing to download

SET statement_timeout = 0;

-- Top failure reasons (what's preventing PDFs from being retrieved)
SELECT 
    failure_reason,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM vyraid.xml_failed_downloads), 2) AS pct
FROM vyraid.xml_failed_downloads
GROUP BY failure_reason
ORDER BY count DESC
LIMIT 20;

-- Recent failures (last 24h) by reason
SELECT 
    failure_reason,
    COUNT(*) AS count
FROM vyraid.xml_failed_downloads
WHERE failure_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY failure_reason
ORDER BY count DESC;

-- Failures by XML file (which XML sources have most failures)
SELECT 
    xml_filename,
    COUNT(*) AS failure_count,
    COUNT(DISTINCT failed_pmcid) AS unique_pmcids
FROM vyraid.xml_failed_downloads
GROUP BY xml_filename
ORDER BY failure_count DESC
LIMIT 20;

-- Timeline of failures (are they getting better or worse?)
SELECT 
    DATE_TRUNC('week', failure_timestamp) AS week,
    COUNT(*) AS failure_count
FROM vyraid.xml_failed_downloads
GROUP BY DATE_TRUNC('week', failure_timestamp)
ORDER BY week DESC
LIMIT 12;

-- Specific failure breakdown
SELECT 
    failure_reason,
    COUNT(*) AS total,
    MIN(failure_timestamp) AS first_seen,
    MAX(failure_timestamp) AS last_seen
FROM vyraid.xml_failed_downloads
GROUP BY failure_reason
ORDER BY total DESC;
