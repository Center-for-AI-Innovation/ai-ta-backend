"""Database access layer for PubMed pipeline."""
import functools
import logging
import time
from typing import Optional, Tuple

import psycopg2
from psycopg2 import sql, OperationalError as PgOperationalError

from .models import ArticleRow


def pg_retry(max_attempts=4, initial_delay=1.0, max_delay=10.0, backoff_factor=2.0):
    """
    Decorator to retry PostgreSQL operations on connection/network failures.
    Handles psycopg2.OperationalError which covers connection timeouts and network issues.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(self, *args, **kwargs)
                except PgOperationalError as e:
                    last_exception = e
                    error_msg = str(e).lower()

                    # Leave the connection usable for the next attempt
                    try:
                        self._safe_rollback()
                    except Exception:
                        pass
                    
                    # Only retry on connection/network related errors
                    if any(keyword in error_msg for keyword in [
                        'connection', 'timeout', 'network', 'server closed',
                        'connection refused', 'connection timed out',
                        'could not connect', 'connection lost'
                    ]):
                        if attempt < max_attempts:
                            self._log.warning(
                                "PostgreSQL connection error (attempt %d/%d): %s. Retrying in %.1fs...",
                                attempt, max_attempts, e, delay
                            )
                            time.sleep(delay)
                            delay = min(delay * backoff_factor, max_delay)
                            
                            # Try to reconnect
                            try:
                                self._reconnect()
                            except Exception as reconnect_err:
                                self._log.warning("Reconnection failed: %s", reconnect_err)
                            continue
                    
                    # Re-raise if not a retryable error or max attempts reached
                    raise
                except Exception:
                    # Make sure we are not stuck in an aborted transaction before bubbling up
                    try:
                        self._safe_rollback()
                    except Exception:
                        pass
                    # Re-raise non-PostgreSQL exceptions immediately
                    raise
            
            # If we get here, all retries failed
            self._log.error("PostgreSQL operation failed after %d attempts", max_attempts)
            raise last_exception
        
        return wrapper
    return decorator


class DB:
    """Database access layer for PubMed pipeline (Supabase PostgreSQL)."""

    def __init__(self, dsn: str, schema: Optional[str]):
        self._dsn = dsn
        self._schema = schema
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False
        self._log = logging.getLogger("pipeline.db")

    def _safe_rollback(self) -> None:
        """Rollback if the connection is in an aborted state; ignore rollback errors."""
        try:
            if self._conn:
                self._conn.rollback()
        except Exception:
            pass

    def _reconnect(self) -> None:
        """Attempt to reconnect to the database."""
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass
        
        self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = False
        self._log.info("Successfully reconnected to PostgreSQL")
    
    def _with_reconnect(self, operation_func):
        """Execute database operation with automatic reconnection on connection errors."""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                return operation_func()
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                self._log.warning("DB operation failed (attempt %d/%d): %s", attempt, max_attempts, e)
                if attempt < max_attempts:
                    try:
                        self._reconnect()
                    except Exception as reconnect_err:
                        self._log.error("Reconnection failed: %s", reconnect_err)
                        if attempt == max_attempts - 1:
                            raise
                else:
                    raise
        raise RuntimeError("DB operation failed after all reconnection attempts")

    def close(self) -> None:
        """Close database connection."""
        try:
            self._conn.close()
        except Exception:
            pass

    def _tbl(self, name: str) -> sql.Identifier:
        """Create table identifier with optional schema prefix."""
        return sql.Identifier(self._schema, name) if self._schema else sql.Identifier(name)

    # ========== Articles ==========

    @pg_retry(max_attempts=3)
    def get_article_status(self, pmid: int) -> Tuple[Optional[str], bool]:
        """
        Get article PDF location and vector_indexed status.
        Returns (pdf_location, vector_indexed).
        """
        tbl = self._tbl("articles")
        q = sql.SQL("SELECT pdf_location, COALESCE(vector_indexed,false) FROM {tbl} WHERE pmid = %s").format(tbl=tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (pmid,))
            row = cur.fetchone()
            if not row:
                return None, False
            return (row[0], bool(row[1]))

    @pg_retry(max_attempts=3)
    def upsert_article_after_retrieval(self, row: ArticleRow, pdf_location: str) -> None:
        """
        Upsert article after successful retrieval & MinIO upload.
        Sets vector_indexed=false initially.
        """
        tbl = self._tbl("articles")
        q = sql.SQL(
            """
            INSERT INTO {tbl} (pmid, pmcid, doi, title, abstract, publication_date, pdf_location, vector_indexed, last_updated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,false,NOW())
            ON CONFLICT (pmid) DO UPDATE SET
                pmcid = EXCLUDED.pmcid,
                doi = EXCLUDED.doi,
                title = EXCLUDED.title,
                abstract = EXCLUDED.abstract,
                publication_date = EXCLUDED.publication_date,
                pdf_location = EXCLUDED.pdf_location,
                vector_indexed = false,
                last_updated = NOW()
            """
        ).format(tbl=tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (
                row.pmid,
                row.pmcid,
                row.doi,
                row.title,
                row.abstract,
                row.publication_date,
                pdf_location,
            ))
        self._conn.commit()

    @pg_retry(max_attempts=3)
    def upsert_article_after_vectorize(self, row: ArticleRow, pdf_location: str) -> None:
        """
        Upsert article after successful vectorization.
        Sets vector_indexed=true.
        """
        tbl = self._tbl("articles")
        q = sql.SQL(
            """
            INSERT INTO {tbl} (pmid, pmcid, doi, title, abstract, publication_date, pdf_location, vector_indexed, last_updated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,true,NOW())
            ON CONFLICT (pmid) DO UPDATE SET
                pmcid = EXCLUDED.pmcid,
                doi = EXCLUDED.doi,
                title = EXCLUDED.title,
                abstract = EXCLUDED.abstract,
                publication_date = EXCLUDED.publication_date,
                pdf_location = EXCLUDED.pdf_location,
                vector_indexed = true,
                last_updated = NOW()
            """
        ).format(tbl=tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (
                row.pmid,
                row.pmcid,
                row.doi,
                row.title,
                row.abstract,
                row.publication_date,
                pdf_location,
            ))
        self._conn.commit()

    @pg_retry(max_attempts=3)
    def query_unindexed_articles(self, limit: Optional[int] = None) -> list:
        """
        Query articles that have not been vectorized yet (vector_indexed=false).
        Returns list of (pmid, pmcid, pdf_location).
        """
        tbl = self._tbl("articles")
        if limit:
            q = sql.SQL(
                "SELECT pmid, pmcid, pdf_location FROM {tbl} WHERE vector_indexed=false LIMIT %s"
            ).format(tbl=tbl)
            with self._conn.cursor() as cur:
                cur.execute(q.as_string(self._conn), (limit,))
                return cur.fetchall()
        else:
            q = sql.SQL("SELECT pmid, pmcid, pdf_location FROM {tbl} WHERE vector_indexed=false").format(tbl=tbl)
            with self._conn.cursor() as cur:
                cur.execute(q.as_string(self._conn))
                return cur.fetchall()

    # ========== XML Processing Log ==========

    @pg_retry(max_attempts=3)
    def ensure_xml_log_row(self, xml_filename: str) -> None:
        """Create XML processing log row if it doesn't exist."""
        log_tbl = self._tbl("xml_processing_log")
        q = sql.SQL(
            """
            INSERT INTO {log_tbl} (xml_filename, last_processed_pmcid, total_processing_time, total_pmcid_processed, processed_all_pmcid)
            VALUES (%s, NULL, INTERVAL '0 seconds', 0, false)
            ON CONFLICT (xml_filename) DO NOTHING
            """
        ).format(log_tbl=log_tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (xml_filename,))
        self._conn.commit()

    @pg_retry(max_attempts=3)
    def get_xml_log_progress(self, xml_filename: str) -> Tuple[Optional[str], bool]:
        """
        Get XML processing progress.
        Returns (last_processed_pmcid, processed_all_pmcid).
        """
        log_tbl = self._tbl("xml_processing_log")
        q = sql.SQL("SELECT last_processed_pmcid, COALESCE(processed_all_pmcid,false) FROM {t} WHERE xml_filename=%s").format(t=log_tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (xml_filename,))
            row = cur.fetchone()
            if not row:
                return None, False
            return row[0], bool(row[1])

    @pg_retry(max_attempts=3)
    def update_xml_log_progress(self, xml_filename: str, last_processed_pmcid: str, delta_seconds: float, newly_processed_count: int) -> None:
        """
        Update XML processing progress metrics.
        Adds elapsed seconds to total_processing_time and increments total_pmcid_processed.
        """
        log_tbl = self._tbl("xml_processing_log")
        q = sql.SQL(
            """
            UPDATE {t}
               SET last_processed_pmcid = %s,
                   total_processing_time = COALESCE(total_processing_time, INTERVAL '0 seconds') + make_interval(secs => %s),
                   total_pmcid_processed = COALESCE(total_pmcid_processed, 0) + %s,
                   last_updated = NOW()
             WHERE xml_filename = %s
            """
        ).format(t=log_tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (last_processed_pmcid, int(delta_seconds), newly_processed_count, xml_filename))
        self._conn.commit()

    @pg_retry(max_attempts=3)
    def mark_xml_completed(self, xml_filename: str, last_processed_pmcid: Optional[str]) -> None:
        """Mark XML file as fully processed (processed_all_pmcid = true)."""
        log_tbl = self._tbl("xml_processing_log")
        q = sql.SQL(
            """
            UPDATE {t}
               SET processed_all_pmcid = true,
                   last_processed_pmcid = COALESCE(%s, last_processed_pmcid)
             WHERE xml_filename = %s
            """
        ).format(t=log_tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (last_processed_pmcid, xml_filename))
        self._conn.commit()

    # ========== Failures ==========

    @pg_retry(max_attempts=3)
    def record_xml_failure(self, xml_filename: str, pmcid: str, reason: Optional[str]) -> None:
        """Record a failed download for an article."""
        fail_tbl = self._tbl("xml_failed_downloads")
        q = sql.SQL(
            """
            INSERT INTO {t} (xml_filename, failed_pmcid, failure_reason)
            VALUES (%s,%s,%s)
            ON CONFLICT (xml_filename, failed_pmcid) DO UPDATE SET
                failure_timestamp = NOW(),
                failure_reason = EXCLUDED.failure_reason
            """
        ).format(t=fail_tbl)
        with self._conn.cursor() as cur:
            cur.execute(q.as_string(self._conn), (xml_filename, pmcid, reason))
        self._conn.commit()
