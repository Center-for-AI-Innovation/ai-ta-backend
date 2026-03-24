#!/usr/bin/env python3
"""
Quick production status summary.
Connects to PostgreSQL, MinIO, and Qdrant to show key metrics.
Focuses on fast, reliable queries.
"""
import argparse
import os
import sys
from datetime import datetime

import psycopg2
from dotenv import load_dotenv


def get_postgres_summary(dsn: str, schema: str = "vyraid") -> dict:
    """Query PostgreSQL for key metrics using table stats (fast)."""
    try:
        conn = psycopg2.connect(dsn, connect_timeout=10)
        cur = conn.cursor()
        
        results = {}
        
        # Fast: table stats (no COUNT(*))
        try:
            cur.execute("SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'articles';")
            row = cur.fetchone()
            results["total_articles"] = row[0] if row and row[0] else 0
        except Exception as e:
            results["total_articles"] = f"Error: {type(e).__name__}"
        
        # Fast: recent XML files (should have index)
        try:
            cur.execute(f"""
                SELECT xml_filename, total_pmcid_processed, processed_all_pmcid, processed_at
                FROM {schema}.xml_processing_log
                ORDER BY processed_at DESC
                LIMIT 5;
            """)
            files = cur.fetchall()
            results["recent_xml_files"] = [
                {
                    "filename": f[0],
                    "articles": f[1],
                    "complete": f[2],
                    "at": str(f[3])[:19] if f[3] else "unknown"
                }
                for f in files
            ]
        except Exception as e:
            results["recent_xml_files"] = []
        
        cur.close()
        conn.close()
        return results
        
    except Exception as e:
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Quick production summary")
    parser.add_argument("--env", default="/home/dadams/pub-med-daily/.env.production", help="Env file path")
    parser.add_argument("--schema", default="vyraid", help="Database schema")
    args = parser.parse_args()
    
    if os.path.exists(args.env):
        load_dotenv(args.env)
    else:
        load_dotenv()
    
    print("\n" + "="*70)
    print("PRODUCTION STATUS SUMMARY")
    print("="*70)
    print(f"Time: {datetime.now().astimezone().isoformat()}\n")
    
    # PostgreSQL
    dsn = os.getenv("POSTGRES_DSN")
    if dsn:
        print("--- DATABASE (Supabase) ---")
        stats = get_postgres_summary(dsn, args.schema)
        
        if "error" in stats:
            print(f"ERROR: {stats['error']}")
        else:
            total = stats.get('total_articles', 0)
            if isinstance(total, int):
                print(f"Articles: {total:,}")
            else:
                print(f"Articles: {total}")
            
            files = stats.get('recent_xml_files', [])
            if files:
                print(f"\nLatest XML files:")
                for f in files[:3]:
                    status = "✓" if f['complete'] else "⏳"
                    print(f"  {status} {f['filename']:28s} {f['articles']:>7,d} articles  {f['at']}")
    else:
        print("DATABASE: No POSTGRES_DSN configured")
    
    # MinIO
    endpoint = os.getenv("MINIO_ENDPOINT", "")
    bucket = os.getenv("MINIO_BUCKET", "")
    if endpoint and bucket:
        print(f"\n--- STORAGE (MinIO) ---")
        print(f"Bucket: {bucket}")
        print("(Use MinIO console for file/size counts)")
    
    # Qdrant
    q_url = os.getenv("QDRANT_URL", "")
    q_collection = os.getenv("QDRANT_COLLECTION", "")
    if q_url and q_collection:
        print(f"\n--- VECTORS (Qdrant) ---")
        print(f"Collection: {q_collection}")
        print("(Use Qdrant dashboard for vector count)")
    
    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    sys.exit(main())
