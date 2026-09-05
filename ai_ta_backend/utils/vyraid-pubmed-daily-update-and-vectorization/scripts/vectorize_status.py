#!/usr/bin/env python3
"""Quick check of vectorization status for planning."""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv('/home/dadams/pub-med-daily/.env.production')

import psycopg2

dsn = os.getenv('POSTGRES_DSN')
conn = psycopg2.connect(dsn, connect_timeout=15)
cur = conn.cursor()

# Use estimated count (fast) for total
cur.execute("SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname='vyraid' AND relname='articles'")
print(f'Total articles (approx): ~{cur.fetchone()[0]:,}')

# Use indexed counts - these should be fast with proper indexes
cur.execute('SELECT COUNT(*) FROM vyraid.articles WHERE vector_indexed = true')
print(f'Vectorized: {cur.fetchone()[0]:,}')

cur.execute('''SELECT COUNT(*) FROM vyraid.articles 
    WHERE pdf_location IS NOT NULL AND COALESCE(vector_indexed, false) = false''')
print(f'Backlog (PDF + not vectorized): {cur.fetchone()[0]:,}')

# Recent articles added (use created_at index)
cur.execute('''SELECT DATE(created_at) as day, COUNT(*), 
    COUNT(*) FILTER(WHERE pdf_location IS NOT NULL), 
    COUNT(*) FILTER(WHERE vector_indexed = true)
    FROM vyraid.articles WHERE created_at >= NOW() - INTERVAL '60 days'
    GROUP BY DATE(created_at) ORDER BY day DESC LIMIT 30''')
rows = cur.fetchall()
print(f'\nRecent activity (date | added | w/PDF | vectorized):')
for day, total, wpdf, vec in rows:
    print(f'  {day}: {total:>6,} added | {wpdf:>6,} w/PDF | {vec:>6,} vectorized')

# Check Qdrant collection point count
try:
    from qdrant_client import QdrantClient
    qc = QdrantClient(url="http://localhost", port=6333, timeout=5)
    cols = qc.get_collections()
    for c in cols.collections:
        if c.name in ('ncbi_pdfs', 'pubmed'):
            print(f'\nQdrant collection "{c.name}": {c.points_count:,} points')
except Exception as e:
    print(f'\nQdrant: {e}')

# Check MinIO oa_pdf count (sample)
try:
    from minio import Minio
    mc = Minio("localhost:9000", access_key=os.getenv('MINIO_ACCESS_KEY', 'admin'),
               secret_key=os.getenv('MINIO_SECRET_KEY'), secure=False)
    count = 0
    for obj in mc.list_objects('pubmed', prefix='oa_pdf/', recursive=True):
        if obj.object_name.endswith('.pdf'):
            count += 1
        if count >= 100000:
            print(f'\nMinIO oa_pdf/: >= {count:,} PDFs (stopped counting)')
            break
    else:
        print(f'\nMinIO oa_pdf/: {count:,} PDFs')
except Exception as e:
    print(f'\nMinIO: {e}')

cur.close()
conn.close()
