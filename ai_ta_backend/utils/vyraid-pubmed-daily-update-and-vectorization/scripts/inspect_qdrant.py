#!/usr/bin/env python3
"""Quick Qdrant inspection — writes to /tmp/qdrant_inspect.txt"""
from qdrant_client import QdrantClient
from dotenv import load_dotenv
import os, sys

load_dotenv('/home/dadams/pub-med-daily/.env.production')

OUT = '/tmp/qdrant_inspect.txt'
f = open(OUT, 'w')

def p(msg=''):
    f.write(msg + '\n')
    f.flush()

targets = []
remote_url = os.getenv('QDRANT_URL', '').rstrip('/')
if remote_url:
    targets.append((remote_url, int(os.getenv('QDRANT_PORT', '6333')), os.getenv('QDRANT_API_KEY', '') or None))
targets.append(('http://localhost', 6333, None))

qc = None
for url, port, api_key in targets:
    p(f'Trying {url}:{port} ...')
    kw = {'url': url, 'port': port, 'timeout': 10}
    if api_key:
        kw['api_key'] = api_key
    try:
        qc = QdrantClient(**kw)
        qc.get_collections()
        p(f'  Connected!')
        break
    except Exception as e:
        p(f'  Failed: {type(e).__name__}')
        qc = None

if not qc:
    p('No Qdrant reachable')
    f.close()
    print(open(OUT).read())
    sys.exit(1)

cols = qc.get_collections()
p(f'Collections: {len(cols.collections)}')
for c in cols.collections:
    p(f'  {c.name}: {c.points_count:,} points')

for c in cols.collections:
    try:
        info = qc.get_collection(c.name)
        p(f'\n=== {c.name} ===')
        p(f'  points={info.points_count:,}  vectors={info.vectors_count:,}  indexed={info.indexed_vectors_count:,}')
        p(f'  vector_config={info.config.params.vectors}')
        pts, _ = qc.scroll(collection_name=c.name, limit=3, with_payload=True, with_vectors=False)
        for pt in pts:
            p(f'  sample id={pt.id}')
            p(f'    keys={sorted(pt.payload.keys())}')
            for k in ['s3_path', 'pmcid', 'pmid', 'readable_filename', 'pagenumber', 'chunk_index', 'total_chunks']:
                if k in pt.payload:
                    p(f'    {k}={pt.payload[k]}')
    except Exception as e:
        p(f'  Error on {c.name}: {e}')

f.close()
print(open(OUT).read())
