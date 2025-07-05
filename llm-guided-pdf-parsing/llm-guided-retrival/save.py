import json
import sqlite3
import struct

import sqlite_vec

db_path = '/home/guest/ai-ta-backend/UIUC_Chat/pdf-parsing/articles-test.db'


def serialize_f32(vector):
  return struct.pack('%sf' % len(vector), *vector)


def deserialize_f32(blob):
  return list(struct.unpack('%sf' % (len(blob) // 4), blob))


conn = sqlite3.connect(db_path)

conn.enable_load_extension(True)
sqlite_vec.load(conn)
conn.enable_load_extension(False)

cursor = conn.cursor()
cursor.execute('SELECT `Embedding_nomic_1.5` FROM contexts')
sample_embedding = cursor.fetchone()[0]

if isinstance(sample_embedding, str):
  sample_embedding_vector = json.loads(sample_embedding)
else:
  sample_embedding_vector = deserialize_f32(sample_embedding)

vector_dim = len(sample_embedding_vector)

conn.execute(f'CREATE VIRTUAL TABLE IF NOT EXISTS vec_item USING vec0(embedding float[{vector_dim}])')

# Fetch and insert all embeddings
cursor.execute('SELECT `Embedding_nomic_1.5` FROM contexts')
rows = cursor.fetchall()

with conn:
  for row in rows:
    embedding = row[0]
    conn.execute('INSERT INTO vec_item(embedding) VALUES (?)', (embedding,))

query_vector = [0.3] * vector_dim
query_blob = serialize_f32(query_vector)

result = conn.execute(
    '''
    SELECT rowid, distance(embedding, ?) as dist
    FROM vec_item
    ORDER BY dist
    LIMIT 3
''', (query_blob,)).fetchall()

for row in result:
  print(f"{row[0]:.4f}")

conn.close()
