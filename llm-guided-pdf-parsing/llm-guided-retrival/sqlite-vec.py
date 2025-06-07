import json
import sqlite3
import struct
from typing import List

import sqlite_vec

db_path = '/home/guest/ai-ta-backend/UIUC_Chat/pdf-parsing/articles-test.db'


def serialize_f32(vector: List[float]) -> bytes:
  """Serializes a list of floats into a compact "raw bytes" format."""
  return struct.pack("%sf" % len(vector), *vector)


conn = sqlite3.connect(db_path, timeout=30)
conn.enable_load_extension(True)
sqlite_vec.load(conn)
conn.enable_load_extension(False)

query_vector = [0.1] * 768
query_blob = serialize_f32(query_vector)

cursor = conn.cursor()

cursor.execute('SELECT `Embedding_nomic_1.5` FROM contexts')
rows = cursor.fetchall()

if rows:
  vector_dim = len(json.loads(rows[0][0]))

conn.execute(f"CREATE VIRTUAL TABLE IF NOT EXISTS vec_items USING vec0(embedding float[{vector_dim}])")
count = 0
for row in rows:
  embedding = json.loads(row[0])
  if embedding is None:
    print("Embedding is empty")
    count += 1
    continue
  count += 1
  if type(embedding[0]) is list:
    embedding = embedding[0]
  embedding_blob = serialize_f32(embedding)
  conn.execute('INSERT INTO vec_items(embedding) VALUES (?)', (embedding_blob,))

result = conn.execute(
    '''
    SELECT rowid, distance
    FROM vec_items
    WHERE embedding MATCH ?
    ORDER BY distance
    LIMIT 3
''', (query_blob,)).fetchall()

conn.close()

print(result)
