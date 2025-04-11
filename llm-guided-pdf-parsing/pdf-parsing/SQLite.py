import json
import os
import sqlite3
from typing import Dict, List, Optional
from uuid import uuid4

import fastnanoid  # type: ignore
from qdrant_client import QdrantClient, models


def initialize_database(db_path):
  """
    Initializes the database and creates the necessary tables if they don't already exist.

    :param db_path: Path to the SQLite database file.
    """
  if not os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA cache_size = 10000;")
    cur.execute("PRAGMA synchronous = NORMAL;")

    cur.execute('''
            CREATE TABLE IF NOT EXISTS articles (
                ID TEXT PRIMARY KEY,
                Num_tokens INTEGER,
                Title TEXT,
                Date_published TEXT,
                Journal TEXT,
                Authors TEXT,
                Outline TEXT,
                Minio_Path TEXT
            )
        ''')

    cur.execute('''
            CREATE TABLE IF NOT EXISTS sections (
                ID TEXT PRIMARY KEY,
                Article_ID TEXT,
                num_tokens INTEGER,
                Section_Title TEXT,
                Section_Num TEXT,
                FOREIGN KEY (Article_ID) REFERENCES articles(ID)
            )
        ''')

    cur.execute('''
            CREATE TABLE IF NOT EXISTS contexts (
                ID TEXT PRIMARY KEY,
                Section_ID TEXT,
                text TEXT,
                num_tokens INTEGER,
                context_idx TEXT,
                `Embedding_nomic_1.5` TEXT,
                Stop_Reason TEXT,
                FOREIGN KEY (Section_ID) REFERENCES sections(ID)
            )
        ''')

    conn.commit()
    conn.close()

    print("Database and tables created successfully.")
  else:
    print("Database already exists. No changes made.")


def insert_data(metadata: Dict,
                total_tokens: int,
                grouped_data: List[Dict],
                db_path: str,
                references: Optional[Dict] = None,
                ref_num_tokens: Optional[Dict] = None,
                minio_path: Optional[str] = None,
                client=None):
  """
    Inserts article metadata and sections into the database.

    :param metadata: Dictionary containing article metadata (title, date_published, journal, authors).
    :param total_tokens: Total number of tokens in the article.
    :param grouped_data: List of dictionaries containing section data (tokens, sec_num, sec_title).
    :param db_path: Path to the SQLite database file.
    """
  if references is None:
    references = {}
  if ref_num_tokens is None:
    ref_num_tokens = {}

  conn = sqlite3.connect(db_path)
  cur = conn.cursor()

  outline = '\n'.join([f"{section['sec_num']}: {section['sec_title']}" for section in grouped_data])

  article_id = fastnanoid.generate()
  authors = metadata.get('authors', [])
  if not isinstance(authors, list):
    authors = [authors]
  filtered_authors = [author for author in authors if author is not None]
  cur.execute(
      '''
      INSERT INTO articles (ID, Title, Date_published, Journal, Authors, Num_tokens, Outline, Minio_Path)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ''', (article_id, metadata['title'], metadata['date_published'], metadata['journal'], ', '.join(filtered_authors),
            total_tokens, outline, minio_path))

  context_idx = 0

  for section in grouped_data:
    section_id = fastnanoid.generate()
    cur.execute(
        '''
            INSERT INTO sections (ID, Article_ID, num_tokens, Section_Num, Section_Title)
            VALUES (?, ?, ?, ?, ?)
        ''', (section_id, article_id, section["tokens"], section["sec_num"], section["sec_title"]))

    if section["tokens"] > 7000:
      for text, token in zip(section["chunk_text"], section["chunk_tokens"]):
        context_id = fastnanoid.generate()
        embedding = json.dumps(section["embedding"][0])
        cur.execute(
            '''
                    INSERT INTO contexts (ID, Section_ID, text, num_tokens, `Embedding_nomic_1.5`, stop_reason, context_idx)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (context_id, section_id, text, token, embedding, "Token limit", context_idx))

        qdrant_insert(embedding, article_id, section_id, minio_path, context_id, client)

        context_idx += 1

    else:
      context_id = fastnanoid.generate()
      embedding = json.dumps(section["embedding"][0])
      cur.execute(
          '''
                INSERT INTO contexts (ID, Section_ID, text, num_tokens, `Embedding_nomic_1.5`, stop_reason, context_idx)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (context_id, section_id, section["text"], section["tokens"], embedding, "Section", context_idx))
      qdrant_insert(embedding, article_id, section_id, minio_path, context_id, client)
      context_idx += 1

  conn.commit()
  conn.close()


def qdrant_insert(embedding, article_id, section_id, minio_path, context_id, client):
  points = []
  qd_embedding = json.loads(embedding if isinstance(embedding, str) else embedding)
  points.append(
      models.PointStruct(
          id=str(uuid4()),
          payload={
              "article_id": article_id,
              "section_id": section_id,
              "minio_path": minio_path,
              "context_id": context_id,
          },
          vector=qd_embedding,
      ))

  client.upsert(
      collection_name="embedding",
      points=points,
  )
