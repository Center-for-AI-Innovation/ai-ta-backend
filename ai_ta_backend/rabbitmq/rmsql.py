import os
import logging
from contextlib import contextmanager

from dotenv import load_dotenv
from typing import List, TypeVar, Generic, TypedDict
load_dotenv()

from sqlalchemy import create_engine, NullPool
from sqlalchemy.orm import sessionmaker
from sqlalchemy import insert
from sqlalchemy import delete
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy import select, desc


try:
    import ai_ta_backend.rabbitmq.models as models
except ModuleNotFoundError:
    import models


# Define your base if you haven’t already
Base = declarative_base()

# Replace T's bound to use SQLAlchemy’s Base
T = TypeVar('T', bound=DeclarativeMeta)


class DatabaseResponse(Generic[T]):
    def __init__(self, data: List[T], count: int):
        self.data = data
        self.count = count

    def to_dict(self):
        return {
            "data": self.data,  # Convert each row to dict
            "count": self.count
        }

class ProjectStats(TypedDict):
  total_messages: int
  total_conversations: int
  unique_users: int
  avg_conversations_per_user: float
  avg_messages_per_user: float
  avg_messages_per_conversation: float

class WeeklyMetric(TypedDict):
  current_week_value: int
  metric_name: str
  percentage_change: float
  previous_week_value: int

class ModelUsage(TypedDict):
  model_name: str
  count: int
  percentage: float


class SQLAlchemyIngestDB:
    def __init__(self) -> None:
        # Define supported database configurations and their required env vars
        DB_CONFIGS = {
            'sqlite': ['SQLITE_DB_NAME'],
            'postgres': ['POSTGRES_USERNAME', 'POSTGRES_PASSWORD', 'POSTGRES_ENDPOINT']
        }

        # Detect which database configuration is available
        db_type = None
        for db, required_vars in DB_CONFIGS.items():
            if all(os.getenv(var) for var in required_vars):
                db_type = db
                break

        if not db_type:
            raise ValueError("No valid database configuration found in environment variables")

        # Build the appropriate connection string
        if db_type == 'sqlite':
            db_uri = f"sqlite:///{os.getenv('SQLITE_DB_NAME')}"
        else:
            # postgres
            db_uri = f"postgresql://{os.getenv('POSTGRES_USERNAME')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_ENDPOINT')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DATABASE')}"

        # Create engine and session
        logging.info("About to connect to DB from IngestSQL.py.")
        self.engine = create_engine(db_uri, poolclass=NullPool)
        self.Session = sessionmaker(bind=self.engine)
        logging.info("Successfully connected to DB from IngestSQL.py")


    @contextmanager
    def get_session(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # INGEST FLOW

    def insert_document_in_progress(self, doc_progress: models.DocumentsInProgress):
        with self.get_session() as session:
            session.add(doc_progress)

            # Unlike session.commit(), session.flush() sends SQL to the database within the current transaction
            # but doesn’t make changes permanent or visible to others.
            session.flush()

            session.refresh(doc_progress)
            return doc_progress.to_dict()

    def insert_failed_document(self, failed_doc_payload: dict):
        with self.get_session() as session:
            try:
                insert_stmt = insert(models.DocumentsFailed).values(failed_doc_payload)
                session.execute(insert_stmt)
                return True
            except SQLAlchemyError as e:
                logging.error(f"Insertion failed: {e}")
                return False

    def delete_document_in_progress(self, beam_task_id: str):
        with self.get_session() as session:
            try:
                logging.info("Deleting task id "+beam_task_id)
                delete_stmt = (
                    delete(models.DocumentsInProgress)
                    .where(models.DocumentsInProgress.beam_task_id == beam_task_id))
                session.execute(delete_stmt)
                return True
            except SQLAlchemyError as e:
                logging.error(f"Deletion failed: {e}")
                return False

    def insert_document(self, doc_payload: dict) -> bool:
        with self.get_session() as session:
            try:
                insert_stmt = insert(models.Document).values(doc_payload)
                session.execute(insert_stmt)
                return True  # Insertion successful
            except SQLAlchemyError as e:
                logging.error(f"Insertion failed: {e}")
                return False  # Insertion failed

    def add_document_to_group_url(self, contexts, groups):
        params = {
            "p_course_name": contexts[0].metadata.get('course_name'),
            "p_s3_path": contexts[0].metadata.get('s3_path'),
            "p_url": contexts[0].metadata.get('url'),
            "p_readable_filename": contexts[0].metadata.get('readable_filename'),
            "p_doc_groups": groups,
        }

        with self.get_session() as session:
            try:
                result = session.execute(text(
                    "SELECT * FROM add_document_to_group_url(:p_course_name, :p_s3_path, :p_url, :p_readable_filename, :p_doc_groups)"),
                                              params)
                count = result.rowcount if result.returns_rows else 0  # Number of affected rows or results
                return count
            except Exception as e:
                logging.error(f"Stored procedure execution failed: {e}")
                return None, 0

    def add_document_to_group(self, contexts, groups):
        params = {
            "p_course_name": contexts[0].metadata.get('course_name'),
            "p_s3_path": contexts[0].metadata.get('s3_path'),
            "p_url": contexts[0].metadata.get('url'),
            "p_readable_filename": contexts[0].metadata.get('readable_filename'),
            "p_doc_groups": groups,
        }
        with self.get_session() as session:
            try:
                result = session.execute(text(
                    "SELECT * FROM add_document_to_group(:p_course_name, :p_s3_path, :p_url, :p_readable_filename, :p_doc_groups)"),
                                              params)

                count = result.rowcount if result.returns_rows else 0  # Number of affected rows or results
                return count
            except Exception as e:
                logging.error(f"Stored procedure execution failed: {e}")
                return None, 0

    def get_like_docs_by_s3_path(self, course_name, original_filename):
        query = (
            select(models.Document.id, models.Document.contexts, models.Document.s3_path)
            .where(models.Document.course_name == course_name)
            .where(models.Document.s3_path.like(f"%{original_filename}%"))
            .order_by(desc(models.Document.id))
        )

        with self.get_session() as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()

        return response

    def get_like_docs_by_url(self, course_name, url):
        query = (
            select(models.Document.id, models.Document.contexts, models.Document.url)
            .where(models.Document.course_name == course_name)
            .where(models.Document.url.like(f"%{url}%"))
            .order_by(desc(models.Document.id))
        )
        with self.get_session() as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()
        return response

    def delete_document_by_s3_path(self, course_name: str, s3_path: str):
        delete_stmt = (
            delete(models.Document)
            .where(models.Document.s3_path == s3_path)
            .where(models.Document.course_name == course_name)
        )

        with self.get_session() as session:
            result = session.execute(delete_stmt)

        return result.rowcount  # Number of rows deleted

    def delete_document_by_url(self, course_name: str, url: str):
        delete_stmt = (
            delete(models.Document)
            .where(models.Document.url == url)
            .where(models.Document.course_name == course_name)
        )
        with self.get_session() as session:
            result = session.execute(delete_stmt)

        return result.rowcount  # Number of rows deleted
