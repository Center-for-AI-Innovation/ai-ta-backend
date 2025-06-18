import os
import logging
from urllib.parse import quote_plus
from dotenv import load_dotenv
from typing import List, TypeVar, Generic
load_dotenv()

from sqlalchemy import create_engine, NullPool
from sqlalchemy.orm import sessionmaker
from sqlalchemy import insert
from sqlalchemy import delete
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from sqlalchemy import select, desc
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

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


class SQLAlchemyIngestDB:
    def __init__(self) -> None:
        # Define supported database configurations and their required env vars
        DB_CONFIGS = {
            'supabase': ['SUPABASE_USER', 'SUPABASE_PASSWORD', 'SUPABASE_URL'],
            'sqlite': ['SQLITE_DB_NAME'],
            'postgres': ['POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_HOST']
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
        if db_type == 'supabase':
            encoded_password = quote_plus(os.getenv('SUPABASE_PASSWORD'))
            db_uri = f"postgresql://{os.getenv('SUPABASE_USER')}:{encoded_password}@{os.getenv('SUPABASE_PG_URL')}"
        elif db_type == 'sqlite':
            db_uri = f"sqlite:///{os.getenv('SQLITE_DB_NAME')}"
        else:
            # postgres
            db_uri = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"

        # Create engine and session
        print("About to connect to DB from IngestSQL.py, with URI:", db_uri)
        engine = create_engine(db_uri, poolclass=NullPool)
        Session = sessionmaker(bind=engine)
        # TODO: Move to self.connect() & handle if the session is broken before executing statements
        self.session = Session()
        print("Successfully connected to DB from IngestSQL.py")

    def insert_document_in_progress(self, doc_progress_payload: dict):
        insert_stmt = insert(models.DocumentsInProgress).values(doc_progress_payload)
        self.session.execute(insert_stmt)
        self.session.commit()

    def insert_failed_document(self, failed_doc_payload: dict):
        try:
            insert_stmt = insert(models.DocumentsFailed).values(failed_doc_payload)
            self.session.execute(insert_stmt)
            self.session.commit()
            return True
        except SQLAlchemyError as e:
            self.session.rollback()
            print(f"Insertion failed: {e}")
            return False

    def delete_document_in_progress(self, beam_task_id: str):
        try:
            delete_stmt = delete(models.DocumentsInProgress).where(
                models.DocumentsInProgress.beam_task_id == beam_task_id)
            self.session.execute(delete_stmt)
            self.session.commit()
            return True
        except SQLAlchemyError as e:
            self.session.rollback()
            print(f"Deletion failed: {e}")
            return False

    def insert_document(self, doc_payload: dict) -> bool:
        try:
            insert_stmt = insert(models.Document).values(doc_payload)
            self.session.execute(insert_stmt)
            self.session.commit()
            return True  # Insertion successful
        except SQLAlchemyError as e:
            self.session.rollback()  # Rollback in case of error
            print(f"Insertion failed: {e}")
            return False  # Insertion failed

    def add_document_to_group_url(self, contexts, groups):
        params = {
            "p_course_name": contexts[0].metadata.get('course_name'),
            "p_s3_path": contexts[0].metadata.get('s3_path'),
            "p_url": contexts[0].metadata.get('url'),
            "p_readable_filename": contexts[0].metadata.get('readable_filename'),
            "p_doc_groups": groups,
        }

        try:
            result = self.session.execute(text(
                "SELECT * FROM add_document_to_group_url(:p_course_name, :p_s3_path, :p_url, :p_readable_filename, :p_doc_groups)"),
                                          params)
            self.session.commit()
            count = result.rowcount if result.returns_rows else 0  # Number of affected rows or results
            return count
        except Exception as e:
            print(f"Stored procedure execution failed: {e}")
            self.session.rollback()
            return None, 0

    def add_document_to_group(self, contexts, groups):
        params = {
            "p_course_name": contexts[0].metadata.get('course_name'),
            "p_s3_path": contexts[0].metadata.get('s3_path'),
            "p_url": contexts[0].metadata.get('url'),
            "p_readable_filename": contexts[0].metadata.get('readable_filename'),
            "p_doc_groups": groups,
        }

        try:
            result = self.session.execute(text(
                "SELECT * FROM add_document_to_group(:p_course_name, :p_s3_path, :p_url, :p_readable_filename, :p_doc_groups)"),
                                          params)
            self.session.commit()

            count = result.rowcount if result.returns_rows else 0  # Number of affected rows or results
            return count
        except Exception as e:
            print(f"Stored procedure execution failed: {e}")
            self.session.rollback()
            return None, 0

    def get_like_docs_by_s3_path(self, course_name, original_filename):
        logging.info(f"In get_like_docs_by_s3_path")
        query = (
            select(models.Document.id, models.Document.contexts, models.Document.s3_path)
            .where(models.Document.course_name == course_name)
            .where(models.Document.s3_path.like(f"%{original_filename}%"))
            .order_by(desc(models.Document.id))
        )
        result = self.session.execute(query).mappings().all()
        logging.info(f"In get_like_docs_by_s3_path, result: {result}")
        response = DatabaseResponse(data=result, count=len(result)).to_dict()
        return response

    def get_like_docs_by_url(self, course_name, url):
        query = (
            select(models.Document.id, models.Document.contexts, models.Document.url)
            .where(models.Document.course_name == course_name)
            .where(models.Document.url.like(f"%{url}%"))
            .order_by(desc(models.Document.id))
        )
        result = self.session.execute(query).mappings().all()
        response = DatabaseResponse(data=result, count=len(result)).to_dict()
        return response

    def delete_document_by_s3_path(self, course_name: str, s3_path: str):
        delete_stmt = (
            delete(models.Document)
            .where(models.Document.s3_path == s3_path)
            .where(models.Document.course_name == course_name)
        )

        result = self.session.execute(delete_stmt)
        self.session.commit()
        return result.rowcount  # Number of rows deleted

    def delete_document_by_url(self, course_name: str, url: str):
        delete_stmt = (
            delete(models.Document)
            .where(models.Document.url == url)
            .where(models.Document.course_name == course_name)
        )

        result = self.session.execute(delete_stmt)
        self.session.commit()
        return result.rowcount  # Number of rows deleted
