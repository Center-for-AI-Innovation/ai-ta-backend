"""
Create models for each table in the database.
"""
from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import JSON
from sqlalchemy import Text
from sqlalchemy import VARCHAR
from sqlalchemy import Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from uuid import uuid4

try:
    from ai_ta_backend.rabbitmq.extensions import db
except ModuleNotFoundError:
    from extensions import db


class Base(db.Model):
    __abstract__ = True


class Document(Base):
    __tablename__ = 'documents'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=func.now())
    s3_path = Column(Text)
    readable_filename = Column(Text)
    course_name = Column(Text)
    url = Column(Text)
    contexts = Column(JSON, default=lambda: [{"text": "", "timestamp": "", "embedding": "", "pagenumber": ""}])
    base_url = Column(Text)

    __table_args__ = (
        Index('documents_course_name_idx', 'course_name', postgresql_using='hash'),
        Index('documents_created_at_idx', 'created_at', postgresql_using='btree'),
        Index('idx_doc_s3_path', 's3_path', postgresql_using='btree'),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "created_at": self.created_at,
            "s3_path": self.s3_path,
            "readable_filename": self.readable_filename,
            "course_name": self.course_name,
            "url": self.url,
            "contexts": self.contexts,
            "base_url": self.base_url
        }


class DocumentDocGroup(Base):
    __tablename__ = 'documents_doc_groups'
    document_id = Column(BigInteger, primary_key=True)
    doc_group_id = Column(BigInteger, ForeignKey('doc_groups.id', ondelete='CASCADE'), primary_key=True)
    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        Index('documents_doc_groups_doc_group_id_idx', 'doc_group_id', postgresql_using='btree'),
        Index('documents_doc_groups_document_id_idx', 'document_id', postgresql_using='btree'),
    )

    def to_dict(self):
        return {
            "document_id": self.document_id,
            "doc_group_id": self.doc_group_id,
            "created_at": self.created_at
        }


class DocGroup(Base):
    __tablename__ = 'doc_groups'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False)
    course_name = Column(Text, nullable=False)
    created_at = Column(DateTime, default=func.now())
    enabled = Column(Boolean, default=True)
    private = Column(Boolean, default=True)
    doc_count = Column(BigInteger)

    __table_args__ = (Index('doc_groups_enabled_course_name_idx', 'enabled', 'course_name', postgresql_using='btree'),)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "course_name": self.course_name,
            "created_at": self.created_at,
            "enabled": self.enabled,
            "private": self.private,
            "doc_count": self.doc_count
        }

def get_uuid():
    return str(uuid4())

class DocumentsInProgress(Base):
    __tablename__ = 'documents_in_progress'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=func.now())
    s3_path = Column(Text)
    readable_filename = Column(Text)
    course_name = Column(Text)
    url = Column(Text)
    contexts = Column(JSON, default=lambda: [{"text": "", "timestamp": "", "embedding": "", "pagenumber": ""}])
    base_url = Column(Text)
    doc_groups = Column(Text)
    error = Column(Text)
    beam_task_id = Column(Text, default=get_uuid)

    # __table_args__ = (Index('documents_in_progress_pkey', 'id', postgresql_using='btree'),)

    def to_dict(self):
        return {
            "id": self.id,
            "created_at": self.created_at,
            "s3_path": self.s3_path,
            "readable_filename": self.readable_filename,
            "course_name": self.course_name,
            "url": self.url,
            "contexts": self.contexts,
            "base_url": self.base_url,
            "doc_groups": self.doc_groups,
            "error": self.error,
            "beam_task_id": self.beam_task_id
        }


class DocumentsFailed(Base):
    __tablename__ = 'documents_failed'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=func.now())
    s3_path = Column(Text)
    readable_filename = Column(Text)
    course_name = Column(Text)
    url = Column(Text)
    contexts = Column(JSON, default=lambda: [{"text": "", "timestamp": "", "embedding": "", "pagenumber": ""}])
    base_url = Column(Text)
    doc_groups = Column(Text)
    error = Column(Text)

    # __table_args__ = (Index('documents_failed_pkey', 'id', postgresql_using='btree'),)

    def to_dict(self):
        return {
            "id": self.id,
            "created_at": self.created_at,
            "s3_path": self.s3_path,
            "readable_filename": self.readable_filename,
            "course_name": self.course_name,
            "url": self.url,
            "contexts": self.contexts,
            "base_url": self.base_url,
            "doc_groups": self.doc_groups,
            "error": self.error,
        }


class Project(Base):
    __tablename__ = 'projects'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=func.now())
    course_name = Column(Text)
    doc_map_id = Column(Text)
    convo_map_id = Column(Text)
    n8n_api_key = Column(Text)
    last_uploaded_doc_id = Column(BigInteger)
    last_uploaded_convo_id = Column(BigInteger)
    subscribed = Column(BigInteger, ForeignKey('doc_groups.id', onupdate='CASCADE', ondelete='SET NULL'))
    description = Column(Text)
    metadata_schema = Column(JSON)

    __table_args__ = (
        Index('projects_course_name_key', 'course_name', postgresql_using='btree'),
        # Index('projects_pkey', 'id', postgresql_using='btree'),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "created_at": self.created_at,
            "course_name": self.course_name,
            "doc_map_id": self.doc_map_id,
            "convo_map_id": self.convo_map_id,
            "n8n_api_key": self.n8n_api_key,
            "last_uploaded_doc_id": self.last_uploaded_doc_id,
            "last_uploaded_convo_id": self.last_uploaded_convo_id,
            "subscribed": self.subscribed,
            "description": self.description,
            "metadata_schema": self.metadata_schema
        }


class ProjectStats(Base):
    __tablename__ = 'project_stats'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    project_id = Column(BigInteger, ForeignKey('project.id')) # project_id = Column(BigInteger, ForeignKey('project.id'))
    project_name = Column(Text)
    total_messages= Column(BigInteger)
    total_conversations= Column(BigInteger)
    unique_users= Column(BigInteger)
    created_at= Column(DateTime, default=func.now())
    updated_at= Column(DateTime, default=func.now())
    model_usage_counts= Column(JSON)

    def to_dict(self):
        return {
            "id": self.id,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "total_messages": self.total_messages,
            "total_conversations": self.total_conversations,
            "unique_users": self.unique_users,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "model_usage_counts": self.model_usage_counts
        }

class N8nWorkflows(Base):
    __tablename__ = 'n8n_workflows'
    latest_workflow_id = Column(BigInteger, primary_key=True, autoincrement=True)
    is_locked = Column(Boolean, nullable=False)

    def __init__(self, is_locked: bool):
        self.is_locked = is_locked

    def to_dict(self):
        return {
            "latest_workflow_id": self.latest_workflow_id,
            "is_locked": self.is_locked
        }


class LlmConvoMonitor(Base):
    __tablename__ = 'llm-convo-monitor'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=func.now())
    convo = Column(JSON)
    convo_id = Column(Text, unique=True)
    course_name = Column(Text)
    user_email = Column(Text)

    __table_args__ = (
        Index('llm_convo_monitor_course_name_idx', 'course_name', postgresql_using='hash'),
        # Index('llm-convo-monitor_convo_id_key', 'convo_id', postgresql_using='btree'),
        # Index('llm-convo-monitor_pkey', 'id', postgresql_using='btree'),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "created_at": self.created_at,
            "convo": self.convo,
            "convo_id": self.convo_id,
            "course_name": self.course_name,
            "user_email": self.user_email
        }


class Conversations(Base):
    __tablename__ = 'conversations'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    name = Column(VARCHAR)
    model = Column(VARCHAR)
    prompt = Column(Text)
    temperature = Column(Float)
    user_email = Column(VARCHAR)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    project_name = Column(Text)
    folder_id = Column(UUID(as_uuid=True))

    __table_args__ = (
        # Index('conversations_pkey', 'id', postgresql_using='btree'),
        Index('idx_user_email_updated_at', 'user_email', 'updated_at', postgresql_using='btree'),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "model": self.model,
            "prompt": self.prompt,
            "temperature": self.temperature,
            "user_email": self.user_email,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "project_name": self.project_name,
            "messages": self.messages,
            "folder_id": self.folder_id
        }


class Messages(Base):
    __tablename__ = 'messages'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    conversation_id = Column(UUID(as_uuid=True), ForeignKey('conversations.id', ondelete='CASCADE'))
    role = Column(Text)
    created_at = Column(DateTime, default=func.now())
    contexts = Column(JSON)
    tools = Column(JSON)
    latest_system_message = Column(Text)
    final_prompt_engineered_message = Column(Text)
    response_time_sec = Column(BigInteger)
    content_text = Column(Text)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    content_image_url = Column(Text)
    image_description = Column(Text)

    # __table_args__ = (
    #     Index('messages_pkey', 'id', postgresql_using='btree'),
    # )

    def to_dict(self):
        return {
            "id": self.id,
            "conversation_id": self.conversation_id,
            "role": self.role,
            "created_at": self.created_at,
            "contexts": self.contexts,
            "tools": self.tools,
            "latest_system_message": self.latest_system_message,
            "final_prompt_engineered_message": self.final_prompt_engineered_message,
            "response_time_sec": self.response_time_sec,
            "content_text": self.content_text,
            "updated_at": self.updated_at,
            "content_image_url": self.content_image_url,
            "image_description": self.image_description
        }


class PreAuthAPIKeys(Base):
    __tablename__ = 'pre_authorized_api_keys'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=func.now())
    emails = Column(JSONB)
    providerBodyNoModels = Column(JSON)
    providerName = Column(Text)
    notes = Column(Text)

    # __table_args__ = (
    #     Index('pre-authorized-api-keys_pkey', 'id', postgresql_using='btree'),
    # )

    def to_dict(self):
        return {
            "id": self.id,
            "created_at": self.created_at,
            "emails": self.emails,
            "providerBodyNoModels": self.providerBodyNoModels,
            "providerName": self.providerName,
            "notes": self.notes
        }
