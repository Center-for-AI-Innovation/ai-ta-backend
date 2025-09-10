import logging
import os
from contextlib import contextmanager
from typing import List, TypedDict, TypeVar, Generic

from sqlalchemy import create_engine, NullPool, func, insert, delete, select, desc, literal, ARRAY
from sqlalchemy.orm import sessionmaker, Session, aliased
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import JSONB

from ..utils.datetime_utils import to_utc_datetime

try:
    import ai_ta_backend.rabbitmq.models as models
except ModuleNotFoundError:
    import models

# Define your base if you haven’t already
Base = declarative_base()

# Replace T's bound to use SQLAlchemy’s Base
T = TypeVar('T', bound=DeclarativeMeta)


def orm_to_dict(obj):
    """Convert a SQLAlchemy ORM instance to a plain dict of its columns."""
    if obj is None:
        return None
    if hasattr(obj, "__table__"):  # it's a SQLAlchemy model instance
        return {col.name: getattr(obj, col.name) for col in obj.__table__.columns}
    return obj


class DatabaseResponse(Generic[T]):
    def __init__(self, data: List[T], count: int):
        self.data = data
        self.count = count

    def to_dict(self):
        return {
            "data": self.data,
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


class SQLDatabase:
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

    def getAllMaterialsForCourse(self, course_name: str):
        query = (
            select(models.Document.course_name,
                   models.Document.s3_path,
                   models.Document.readable_filename,
                   models.Document.url,
                   models.Document.base_url
                   )
            .where(models.Document.course_name == course_name)
        )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def getMaterialsForCourseAndS3Path(self, course_name: str, s3_path: str):
        query = (
            select(models.Document)
            .where(models.Document.s3_path == s3_path)
            .where(models.Document.course_name == course_name)
        )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def getMatrialsForCourseAndUrl(self, course_name: str, url: str):
        query = (
            select(models.Document)
            .where(models.Document.url == url)
            .where(models.Document.course_name == course_name)
        )

        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def getMaterialsForCourseAndKeyAndValue(self, course_name: str, key: str, value: str):
        query = (
            select(models.Document.id, models.Document.s3_path, models.Document.contexts)
            .where(getattr(models.Document, key) == value)
            .where(models.Document.course_name == course_name)
        )

        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def deleteMaterialsForCourseAndKeyAndValue(self, course_name: str, key: str, value: str):
        delete_stmt = (
            delete(models.Document)
            .where(getattr(models.Document, key) == value)
            .where(models.Document.course_name == course_name)
        )

        with self.get_session() as session:
            result = session.execute(delete_stmt)

        return result.rowcount  # Number of rows deleted

    def deleteMaterialsForCourseAndS3Path(self, course_name: str, s3_path: str):
        delete_stmt = (
            delete(models.Document)
            .where(models.Document.s3_path == s3_path)
            .where(models.Document.course_name == course_name)
        )

        with self.get_session() as session:
            result = session.execute(delete_stmt)

        return result.rowcount  # Number of rows deleted

    def getProjectsMapForCourse(self, course_name: str):
        query = (
            select(models.Project.doc_map_id)
            .where(models.Project.course_name == course_name)
        )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def getDocumentsBetweenDates(self, course_name: str, from_date: str, to_date: str):
        query = (
            select(models.Document)
            .where(models.Document.course_name == course_name)
        )
        from_date = to_utc_datetime(from_date)
        to_date = to_utc_datetime(to_date, end_of_day=True)

        if from_date:
            query = query.where(models.Document.created_at >= from_date)
        if to_date:
            query = query.where(models.Document.created_at <= to_date)

        query = query.order_by(models.Document.id.asc())

        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def getConversationsBetweenDates(self, course_name: str, from_date: str, to_date: str):
        query = (
            select(models.LlmConvoMonitor)
            .where(models.LlmConvoMonitor.course_name == course_name)
        )
        from_date = to_utc_datetime(from_date)
        to_date = to_utc_datetime(to_date, end_of_day=True)
        if from_date:
            query = query.where(models.LlmConvoMonitor.created_at >= from_date)
        if to_date:
            query = query.where(models.LlmConvoMonitor.created_at <= to_date)

        query = query.order_by(models.LlmConvoMonitor.id.asc())

        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def getAllFromTableForDownloadType(self, course_name: str, download_type: str, first_id: int):
        if download_type == 'documents':
            query = (
                select(models.Document)
                .where(models.Document.course_name == course_name)
                .where(models.Document.id >= first_id)
                .order_by(models.Document.id.asc())
                .limit(100)
            )
        else:
            query = (
                select(models.LlmConvoMonitor)
                .where(models.LlmConvoMonitor.course_name == course_name)
                .where(models.LlmConvoMonitor.id >= first_id)
                .order_by(models.LlmConvoMonitor.id.asc())
                .limit(100)
            )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def getAllConversationsBetweenIds(self, course_name: str, first_id: int, last_id: int, limit: int = 50):
        query = select(models.LlmConvoMonitor).where(
            models.LlmConvoMonitor.course_name == course_name
        )

        if last_id == 0:
            query = query.where(models.LlmConvoMonitor.id > first_id)
        else:
            query = query.where(models.LlmConvoMonitor.id >= first_id).where(models.LlmConvoMonitor.id <= last_id)

        query = (
            query
            .order_by(models.LlmConvoMonitor.id.asc())
            .limit(limit)
        )

        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(r) for r in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def getDocsForIdsGte(self, course_name: str, first_id: int, fields: str = "*", limit: int = 100):
        if fields != "*":
            query = text(f"""
              SELECT {fields} FROM documents 
              WHERE documents.course_name = {course_name} AND documents.id >= {first_id}
              limit {limit} ORDER BY documents.id asc
            """)
        else:
            query = (select(models.Document)
                     .where(models.Document.course_name == course_name)
                     .where(models.Document.id >= first_id)
                     .order_by(models.Document.id.asc())
                     .limit(limit)
                     )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(r) for r in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def insertProject(self, project_info):
        with self.get_session() as session:
            try:
                insert_stmt = insert(models.Project).values(project_info)
                session.execute(insert_stmt)
                return True  # Insertion successful
            except SQLAlchemyError as e:
                logging.error(f"Insertion failed: {e}")
                return False  # Insertion failed

    def getLLMConvo(self):
        query = (
            select(models.LlmConvoMonitor.convo)
        )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(r) for r in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def getAllFromLLMConvoMonitor(self, course_name: str):
        query = (
            select(models.LlmConvoMonitor)
            .where(models.LlmConvoMonitor.course_name == course_name)
            .order_by(models.LlmConvoMonitor.id.asc())
        )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(r) for r in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response

    def getCountFromLLMConvoMonitor(self, course_name: str, last_id: int):
        if last_id == 0:
            query = (
                select(models.LlmConvoMonitor.id)
                .where(models.LlmConvoMonitor.course_name == course_name)
                .order_by(models.LlmConvoMonitor.id.asc())
            )
        else:
            query = (
                select(models.LlmConvoMonitor.id)
                .where(models.LlmConvoMonitor.course_name == course_name)
                .where(models.LlmConvoMonitor.id > last_id)
                .order_by(models.LlmConvoMonitor.id.asc())
            )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(r) for r in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()
        return response

    def getCountFromDocuments(self, course_name: str, last_id: int):
        if last_id == 0:
            query = (
                select(models.Document.id)
                .where(models.Document.course_name == course_name)
                .order_by(models.Document.id.asc())
            )
        else:
            query = (
                select(models.Document.id)
                .where(models.Document.course_name == course_name)
                .where(models.Document.id > last_id)
                .order_by(models.Document.id.asc())
            )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(r) for r in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response


    def getDocMapFromProjects(self, course_name: str):
        query = (
            select(models.Project.doc_map_id)
            .where(models.Project.course_name == course_name)
        )
        with self.get_session() as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()

        return response


    def getConvoMapFromProjects(self, course_name: str):
        query = (
            select(models.Project)
            .where(models.Project.course_name == course_name)
        )
        with self.get_session() as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()

        return response


    def updateProjects(self, course_name: str, data: dict):
        query = (
            select(models.Project)
            .where(models.Project.course_name == course_name)
            .update(data)
        )
        with self.get_session() as session:
            result = session.execute(query)

        return result


    def getLatestWorkflowId(self):
        query = (
            select(models.N8nWorkflows)
        )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response


    def lockWorkflow(self, id: int):
        with self.get_session() as session:
            try:
                insert_stmt = insert(models.N8nWorkflows).values({"latest_workflow_id": id, "is_locked": True})
                session.execute(insert_stmt)
                return True  # Insertion successful
            except SQLAlchemyError as e:
                logging.error(f"Insertion failed: {e}")
                return False  # Insertion failed


    def deleteLatestWorkflowId(self, id: int):
        query = (
            delete(models.N8nWorkflows)
            .where(models.N8nWorkflows.latest_workflow_id == id)
        )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response


    def unlockWorkflow(self, id: int):
        query = (
            select(models.N8nWorkflows)
            .where(models.N8nWorkflows.latest_workflow_id == id)
            .update({"is_locked": False})
        )
        with self.get_session() as session:
            result = session.execute(query)

        return result


    def check_and_lock_flow(self, id):
        with self.get_session() as session:
            return session.query(func.check_and_lock_flows_v2(id)).all()


    def getConversation(self, course_name: str, key: str, value: str):
        query = (
            select(models.LlmConvoMonitor)
            .where(getattr(models.LlmConvoMonitor, key) == value)
            .where(models.LlmConvoMonitor.course_name == course_name)
        )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response


    def getDisabledDocGroups(self, course_name: str):
        query = (
            select(models.DocGroup.name)
            .where(models.DocGroup.course_name == course_name)
            .where(models.DocGroup.enabled == False)
        )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response


    def getPublicDocGroups(self, course_name: str):
        query = (
            select(models.DocGroup.name, models.DocGroup.course_name, models.DocGroup.enabled,
                   models.DocGroup.private, models.DocGroup.doc_count)
            .where(models.DocGroup.course_name == course_name)
        )
        with Session(self.engine) as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()
            return response


    def getAllConversationsForUserAndProject(self, user_email: str, project_name: str, curr_count: int = 0):
        C, M = models.Conversations, models.Messages
        conv_page = (
            select(models.Conversations)
            .where(C.user_email == user_email, C.project_name == project_name)
            .order_by(models.Conversations.updated_at.desc())
            .limit(500)
            .offset(curr_count)
            .subquery()
        )
        CP = aliased(C, conv_page)  # alias to refer to columns

        msg_obj = func.jsonb_build_object(
            'id', M.id,
            'conversation_id', M.conversation_id,
            'role', M.role,
            'created_at', M.created_at,
            'updated_at', M.updated_at,
            'contexts', M.contexts,
            'tools', M.tools,
            'latest_system_message', M.latest_system_message,
            'final_prompt_engineered_message', M.final_prompt_engineered_message,
            'response_time_sec', M.response_time_sec,
            'content_text', M.content_text,
            'content_image_url', M.content_image_url,
            'image_description', M.image_description,
        )

        messages_agg = func.coalesce(
            func.jsonb_agg(msg_obj.op("ORDER BY")(M.created_at)).filter(M.id.isnot(None)),
            func.cast('[]', JSONB)
        ).label("messages")

        query = (
            select(
                CP.id.label("id"),
                CP.name.label("name"),
                CP.model, CP.prompt, CP.temperature,
                CP.user_email, CP.project_name,
                CP.created_at, CP.updated_at, CP.folder_id,
                messages_agg
            )
            .select_from(conv_page)
            .join(M, M.conversation_id == CP.id, isouter=True)
            .group_by(
                CP.id, CP.name, CP.model, CP.prompt, CP.temperature,
                CP.user_email, CP.project_name, CP.created_at, CP.updated_at, CP.folder_id
            )
            .order_by(desc(CP.updated_at))
        )

        with self.get_session() as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()

        return response

    def getPreAssignedAPIKeys(self, email: str):
        query = (
            select(models.PreAuthAPIKeys)
            .where(models.PreAuthAPIKeys.emails.contains([email]))
        )
        with self.get_session() as session:
            result = session.execute(query).scalars().all()
            data = [orm_to_dict(doc) for doc in result]
            response = DatabaseResponse(data=data, count=len(result)).to_dict()

        return response


    def getConversationsCreatedAtByCourse(
            self, course_name: str, from_date: str = "", to_date: str = ""
    ):
        try:
            query = (
                select(models.LlmConvoMonitor.created_at)
                .where(models.LlmConvoMonitor.course_name == course_name)
            )

            from_date = to_utc_datetime(from_date)
            to_date = to_utc_datetime(to_date, end_of_day=True)

            if from_date:
                query = query.where(models.LlmConvoMonitor.created_at >= from_date)

            if to_date:
                query = query.where(models.LlmConvoMonitor.created_at <= to_date)

            with self.get_session() as session:
                results = session.execute(query).scalars().all()
                response = DatabaseResponse(data=results, count=len(results)).to_dict()

            if response["count"] <= 0:
                logging.error(f"No conversations found for course: {course_name} for duration {from_date} to {to_date}")
                return [], 0

            return response["data"], response["count"]

        except Exception as e:
            logging.error(f"Error in getConversationsCreatedAtByCourse for {course_name}: {e}")
            return [], 0


    def getProjectStats(self, project_name: str) -> ProjectStats:
        try:
            query = (
                select(models.ProjectStats.total_messages, models.ProjectStats.total_conversations,
                       models.ProjectStats.unique_users)
                .where(models.ProjectStats.project_name == project_name)
            )
            with self.get_session() as session:
                result = session.execute(query).mappings().all()
                response = DatabaseResponse(data=result, count=len(result)).to_dict()

            stats = {
                "total_messages": 0,
                "total_conversations": 0,
                "unique_users": 0,
                "avg_conversations_per_user": 0.0,
                "avg_messages_per_user": 0.0,
                "avg_messages_per_conversation": 0.0
            }

            if response and "data" in response and response["data"]:
                base_stats = response["data"][0]
                stats.update(base_stats)

                if stats["unique_users"] > 0:
                    stats["avg_conversations_per_user"] = float(
                        round(stats["total_conversations"] / stats["unique_users"], 2))
                    stats["avg_messages_per_user"] = float(round(stats["total_messages"] / stats["unique_users"], 2))

                if stats["total_conversations"] > 0:
                    stats["avg_messages_per_conversation"] = float(
                        round(stats["total_messages"] / stats["total_conversations"], 2))

            stats_typed = {
                "total_messages": int(stats["total_messages"]),
                "total_conversations": int(stats["total_conversations"]),
                "unique_users": int(stats["unique_users"]),
                "avg_conversations_per_user": float(stats["avg_conversations_per_user"]),
                "avg_messages_per_user": float(stats["avg_messages_per_user"]),
                "avg_messages_per_conversation": float(stats["avg_messages_per_conversation"])
            }
            return ProjectStats(**stats_typed)

        except Exception as e:
            logging.error(f"Error fetching project stats for {project_name}: {str(e)}")
            return ProjectStats(total_messages=0,
                                total_conversations=0,
                                unique_users=0,
                                avg_conversations_per_user=0.0,
                                avg_messages_per_user=0.0,
                                avg_messages_per_conversation=0.0)


    def getWeeklyTrends(self, project_name: str) -> List[WeeklyMetric]:
        with self.get_session() as session:
            response = session.query(func.calculate_weekly_trends(project_name)).all()
            if response and hasattr(response, 'data'):
                return [
                    WeeklyMetric(current_week_value=item['current_week_value'],
                                 metric_name=item['metric_name'],
                                 percentage_change=item['percentage_change'],
                                 previous_week_value=item['previous_week_value']) for item in response.data
                ]

        return []


    def getModelUsageCounts(self, project_name: str) -> List[ModelUsage]:
        with self.get_session() as session:
            response = session.query(func.count_models_by_project(project_name)).all()
            if response and hasattr(response, 'data'):
                total_count = sum(item['count'] for item in response.data if item.get('model'))

                model_counts = []
                for item in response.data:
                    if item.get('model'):
                        percentage = round((item['count'] / total_count * 100), 2) if total_count > 0 else 0
                        model_counts.append(
                            ModelUsage(model_name=item['model'], count=item['count'], percentage=percentage))

                return model_counts

        return []


    def getAllProjects(self):
        query = (
            select(models.Project.course_name,
                   models.Project.doc_map_id,
                   models.Project.convo_map_id,
                   models.Project.last_uploaded_doc_id,
                   models.Project.last_uploaded_convo_id
                   )
        )
        with self.get_session() as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()

        return response


    def getConvoMapDetails(self):
        with self.get_session() as session:
            return session.query(func.get_convo_maps()).all()


    def getDocMapDetails(self):
        with self.get_session() as session:
            return session.query(func.get_doc_map_details()).all()


    def getProjectsWithConvoMaps(self):
        query = (
            select(models.Project.course_name,
                   models.Project.convo_map_id,
                   models.Project.last_uploaded_doc_id,
                   models.Project.last_uploaded_convo_id)
            .where(models.Project.convo_map_id is not None)
        )
        with self.get_session() as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()

        return response


    def getProjectsWithDocMaps(self):
        query = (
            select(models.Project.course_name,
                   models.Project.doc_map_id,
                   models.Project.last_uploaded_doc_id,
                   models.Project.document_map_index
                   )
            .where(models.Project.doc_map_id is not None)
        )
        with self.get_session() as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()

        return response


    def getProjectMapName(self, course_name, field_name):
        query = text(f"""
                        SELECT {field_name} FROM projects 
                        WHERE projects.course_name = {course_name}
                      """)
        with self.get_session() as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()

        return response


    def getMessagesFromConvoID(self, convo_id):
        query = (
            select(models.Messages)
            .where(models.Messages.conversation_id == convo_id)
            .limit(500)
        )
        with self.get_session() as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()

        return response


    def updateMessageFromLlmMonitor(self, message_id, llm_monitor_tags):
        query = (
            select(models.Message)
            .where(models.Message.id == message_id)
            .update({"llm_monitor_tags": llm_monitor_tags})
        )
        with self.get_session() as session:
            result = session.execute(query).mappings().all()
            response = DatabaseResponse(data=result, count=len(result)).to_dict()

        return response
