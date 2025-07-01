import os
from typing import Dict, List, TypedDict, Union, TypeVar, Generic

from sqlalchemy import create_engine, NullPool, func, insert, delete, select, desc
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.sql import text

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

class SQLDatabase:
  def __init__(self) -> None:
      # Define supported database configurations and their required env vars
      DB_CONFIGS = {
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
      if db_type == 'sqlite':
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
      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getMaterialsForCourseAndS3Path(self, course_name: str, s3_path: str):
      query = (
          select(models.Document)
          .where(models.Document.s3_path == s3_path)
          .where(models.Document.course_name == course_name)
      )
      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getMatrialsForCourseAndUrl(self, course_name: str, url: str):
        query = (
            select(models.Document)
            .where(models.Document.url == url)
            .where(models.Document.course_name == course_name)
        )
        result = self.session.execute(query).scalars().all()
        response = DatabaseResponse(data=result, count=len(result)).to_dict()
        return response

  def getMaterialsForCourseAndKeyAndValue(self, course_name: str, key: str, value: str):
      query = (
          select(models.Document.id, models.Document.s3_path, models.Document.contexts)
          .where(getattr(models.Document, key) == value)
          .where(models.Document.course_name == course_name)
      )
      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def deleteMaterialsForCourseAndKeyAndValue(self, course_name: str, key: str, value: str):
      delete_stmt = (
          delete(models.Document)
          .where(getattr(models.Document, key) == value)
          .where(models.Document.course_name == course_name)
      )

      result = self.session.execute(delete_stmt)
      self.session.commit()
      return result.rowcount  # Number of rows deleted

  def deleteMaterialsForCourseAndS3Path(self, course_name: str, s3_path: str):
      delete_stmt = (
          delete(models.Document)
          .where(models.Document.s3_path == s3_path)
          .where(models.Document.course_name == course_name)
      )

      result = self.session.execute(delete_stmt)
      self.session.commit()
      return result.rowcount  # Number of rows deleted

  def getProjectsMapForCourse(self, course_name: str):
      query = (
          select(models.Project.doc_map_id)
          .where(models.Project.course_name == course_name)
      )
      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getDocumentsBetweenDates(self, course_name: str, from_date: str, to_date: str):
      query = (
          select(models.Document.id)
          .where(models.Document.course_name == course_name)
      )
      if from_date != '':
          query = query.where(models.Document.created_at >= from_date)
          if to_date != '':
              query = query.where(models.Document.created_at <= to_date)
      query = query.order_by(models.Document.id.asc())

      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getDocumentsBetweenDatesLLM(self, course_name: str, from_date: str, to_date: str):
      query = (
          select(models.LlmConvoMonitor.id)
          .where(models.LlmConvoMonitor.course_name == course_name)
      )
      if from_date != '':
          query = query.where(models.LlmConvoMonitor.created_at >= from_date)
          if to_date != '':
              query = query.where(models.LlmConvoMonitor.created_at <= to_date)
      query = query.order_by(models.LlmConvoMonitor.id.asc())

      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
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

      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getAllConversationsBetweenIds(self, course_name: str, first_id: int, last_id: int, limit: int = 50):
      query = (
          select(models.LlmConvoMonitor)
          .where(models.LlmConvoMonitor.course_name == course_name)
          .where(models.LlmConvoMonitor.id > first_id)
          .where(models.LlmConvoMonitor.id < last_id)

      )
      if last_id == 0:
          query = query.where(models.LlmConvoMonitor.id < last_id)
      query = (
          query
          .order_by(models.LlmConvoMonitor.id.asc())
          .limit(limit)
      )

      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
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

      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def insertProject(self, project_info):
      try:
          insert_stmt = insert(models.Project).values(project_info)
          self.session.execute(insert_stmt)
          self.session.commit()
          return True  # Insertion successful
      except SQLAlchemyError as e:
          self.session.rollback()  # Rollback in case of error
          print(f"Insertion failed: {e}")
          return False  # Insertion failed

  def getLLMConvo(self):
      query = (
          select(models.LlmConvoMonitor.convo)
      )
      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getAllFromLLMConvoMonitor(self, course_name: str):
      query = (
          select(models.LlmConvoMonitor)
          .where(models.LlmConvoMonitor.course_name == course_name)
          .order_by(models.LlmConvoMonitor.id.asc())
      )

      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
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

      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
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

      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getDocMapFromProjects(self, course_name: str):
      query = (
          select(models.Project.doc_map_id)
          .where(models.Project.course_name == course_name)
      )
      result = self.session.execute(query).mappings().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getConvoMapFromProjects(self, course_name: str):
      query = (
          select(models.Project)
          .where(models.Project.course_name == course_name)
      )
      result = self.session.execute(query).mappings().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def updateProjects(self, course_name: str, data: dict):
      query = (
          select(models.Project)
          .where(models.Project.course_name == course_name)
          .update(data)
      )
      result = self.session.execute(query)
      return result

  def getLatestWorkflowId(self):
      query = (
          select(models.N8nWorkflows)
      )
      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def lockWorkflow(self, id: int):
      try:
          insert_stmt = insert(models.N8nWorkflows).values({"latest_workflow_id": id, "is_locked": True})
          self.session.execute(insert_stmt)
          self.session.commit()
          return True  # Insertion successful
      except SQLAlchemyError as e:
          self.session.rollback()  # Rollback in case of error
          print(f"Insertion failed: {e}")
          return False  # Insertion failed

  def deleteLatestWorkflowId(self, id: int):
      query = (
          delete(models.N8nWorkflows)
          .where(models.N8nWorkflows.latest_workflow_id == id)
      )
      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def unlockWorkflow(self, id: int):
      query = (
          select(models.N8nWorkflows)
          .where(models.N8nWorkflows.latest_workflow_id == id)
          .update({"is_locked": False})
      )
      result = self.session.execute(query)
      return result

  def check_and_lock_flow(self, id):
      return self.session.query(func.check_and_lock_flows_v2(id)).all()

  def getConversation(self, course_name: str, key: str, value: str):
      query = (
          select(models.LlmConvoMonitor)
          .where(getattr(models.LlmConvoMonitor, key) == value)
          .where(models.LlmConvoMonitor.course_name == course_name)
      )
      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getDisabledDocGroups(self, course_name: str):
      query = (
          select(models.DocGroups.name)
          .where(models.DocGroups.course_name == course_name)
          .where(models.DocGroups.enabled == False)
      )
      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getPublicDocGroups(self, course_name: str):
      query = (
          select(models.DocGroup.name, models.DocGroup.course_name, models.DocGroup.enabled,
                 models.DocGroup.private, models.DocGroup.doc_count)
          .where(models.DocGroup.course_name == course_name)
      )
      result = self.session.execute(query).mappings().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getAllConversationsForUserAndProject(self, user_email: str, project_name: str, curr_count: int = 0):
      query = (
          select(models.Conversations, models.Messages)
          .join(models.Messages, models.Messages.conversation_id == models.Conversations.id)
          .where(models.Conversations.user_email == user_email)
          .where(models.Conversations.project_name == project_name)
          .order_by(models.Conversations.updated_at.desc())
          .limit(500)
          .offset(curr_count)
      )
      result = self.session.execute(query).mappings().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getPreAssignedAPIKeys(self, email: str):
      query = (
          select(models.PreAuthAPIKeys)
          .where(models.PreAuthAPIKeys.emails.contains([email]))
      )
      result = self.session.execute(query).scalars().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getConversationsCreatedAtByCourse(self, course_name: str, from_date: str = '', to_date: str = ''):
      try:
          query = (
              select(models.LlmConvoMonitor.created_at)
              .where(models.LlmConvoMonitor.course_name == course_name)
          )
          if from_date:
              query = query.where(models.LlmConvoMonitor.created_at >= from_date)
          if to_date:
              query = query.where(models.LlmConvoMonitor.created_at <= to_date)
          query = query.order_by(models.LlmConvoMonitor.created_at.asc())
          result = self.session.execute(query).scalars().all()
          response = DatabaseResponse(data=result, count=len(result)).to_dict()
          total_count = response.count if hasattr(response, 'count') else 0
          if total_count <= 0:
              print(f"No conversations found for course: {course_name}")
              return [], 0

          all_data = []
          batch_size = 1000
          start = 0
          while start < total_count:
              end = min(start + batch_size - 1, total_count - 1)

              try:
                  batch_query = (
                      select(models.LlmConvoMonitor.created_at)
                      .where(models.LlmConvoMonitor.course_name == course_name)
                  )
                  if from_date:
                      batch_query = batch_query.where(models.LlmConvoMonitor.created_at >= from_date)
                  if to_date:
                      batch_query = batch_query.where(models.LlmConvoMonitor.created_at <= to_date)
                  batch_query = batch_query.order_by(models.LlmConvoMonitor.created_at.asc())
                  batch_query = batch_query.range(start, end)
                  result = self.session.execute(batch_query).scalars().all()
                  response = DatabaseResponse(data=result, count=len(result)).to_dict()
                  total_count = response.count if hasattr(response, 'count') else 0
                  if not response or not hasattr(response, 'data') or not response.data:
                      print(f"No data returned for range {start} to {end}.")
                      break
                  all_data.extend(response.data)
                  start += batch_size
              except Exception as batch_error:
                  print(f"Error fetching batch {start}-{end}: {str(batch_error)}")
                  continue
          if not all_data:
              print(f"No conversation data could be retrieved for course: {course_name}")
              return [], 0
          return all_data, len(all_data)
      except Exception as e:
          print(f"Error in getConversationsCreatedAtByCourse for {course_name}: {str(e)}")
          return [], 0

  def getProjectStats(self, project_name: str) -> ProjectStats:
      try:
          query = (
              select(models.ProjectStats.total_messages, models.ProjectStats.total_conversations, models.ProjectStats.unique_users)
              .where(models.ProjectStats.project_name == project_name)
          )
          result = self.session.execute(query).mappings().all()
          response = DatabaseResponse(data=result, count=len(result)).to_dict()

          stats = {
              "total_messages": 0,
              "total_conversations": 0,
              "unique_users": 0,
              "avg_conversations_per_user": 0.0,
              "avg_messages_per_user": 0.0,
              "avg_messages_per_conversation": 0.0
          }

          if response and hasattr(response, 'data') and response.data:
              base_stats = response.data[0]
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
          print(f"Error fetching project stats for {project_name}: {str(e)}")
          return ProjectStats(total_messages=0,
                              total_conversations=0,
                              unique_users=0,
                              avg_conversations_per_user=0.0,
                              avg_messages_per_user=0.0,
                              avg_messages_per_conversation=0.0)

  def getWeeklyTrends(self, project_name: str) -> List[WeeklyMetric]:
      response = self.session.query(func.calculate_weekly_trends(project_name)).all()
      if response and hasattr(response, 'data'):
          return [
              WeeklyMetric(current_week_value=item['current_week_value'],
                           metric_name=item['metric_name'],
                           percentage_change=item['percentage_change'],
                           previous_week_value=item['previous_week_value']) for item in response.data
          ]

      return []

  def getModelUsageCounts(self, project_name: str) -> List[ModelUsage]:
      response = self.session.query(func.count_models_by_project(project_name)).all()
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
      result = self.session.execute(query).mappings().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getConvoMapDetails(self):
      return self.session.query(func.get_convo_maps()).all()

  def getDocMapDetails(self):
      return self.session.query(func.get_doc_map_details()).all()

  def getProjectsWithConvoMaps(self):
      query = (
          select(models.Project.course_name,
                 models.Project.convo_map_id,
                 models.Project.last_uploaded_doc_id,
                 models.Project.last_uploaded_convo_id)
          .where(models.Project.convo_map_id is not None)
      )
      result = self.session.execute(query).mappings().all()
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
      result = self.session.execute(query).mappings().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getProjectMapName(self, course_name, field_name):
      query = text(f"""
                    SELECT {field_name} FROM projects 
                    WHERE projects.course_name = {course_name}
                  """)
      result = self.session.execute(query).mappings().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def getMessagesFromConvoID(self, convo_id):
      query = (
          select(models.Messages)
          .where(models.Messages.conversation_id == convo_id)
          .limit(500)
      )
      result = self.session.execute(query).mappings().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

  def updateMessageFromLlmMonitor(self, message_id, llm_monitor_tags):
      query = (
          select(models.Message)
          .where(models.Message.id == message_id)
          .update({"llm_monitor_tags": llm_monitor_tags})
      )
      result = self.session.execute(query).mappings().all()
      response = DatabaseResponse(data=result, count=len(result)).to_dict()
      return response

