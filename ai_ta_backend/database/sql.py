import os
from typing import Dict, List, TypedDict, Union, Optional

import sentry_sdk
import supabase
from injector import inject
from tenacity import retry, stop_after_attempt, wait_exponential


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

  @inject
  def __init__(self):
    # Create Supabase clients
    self.supabase_client = supabase.create_client(  # type: ignore
        supabase_url=os.environ['SUPABASE_URL'], supabase_key=os.environ['SUPABASE_API_KEY'])
    
    self.cropwizard_supabase_client = supabase.create_client(  # type: ignore
        supabase_url=os.environ['CROPWIZARD_SUPABASE_URL'], supabase_key=os.environ['CROPWIZARD_SUPABASE_SECRET'])

    # Default to regular client
    self.current_client = self.supabase_client

    sentry_sdk.init(
        dsn=os.environ['SENTRY_DSN'],
        enable_tracing=True,
    )

  def set_client_for_course(self, course_name: str):
    """
    Set the current client based on course name.
    If course_name starts with 'cropwizard', use CropWizard database.
    Otherwise, use the regular database.
    """
    if course_name and course_name.startswith('cropwizard'):
      self.current_client = self.cropwizard_supabase_client
    else:
      self.current_client = self.supabase_client

  def getAllMaterialsForCourse(self, course_name: str):
    self.set_client_for_course(course_name)
    return self.current_client.table(
        os.environ['SUPABASE_DOCUMENTS_TABLE']).select('course_name, s3_path, readable_filename, url, base_url').eq(
            'course_name', course_name).execute()

  def getMaterialsForCourseAndS3Path(self, course_name: str, s3_path: str):
    self.set_client_for_course(course_name)
    return self.current_client.from_(os.environ['SUPABASE_DOCUMENTS_TABLE']).select("id, s3_path, contexts").eq(
        's3_path', s3_path).eq('course_name', course_name).execute()

  def getMaterialsForCourseAndKeyAndValue(self, course_name: str, key: str, value: str):
    self.set_client_for_course(course_name)
    return self.current_client.from_(os.environ['SUPABASE_DOCUMENTS_TABLE']).select("id, s3_path, contexts").eq(
        key, value).eq('course_name', course_name).execute()

  def deleteMaterialsForCourseAndKeyAndValue(self, course_name: str, key: str, value: str):
    self.set_client_for_course(course_name)
    return self.current_client.from_(os.environ['SUPABASE_DOCUMENTS_TABLE']).delete().eq(key, value).eq(
        'course_name', course_name).execute()

  def deleteMaterialsForCourseAndS3Path(self, course_name: str, s3_path: str):
    self.set_client_for_course(course_name)
    return self.current_client.from_(os.environ['SUPABASE_DOCUMENTS_TABLE']).delete().eq('s3_path', s3_path).eq(
        'course_name', course_name).execute()

  def getProjectsMapForCourse(self, course_name: str):
    self.set_client_for_course(course_name)
    return self.current_client.table("projects").select("doc_map_id").eq("course_name", course_name).execute()

  def getDocumentsBetweenDates(self, course_name: str, from_date: str, to_date: str, table_name: str):
    self.set_client_for_course(course_name)
    if from_date != '' and to_date != '':
      # query between the dates
      print("from_date and to_date")

      response = self.current_client.table(table_name).select("id").eq("course_name", course_name).gte(
          'created_at', from_date).lte('created_at', to_date).order('id', desc=False).execute()

    elif from_date != '' and to_date == '':
      # query from from_date to now
      print("only from_date")
      response = self.current_client.table(table_name).select("id").eq("course_name", course_name).gte(
          'created_at', from_date).order('id', desc=False).execute()

    elif from_date == '' and to_date != '':
      # query from beginning to to_date
      print("only to_date")
      response = self.current_client.table(table_name).select("id").eq("course_name", course_name).lte(
          'created_at', to_date).order('id', desc=False).execute()

    else:
      # query all data
      print("No dates")
      response = self.current_client.table(table_name).select("id").eq(
          "course_name", course_name).order('id', desc=False).execute()
    return response

  def getAllFromTableForDownloadType(self, course_name: str, download_type: str, first_id: int):
    self.set_client_for_course(course_name)
    if download_type == 'documents':
      response = self.current_client.table("documents").select("*").eq("course_name", course_name).gte(
          'id', first_id).order('id', desc=False).limit(100).execute()
    else:
      response = self.current_client.table("llm-convo-monitor").select("*").eq("course_name", course_name).gte(
          'id', first_id).order('id', desc=False).limit(100).execute()
    return response

  def getAllConversationsBetweenIds(self, course_name: str, first_id: int, last_id: int, limit: int = 50):
    self.set_client_for_course(course_name)
    return self.current_client.table("llm-convo-monitor").select("*").eq("course_name", course_name).gte(
        'id', first_id).lte('id', last_id).order('id', desc=False).limit(limit).execute()

  #@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=10, max=600))
  def getDocsForIdsGte(self, course_name: str, first_id: int, fields: str = "*", limit: int = 100):
    self.set_client_for_course(course_name)
    return self.current_client.table("documents").select(fields).eq("course_name", course_name).gte(
        'id', first_id).order('id', desc=False).limit(limit).execute()

  def insertProjectInfo(self, project_info):
    # For project insertion, we'll use the regular client since projects are typically managed centrally
    return self.supabase_client.table("projects").insert(project_info).execute()

  def getAllFromLLMConvoMonitor(self, course_name: str):
    self.set_client_for_course(course_name)
    return self.current_client.table("llm-convo-monitor").select("*").eq("course_name",
                                                             course_name).order('id', desc=False).execute()

  def getCountFromLLMConvoMonitor(self, course_name: str, last_id: int):
    self.set_client_for_course(course_name)
    return self.current_client.table("llm-convo-monitor").select("id").eq(
        "course_name", course_name).gt('id', last_id).execute()

  def getCountFromDocuments(self, course_name: str, last_id: int):
    self.set_client_for_course(course_name)
    return self.current_client.table("documents").select("id").eq("course_name", course_name).gt('id', last_id).execute()

  def getDocMapFromProjects(self, course_name: str):
    self.set_client_for_course(course_name)
    return self.current_client.table("projects").select("doc_map_id").eq("course_name", course_name).execute()

  def getConvoMapFromProjects(self, course_name: str):
    self.set_client_for_course(course_name)
    return self.current_client.table("projects").select("*").eq("course_name", course_name).execute()

  def updateProjects(self, course_name: str, data: dict):
    self.set_client_for_course(course_name)
    return self.current_client.table("projects").update(data).eq("course_name", course_name).execute()

  def getLatestWorkflowId(self):
    return self.supabase_client.table('n8n_workflows').select("*").execute()

  def lockWorkflow(self, id: int):
    return self.supabase_client.table('n8n_workflows').insert({"latest_workflow_id": id, "is_locked": True}).execute()
    # return self.supabase_client.table('n8n_workflows').update({"latest_workflow_id":id, "is_locked": True}).eq('latest_workflow_id', supabase_id).execute()

  def deleteLatestWorkflowId(self, id: int):
    return self.supabase_client.table('n8n_workflows').delete().eq('latest_workflow_id', id).execute()

  def unlockWorkflow(self, id: int):
    return self.supabase_client.table('n8n_workflows').update({
        "latest_workflow_id": id,
        "is_locked": False
    }).eq('latest_workflow_id', id).execute()

  def check_and_lock_flow(self, id):
    return self.supabase_client.rpc('check_and_lock_flows_v2', {'id': id}).execute()

  def getConversation(self, course_name: str, key: str, value: str):
    self.set_client_for_course(course_name)
    return self.current_client.table("llm-convo-monitor").select("*").eq(key, value).eq("course_name",
                                                                           course_name).execute()

  def getDisabledDocGroups(self, course_name: str):
    self.set_client_for_course(course_name)
    return self.current_client.table("doc_groups").select("name").eq("course_name", course_name).eq("enabled",
                                                                                        False).execute()

  def getPublicDocGroups(self, course_name: str):
    self.set_client_for_course(course_name)
    return self.current_client.from_("doc_groups_sharing") \
        .select("doc_group_name, shared_with") \
        .eq("course_name", course_name) \
        .execute()

  def getAllConversationsForUserAndProject(self, user_email: str, project_name: str, curr_count: int = 0):
    self.set_client_for_course(project_name)
    return self.current_client.table('conversations').select(
        "id, conversation_id, user_email, course_name, created_at, updated_at").eq("user_email", user_email).eq(
            "course_name", project_name).order('created_at', desc=True).limit(50).execute()

  def insertProject(self, project_info):
    # For project insertion, we'll use the regular client since projects are typically managed centrally
    return self.supabase_client.table("projects").insert(project_info).execute()

  def getPreAssignedAPIKeys(self, email: str):
    return self.supabase_client.table("pre_authorized_api_keys").select("*").contains("emails",
                                                                                       [email]).execute()

  def getConversationsCreatedAtByCourse(self, course_name: str, from_date: str = '', to_date: str = ''):
    self.set_client_for_course(course_name)
    try:
      query = self.current_client.table("llm-convo-monitor")\
          .select("created_at")\
          .eq("course_name", course_name)

      if from_date and to_date:
        query = query.gte('created_at', from_date).lte('created_at', to_date)
      elif from_date:
        query = query.gte('created_at', from_date)
      elif to_date:
        query = query.lte('created_at', to_date)

      count_response = query.execute()

      total_count = count_response.count if hasattr(count_response, 'count') else 0

      if total_count <= 0:
        print(f"No conversations found for course: {course_name}")
        return [], 0

      all_data = []
      batch_size = 1000
      start = 0

      while start < total_count:
        end = min(start + batch_size - 1, total_count - 1)

        try:
          batch_query = self.current_client.table("llm-convo-monitor")\
              .select("created_at")\
              .eq("course_name", course_name)

          if from_date and to_date:
            batch_query = batch_query.gte('created_at', from_date).lte('created_at', to_date)
          elif from_date:
            batch_query = batch_query.gte('created_at', from_date)
          elif to_date:
            batch_query = batch_query.lte('created_at', to_date)

          response = batch_query.range(start, end).execute()

          if not response or not hasattr(response, 'data') or not response.data:
            print(f"No data returned for range {start} to {end}.")
            break

          all_data.extend(response.data)
          start += batch_size

        except Exception as batch_error:
          sentry_sdk.capture_exception(batch_error)
          print(f"Error fetching batch {start}-{end}: {str(batch_error)}")
          continue

      if not all_data:
        print(f"No conversation data could be retrieved for course: {course_name}")
        return [], 0

      return all_data, len(all_data)

    except Exception as e:
      print(
          f"Error in getConversationsCreatedAtByCourse for course {course_name}: {str(e)}")
      sentry_sdk.capture_exception(e)
      return [], 0

  def getProjectStats(self, project_name: str) -> ProjectStats:
    self.set_client_for_course(project_name)
    response = self.current_client.table("project_stats").select("total_messages, total_conversations, unique_users")\
        .eq("project_name", project_name)\
        .execute()

    if not response.data:
      return {
          "total_messages": 0,
          "total_conversations": 0,
          "unique_users": 0,
          "avg_conversations_per_user": 0.0,
          "avg_messages_per_user": 0.0,
          "avg_messages_per_conversation": 0.0
      }

    stats = response.data[0]
    total_messages = stats.get("total_messages", 0)
    total_conversations = stats.get("total_conversations", 0)
    unique_users = stats.get("unique_users", 0)

    avg_conversations_per_user = total_conversations / unique_users if unique_users > 0 else 0.0
    avg_messages_per_user = total_messages / unique_users if unique_users > 0 else 0.0
    avg_messages_per_conversation = total_messages / total_conversations if total_conversations > 0 else 0.0

    return {
        "total_messages": total_messages,
        "total_conversations": total_conversations,
        "unique_users": unique_users,
        "avg_conversations_per_user": avg_conversations_per_user,
        "avg_messages_per_user": avg_messages_per_user,
        "avg_messages_per_conversation": avg_messages_per_conversation
    }

  def getWeeklyTrends(self, project_name: str) -> List[WeeklyMetric]:
    self.set_client_for_course(project_name)
    response = self.current_client.rpc('calculate_weekly_trends', {'course_name_input': project_name}).execute()

    if not response.data:
      return []

    trends = []
    for trend in response.data:
      trends.append({
          "current_week_value": trend.get("current_week_value", 0),
          "metric_name": trend.get("metric_name", ""),
          "percentage_change": trend.get("percentage_change", 0.0),
          "previous_week_value": trend.get("previous_week_value", 0)
      })

    return trends

  def getModelUsageCounts(self, project_name: str) -> List[ModelUsage]:
    self.set_client_for_course(project_name)
    response = self.current_client.rpc('count_models_by_project', {'project_name_input': project_name}).execute()

    if not response.data:
      return []

    model_usage = []
    for usage in response.data:
      model_usage.append({
          "model_name": usage.get("model_name", ""),
          "count": usage.get("count", 0),
          "percentage": usage.get("percentage", 0.0)
      })

    return model_usage

  def getAllProjects(self):
    return self.supabase_client.table("projects").select(
        "course_name, course_owner, course_admins, created_at, updated_at").execute()

  def getConvoMapDetails(self):
    return self.supabase_client.rpc("get_convo_maps", params={}).execute()

  def getDocMapDetails(self):
    return self.supabase_client.rpc("get_doc_map_details", params={}).execute()

  def getProjectsWithConvoMaps(self):
    return self.supabase_client.table("projects").select(
        "course_name, convo_map_id, last_uploaded_convo_id, conversation_map_index").neq("convo_map_id",
                                                                                         None).execute()

  def getProjectsWithDocMaps(self):
    return self.supabase_client.table("projects").select(
        "course_name, doc_map_id, last_uploaded_doc_id, document_map_index").neq("doc_map_id", None).execute()

  def getProjectMapName(self, course_name, field_name):
    self.set_client_for_course(course_name)
    return self.current_client.table("projects").select(field_name).eq("course_name", course_name).execute()

  def getMessagesFromConvoID(self, convo_id):
    # For messages, we'll use the regular client since they're typically managed centrally
    return self.supabase_client.table("messages").select("*").eq("conversation_id", convo_id).limit(500).execute()

  def updateMessageFromLlmMonitor(self, message_id, llm_monitor_tags):
    # For messages, we'll use the regular client since they're typically managed centrally
    return self.supabase_client.table("messages").update({
        "llm-monitor-tags": llm_monitor_tags
    }).eq("id", message_id).execute()
