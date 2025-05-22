import json
import os

import redis
import requests
from injector import inject

from ai_ta_backend.database.sql import SQLDatabase
from ai_ta_backend.service.posthog_service import PosthogService
from ai_ta_backend.service.sentry_service import SentryService
from ai_ta_backend.utils.crypto import encrypt_if_needed
from ai_ta_backend.utils.schema_generation import (
    generate_schema_from_project_description,)


class ProjectService:
  """
    This class contains all methods related to project management.
    """

  @inject
  def __init__(self, sql_db: SQLDatabase, posthog_service: PosthogService, sentry_service: SentryService):
    self.sqlDb = sql_db
    self.posthog = posthog_service
    self.sentry = sentry_service

    print("Connecting to Redis... with url: ", os.environ['REDIS_URL'])
    self.redis_client = redis.Redis.from_url(os.environ['REDIS_URL'], db=0)

  def generate_json_schema(self, project_name: str, project_description: str | None) -> None:
    # Generate metadata schema using project_name and project_description
    json_schema = generate_schema_from_project_description(project_name, project_description)

    # Insert project into Supabase
    sql_row = {
        "course_name": project_name,
        "description": project_description,
        "metadata_schema": json_schema,
    }
    self.sqlDb.insertProject(sql_row)

  def create_project(self, project_name: str, project_description: str | None, project_owner_email: str) -> str:
    """
        This function takes in a project name and description and creates a project in the database.
        1. Generate metadata schema using project_name and project_description
        2. Insert project into Supabase
        3. Insert project into Redis
        """
    try:
      # Insert project into Redis
      headers = {
          "Authorization":
              f"Bearer {os.environ['KV_REST_API_TOKEN']}",  # Ensure you use the appropriate write-access API key
          "Content-Type": "application/json"
      }
      value = {
          "is_private": False,
          "course_owner": project_owner_email,
          "course_admins": ['kvday2@illinois.edu'],
          "approved_emails_list": None,
          "example_questions": None,
          "banner_image_s3": None,
          "course_intro_message": None,
          "openai_api_key": None,
          "system_prompt": None,
          "disabled_models": None,
          "project_description": project_description if project_description else None,
      }

      # Set course_metadatas
      print("Setting course_metadatas. value: ", value)
      self.redis_client.hset('course_metadatas', key=project_name, value=json.dumps(value))

      # check if the project owner has pre-assigned API keys
      if project_owner_email:
        pre_assigned_response = self.sqlDb.getPreAssignedAPIKeys(project_owner_email)
        if len(pre_assigned_response.data) > 0:
          redis_key = project_name + "-llms"
          llm_val = {
              "defaultModel": None,
              "defaultTemp": None,
          }
          # pre-assigned key exists
          for row in pre_assigned_response.data:
            # encrypt JUST the API keys field, which is row['providerBodyNoModels']['apiKey]
            row['providerBodyNoModels']['apiKey'] = encrypt_if_needed(row['providerBodyNoModels']['apiKey'])
            llm_val[row['providerName']] = row['providerBodyNoModels']

          print(f"Setting -llms default values. Key: `{redis_key}`, value: `{llm_val}`")
          self.redis_client.set(redis_key, json.dumps(llm_val))

      return "success"
    except Exception as e:
      print("Error in create_project: ", e)
      self.sentry.capture_exception(e)
      return f"Error while creating project: {e}"
