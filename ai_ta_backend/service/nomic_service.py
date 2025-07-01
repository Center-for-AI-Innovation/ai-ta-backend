import datetime
import os
import re
import time

import nomic
import pandas as pd
from injector import inject
from nomic import AtlasDataset, atlas
from tenacity import retry, stop_after_attempt, wait_exponential

class NomicService():

  @inject
  def __init__(self, sentry: SentryService, sql: SQLDatabase):
    # nomic.login(os.environ['NOMIC_API_KEY'])
    nomic.cli.login(os.environ['NOMIC_API_KEY'])
    self.sentry = sentry
    self.sql = sql

  def get_nomic_map(self, course_name: str, type: str):
    """
		Returns the variables necessary to construct an iframe of the Nomic map given a course name.
		We just need the ID and URL.
		Example values:
			map link: https://atlas.nomic.ai/map/ed222613-97d9-46a9-8755-12bbc8a06e3a/f4967ad7-ff37-4098-ad06-7e1e1a93dd93
			map id: f4967ad7-ff37-4098-ad06-7e1e1a93dd93
		"""
    if not course_name or not type:
      raise ValueError("Course name and type are required")
    if type.lower() not in ['document', 'conversation']:
      raise ValueError("Invalid map type")

    start_time = time.monotonic()
    try:
      if type.lower() == 'document':
        map_prefix = 'Document Map for '
        index_suffix = "_doc_index"
      else:
        map_prefix = 'Conversation Map for '
        index_suffix = "_convo_index"

      project_name = map_prefix + course_name
      project_name = project_name.replace(" ", "-").lower()  # names are like this - conversation-map-for-cropwizard-15
      project = AtlasDataset(project_name)
      map = project.get_map(course_name + index_suffix)

      print(f"⏰ Nomic Full Map Retrieval: {(time.monotonic() - start_time):.2f} seconds")
      return {"map_id": f"iframe{map.id}", "map_link": map.map_link}

    except Exception as e:
      # Error: ValueError: You must specify a unique_id_field when creating a new project.
      if str(e) == 'You must specify a unique_id_field when creating a new project.':  # type: ignore
        print(
            "Nomic map does not exist yet, probably because you have less than 20 queries/documents on your project: ",
            e)
      else:
        print("ERROR in get_nomic_map():", e)
        self.sentry.capture_exception(e)
      return {"map_id": None, "map_link": None}

  def update_conversation_maps(self):
    """
    Updates all conversation maps in UIUC.Chat. To be called via a CRON job.
    Returns:
        str: 'success' or error message
    """
    try:
      projects = self.sql.getAllProjects().data

      for project in projects:
        course_name = project['course_name']
        print(f"Processing course: {course_name}")

        if not project['convo_map_id']:
          print(f"Creating new conversation map for {course_name}")
          self.create_conversation_map(course_name)
          continue

        print(f"Updating existing conversation map for {course_name}")
        last_uploaded_id = project['last_uploaded_convo_id']
        total_convo_count = self.sql.getCountFromLLMConvoMonitor(course_name, last_id=last_uploaded_id).count

        if total_convo_count == 0:
          print("No new conversations to log.")
          continue

        print(f"Found {total_convo_count} unlogged conversations")
        combined_dfs = []
        current_count = 0

        while current_count < total_convo_count:
          response = self.sql.getAllConversationsBetweenIds(course_name, last_uploaded_id, 0, 100)
          if not response.data:
            break

          combined_dfs.append(pd.DataFrame(response.data))
          current_count += len(response.data)

          if combined_dfs:
            final_df = pd.concat(combined_dfs, ignore_index=True)
            metadata = self.data_prep_for_convo_map(final_df)

            print("Appending data to existing map...")
            map_name = re.sub(r'[^a-zA-Z0-9\s-]', '',
                              f"Conversation Map for {course_name}".replace("_", "-")).replace(" ", "-").lower()

            result = self.append_to_map(metadata=metadata, map_name=map_name)

            if result == "success":
              last_id = int(final_df['id'].iloc[-1])
              self.sql.updateProjects(course_name, {'last_uploaded_convo_id': last_id})
              self.rebuild_map(course_name, "conversation")
            else:
              print(f"Error in updating conversation map: {result}")

      return "success"

    except Exception as e:
      error_msg = f"Error in updating conversation maps: {e}"
      print(error_msg)
      self.sentry.capture_exception(e)
      return error_msg

  def update_document_maps(self):
    """
    Updates all document maps in UIUC.Chat by processing and uploading documents in batches.
    
    Returns:
        str: Status of document maps update process
    """
    DOCUMENT_MAP_PREFIX = "Document Map for "
    BATCH_SIZE = 100
    UPLOAD_THRESHOLD = 500

    try:
      # Fetch all projects
      projects = self.sql.getAllProjects().data

      for project in projects:
        try:
          course_name = project['course_name']

          # Determine whether to create or update map
          if not project.get('doc_map_id'):
            print(f"Creating new document map for course: {course_name}")
            status = self.create_document_map(course_name)
            print(f"Status of document map creation: {status}")
            continue

          # Check for new documents
          last_uploaded_doc_id = project['last_uploaded_doc_id']
          response = self.sql.getCountFromDocuments(course_name, last_id=last_uploaded_doc_id)

          if not response.count:
            print("No new documents to log.")
            continue

          # Prepare update process
          total_doc_count = response.count
          print(f"Total unlogged documents in Supabase: {total_doc_count}")

          project_name = re.sub(r'[^a-zA-Z0-9\s-]', '',
                                f"{DOCUMENT_MAP_PREFIX}{course_name}".replace(" ", "-").replace("_", "-").lower())
          first_id = last_uploaded_doc_id

          combined_dfs = []
          current_doc_count = 0
          doc_count = 0
          batch_number = 0

          while current_doc_count < total_doc_count:
            # Fetch documents in batches
            response = self.sql.getDocsForIdsGte(course_name=course_name, first_id=first_id, limit=BATCH_SIZE)

            if not response.data:
              break

            df = pd.DataFrame(response.data)
            combined_dfs.append(df)
            current_doc_count += len(response.data)
            doc_count += len(response.data)

            # Determine if we should process the batch
            should_process = (doc_count >= UPLOAD_THRESHOLD or current_doc_count >= total_doc_count)

            if should_process:
              batch_number += 1
              print(f"\nProcessing batch #{batch_number}")

              final_df = pd.concat(combined_dfs, ignore_index=True)
              metadata = self.data_prep_for_doc_map(final_df)

              # Upload to map
              result = self.append_to_map(metadata=metadata, map_name=project_name)

              if result == "success":
                last_id = int(final_df['id'].iloc[-1])
                project_info = {'last_uploaded_doc_id': last_id}
                self.sql.updateProjects(course_name, project_info)

                print(f"Completed batch #{batch_number}. "
                      f"Documents processed: {current_doc_count}/{total_doc_count}")
              else:
                print(f"Error in uploading batch for {course_name}: {result}")
                raise Exception(f"Batch upload failed: {result}")

              # Reset for next batch
              combined_dfs = []
              doc_count = 0

            # Prepare for next iteration
            first_id = response.data[-1]['id'] + 1

            # Exit condition to prevent infinite loop
            if current_doc_count >= total_doc_count:
              break

          # Rebuild map after all documents are processed
          self.rebuild_map(course_name, "document")
          print(f"\nSuccessfully processed all documents for {course_name}")
          print(f"Total batches processed: {batch_number}")

        except Exception as e:
          print(f"Error in updating document map for {course_name}: {e}")
          self.sentry.capture_exception(e)
          continue

      return "success"

    except Exception as e:
      print(f"Error in update_document_maps: {e}")
      self.sentry.capture_exception(e)
      return f"Error in update_document_maps: {e}"

  def create_conversation_map(self, course_name: str):
    """
    Creates a conversation map for a given course from conversations in the database.
    Args:
        course_name (str): Name of the course to create a conversation map for.
    Returns:
        str: Status of map creation process
    """
    NOMIC_MAP_NAME_PREFIX = 'Conversation Map for '
    BATCH_SIZE = 100
    MIN_CONVERSATIONS = 20
    UPLOAD_THRESHOLD = 500

    try:
      # Check if map already exists
      existing_map = self.sql.getConvoMapFromProjects(course_name)
      if existing_map.data and existing_map.data[0]['convo_map_id']:
        return "Map already exists for this course."

      # Validate conversation count
      response = self.sql.getCountFromLLMConvoMonitor(course_name, last_id=0)
      if not response.count or response.count < MIN_CONVERSATIONS:
        return f"Cannot create map: {'No new convos present' if not response.count else 'Less than 20 conversations'}"

      # Prepare map creation
      total_convo_count = response.count
      print(f"Total conversations in Supabase: {total_convo_count}")

      project_name = re.sub(r'[^a-zA-Z0-9\s-]', '',
                            (NOMIC_MAP_NAME_PREFIX + course_name).replace(" ", "-").replace("_", "-").lower())
      first_id = response.data[0]['id'] - 1

      combined_dfs = []
      current_convo_count = 0
      convo_count = 0
      first_batch = True

      while current_convo_count < total_convo_count:
        # Fetch conversations in batches
        response = self.sql.getAllConversationsBetweenIds(course_name, first_id, 0, BATCH_SIZE)
        if not response.data:
          break

        df = pd.DataFrame(response.data)
        combined_dfs.append(df)
        current_convo_count += len(response.data)
        convo_count += len(response.data)
        print(f"Conversations processed: {convo_count}")
        print(f"Current conversation count: {current_convo_count}")

        # Process and upload batches when threshold is reached
        if convo_count >= UPLOAD_THRESHOLD or current_convo_count >= total_convo_count:
          print("Processing batch...")
          final_df = pd.concat(combined_dfs, ignore_index=True)
          print(f"length of final_df: {len(final_df)}")
          metadata = self.data_prep_for_convo_map(final_df)

          # Create or append to map
          if first_batch:
            print("in first batch")
            index_name = f"{course_name}_convo_index"
            map_title = f"{NOMIC_MAP_NAME_PREFIX}{course_name}"
            result = self.create_map(metadata, map_title, index_name, index_field="first_query")
          else:
            result = self.append_to_map(metadata=metadata, map_name=project_name)

          if result == "success":
            project = AtlasDataset(project_name)
            last_id = int(final_df['id'].iloc[-1])
            project_info = {'course_name': course_name, 'convo_map_id': project.id, 'last_uploaded_convo_id': last_id}
            print("project_info", project_info)
            # Update or insert project info
            if existing_map.data:
              self.sql.updateProjects(course_name, project_info)
            else:
              self.sql.insertProjectInfo(project_info)

          # Reset for next batch
          combined_dfs = []
          convo_count = 0
          first_batch = False

        # Prepare for next iteration
        first_id = response.data[-1]['id']

      # Rebuild map
      self.rebuild_map(course_name, "conversation")
      return "success"

    except Exception as e:
      print(e)
      self.sentry.capture_exception(e)
      return f"Error in creating conversation map: {str(e)}"

  def create_document_map(self, course_name: str):
    """
    Creates a document map for a given course from documents in the database.
    
    Args:
        course_name (str): Name of the course to create a document map for.
    
    Returns:
        str: Status of map creation process
    """
    DOCUMENT_MAP_PREFIX = "Document Map for "
    BATCH_SIZE = 100
    UPLOAD_THRESHOLD = 500
    MIN_DOCUMENTS = 20

    try:
      # Check if document map already exists
      project_name = DOCUMENT_MAP_PREFIX + course_name
      existing_map = self.sql.getDocMapFromProjects(course_name=course_name)
      if existing_map.data and existing_map.data[0]['doc_map_id']:
        return "Map already exists."

      # Validate document count
      response = self.sql.getCountFromDocuments(course_name, last_id=0)
      if not response.count or response.count < MIN_DOCUMENTS:
        return f"Cannot create map: {'No new docs present' if not response.count else 'Less than 20 documents'}"

      # Prepare map creation
      total_doc_count = response.count
      print(f"Total documents in Supabase: {total_doc_count}")

      project_name = re.sub(r'[^a-zA-Z0-9\s-]', '', project_name.replace(" ", "-").replace("_", "-").lower())
      first_id = response.data[0]['id'] - 1

      combined_dfs = []
      current_doc_count = 0
      doc_count = 0
      first_batch = True

      while current_doc_count < total_doc_count:
        # Fetch documents in batches
        response = self.sql.getDocsForIdsGte(course_name=course_name, first_id=first_id, limit=BATCH_SIZE)
        if not response.data:
          break

        df = pd.DataFrame(response.data)
        combined_dfs.append(df)
        current_doc_count += len(response.data)
        doc_count += len(response.data)

        # Determine if we should process the batch
        should_process = (doc_count >= UPLOAD_THRESHOLD or current_doc_count >= total_doc_count or
                          current_doc_count == total_doc_count)

        if should_process:
          final_df = pd.concat(combined_dfs, ignore_index=True)
          metadata = self.data_prep_for_doc_map(final_df)

          # Create or append to map
          index_name = f"{course_name}_doc_index"
          if first_batch:
            map_title = f"{DOCUMENT_MAP_PREFIX}{course_name}"
            result = self.create_map(metadata, map_title, index_name, index_field="text")
          else:
            result = self.append_to_map(metadata=metadata, map_name=project_name)

          if result == "success":
            project = AtlasDataset(project_name)
            last_id = int(final_df['id'].iloc[-1])
            project_info = {'course_name': course_name, 'doc_map_id': project.id, 'last_uploaded_doc_id': last_id}

            # Update or insert project info
            if existing_map.data:
              self.sql.updateProjects(course_name, project_info)
            else:
              self.sql.insertProjectInfo(project_info)

          # Reset for next batch
          combined_dfs = []
          doc_count = 0
          first_batch = False

        # Prepare for next iteration
        first_id = response.data[-1]['id'] + 1

        # Exit condition to prevent infinite loop
        if current_doc_count >= total_doc_count:
          break

      # Rebuild the map
      self.rebuild_map(course_name, "document")
      return "success"

    except Exception as e:
      print(e)
      self.sentry.capture_exception(e)
      return f"Error in creating document map: {str(e)}"

#   ## -------------------------------- SUPPLEMENTARY MAP FUNCTIONS --------------------------------- ##

  def rebuild_map(self, course_name: str, map_type: str):
    """
    Rebuilds a given map in Nomic.
    Args:
        course_name (str): Name of the course
        map_type (str): Type of map ('document' or 'conversation')
    Returns:
        str: Status of map rebuilding process
    """
    MAP_PREFIXES = {'document': 'Document Map for ', 'conversation': 'Conversation Map for '}

    try:
      project_name = re.sub(r'[^a-zA-Z0-9\s-]', '',
                            (MAP_PREFIXES.get(map_type.lower(), '') + course_name).replace(" ",
                                                                                           "-").replace("_",
                                                                                                        "-").lower())
      print(f"Rebuilding map: {project_name}")
      project = AtlasDataset(project_name)

      if project.is_accepting_data:
        project.update_indices(rebuild_topic_models=True)

      return "success"

    except Exception as e:
      print(e)
      self.sentry.capture_exception(e)
      return f"Error in rebuilding map: {e}"

  def create_map(self, metadata, map_name, index_name, index_field):
    """
    Creates a Nomic map with topic modeling and duplicate detection.
    
    Args:
        metadata (pd.DataFrame): Metadata for the map
        map_name (str): Name of the map to create
        index_name (str): Name of the index to create
        index_field (str): Field to be indexed
        
    Returns:
        str: 'success' or error message
    """
    print(f"Creating map: {map_name}")

    try:
      project = atlas.map_data(data=metadata,
                               identifier=map_name,
                               id_field="id",
                               topic_model=True,
                               duplicate_detection=True,
                               indexed_field=index_field)

      project.create_index(name=index_name, indexed_field=index_field, topic_model=True, duplicate_detection=True)

      return "success"

    except Exception as e:
      print(e)
      return f"Error in creating map: {e}"

  @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=10, max=600))
  def append_to_map(self, metadata, map_name):
    """
    Appends new data to an existing Nomic map.
    
    Args:
        metadata (pd.DataFrame): Metadata for the map update
        map_name (str): Name of the target map
        
    Returns:
        str: 'success' or error message
    """
    try:
      print(f"Appending to map: {map_name}")
      project = AtlasDataset(map_name)

      # if not project.is_accepting_data:
      #   print("Project is currently indexing. Try again later.")
      #   return "Project busy"
      with project.wait_for_dataset_lock():
        project.add_data(data=metadata)
      return "success"

    except Exception as e:
      print(e)
      return f"Error in appending to map: {e}"

  def data_prep_for_convo_map(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares conversation data from Supabase for Nomic map upload.
    Args:
        df (pd.DataFrame): Dataframe of documents from Supabase   
    Returns:
        pd.DataFrame: Processed metadata for map creation, or None if error occurs
    """
    print("Preparing conversation data for map")

    try:
      metadata = []
      current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

      for _, row in df.iterrows():
        created_at = datetime.datetime.strptime(row['created_at'],
                                                "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y-%m-%d %H:%M:%S")
        messages = row['convo']['messages']
        first_message = messages[0]['content']
        if isinstance(first_message, list):
          first_message = first_message[0].get('text', '')

        conversation = []
        for message in messages:
          emoji = "🙋 " if message['role'] == 'user' else "🤖 "
          content = message['content']
          text = content[0].get('text', '') if isinstance(content, list) else content
          conversation.append(f"\n>>> {emoji}{message['role']}: {text}\n")

        metadata.append({
            "course": row['course_name'],
            "conversation": ''.join(conversation),
            "conversation_id": row['convo']['id'],
            "id": row['id'],
            "user_email": row['user_email'] or "",
            "first_query": first_message,
            "created_at": created_at,
            "modified_at": current_time
        })

      result = pd.DataFrame(metadata)
      print(f"Metadata shape: {result.shape}")
      return result

    except Exception as e:
      print(f"Error in data preparation: {e}")
      self.sentry.capture_exception(e)
      return pd.DataFrame()

  def data_prep_for_doc_map(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares document metadata for Nomic map upload.
    
    Args:
        df (pd.DataFrame): Source documents dataframe
        
    Returns:
        pd.DataFrame: Processed metadata for map creation, or None if error occurs
    """
    try:
      metadata = []
      current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

      for _, row in df.iterrows():
        created_at = datetime.datetime.strptime(row['created_at'],
                                                "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y-%m-%d %H:%M:%S")

        for idx, context in enumerate(row['contexts'], 1):
          metadata.append({
              "id": f"{row['id']}_{idx}",
              "created_at": created_at,
              "s3_path": row['s3_path'],
              "url": row['url'] or "",
              "base_url": row['base_url'] or "",
              "readable_filename": row['readable_filename'],
              "modified_at": current_time,
              "text": context['text']
          })

      return pd.DataFrame(metadata)

    except Exception as e:
      print(f"Error in document data preparation: {e}")
      self.sentry.capture_exception(e)
      return pd.DataFrame()
