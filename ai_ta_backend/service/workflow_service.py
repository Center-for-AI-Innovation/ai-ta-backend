import json
import os
import time
import logging
from datetime import datetime
from urllib.parse import quote

import requests
from injector import inject

from ai_ta_backend.database.sql import SQLDatabase

# Configure logging for workflow debugging
logging.basicConfig(level=logging.INFO)
workflow_logger = logging.getLogger('workflow_service')

"""
🔍 WORKFLOW LOGGING GUIDE FOR DEBUGGING LONG-RUNNING TOOL EXECUTIONS 🔍

This enhanced logging system helps debug the N8N execution retrieval issue mentioned in:
https://github.com/n8n-io/n8n/issues/14237

Search for these bracketed labels in your logs:

📋 HIGH-LEVEL FLOW:
  [RUN-FLOW]         - Main Flask endpoint handling
  [WORKFLOW]         - Overall workflow management
  [WORKFLOW-SUCCESS] - Successful workflow completion
  [WORKFLOW-ERROR]   - Workflow failures and errors

🔒 WORKFLOW LOCKING:
  [WORKFLOW-LOCK]    - Database locking for concurrency control

📝 DATA HANDLING:
  [WORKFLOW-DATA]    - Data formatting and preparation
  [WORKFLOW-HOOK]    - N8N webhook URL generation

▶️ EXECUTION:
  [WORKFLOW-EXEC]    - Workflow execution timing
  [N8N-WEBHOOK]      - HTTP requests to N8N webhooks

🔍 EXECUTION RETRIEVAL (the problematic area):
  [EXECUTION-RETRIEVAL] - Starting execution retrieval
  [EXECUTION-POLLING]   - Polling attempts for long-running workflows
  [EXECUTION-TIMEOUT]   - Execution retrieval timeouts
  [EXECUTION-STATUS]    - Execution status information
  [EXECUTION-DEBUG]     - Debug info about executions in different states

🌐 N8N API CALLS:
  [N8N-API]          - N8N API requests and responses
  [N8N-SEARCH]       - Searching for specific execution IDs
  [N8N-DEBUG]        - Status summaries and debugging info

EXAMPLE SEARCHES:
  grep "[EXECUTION-TIMEOUT]" your_log_file.log     # Find timeout issues
  grep "[N8N-WEBHOOK]" your_log_file.log           # Debug webhook calls
  grep "[WORKFLOW-ERROR]" your_log_file.log        # Find workflow failures
  grep "waiting=" your_log_file.log                # Find executions in waiting state

This addresses the known N8N issue where executions in 'waiting' status
are not properly retrieved via the API.
"""


class WorkflowService:

  @inject
  def __init__(self, sqlDb: SQLDatabase):
    self.sqlDb = sqlDb
    self.flows = []
    self.url = os.getenv('N8N_URL', "https://primary-production-1817.up.railway.app")

  def get_users(self, limit: int = 50, pagination: bool = True, api_key: str = ""):
    if not api_key:
      raise ValueError('api_key is required')
    all_users = []
    headers = {"X-N8N-API-KEY": api_key, "Accept": "application/json"}
    url = self.url + '/api/v1/users?limit=%s&includeRole=true' % str(limit)
    response = requests.get(url, headers=headers, timeout=8)
    data = response.json()
    if not pagination:
      return data['data']
    else:
      all_users.append(data['data'])
      cursor = data.get('nextCursor')
      while cursor is not None:
        url = self.url + '/api/v1/users?limit=%s&cursor=%s&includeRole=true' % (str(limit), quote(cursor))
        response = requests.get(url, headers=headers, timeout=8)
        data = response.json()
        all_users.append(data['data'])
        cursor = data.get('nextCursor')

    return all_users

  def execute_flow(self, hook: str, data=None) -> None:
    """
    Execute N8N workflow with enhanced logging and error handling for long-running tools.
    """
    if not data:
      data = {'field-0': ''}
    
    workflow_logger.info(f"[N8N-WEBHOOK] 🌐 Making HTTP POST to N8N webhook: {hook}")
    workflow_logger.info(f"[N8N-WEBHOOK] 📦 Payload size: {len(str(data))} characters")
    workflow_logger.info(f"[N8N-WEBHOOK] 📝 Data fields: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
    
    # Increased timeout for long-running workflows (15 minutes)
    request_timeout = 900
    
    try:
      request_start = time.time()
      response = requests.post(
        hook, 
        files=data, 
        timeout=request_timeout,
        # Add headers for better debugging
        headers={
          'User-Agent': 'UIUC-Chat-Workflow-Service/1.0',
        }
      )
      request_time = time.time() - request_start
      
      workflow_logger.info(f"[N8N-WEBHOOK] 📡 HTTP request completed in {request_time:.2f}s")
      workflow_logger.info(f"[N8N-WEBHOOK] 📊 Response status: {response.status_code}")
      workflow_logger.info(f"[N8N-WEBHOOK] 📏 Response size: {len(response.content)} bytes")
      
      # Log response headers for debugging
      content_type = response.headers.get('content-type', 'unknown')
      workflow_logger.info(f"[N8N-WEBHOOK] 🏷️ Response content-type: {content_type}")
      
      if not response.ok:
        workflow_logger.error(f"[N8N-WEBHOOK] ❌ HTTP Error {response.status_code}: {response.reason}")
        workflow_logger.error(f"[N8N-WEBHOOK] 🔍 Response body: {response.text[:500]}...")  # First 500 chars
        raise Exception(f"N8N Webhook Error {response.status_code}: {response.reason}")
      
      workflow_logger.info("[N8N-WEBHOOK] ✅ N8N webhook executed successfully")
      
      # Log a preview of the response for debugging
      if response.text:
        preview = response.text[:200] + "..." if len(response.text) > 200 else response.text
        workflow_logger.info(f"[N8N-WEBHOOK] 📄 Response preview: {preview}")
      
    except requests.exceptions.Timeout:
      workflow_logger.error(f"[N8N-WEBHOOK] ⏰ Request timeout after {request_timeout}s - this may indicate the workflow is still running")
      raise Exception(f"N8N webhook timeout after {request_timeout}s")
    except requests.exceptions.ConnectionError as e:
      workflow_logger.error(f"[N8N-WEBHOOK] 🔌 Connection error to N8N: {str(e)}")
      raise Exception(f"Failed to connect to N8N: {str(e)}")
    except requests.exceptions.RequestException as e:
      workflow_logger.error(f"[N8N-WEBHOOK] 🌐 Request exception: {str(e)}")
      raise Exception(f"N8N request failed: {str(e)}")

  def get_executions(self, limit, id=None, pagination: bool = True, api_key: str = ""):
    """
    Enhanced get_executions with comprehensive logging to debug the N8N execution retrieval issue.
    """
    # limit <= 250
    if not api_key:
      raise ValueError('api_key is required')
    
    workflow_logger.info(f"[N8N-API] 🔍 Fetching executions - limit: {limit}, target_id: {id}, pagination: {pagination}")
    
    headers = {"X-N8N-API-KEY": api_key, "Accept": "application/json"}
    url = self.url + f"/api/v1/executions?includeData=true&limit={limit}"
    
    try:
      workflow_logger.info(f"[N8N-API] 📡 GET request to: {url}")
      response = requests.get(url, headers=headers, timeout=30)  # Increased timeout
      
      workflow_logger.info(f"[N8N-API] 📊 Executions API response: {response.status_code}")
      
      if not response.ok:
        workflow_logger.error(f"[N8N-API] ❌ Failed to fetch executions: {response.status_code} - {response.text}")
        return None
      
      executions = response.json()
      total_found = len(executions.get('data', []))
      workflow_logger.info(f"[N8N-API] 📋 Retrieved {total_found} executions from API")
      
      # Log execution details for debugging
      if executions.get('data'):
        workflow_logger.info("[N8N-API] 📄 Execution details:")
        for i, exec_item in enumerate(executions['data'][:3]):  # Log first 3
          exec_id = exec_item.get('id', 'unknown')
          exec_status = exec_item.get('status', 'unknown')
          exec_workflow = exec_item.get('workflowData', {}).get('name', 'unknown')
          exec_start = exec_item.get('startedAt', 'unknown')
          exec_end = exec_item.get('stoppedAt', 'unknown')
          
          workflow_logger.info(f"[N8N-API]   #{i+1}: ID={exec_id}, Status={exec_status}, Workflow={exec_workflow}")
          workflow_logger.info(f"[N8N-API]        Started={exec_start}, Stopped={exec_end}")
          
          # Check if this is our target execution
          if id and str(exec_id) == str(id):
            workflow_logger.info(f"[N8N-API] 🎯 Found target execution ID: {id}")
      
      if not pagination:
        all_executions = executions['data']
        workflow_logger.info(f"[N8N-API] 📦 Returning {len(all_executions)} executions (no pagination)")
      else:
        all_executions = []
        all_executions.append(executions['data'])
        cursor = executions.get('nextCursor')
        page_count = 1
        
        while cursor is not None:
          page_count += 1
          workflow_logger.info(f"[N8N-API] 📄 Fetching page {page_count} with cursor: {cursor[:50]}...")
          
          paginated_url = self.url + f'/api/v1/executions?includeData=true&limit={str(limit)}&cursor={quote(cursor)}'
          response = requests.get(paginated_url, headers=headers, timeout=30)
          
          if not response.ok:
            workflow_logger.error(f"[N8N-API] ❌ Failed to fetch page {page_count}: {response.status_code}")
            break
            
          executions = response.json()
          page_results = len(executions.get('data', []))
          workflow_logger.info(f"[N8N-API] 📋 Page {page_count} returned {page_results} executions")
          
          all_executions.append(executions['data'])
          cursor = executions.get('nextCursor')
          
          # If looking for specific ID, check each page
          if id:
            for execution_list in all_executions:
              if isinstance(execution_list, list):
                for execution in execution_list:
                  if str(execution.get('id', '')) == str(id):
                    workflow_logger.info(f"[N8N-API] 🎯 Found target execution {id} on page {page_count}")
                    return execution
            
        workflow_logger.info(f"[N8N-API] 📚 Total pages fetched: {page_count}")
    
      # Final search for target ID
      if id:
        workflow_logger.info(f"[N8N-SEARCH] 🔍 Searching for execution ID: {id}")
        
        if pagination:
          # Search through all paginated results
          for page_num, execution_list in enumerate(all_executions):
            if isinstance(execution_list, list):
              for execution in execution_list:
                if str(execution.get('id', '')) == str(id):
                  workflow_logger.info(f"[N8N-SEARCH] ✅ Found execution {id} on page {page_num + 1}")
                  return execution
        else:
          # Search through single page results
          for execution in executions['data']:
            if str(execution.get('id', '')) == str(id):
              workflow_logger.info(f"[N8N-SEARCH] ✅ Found execution {id} in results")
              return execution
        
        workflow_logger.warning(f"[N8N-SEARCH] ⚠️ Execution ID {id} not found in {total_found} results")
        
        # Additional debugging: check if execution might be in "waiting" state
        waiting_count = 0
        running_count = 0
        success_count = 0
        error_count = 0
        
        search_data = executions['data'] if not pagination else []
        if pagination:
          for page in all_executions:
            if isinstance(page, list):
              search_data.extend(page)
        
        for exec_item in search_data:
          status = exec_item.get('status', 'unknown').lower()
          if 'wait' in status:
            waiting_count += 1
          elif 'running' in status:
            running_count += 1
          elif 'success' in status:
            success_count += 1
          elif 'error' in status or 'fail' in status:
            error_count += 1
            
        workflow_logger.info(f"[N8N-DEBUG] 📊 Execution status summary: waiting={waiting_count}, running={running_count}, success={success_count}, error={error_count}")
        
        return None
      else:
        total_executions = len(all_executions) if not pagination else sum(len(page) for page in all_executions if isinstance(page, list))
        workflow_logger.info(f"[N8N-API] 📦 Returning all executions: {total_executions} total")
        return all_executions
        
    except requests.exceptions.RequestException as e:
      workflow_logger.error(f"[N8N-API] 🌐 Network error fetching executions: {str(e)}")
      return None
    except Exception as e:
      workflow_logger.error(f"[N8N-API] 💥 Unexpected error in get_executions: {str(e)}")
      return None

  def get_workflows(self,
                    limit,
                    pagination: bool = True,
                    api_key: str = "",
                    active: bool = False,
                    workflow_name: str = ''):
    if not api_key:
      raise ValueError('api_key is required')
    headers = {"X-N8N-API-KEY": api_key, "Accept": "application/json"}
    url = self.url + f"/api/v1/workflows?limit={limit}"
    if active:
      url = url + "&active=true"
    response = requests.get(url, headers=headers, timeout=8)
    workflows = response.json()
    if workflows.get('message') == 'unauthorized' and not response.ok:
      raise Exception('Unauthorized')

    if not pagination:
      return workflows['data']
    else:
      all_workflows = []
      all_workflows.append(workflows['data'])
      cursor = workflows.get('nextCursor')
      while cursor is not None:
        url = self.url + f"/api/v1/workflows?limit={limit}&cursor={quote(cursor)}"
        response = requests.get(url, headers=headers, timeout=8)
        workflows = response.json()
        all_workflows.append(workflows['data'])
        cursor = workflows.get('nextCursor')

    if workflow_name:
      for workflow in all_workflows[0]:
        if workflow['name'] == workflow_name:
          return workflow
      else:
        raise Exception('Workflow not found')
    return all_workflows

  def get_hook(self, name: str, api_key: str = ""):
    work_flow = self.get_workflows(limit=100, api_key=api_key, workflow_name=name)
    if isinstance(work_flow, dict) and 'nodes' in work_flow:
      for node in work_flow['nodes']:
        if node['name'] == 'n8n Form Trigger':
          return node['parameters']['path']
    else:
      raise Exception('No nodes found in the workflow')

  def format_data(self, inputted, api_key: str, workflow_name):
    try:
      work_flow = self.get_workflows(100, api_key=api_key, workflow_name=workflow_name)
      values = []
      if isinstance(work_flow, dict) and 'nodes' in work_flow:
        for node in work_flow['nodes']:
          if node['name'] == 'n8n Form Trigger':
            values = node['parameters']['formFields']['values']
      data = {}
      # Check if inputted is already a dict, if not, try to load it as JSON
      if not isinstance(inputted, dict):
        inputted = json.loads(inputted)
      for i, value in enumerate(values):
        field_name = 'field-' + str(i)
        data[value['fieldLabel']] = field_name
      new_data = {}
      for k, v in inputted.items():
        if isinstance(v, list):
          new_data[data[k]] = json.dumps(v)
        else:
          new_data[data[k]] = v
      return new_data
    except Exception as e:
      print("❌ Major error in format_data: ", e)

  def switch_workflow(self, id, api_key: str = "", activate: 'str' = 'True'):
    if not api_key:
      raise ValueError('api_key is required')
    headers = {"X-N8N-API-KEY": api_key, "Accept": "application/json"}
    if activate == "True" or activate == "true":
      url = self.url + f"/api/v1/workflows/{id}/activate"
    else:
      url = self.url + f"/api/v1/workflows/{id}/deactivate"
    response = requests.post(url, headers=headers, timeout=8)
    result = response.json()
    return result

  def latest_execution(self, api_key: str = ""):
    execution = self.get_executions(limit=1, api_key=api_key, pagination=False)
    if execution:
      n8n_id = int(execution[0]['id']) + 1
    else:
      raise Exception('No executions found')

    return n8n_id

  def main_flow(self, name: str, api_key: str = "", data: str = ""):
    """
    Enhanced main_flow with comprehensive logging for debugging long-running tool executions.
    """
    workflow_start_time = datetime.now()
    execution_id = None
    
    try:
      if not api_key:
        raise ValueError('api_key is required')
      
      workflow_logger.info(f"[WORKFLOW] 🚀 Starting main flow - Workflow: '{name}' at {workflow_start_time}")
      workflow_logger.info(f"[WORKFLOW] 📊 Input data size: {len(str(data))} characters")
      workflow_logger.info(f"[WORKFLOW] 🔗 N8N URL: {self.url}")

      # Get execution ID for tracking
      id = self.latest_execution(api_key)
      execution_id = id
      workflow_logger.info(f"[WORKFLOW] 🆔 Generated execution ID: {id}")

      # Lock the workflow with enhanced logging
      workflow_logger.info(f"[WORKFLOW-LOCK] 🔒 Attempting to lock workflow ID: {id}")
      locked = self.sqlDb.check_and_lock_flow(id)

      if locked.data == 'Workflow updated':
        workflow_logger.info(f"[WORKFLOW-LOCK] ✅ Successfully locked workflow - ID: {id}, Name: '{name}'")
      else:
        workflow_logger.warning(f"[WORKFLOW-LOCK] ⚠️ Workflow already locked, entering retry loop - Name: '{name}'")
        start_time = time.time()
        timeout = 600  # Increased timeout for long-running workflows
        retry_count = 0
        
        while locked.data == 'Workflow is locked' or 'id already exists':
          retry_count += 1
          workflow_logger.info(f"[WORKFLOW-LOCK] 🔄 Retry attempt #{retry_count} - waiting for workflow unlock")
          
          time.sleep(2)  # Brief pause between retries
          id = self.latest_execution(api_key)
          locked = self.sqlDb.check_and_lock_flow(id)
          
          if locked.data == 'Workflow updated':
            workflow_logger.info(f"[WORKFLOW-LOCK] ✅ Lock acquired after {retry_count} retries")
            break
            
          if time.time() - start_time > timeout:
            workflow_logger.error(f"[WORKFLOW-LOCK] ❌ Lock timeout reached after {timeout}s and {retry_count} retries")
            return None
            
        workflow_logger.info(f"[WORKFLOW-LOCK] 🔒 Final lock status - ID: {id}, Name: '{name}'")

      # Format data and get hook
      workflow_logger.info(f"[WORKFLOW-DATA] 📝 Formatting data for workflow '{name}'")
      new_data = self.format_data(data, api_key, name)
      workflow_logger.info(f"[WORKFLOW-DATA] 📊 Formatted data keys: {list(new_data.keys()) if new_data else 'None'}")
      
      hookId = self.get_hook(name, api_key)
      hook = self.url + f"/form/{hookId}"
      workflow_logger.info(f"[WORKFLOW-HOOK] 🪝 Hook URL: {hook}")

      # Execute the flow with timing
      execution_start = time.time()
      workflow_logger.info(f"[WORKFLOW-EXEC] ▶️ Starting workflow execution at {datetime.now()}")
      
      try:
        self.execute_flow(hook, new_data)
        execution_time = time.time() - execution_start
        workflow_logger.info(f"[WORKFLOW-EXEC] ✅ Workflow execution completed in {execution_time:.2f}s")
      except Exception as e:
        execution_time = time.time() - execution_start
        workflow_logger.error(f"[WORKFLOW-EXEC] ❌ Workflow execution failed after {execution_time:.2f}s - Error: {str(e)}")
        raise
      finally:
        # Always unlock the workflow
        unlock_start = time.time()
        self.sqlDb.unlockWorkflow(id)
        unlock_time = time.time() - unlock_start
        workflow_logger.info(f"[WORKFLOW-LOCK] 🔓 Unlocked workflow ID: {id} in {unlock_time:.2f}s")

      # Enhanced execution retrieval with better timeout handling
      workflow_logger.info(f"[EXECUTION-RETRIEVAL] 🔍 Starting execution retrieval for ID: {id}")
      retrieval_start = time.time()
      
      # First attempt - immediate check
      executions = self.get_executions(20, str(id), True, api_key)
      
      if executions:
        retrieval_time = time.time() - retrieval_start
        workflow_logger.info(f"[EXECUTION-RETRIEVAL] ✅ Executions retrieved immediately in {retrieval_time:.2f}s")
        workflow_logger.info(f"[EXECUTION-STATUS] 📋 Execution status: {executions.get('status', 'unknown') if isinstance(executions, dict) else 'unknown'}")
      else:
        # Polling with exponential backoff for long-running workflows
        workflow_logger.info("[EXECUTION-POLLING] ⏳ No immediate results, starting polling with exponential backoff...")
        
        poll_timeout = 900  # 15 minutes for long-running tools
        poll_start = time.time()
        poll_interval = 2  # Start with 2 second intervals
        max_interval = 30  # Max 30 second intervals
        poll_count = 0
        
        while executions is None and (time.time() - poll_start) < poll_timeout:
          poll_count += 1
          
          # Check for executions with status logging
          workflow_logger.info(f"[EXECUTION-POLLING] 🔍 Poll attempt #{poll_count} (interval: {poll_interval}s)")
          executions = self.get_executions(20, str(id), True, api_key)
          
          # Also try to get all recent executions to see if our execution is in "waiting" state
          all_recent_executions = self.get_executions(50, pagination=False, api_key=api_key)
          if all_recent_executions:
            workflow_logger.info(f"[EXECUTION-DEBUG] 📋 Found {len(all_recent_executions) if isinstance(all_recent_executions, list) else 1} recent executions")
            
            # Log execution statuses for debugging the N8N issue
            if isinstance(all_recent_executions, list):
              for exec_item in all_recent_executions[:5]:  # Log first 5
                if isinstance(exec_item, dict):
                  exec_status = exec_item.get('status', 'unknown')
                  exec_id = exec_item.get('id', 'unknown')
                  workflow_logger.info(f"[EXECUTION-DEBUG] 📄 Recent execution - ID: {exec_id}, Status: {exec_status}")
          
          if executions:
            retrieval_time = time.time() - retrieval_start
            workflow_logger.info(f"[EXECUTION-POLLING] ✅ Executions retrieved after {poll_count} polls in {retrieval_time:.2f}s")
            break
            
          # Exponential backoff with jitter
          time.sleep(poll_interval)
          poll_interval = min(poll_interval * 1.5, max_interval)
          
          elapsed = time.time() - poll_start
          workflow_logger.info(f"[EXECUTION-POLLING] ⏱️ Polling elapsed: {elapsed:.1f}s / {poll_timeout}s")
        
        if executions is None:
          total_time = time.time() - workflow_start_time.timestamp()
          workflow_logger.error(f"[EXECUTION-TIMEOUT] ❌ Failed to retrieve executions after {poll_timeout}s timeout and {poll_count} polls")
          workflow_logger.error(f"[EXECUTION-TIMEOUT] 💀 Total workflow time: {total_time:.2f}s")
          return None

      # Final success logging
      total_time = time.time() - workflow_start_time.timestamp()
      workflow_logger.info(f"[WORKFLOW-SUCCESS] 🎉 Workflow '{name}' completed successfully in {total_time:.2f}s")
      
      if isinstance(executions, dict):
        final_status = executions.get('status', 'unknown')
        workflow_logger.info(f"[EXECUTION-STATUS] 📊 Final execution status: {final_status}")
      
      return executions
      
    except Exception as e:
      total_time = time.time() - workflow_start_time.timestamp()
      workflow_logger.error(f"[WORKFLOW-ERROR] 💥 Workflow '{name}' failed after {total_time:.2f}s")
      workflow_logger.error(f"[WORKFLOW-ERROR] 🔥 Error details: {str(e)}")
      if execution_id:
        workflow_logger.error(f"[WORKFLOW-ERROR] 🆔 Failed execution ID: {execution_id}")
      raise
