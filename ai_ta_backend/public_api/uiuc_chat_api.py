import json

import requests
"""
# Example usage

call_chat_endpoint(
  model_id="gpt-4-1106-preview",
  messages=[
    {
      "role": "system", 
      "content": [{"type": "text", "text": "You are a helpful assistant."}],
    },
    {
      "role": "user", 
      "content": [{"type": "text", "text": "What's in these documents? Use one sentence."}],
      "contexts": c
    }
  ],
  openai_api_key="TODO",
  course_name="test-video-ingest-21",
  stream=True
  # system_prompt_override="What's in these documents?",
)
"""


def call_chat_endpoint(model_id,
                       messages,
                       openai_api_key,
                       course_name,
                       temperature=0.4,
                       stream=False,
                       system_prompt_override=None):
  """
    model_id must be one of: 
      'gpt-3.5-turbo'
      'gpt-3.5-turbo-16k'
      'gpt-4'
      'gpt-4-1106-preview'
      'gpt-4-vision-preview'
      'gpt-4-from-canada-east' # Only for use with CAII Azure key
    """
  # url = "http://localhost:3000/api/chat"
  url = "https://www.uiuc.chat/api/chat"
  headers = {"Content-Type": "application/json"}
  data = {
      "model": model_id,
      "messages": messages,
      "key": openai_api_key,
      "prompt": system_prompt_override,
      "temperature": temperature,
      "course_name": course_name,
      "stream": stream
  }
  response = requests.post(url, headers=headers, data=json.dumps(data), stream=True, timeout=180)

  if stream:
    for chunk in response.iter_content(chunk_size=1):
      yield chunk.decode()
    # Check if the stream is still open before trying to close it. Helps our server close connections.
    if not response.raw.closed:
      response.raw.close()
  else:
    return response.json()
