import json
import os

from dotenv import load_dotenv
from openai import OpenAI
from read_sql import (
    get_context_given_contextID,
    get_next_context_id,
    get_previous_context_id,
)


def evaluate_chunks(query, chunks, outline):
  load_dotenv()

  api_key = os.getenv("AZURE_OPENAI_KEY")
  endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
  deployment = os.getenv("DEPLOYMENT")
  api_version = os.getenv("OPENAI_API_VERSION")
  openai_api = os.getenv("OPENAI_API")

  client = OpenAI(api_key=openai_api)

  tools = [
      {
          "type": "function",
          "function": {
              "name": "keep_current_chunk",
              "description": "Check if the current chunk of context is relevant to the query",
              "parameters": {
                  "type": "object",
                  "properties": {
                      "keep": {
                          "type": "boolean",
                          "description": "Whether to keep the current chunk or not"
                      }
                  },
                  "required": ["keep"]
              }
          },
          "required": True
      },
      {
          "type": "function",
          "function": {
              "name": "check_previous_chunk",
              "description": "Check if the previous chunk of context is relevant to the query",
              "parameters": {
                  "type": "object",
                  "properties": {
                      "check_previous": {
                          "type": "boolean",
                          "description": "True if the previous chunk is relevant, False otherwise"
                      }
                  },
                  "required": ["check_previous"]
              }
          },
      },
      {
          "type": "function",
          "function": {
              "name": "check_next_chunk",
              "description": "Check if the next chunk of context is relevant to the query",
              "parameters": {
                  "type": "object",
                  "properties": {
                      "check_next": {
                          "type": "boolean",
                          "description": "True if the next chunk is relevant, False otherwise"
                      }
                  },
                  "required": ["check_next"]
              }
          }
      },
      {
          "type": "function",
          "function": {
              "name": "go_to_URL",
              "description": "Click link or go to URL referenced in chunk.",
              "parameters": {
                  "type": "object",
                  "properties": {
                      "go_to_URL": {
                          "type": "boolean",
                          "description": "Whether to go to the URL or not"
                      }
                  },
                  "required": ["go_to_URL"]
              }
          }
      },
      {
          "type": "function",
          "function": {
              "name": "go_to_section",
              "description": "Navigate to a specific section in the document.",
              "parameters": {
                  "type": "object",
                  "properties": {
                      "go_to_section": {
                          "type": "boolean",
                          "description": "Whether to go to the section or not"
                      },
                      "section": {
                          "type": "string",
                          "description": "The section to navigate to"
                      }
                  },
                  "required": ["go_to_section", "section"]
              }
          }
      },
  ]

  messages = [{
      "role":
          "system",
      "content": (
          "You are an expert in information retrieval. Your task is to evaluate the relevance of a given text chunk to a specific research question. "
          "You have four functions at your disposal: 'keep_current_chunk', 'check_previous_chunk', 'check_next_chunk', and 'go_to_url'. "
          "Always use 'keep_current_chunk' to determine if the current chunk is relevant. Then, consider using 'check_previous_chunk' or 'check_next_chunk' or 'go_to_url'. "
          "When using 'check_previous_chunk', if you find previous chunk relevant to the research question, set 'check_previous' to 'True', otherwise false. "
          "When using 'check_next_chunk', if you find next chunk relevant to the research question, set 'check_next' to 'True', otherwise false. "
          "Use 'go_to_url' if the chunk suggests checking an external link."
          "You can also use 'go_to_section' to navigate to a specific section in the document.")
  }, {
      "role":
          "user",
      "content": (
          f"Research Question: '{query}'\n\n"
          f"Table of Contents: '{outline}'\n\n"
          f"Current Text Chunk: '{chunks}'\n\n"
          "Evaluate the relevance of the current chunk to the research question. Determine if the current chunk should be kept. "
          "Also, decide whether to check the previous chunk by calling 'check_previous_chunk', or the next chunk by calling 'check_next_chunk', or whether to follow an external link using the respective functions. "
          "Make sure you call other functions and determine if previous or next chunks are relevant to the research question."
      )
  }]

  completion = client.chat.completions.create(
      model='gpt-4o',
      messages=messages,
      tools=tools,
      # tool_choice={"type": "function", "function": {"name": "keep_current_chunk"}},
  )

  return completion


# query = "What is the name of Polly's son?"
# chunks = [
#     "Polly's son, Michael Gray, was taken away when he was a baby. He was raised by a family in Australia. He was brought back to Birmingham by Polly in season 2.",
#     "The Blinders' dominance came about from beating rivals, including the 'Sloggers', 'a pugilistic term for someone who could strike a heavy blow in the ring', whom they fought for territory in Birmingham and its surrounding districts.",
#     "Britain is a mixture of despair and hedonism in 1919 in the aftermath of the Great War. Returning soldiers, newly minted revolutions and criminal gangs are fighting for survival in a nation rocked by economic upheaval."
# ]
# outline = "1. Introduction\n2. Polly Gray\n3. The Blinders\n4. Britain in 1919"
# result = evaluate_chunks(query, chunks[1], outline)
# print(result)
# for tool in result.choices[0].message.tool_calls:
#     print("true" in tool.function.arguments)
#     print("---")


def evaluate_chunks_with_step(query, chunk_id, step, chunks_to_keep, is_visited):
  if step > 4:
    return
  if chunk_id in is_visited:
    print("already visited")
    return
  is_visited[chunk_id] = True
  step += 1

  print(chunk_id)
  context_data, current_context, outline = get_context_given_contextID(chunk_id)
  completion = evaluate_chunks(query, current_context, outline)

  if completion is None:
    return
  # print(completion)

  for tool in completion.choices[0].message.tool_calls:
    if tool.function.name == "keep_current_chunk" and "true" in tool.function.arguments:
      print("Keeping current chunk")
      chunks_to_keep.append(current_context)
    if tool.function.name == "check_previous_chunk" and "true" in tool.function.arguments:
      previous_context_id = get_previous_context_id(context_data, chunk_id)
      if previous_context_id is not None:
        print("Checking previous chunk")
        evaluate_chunks_with_step(query, previous_context_id, step, chunks_to_keep)
    if tool.function.name == "check_next_chunk" and "true" in tool.function.arguments:
      next_context_id = get_next_context_id(context_data, chunk_id)
      if next_context_id is not None:
        print("Checking next chunk")
        evaluate_chunks_with_step(query, next_context_id, step, chunks_to_keep)
