---
description: The most important API endpoints developers will want to use.
layout:
  title:
    visible: true
  description:
    visible: true
  tableOfContents:
    visible: true
  outline:
    visible: true
  pagination:
    visible: true
---

# Endpoints

## **`/chat` API Endpoint**

\
The /chat endpoint is designed to handle chat requests, providing both streaming and non-streaming response options. It supports image content and ensures that the chat responses are processed efficiently. Before you can start using the API, you need to [generate an API key](api-keys.md).

#### **Using the API Key**

With your API key in hand, you can now make authenticated requests to the /chat endpoint. Below are examples of how to use the API for different scenarios.

### Streaming Response Example

For a streaming response, where messages are sent and received in real-time, use the following Python code snippet:\


```python
import requests

url = "https://uiuc.chat/api/chat-api/chat"
headers = {
    'Content-Type': 'application/json',
}
data = {
    "model": "gpt-4o-mini",
    "messages": [
        {
            "role": "system",
            "content": "Your system prompt here"
        },
        {
            "role": "user",
            "content": "What is in these documents?"
        }
    ],
    "openai_key": "YOUR-OPENAI-KEY-HERE",
    "temperature": 0.1,
    "course_name": "your-course-name",
    "stream": True,
    "api_key": "YOUR_API_KEY"
}

response = requests.post(url, headers=headers, json=data)
print(response.text)
```

### Non-Streaming Response Example

The non- streaming response will contain BOTH the LLM response and the relevant contexts

```python
import requests

url = "https://uiuc.chat/api/chat-api/chat"
headers = {
    'Content-Type': 'application/json'
}
data = {
    "model": "gpt-4o-mini",
    "messages": [
        {
            "role": "system",
            "content": "Your system prompt here"
        },
        {
            "role": "user",
            "content": "What is in these documents?"
        }
    ],
    "openai_key": "YOUR-OPENAI-KEY-HERE",
    "temperature": 0.1,
    "course_name": "your-course-name",
    "stream": False,
    "api_key": "YOUR_API_KEY"
}

response = requests.post(url, headers=headers, json=data)
print(response.message)
print(response.contexts)
```

### Retrieval Only

{% hint style="info" %}
Note: This API response is free of cost provided by UIUC chat and will NOT invoke LLM and ONLY return relevant contexts
{% endhint %}

```python
import requests

url = "https://uiuc.chat/api/chat-api/chat"
headers = {
    'Content-Type': 'application/json'
}
data = {
    "model": "gpt-4o-mini",
    "messages": [
        {
            "role": "system",
            "content": "Your system prompt here"
        },
        {
            "role": "user",
            "content": "What is in these documents?"
        }
    ],
    "openai_key": "YOUR-OPENAI-KEY-HERE",
    "temperature": 0.1,
    "course_name": "your-course-name",
    "retrieval_only": true
}

response = requests.post(url, headers=headers, json=data)
print(response.contexts)
```

### Image Input Example

To send an image as part of the conversation, include the image URL in the messages array:

Note: Image input is only allowed with gpt-4-vision-preview model for now

```python
import requests
import json

url = "https://uiuc.chat/api/chat-api/chat"
headers = {
  'Content-Type': 'application/json'
}
payload = {
  "model": "gpt-4-vision-preview",
  "messages": [
    {
      "role": "system",
      "content": "Your system prompt here"
    },
    {
      "role": "user",
      "content": [
        {
          "type": "image_url",
          "image_url": {
            "url": "you image url here"
          }
        },
        {
          "type": "text",
          "text": "Give me more information on the action depicted in this image."
        }
      ]
    }
  ],
  "openai_key": "YOUR-OPENAI-KEY-HERE",
  "temperature": 0.1,
  "course_name": "your-course-name",
  "stream": False,
  "api_key": "YOUR_API_KEY"
}

response = requests.post(url, headers=headers, json=data)
print(response.text)
```

### Multiple Messages in a Conversation

```python
import requests

url = "https://uiuc.chat/api/chat-api/chat"
headers = {
    'Content-Type': 'application/json',
}
data = {
    "model": "gpt-4",
    "messages": [
        {
          "role": "system",
          "content": "You are a helpful assistant."
        },
        {
          "role": "user",
          "content": "What can you tell me about the history of artificial intelligence?"
        },
        {
          "role": "assistant",
          "content": "Artificial intelligence has a long history dating back to the mid-20th century, with key milestones such as the development of the Turing Test and the creation of early neural networks."
        },
        {
          "role": "user",
          "content": [
            {
              "type": "text",
              "text": "Here is an image related to AI, can you analyze it?"
            },
            {
              "type": "image_url",
              "image_url": {
                "url": "https://example.com/path-to-your-image.png"
              }
            }
          ]
        }
    ],
    "openai_key": "YOUR-OPENAI-KEY-HERE",
    "temperature": 0.1,
    "course_name": "your-course-name",
    "stream": True,
    "api_key": "YOUR_API_KEY"
}

response = requests.post(url, headers=headers, json=data)
print(response.text)
```

### NCSA hosted models example

The best free option to use UIUC chat API is with LLAMA 3.1 70b model, hosted at NCSA.&#x20;

{% hint style="warning" %}
This model is free, but it's not the best performing. We recommend `GPT-4o/GPT-4o-mini` for its superior instruction following, response quality and ability to cite its source.
{% endhint %}

```
import requests

url = "https://uiuc.chat/api/chat-api/chat"
headers = {
    'Content-Type': 'application/json',
}
data = {
    "model": "llama3.1:70b",    
    "messages": [
        {
            "role": "system",
            "content": "Your system prompt here"
        },
        {
            "role": "user",
            "content": "What is in these documents?"
        }
    ],
    "temperature": 0.1,
    "course_name": "your-course-name",
    "stream": True,
    "api_key": "YOUR_API_KEY"
}

response = requests.post(url, headers=headers, json=data)
print(response.text)
```

### Tool Use

Tools will be automatically invoked based on LLM's response. There's currently no way to force tool invocation, you will have to encourage the LLM to use tools via prompting.&#x20;

For superior instruction following, GPT-4o model is always used for tool selection.

{% hint style="info" %}
Note: Available tools can be viewed under settings on the chat page.
{% endhint %}



#### Coming soon

Document ingest via API. Currently only supported via the website GUI.

***

## External Connections API

The External Connections API lets you configure per-project infrastructure connections (S3, PostgreSQL, Qdrant). Projects without external connections automatically use the shared platform infrastructure.

{% hint style="info" %}
**`database_config` is document-scoped.** When set, only document-related tables (`documents`, `doc_groups`, `documents_doc_groups`, `documents_in_progress`, `documents_failed`) are routed to the external Postgres. Conversations, messages, projects, stats, API keys, and workflow state always live on the host platform's main DB. See [Scope of the External SQL Connection](../developers/external-connections-config.md#scope-of-the-external-sql-connection).
{% endhint %}

For a detailed configuration reference, see [External Connections Config](../developers/external-connections-config.md).

### Create or Update Connection

`POST /api/project-connections`

Creates or updates the external connection config for a project. You can provide any combination of `s3_config`, `database_config`, and `qdrant_config` -- only the configs you include will be stored or updated.

{% hint style="info" %}
The project must already exist. Create it first via `/createProject` before configuring external connections.
{% endhint %}

**Request body:**

| Field             | Type   | Required | Description                                  |
| ----------------- | ------ | -------- | -------------------------------------------- |
| `project_name`    | string | Yes      | The project to configure                     |
| `s3_config`       | object | No       | S3/MinIO connection config                   |
| `database_config` | object | No       | PostgreSQL connection config                 |
| `qdrant_config`   | object | No       | Qdrant vector DB connection config           |

Each config object has its own required fields. See the [Configuration Reference](../developers/external-connections-config.md) for full details.

```python
import requests

url = "https://uiuc.chat/api/project-connections"
headers = {"Content-Type": "application/json"}
data = {
    "project_name": "my-project",
    "s3_config": {
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "bucket_name": "my-project-bucket"
        # Add "endpoint_url" only when connecting to MinIO or an S3-compatible service
    },
    "qdrant_config": {
        "url": "https://qdrant.example.com",
        "api_key": "your-qdrant-api-key",
        "port": 6333,
        "https": True,
        "default_collection": "my-collection"
    }
}

response = requests.post(url, headers=headers, json=data)
print(response.json())
```

**Response:**

```json
{
  "success": true,
  "project_name": "my-project",
  "project_id": 42
}
```

### Get Connection Config

`GET /api/project-connections?project_name=my-project`

Retrieves the external connection config for a project. All sensitive values are **masked** -- only the last 4 characters are shown.

```python
import requests

url = "https://uiuc.chat/api/project-connections"
params = {"project_name": "my-project"}

response = requests.get(url, params=params)
print(response.json())
```

**Response (found):**

```json
{
  "found": true,
  "project_name": "my-project",
  "is_active": true,
  "created_at": "2025-01-15 10:30:00",
  "updated_at": "2025-01-15 10:30:00",
  "s3_config": {
    "aws_access_key_id": "****MPLE",
    "aws_secret_access_key": "****EKEY",
    "bucket_name": "****cket"
  },
  "qdrant_config": {
    "url": "****e.com",
    "api_key": "****-key",
    "port": 6333,
    "https": true,
    "default_collection": "****tion"
  }
}
```

**Response (not found):**

```json
{
  "found": false,
  "project_name": "my-project"
}
```

### Delete Connection Config

`DELETE /api/project-connections?project_name=my-project`

Deletes the external connection config for a project. You can optionally delete **just one** of the stored configs (S3, database, or Qdrant) by passing a `type` query parameter; omit it to remove the entire row.

**Query parameters:**

| Param          | Required | Description                                                                                                      |
| -------------- | -------- | ---------------------------------------------------------------------------------------------------------------- |
| `project_name` | Yes      | The project whose connection config should be modified.                                                          |
| `type`         | No       | One of: `s3`, `database`, `qdrant`. When provided, only that config is cleared; the row and other configs stay. When omitted, the entire row is deleted. |

Passing any other value for `type` returns a **400** explaining the valid options.

{% hint style="warning" %}
After a row-level delete, the project reverts to using the **shared default infrastructure**. All cached connections for the project are immediately invalidated on either variant.
{% endhint %}

#### Delete the entire row (legacy behavior)

```python
import requests

url = "https://uiuc.chat/api/project-connections"
params = {"project_name": "my-project"}

response = requests.delete(url, params=params)
print(response.json())
```

**Response:**

```json
{
  "success": true,
  "deleted": true,
  "found": true,
  "cleared": null,
  "project_name": "my-project"
}
```

#### Delete a single config (e.g. only the Qdrant override)

```python
params = {"project_name": "my-project", "type": "qdrant"}
response = requests.delete(url, params=params)
print(response.json())
```

**Response:**

```json
{
  "success": true,
  "deleted": true,
  "found": true,
  "cleared": "qdrant",
  "project_name": "my-project"
}
```

**Invalid `type` response (400):**

```json
{
  "description": "Invalid type: 'redis'. Must be one of: s3, database, qdrant. Omit 'type' to delete the entire row."
}
```

### Toggle Connection Active State

`PATCH /api/project-connections/active`

Enables or disables a project's external connection row **without deleting any stored config**. When `is_active` is set to `false` the project falls back to the shared default infrastructure; flipping it back to `true` re-activates the stored configs.

{% hint style="info" %}
The `is_active` flag is the single source of truth used when resolving per-project connections. Both the web backend (`ConnectionManager`) and the ingest worker (`WorkerConnectionResolver`) only load rows where `is_active = true`, so a single PATCH toggles behavior everywhere. Cached connections are invalidated on every toggle.
{% endhint %}

**Request body:**

| Field          | Type    | Required | Description                                    |
| -------------- | ------- | -------- | ---------------------------------------------- |
| `project_name` | string  | Yes      | The project whose row should be toggled.       |
| `is_active`    | boolean | Yes      | `true` to enable overrides, `false` to disable. |

```python
import requests

url = "https://uiuc.chat/api/project-connections/active"
headers = {"Content-Type": "application/json"}
data = {"project_name": "my-project", "is_active": False}

response = requests.patch(url, headers=headers, json=data)
print(response.json())
```

**Response:**

```json
{
  "success": true,
  "project_name": "my-project",
  "is_active": false
}
```

Returns **404** if no connection row exists for the given project.

### Test Connection

`POST /api/project-connections/test`

Tests an external connection **without saving** it. Use this to verify credentials before creating a connection config.

**Request body:**

| Field    | Type   | Required | Description                                      |
| -------- | ------ | -------- | ------------------------------------------------ |
| `type`   | string | Yes      | One of: `s3`, `database`, `qdrant`               |
| `config` | object | Yes      | Connection config to test (type-specific fields)  |

#### Test a Qdrant Connection

```python
import requests

url = "https://uiuc.chat/api/project-connections/test"
headers = {"Content-Type": "application/json"}
data = {
    "type": "qdrant",
    "config": {
        "url": "https://qdrant.example.com",
        "api_key": "your-qdrant-api-key",
        "port": 6333,
        "https": True
    }
}

response = requests.post(url, headers=headers, json=data)
print(response.json())
```

#### Test a Database Connection

```python
data = {
    "type": "database",
    "config": {
        "connection_uri": "postgresql://user:password@db.example.com:5432/mydb"
    }
}

response = requests.post(url, headers=headers, json=data)
print(response.json())
```

#### Test an S3 / MinIO Connection

```python
# AWS S3
data = {
    "type": "s3",
    "config": {
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "bucket_name": "my-bucket"
    }
}

# MinIO (or other S3-compatible storage) -- include endpoint_url
data = {
    "type": "s3",
    "config": {
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "bucket_name": "my-bucket",
        "endpoint_url": "https://minio.example.com"
    }
}

response = requests.post(url, headers=headers, json=data)
print(response.json())
```

**Success response:**

```json
{
  "success": true
}
```

**Failure response:**

```json
{
  "success": false,
  "error": "Connection refused"
}
