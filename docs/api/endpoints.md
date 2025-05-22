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
