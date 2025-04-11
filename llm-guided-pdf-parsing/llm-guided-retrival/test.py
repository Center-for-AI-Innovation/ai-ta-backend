import json
import os

from dotenv import load_dotenv
from openai import AzureOpenAI

load_dotenv()

api_key = os.getenv("AZURE_OPENAI_KEY")
endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
deployment = os.getenv("DEPLOYMENT")
api_version = os.getenv("OPENAI_API_VERSION")

client = AzureOpenAI(azure_endpoint=endpoint, api_key=api_key, api_version=api_version)

custom_functions = [{
    "name": "multi_Func",
    "description": "Call two functions in one call",
    "parameters": {
        "type": "object",
        "properties": {
            "keep_current_chunk": {
                "name": "keep_current_chunk",
                "description": "Check if the current chunk of context is relevant to the query.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The query or question to check relevance"
                        },
                        "chunk": {
                            "type": "string",
                            "description": "The current chunk of context to check"
                        },
                        "keep": {
                            "type": "boolean",
                            "description": "Whether to keep the current chunk or not"
                        }
                    },
                    "required": ["query", "chunk", "keep"]
                }
            },
            "check_preivous_chunk": {
                "name": "check_preivous_chunk",
                "description": "Check if the previous chunk of context is relevant to the query",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The query or question to check relevance"
                        },
                        "chunk": {
                            "type": "string",
                            "description": "The current chunk of context to check"
                        },
                        "check_previous": {
                            "type": "boolean",
                            "description": "Whether to check the previous chunk or not"
                        }
                    },
                    "required": ["query", "chunk", "check_previous"]
                }
            }
        },
        "required": ["keep_current_chunk", "check_preivous_chunk"],
    }
}]

query = "What is shortcomings of existing methodologies?"
chunks = [
    "How might we be able to circumvent the estimation cost of sampling-based attributions? Let us start by examining the existing data attribution methods—specifically, the ones that use only one (or a few) trained models—and evaluate them on our LDS benchmark.",
    "This approach can indeed be a useful proxy for evaluating data attribution methods, but the resulting metrics may be too sensitive to the particulars of the auxiliary task and thus make comparisons across different problems and settings difficult.",
    "Motivated by the above shortcomings of existing methodologies, we propose a new metric for evaluating data attribution methods.",
    "There are also approaches that use more heuristic measures of training example importance for data attribution."
]

messages = [
    {
        "role":
            "system",
        "content":
            "You are an expert at doing literature review and determining if a piece of text is relevant to a research question by calling all four questions: keep_current_chunk, check_preivous_chunk, check_next_chunk, and go_to_url."
    },
    {
        "role":
            "user",
        "content":
            f"Given the question query is {query}, and the current chunk is {chunks[1]}. Is the current chunk relevant to the query? If not, what about the previous"
    },
]
response = client.chat.completions.create(
    model=deployment,
    messages=messages,
    functions=custom_functions,
)

# Loading the response as a JSON object
print(response)
