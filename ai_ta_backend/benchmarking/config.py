from os import environ, getenv
# from typing import List

class CropWizardConfig:
    def __init__(self):
        self.prompt_endpoint = str(getenv("UIUC_CHAT_PROD_URL"))    # API endpoint to retrieve contexts 
        self.answer_endpoint = str(getenv("CROPWIZARD_API_URL"))    # API endpoint to retrieve answers 
        self.cropwiz_api_key = str(getenv("UIUC_CHAT_API_KEY"))     # API key to access UIUC.chat
        self.db_version = "cropwizard-1.5"                          # CropWizard version
        self.cw_groups = ["All Documents"]                          # Subset of documents
        self.token_limit = 128000                                   # Token Limit
        self.model = "gpt-4o-mini"                                  # Default model
        self.temperature = 0.1                                      # Default temperature
        self.cropwiz_sys_prompt = (
            """You are a helpful assistant. Follow the user's instructions carefully. Respond using markdown. If the question is related to farming in ANY WAY then always answer the question even if the documents are not helpful. Give the users the most helpful response possible as best you can. Remember this: always give a reasonable answer to the users question and never refuse to answer even if you have little information to go on; give an array of helpful answers if necessary. ALWAYS respond with something helpful to answer their questions and propose solutions. ALWAYS provide citations, but ONLY when relevant. 
            If the user asks an introductory question or greeting along the lines of "hello" or "what can you do?" or "What's in here?" or "what is CropWizard?" or similar, then please respond with a warm welcome to CropWizard, the AI farm assistant chatbot. Tell them that you can answer questions using the entire knowledge base of Extension plus a growing list of open-access research publications. Whether you need information on crop management, pest control, or any other farming-related topic, feel free to ask!
            When the provided documents don't contain the answer, say in bold italic text "The CropWizard database doesn't have anything covering this exact question, but here's what I know from my general world knowledge." Always refer to the provided documents as "the CropWizard database" and use bold italics when giving this disclaimer."""
        )

    def __repr__(self):
        return f"<CropWizardConfig(db_version={self.db_version}, fetching from={self.cw_groups})>"

    def get_config(self):
        """Returns the configuration as a dictionary."""
        return {
            "prompt_endpoint": self.prompt_endpoint,
            "answer_endpoint": self.answer_endpoint,
            "cropwiz_api_key": self.cropwiz_api_key,
            "db_version": self.db_version,
            "cw_groups": self.cw_groups,
            "token_limit": self.token_limit,
            "model": self.model,
            "temperature": self.temperature,
            "cropwiz_sys_prompt": self.cropwiz_sys_prompt,
        }


class LangchainConfig:
    def __init__(self):
        environ["LANGCHAIN_TRACING_V2"] = "true"
        environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
        environ["LANGCHAIN_API_KEY"] = str(getenv("LANGCHAIN_API_KEY"))
        environ["LANGCHAIN_PROJECT"] = "cropwizard_testing"

        self.tracing_v2 = environ["LANGCHAIN_TRACING_V2"]
        self.endpoint = environ["LANGCHAIN_ENDPOINT"]
        self.api_key = environ["LANGCHAIN_API_KEY"]
        self.project = environ["LANGCHAIN_PROJECT"]

    def __repr__(self):
        return f"<LangchainConfig(project={self.project})>"

    def get_config(self):
        """Returns the Langchain configuration as a dictionary."""
        return {
            "tracing_v2": self.tracing_v2,
            "endpoint": self.endpoint,
            "api_key": self.api_key,
            "project": self.project,
        }


class OpenAIConfig:
    def __init__(self):
        environ["OPENAI_API_KEY"] = str(getenv("OPENAI_API_KEY"))

        self.api_key = environ["OPENAI_API_KEY"]
        self.temperature = 0.1  # Default temperature

    def __repr__(self):
        return f"<OpenAI API key is initialized>"

    def get_config(self):
        """Returns the OpenAI configuration as a dictionary."""
        return {
            "api_key": self.api_key,
            "temperature": self.temperature,
        }


class OllamaConfig:
    def __init__(self):
        # self.base_url = str(getenv("OLLAMA_BASE_URL", "http://localhost:11434"))
        self.base_url = str(getenv("OLLAMA_BASE_URL", str(getenv("OLLAMA_API_URL"))))
        self.available_models = { # Add other self-hosted models as needed
            'llama3.1:8b':'llama3.1:8b-instruct-fp16',
            'llama3.2:1b':'llama3.2:1b-instruct-fp16',
            'llama3.2:3b':'llama3.2:3b-instruct-fp16',
            'deepseek-r1:14b':'deepseek-r1:14b-qwen-distill-fp16',
            'qwen2.5:14b':'qwen2.5:14b-instruct-fp16',
            'qwen2.5:7b':'qwen2.5:7b-instruct-fp16',
        }
        self.temperature = 0.1  # Default temperature

    def __repr__(self):
        return f"<OllamaConfig(base_url={self.base_url})>"

    def get_config(self):
        """Returns the Ollama configuration as a dictionary."""
        return {
            "base_url": self.base_url,
            "available_models": list(self.available_models.keys()),
            "temperature": self.temperature,
        }
