from openai import OpenAI
import re

class VLLMClient:
    def __init__(self,
                 model_name="Qwen/Qwen3-32B",
                 openai_api_key="token-abc123",
                 openai_api_base="None"):
        self.client = OpenAI(
            api_key=openai_api_key,
            base_url=openai_api_base,
        )
        self.model_name = model_name
        self.messages = []
        self.record = []

    def reasoning_chat(self, prompt, temperature=0.6, max_tokens=10000):
        self.messages.append({"role": "user", "content": prompt})
        self.record.append({"role": "user", "content": prompt})

        create_kwargs = {
            "model": self.model_name,
            "messages": self.messages,
        }
        common_args = {
            "max_completion_tokens": max_tokens,
            "temperature": temperature,
        }

        resp = self.client.chat.completions.create(
            **create_kwargs,
            **common_args
        )
        content = resp.choices[0].message.content
        reasoning = resp.choices[0].message.reasoning_content

        self.messages.append({"role": "assistant", "content": content})
        self.record.append({"role": "assistant", "content": content, "reasoning": reasoning})

        info = {
            "model_name": self.model_name,
            "total_cost": 0
        }
        
        return content, reasoning, info, self.record