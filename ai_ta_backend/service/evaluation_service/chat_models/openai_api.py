from openai import OpenAI
import base64
import math
from PIL import Image
import json


def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")


class OpenAIAPI:
    """
    OpenAI API class. Supports single-turn conversation.
    """

    def __init__(self, openai_api_key, model_name="gpt-4o-mini"):
        self.client = OpenAI(api_key=openai_api_key)
        self.model_name = model_name

        self.pricing = {
            "gpt-4o": {
                "input": 2.50 / 1_000_000,  # $2.50 per 1M input tokens
                "output": 10.00 / 1_000_000,  # $10.00 per 1M output tokens
            },
            "gpt-4o-mini": {
                "input": 0.150 / 1_000_000,  # $0.150 per 1M input tokens
                "output": 0.600 / 1_000_000,  # $0.600 per 1M output tokens
            },
            "gpt-3.5-turbo-0125": {
                "input": 0.50 / 1_000_000,  # $0.50 per 1M tokens
                "output": 1.50 / 1_000_000,  # $1.50 per 1M tokens
            },
            "gpt-4.1": {
                "input": 2.00 / 1_000_000,  # $2.00 per 1M tokens
                "output": 8.00 / 1_000_000,  # $8.00 per 1M tokens
            },
            "gpt-4.1-mini": {
                "input": 0.400 / 1_000_000,  # $0.400 per 1M tokens
                "output": 1.600 / 1_000_000,  # $1.600 per 1M tokens
            },
            "gpt-4.1-nano": {
                "input": 0.100 / 1_000_000,  # $0.100 per 1M tokens
                "output": 0.400 / 1_000_000,  # $0.400 per 1M tokens
            },
        }

    def chat(
        self,
        prompt,
        system_prompt=None,
        image_paths=[],
        text_format=None,
        temperature=1.0,
    ):
        """
        Chat with the OpenAI API.
        Args:
            prompt (str): The prompt to send to the API.
            system_prompt (str, optional): The system prompt to send to the API. Defaults to None.
            image_paths (list, optional): The image paths to send to the API. Defaults to [].
            text_format (BaseModel, optional): The text format to send to the API. Defaults to None.
            temperature (float, optional): The temperature to send to the API. Defaults to 1.0.

        Returns:
            str: The response from the API. If text_format is provided, the response will be parsed into the text format.
            dict: The information of the conversation, including the model_name name, tokens and cost.
            list: The conversation history.
        """
        self.messages = []
        self.record = []  # record the conversation history, only store image paths

        # Add system prompt
        if system_prompt:
            self.messages.append({"role": "developer", "content": system_prompt})
            self.record.append({"role": "developer", "content": system_prompt})
        # Add images
        if image_paths:
            # single and multiple images
            content = [{"type": "input_text", "text": prompt}]
            record_content = [{"type": "input_text", "text": prompt}]

            if isinstance(image_paths, str):
                image_paths = [image_paths]
            assert isinstance(
                image_paths, list
            ), "image_paths must be a list or a string"
            for image_path in image_paths:
                base64_image = encode_image(image_path)
                content.append(
                    {
                        "type": "input_image",
                        "image_url": f"data:image/jpeg;base64,{base64_image}",
                    }
                )
                record_content.append({"type": "input_image", "image_url": image_path})
        else:
            content = prompt
            record_content = prompt
        self.messages.append({"role": "user", "content": content})
        self.record.append({"role": "user", "content": record_content})

        if text_format:
            print(dir(self.client))
            response = self.client.responses.parse(
                model=self.model_name,
                input=self.messages,
                text_format=text_format,
                temperature=temperature,
            )
            self.messages.append({"role": "assistant", "content": response.output_text})

            # calculate the tokens and cost
            input_vision_tokens = self.calculate_vision_tokens(image_paths)
            input_text_tokens = response.usage.input_tokens - input_vision_tokens
            output_text_tokens = response.usage.output_tokens

            info = self.info(input_vision_tokens, input_text_tokens, output_text_tokens)

            return response.output_parsed, info, self.record
        else:
            response = self.client.responses.parse(
                model=self.model_name,
                input=self.messages,
                temperature=temperature,
            )

            self.messages.append({"role": "assistant", "content": response.output_text})
            self.record.append({"role": "assistant", "content": response.output_text})

            # calculate the tokens and cost
            input_vision_tokens = self.calculate_vision_tokens(image_paths)
            input_text_tokens = response.usage.input_tokens - input_vision_tokens
            output_text_tokens = response.usage.output_tokens

            info = self.info(input_vision_tokens, input_text_tokens, output_text_tokens)

            return response.output_text, info, self.record

    def calculate_vision_tokens(self, image_paths, detail="high"):
        """
        Calculate the tokens for the vision model_name.
        """
        all_tokens = 0
        if not image_paths:
            return 0

        if isinstance(image_paths, str):
            image_paths = [image_paths]
        assert isinstance(image_paths, list), "image_paths must be a list or a string"

        # Get the image dimensions
        for image_path in image_paths:
            with Image.open(image_path) as img:
                width, height = img.size

            if detail == "low":
                return 85

            # Scale down to fit within a 2048 x 2048 square if necessary
            if width > 2048 or height > 2048:
                max_size = 2048
                aspect_ratio = width / height
                if aspect_ratio > 1:
                    width = max_size
                    height = int(max_size / aspect_ratio)
                else:
                    height = max_size
                    width = int(max_size * aspect_ratio)

            # Resize such that the shortest side is 768px if the original dimensions exceed 768px
            min_size = 768
            aspect_ratio = width / height
            if width > min_size and height > min_size:
                if aspect_ratio > 1:
                    height = min_size
                    width = int(min_size * aspect_ratio)
                else:
                    width = min_size
                    height = int(min_size / aspect_ratio)

            tiles_width = math.ceil(width / 512)
            tiles_height = math.ceil(height / 512)
            all_tokens += 170 * (tiles_width * tiles_height)

        return all_tokens

    def info(self, input_vision_tokens, input_text_tokens, output_text_tokens):
        """
        Get the information of the conversation.
        """

        model_name = self.model_name

        model_name_pricing = self.pricing.get(model_name)
        if not model_name_pricing:
            raise ValueError(
                f"Pricing information for model_name '{model_name}' not found."
            )

        input_cost = (input_text_tokens + input_vision_tokens) * model_name_pricing[
            "input"
        ]
        output_cost = (output_text_tokens) * model_name_pricing["output"]

        total_cost = input_cost + output_cost
        info = {
            "model_name": model_name,
            "input_text_tokens": input_text_tokens,
            "input_image_tokens": input_vision_tokens,
            "output_text_tokens": output_text_tokens,
            "input_cost": input_cost,
            "output_cost": output_cost,
            "total_cost": total_cost,
        }

        return info
