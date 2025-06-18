from os import getenv
from statistics import mean
import uuid
from pathlib import Path

from ai_ta_backend.service.evaluation_service.chat_models.openai_api import OpenAIAPI
from ai_ta_backend.service.evaluation_service.chat_models.vllm_api import VLLMClient
from pydantic import BaseModel

# Import the base class from the other file
from ai_ta_backend.service.evaluation_service.chat_models.multi_processor import (
    BaseMultiProcessor,
)
from typing import Dict, Any, Optional
import os
import time
import json
import re


class Score(BaseModel):
    """
    Score for the evaluation.
    """

    accuracy: int
    relevance: int
    completeness: int
    parsimony: int

    def to_json(self):
        return {
            "accuracy": self.accuracy,
            "relevance": self.relevance,
            "completeness": self.completeness,
            "parsimony": self.parsimony,
        }


class EvaluationProcessor(BaseMultiProcessor):
    """
    Evaluation Processor for specific model.
    It inherits all the multiprocessing and file handling logic from BaseMultiProcessor.
    """

    def __init__(
        self,
        judge_model: str,
        judge_temperature: float = 0.0,
        subject_model: str = "gpt-4o-mini",
        max_retries: int = 5,
        retry_delay: int = 5,
        openai_api_base: str = "None",
        **kwargs,
    ):
        """
        Initializes the evaluation processor.

        Args:
            model_name (str): The model identifier for the API call.
            max_retries (int): Max retries for a failed API call.
            retry_delay (int): Seconds to wait between retries.
            **kwargs: Arguments for the base class (input_path, output_path, etc.).
        """
        # Call the parent constructor with shared arguments
        super().__init__(**kwargs)

        # --- Task-Specific Attributes ---
        self.judge_model = judge_model
        self.judge_model_name_key = judge_model.split("/")[
            -1
        ]  # This is the key we'll use in the JSON output
        self.judge_temperature = judge_temperature
        self.subject_model = subject_model
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.openai_api_base = openai_api_base

    def _is_item_processed(self, item: Dict[str, Any]) -> bool:
        """
        This is YOUR custom logic to check if an item is complete.
        """
        # The check is based on the specific key for this task.
        return "score" in item and item["score"] not in [-1, None]

    def _prepare_prompt(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        This is YOUR custom logic for preparing the prompt.
        """
        user_query = item["question"]
        model_response = item[self.subject_model]
        gold_answer = item["self_answer"]

        score_criteria = """
**Accuracy Definition**: Accuracy evaluates whether the agricultural facts, species identification, diagnostic conclusions, and management recommendations provided by the model align with the expert's response. Emphasis is placed on: 1. Correctness of professional terminology (e.g., precise naming of diseases, pests, or invasive species). 2. Accuracy of key details (e.g., descriptions of lesion characteristics, pest behaviors, or plant symptoms). 3. Logical coherence in describing causal relationships (e.g., disease transmission pathways, pest infestation mechanisms). 4. Appropriateness and effectiveness of the proposed management strategies or interventions.
- 100 points: All agricultural facts, terminologies, diagnostic conclusions, and management recommendations are completely correct, comprehensive, and fully aligned with expert consensus.
- 75 points: Minor inaccuracies or omissions in terminology, descriptive details, or management advice exist, but the core diagnostic conclusions and recommended management practices remain accurate and effective.
- 50 points: Noticeable factual errors, misidentifications (species/disease/pests), or suboptimal management suggestions. However, the response still demonstrates partial accuracy or correctness in key aspects.
- 25 points: Major inaccuracies, such as significant confusion between diseases, pests, or plants, flawed causal logic, or incorrect management practices that could lead to ineffective or detrimental outcomes.
- 0 points: Entirely incorrect, scientifically invalid, or significantly misleading claims without any alignment with expert consensus.

**Relevance Definition**: This measures how closely the model’s response matches the scope and focus of expert answers, ensuring it stays on-topic and avoids tangential information. Responses that digress into unrelated agricultural knowledge or overlook critical points tied to the user’s query are considered less relevant.
- 100 points: The response perfectly mirrors the expert answer and directly addresses the query, using precise terminology and only including question-relevant information.
- 75 points: The answer is mostly aligned with the expert response and user query, with only minor tangents or slight omissions in details.
- 50 points: The response contains noticeable deviations or omissions compared to the expert answer, with several off-topic or less relevant points.
- 25 points: Significant misalignment with the expert answer and the query is evident. The response includes major irrelevant or incorrect content.
- 0 points: The answer is entirely off-topic, failing to reflect the expert response or address the user query.

**Completeness Definition**: Whether the model’s answer covers all key information points mentioned in expert answers to fully address the user’s inquiry. If the model omits critical steps or precautions highlighted in expert answers, it is deemed incomplete. Emphasis is placed on: 1. Professional Terminology: Uses precise terms (e.g., names of diseases, pests, invasive species). 2. Key Details: Includes comprehensive descriptions (e.g., lesion characteristics, pest behaviors, plant symptoms). 3. Logical Causal Relationships: Fully explains connections (e.g., disease transmission, pest infestation mechanisms). 4. Management Recommendations: Details all necessary strategies and precautions.
- 100 points: Covers all key points from the gold answer
- 75 points: Misses 1-2 minor details but addresses core aspects. 
- 50 points: The response contains noticeable deviations or omissions compared to the expert answer.
- 25 points: Omits a major component (e.g.,management recommendations).
- 0 points: Fails to address any key elements of the query.

**Parsimony Definition**: Whether the answer provides actionable guidance that directly addresses the user’s core needs, delivering a concise and unambiguous conclusion and specific recommendations without extraneous technical details. The response should adhere to Occam’s Razor by avoiding unnecessary complexity and focusing only on what is essential for understanding whether intervention is necessary and what exact steps (if any) need to be taken.
- 100 points: The answer is succinct, clear, and directly addresses the user’s concerns. It offers straightforward, practical guidance that is fully aligned with the visible evidence without any unnecessary details. It embodies the principle of Occam’s Razor.
- 75 points: The answer is generally concise and practical, offering useful advice. However, it may include some extraneous details or slight ambiguity that only minimally detracts from its overall clarity and directness.
- 50 points: The answer contains relevant information but is overly theoretical or detailed. Extra technical content obscures the key actionable recommendations, making the response less concise and direct. 
- 25 points: The answer is largely indirect or abstract, with a significant amount of unnecessary information. The lack of clarity in actionable guidance leaves the user uncertain about whether any intervention is needed.
- 0 points: The answer fails to provide practical or actionable recommendations and is cluttered with superfluous details, completely missing the concise, straightforward approach required by Occam’s Razor.
"""

        prompt = f"""
You are now required to rate a model's response to an agriculture-related question. \
Based on the gold answer, and the user's question, you need to score the model's answer according to the following four scoring criteria.

<User Query>{user_query}</User Query>

<Gold Answer>{gold_answer}</Gold Answer>

<Model Response>{model_response}</Model Response>

<Score Criteria>{score_criteria}</Score Criteria>

Please only output the scores without any other content. You should output JSON with four key, accuracy, relevance, completeness, parsimony. The example is shown below:
{{ "accuracy": ..., "relevance": ..., "completeness": ..., "parsimony": ... }}"""
        return {
            "prompt": prompt,
            "gold_answer": gold_answer,
            "model_response": model_response,
        }

    def _parse_score(self, score_text: str) -> Dict[str, int]:
        """Parses the score from the model's text output."""
        try:
            json_data = json.loads(score_text)
            return json_data
        except json.JSONDecodeError:
            if isinstance(score_text, dict):
                return score_text

            pattern = r"```json\s*(\{[\s\S]*?\})\s*```"
            match = re.search(pattern, score_text)

            if match:
                json_str = match.group(1)
            else:
                # Look for JSON object in the string
                start = score_text.find("{")
                end = score_text.rfind("}") + 1
                if start >= 0 and end > start:
                    json_str = score_text[start:end]
                else:
                    json_str = score_text.strip()

            json_data = json.loads(json_str)
            return json_data

    def _process_item_logic(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        This is YOUR core logic for processing one item, including retries.
        """
        prompt = self._prepare_prompt(item)
        item_id = item.get("id", "unknown_id")

        for attempt in range(self.max_retries):
            try:
                if self.judge_model.startswith("Qwen/Qwen3"):
                    client = VLLMClient(
                        model_name=self.judge_model,
                        openai_api_base=self.openai_api_base,
                    )
                    response, reasoning, info, history = client.reasoning_chat(
                        prompt=prompt["prompt"],
                        temperature=self.judge_temperature,
                        max_tokens=8192,
                    )
                    response = self._parse_score(response)
                elif self.judge_model.startswith("gpt"):
                    openai_api_key = str(getenv("EVALUATION_OPENAI_API_KEY"))
                    client = OpenAIAPI(openai_api_key, model_name=self.judge_model)
                    response, info, history = client.chat(
                        prompt=prompt["prompt"],
                        temperature=self.judge_temperature,
                        text_format=Score,
                    )

                    response = response.to_json() # type: ignore

                else:
                    raise ValueError(f"Unsupported judge model: {self.judge_model}")
                assert (
                    "accuracy" in response
                    and "relevance" in response
                    and "completeness" in response
                    and "parsimony" in response
                ), "Score should contain all four keys"

                # Success! Add results to the item and return it.
                new_item = {
                    "id": item_id,
                    "prompt": prompt["prompt"],
                    "gold_answer": prompt["gold_answer"],
                    "subject_model": self.subject_model,
                    "model_response": prompt["model_response"],
                    "judge_model": self.judge_model_name_key,
                    "score": response,
                    "info": info,
                    "history": history,
                }

                if self.judge_model.startswith("Qwen/Qwen3"):
                    new_item["thinking"] = reasoning

                return new_item

            except Exception as e:
                print(
                    f"Error on item {item_id} (attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)

        # All retries failed, return None to signify failure.
        return None


class EvaluationService:
    def evaluate(
        self,
        input: dict,
        num_processes: int,
        judge_model: str,
        subject_model: str,
        judge_temperature: float = 0.0,
        openai_api_base: str = "None",
    ) -> dict:

        unique_filename = str(uuid.uuid4())
        base_path = "ai_ta_backend/service/evaluation_service/processing"
        input_path = f"{base_path}/{unique_filename}.json"
        output_path = f"{base_path}/{unique_filename}.jsonl"

        Path(f"{base_path}").mkdir(parents=True, exist_ok=True)

        with open(input_path, "w") as input_file:
            input_file.write(json.dumps(input))

        processor = EvaluationProcessor(
            input_path=input_path,
            output_path=output_path,
            num_processes=num_processes,
            judge_model=judge_model,
            judge_temperature=judge_temperature,
            subject_model=subject_model,
            openai_api_base=openai_api_base,
        )

        processor.run()

        with open(output_path, "r") as output_file:
            results_list = list(output_file)

        scores = []
        results = []

        for result_str in results_list:
            result = json.loads(result_str)
            results.append(result)
            scores.append(result["score"])

        os.remove(input_path)
        os.remove(output_path)

        processed_scores = self.process_scores(scores)

        return {
            "scores": processed_scores,
            "results": results
        }

    @staticmethod
    def process_scores(scores: list[dict]) -> dict:
        required_metrics = scores[0].keys()
        processed_scores = {}
        for required_metric in required_metrics:
            metric_scores = []
            for score in scores:
                metric_scores.append(score[required_metric])
            processed_scores[required_metric] = mean(metric_scores)

        return processed_scores
