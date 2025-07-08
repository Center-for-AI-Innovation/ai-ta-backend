from statistics import mean
import uuid
from pathlib import Path
import boto3
from injector import inject
import logging

from ai_ta_backend.database.aws import AWSStorage
from ai_ta_backend.service.evaluation_service.evaluation_processor import (
    EvaluationProcessor,
)

# Import the base class from the other file
import os
import json

from supabase import create_client, Client

from ai_ta_backend.service.posthog_service import PosthogService

class EvaluationService:

    @inject
    def __init__(self, aws: AWSStorage, posthog: PosthogService):
        self.aws = aws
        self.posthog = posthog
        self.bucket_name = os.environ["S3_BUCKET_NAME"]
        url = os.environ["EVALUATION_SUPABASE_URL"]
        key = os.environ["EVALUATION_SUPABASE_API_KEY"]
        self.supabase: Client = create_client(url, key)

    def getEvaluationResults(self):
        results = self.supabase.table("evaluation_scores").select("*").execute()
        return results.data

    def evaluate(
        self,
        input: dict,
        num_processes: int,
        judge_model: str,
        subject_model: str,
        judge_temperature: float = 0.0,
        openai_api_base: str = "None",
    ) -> dict:

        properties = {
            "judge_model": judge_model,
            "subject_model": subject_model,
        }

        self.posthog.capture(event_name="evaluation_started", properties=properties)

        s3_client = boto3.client(
            "s3",
            # endpoint_url=os.environ.get(
            #     "MINIO_API_URL"
            # ),  # for Self hosted MinIO bucket
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )

        return s3_client.list_buckets()

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

        processed_scores = self.process_scores(scores)

        url = os.environ["EVALUATION_SUPABASE_URL"]
        key = os.environ["EVALUATION_SUPABASE_API_KEY"]
        supabase: Client = create_client(url, key)

        scores_entry = {
            "accuracy": processed_scores["accuracy"],
            "completeness": processed_scores["completeness"],
            "parsimony": processed_scores["parsimony"],
            "relevance": processed_scores["relevance"],
            "judge_model": judge_model,
            "subject_model": subject_model,
        }

        insert_result = (
            supabase.table("evaluation_scores").insert(scores_entry).execute()
        )

        result_id = insert_result.data[0]["id"]

        # self.aws.upload_file(input_path, self.bucket_name, f"{result_id}_input.json")
        # self.aws.upload_file(output_path, self.bucket_name, f"{result_id}_output.jsonl")

        os.remove(input_path)
        os.remove(output_path)

        self.posthog.capture(event_name="evaluation_complete", properties=properties)
        return {"scores": scores_entry, "results": results_list}

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
