import asyncio
from injector import inject
import logging
import requests
from os import environ, getenv
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_ollama import ChatOllama
from statistics import mean
from ragas.llms import LangchainLLMWrapper
from ragas import evaluate as ragas_eval
from ragas import metrics, EvaluationDataset
from time import sleep

from ai_ta_backend.service.retrieval_service import RetrievalService


class CropWizardConfig:
    def __init__(self):
        self.answer_endpoint = str(
            getenv("EVALUATION_ANSWER_URL")
        )  # API endpoint to retrieve answers
        self.cropwiz_api_key = str(
            getenv("EVALUATION_UIUC_CHAT_API_KEY")
        )  # API key to access UIUC.chat
        # self.db_version = "cropwizard-1.5"  # CropWizard version
        self.cw_groups = ["All Documents"]  # Subset of documents
        self.token_limit = 128000  # Token Limit
        # self.model = "gpt-4o-mini"  # Default model
        # self.temperature = 0.1  # Default temperature
        self.cropwiz_sys_prompt = """You are a helpful assistant. Follow the user's instructions carefully. Respond using markdown. If the question is related to farming in ANY WAY then always answer the question even if the documents are not helpful. Give the users the most helpful response possible as best you can. Remember this: always give a reasonable answer to the users question and never refuse to answer even if you have little information to go on; give an array of helpful answers if necessary. ALWAYS respond with something helpful to answer their questions and propose solutions. ALWAYS provide citations, but ONLY when relevant. 
            If the user asks an introductory question or greeting along the lines of "hello" or "what can you do?" or "What's in here?" or "what is CropWizard?" or similar, then please respond with a warm welcome to CropWizard, the AI farm assistant chatbot. Tell them that you can answer questions using the entire knowledge base of Extension plus a growing list of open-access research publications. Whether you need information on crop management, pest control, or any other farming-related topic, feel free to ask!
            When the provided documents don't contain the answer, say in bold italic text "The CropWizard database doesn't have anything covering this exact question, but here's what I know from my general world knowledge." Always refer to the provided documents as "the CropWizard database" and use bold italics when giving this disclaimer."""

    def get_config(self):
        """Returns the configuration as a dictionary."""
        return {
            "answer_endpoint": self.answer_endpoint,
            "cropwiz_api_key": self.cropwiz_api_key,
            "cw_groups": self.cw_groups,
            "token_limit": self.token_limit,
            "cropwiz_sys_prompt": self.cropwiz_sys_prompt,
        }


class LangchainConfig:
    def __init__(self):
        environ["LANGCHAIN_TRACING_V2"] = "true"
        environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
        environ["LANGCHAIN_API_KEY"] = str(getenv("EVALUATION_LANGCHAIN_API_KEY"))
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


class OllamaConfig:
    def __init__(self):
        self.base_url = str(getenv("OLLAMA_BASE_URL", str(getenv("OLLAMA_API_URL"))))

    def __repr__(self):
        return f"<self.ollama_config(base_url={self.base_url})>"

    def get_config(self):
        """Returns the Ollama configuration as a dictionary."""
        return {
            "base_url": self.base_url,
        }


class EvaluationService:
    @inject
    def __init__(self, retrieval_service: RetrievalService):
        # Load environment variables
        load_dotenv()

        self.retrieval_service = retrieval_service

        # Initialize CropWizard specific variables
        self.config = CropWizardConfig()

        # Initialize Langchain specific environment variables
        self.langchain_config = LangchainConfig()

        # Initialize LLM specific environment variables
        self.ollama_config = OllamaConfig()

    def get_prompt_tokens(
        self,
        prompt: str,
        course_name: str,
    ) -> list[dict] | str:
        """
        Posts a prompt to CropWizard, and returns the token vector as a JSON.
        Arguments:
        prompt -- A string representing the prompt submitted to CropWizard.

        Returns:
        A dictionary of tokens representing the fragments, retrieved from the submitted prompt.
        """
        groups = self.config.cw_groups
        search_query = prompt
        doc_groups = groups
        top_n = 100

        found_documents = asyncio.run(
            self.retrieval_service.getTopContexts(
                search_query, course_name, doc_groups, top_n
            )
        )

        return found_documents

    def create_test_cases(
        self,
        question_answer_pairs: dict,
        course_name: str,
    ) -> dict:
        """
        Creates a test case dictionary from a question-answer dictionary.

        Args:
            question_answer_pairs (dict): Dictionary with keys representing questions and values representing expert answers.

        Returns:
            test_cases (dict): Dictionary with keys "question", "answer", "retrieved_contexts", and "ground_truth".
        """
        test_cases = {
            "question": [],
            "answer": [],
            "retrieved_contexts": [],
            "ground_truth": [],
        }

        for value in question_answer_pairs:
            sleep(0.25)  # Added sleep to avoid issues on the server side
            test_cases["question"].append(value["question"])
            test_cases["answer"].append(value["answer"])
            test_cases["retrieved_contexts"].append(
                self.get_prompt_tokens(value["question"], course_name)
            )
            test_cases["ground_truth"].append(value)

        return test_cases

    @staticmethod
    def preprocess_test_cases(test_cases: dict) -> dict:
        """
        Extracts text from the "retrieved_contexts" key from a test_cases dictionary.

        Args:
            test_cases (dict): Dictionary with keys "question", "answer", "retrieved_contexts", and "ground_truth".

        Returns:
            dict: Dictionary with keys "question", "answer", "retrieved_contexts", and "ground_truth", where "retrieved_contexts"
            now only contains the contents of its "text" key
        """

        return {
            "question": test_cases["question"],
            "answer": test_cases["answer"],
            "retrieved_contexts": [
                (
                    [entry["text"] for entry in inner_list]
                    if isinstance(inner_list, list)
                    else inner_list
                )
                for inner_list in test_cases["retrieved_contexts"]
                if isinstance(test_cases["retrieved_contexts"], list)
            ],
            "ground_truth": test_cases["ground_truth"],
        }

    @staticmethod
    def create_dataset(data: dict):
        """
        Cleans the input dictionary by removing entries where `retrieved_contexts` is a string instead of a list.

        Args:
            data (dict): Dictionary with keys "question", "answer", "retrieved_contexts", and "ground_truths".

        Returns:
            cleaned_data (dict): Cleaned dictionary with valid entries.
            removed_entries (list): List of dictionaries containing removed entries for review.
        """
        removed_entries = []  # To store removed tuples for review

        # Ensure all lists have the same length
        keys = ["question", "answer", "retrieved_contexts", "ground_truth"]
        assert all(
            len(data[key]) == len(data[keys[0]]) for key in keys
        ), "All lists must have the same length."

        # Iterate over retrieved_contexts and remove invalid entries
        valid_indices = []
        for i, retrieved_context in enumerate(data["retrieved_contexts"]):
            if isinstance(retrieved_context, list):
                valid_indices.append(i)  # Keep valid entries
            elif (
                isinstance(retrieved_context, str)
                and "error" in retrieved_context.lower()
            ):
                # Add invalid entries to removed_entries
                removed_entries.append(
                    (
                        data["question"][i],
                        data["answer"][i],
                        data["retrieved_contexts"][i],
                        data["ground_truth"][i],
                    )
                )

        # Filter the dictionary to keep only valid entries
        cleaned_data = {key: [data[key][i] for i in valid_indices] for key in keys}

        return cleaned_data, removed_entries

    @staticmethod
    def convert_dict_to_list(data: dict) -> list:
        """
        Converts a dictionary with keys 'question', 'answer', 'retrieved_contexts', and 'ground_truth'
        into a list of dictionaries with the desired structure.

        Args:
            data (dict): Input dictionary with keys as lists of matching indexes.

        Returns:
            list: A list of dictionaries following the specified layout.
        """
        dataset = []
        for i in range(len(data["question"])):
            dataset.append(
                {
                    "user_input": data["question"][i],
                    "retrieved_contexts": data["retrieved_contexts"][i],
                    "response": data["answer"][i],
                    "reference": data["ground_truth"][i],
                }
            )
        return dataset

    def get_llm_options(self, judges, model_config, temperature):

        llm_options = {}

        for judge in judges:
            model = model_config[judge]
            provider = model["provider"]
            model_id = model["model_id"]

            if provider == "OpenAI":
                llm_options[judge] = ChatOpenAI(
                    model=model_id,
                    temperature=temperature,
                    api_key=model["api_key"],
                )
            else:
                llm_options[judge] = ChatOllama(
                    model=model_id,
                    base_url=self.ollama_config.base_url,
                    temperature=temperature,
                )

        return llm_options

        llm_options = {
            # OpenAI models
            # Commented out models that could be added in the future
            # "claude-3-7-sonnet": ChatAnthropic(model="claude-3-5-sonnet-latest", temperature=0.1),
            # "command-r-plus": ChatCohere(model="command-r-plus", temperature=0.1),
            # "gemini-2-flash": ChatGoogleGenerativeAI(model="gemini-2.0-flash-001", temperature=0.1),
            # "llama3-70b": ChatNVIDIA(model="meta/llama3-70b-instruct", temperature=0.1),
        }

        return llm_options  # type: ign

    def single_judge_evaluation(
        self,
        question_answer_pairs: dict,
        judge: str,
        course_name: str,
        temperature: float,
        model_config: dict,
        log: bool = True,
    ) -> dict:
        """
        Evaluates RAG performance for a set of question-answer pairs using a specified LLM judge.

        Args:
            question_answer_pairs (dict): A dictionary containing question-answer pairs for evaluation.
            judge (str, optional): A string representing the choice of LLM model to use for evaluation. Defaults to "gpt-4o-mini".
            log (bool, optional): Whether to log errors. Defaults to True.

        Returns:
            dict: A dictionary containing the evaluation results and the path to the markdown report.
        """
        # Initialize report

        # Create test cases and preprocess them
        test_cases = self.create_test_cases(
            question_answer_pairs,
            course_name,
        )
        processed_test_cases = self.preprocess_test_cases(test_cases)
        evaluation_dict, errors = self.create_dataset(processed_test_cases)

        # Log errors
        if errors:
            if log:
                logging.error(f"errors in dataset creation: {errors}")

        # Convert dataset to LangSmith format
        langsmith_ragas_eval = EvaluationDataset.from_list(
            self.convert_dict_to_list(evaluation_dict)
        )

        # Initialize Langchain LLM wrapper
        llm_options = self.get_llm_options([judge], model_config, temperature)

        evaluator_llm = LangchainLLMWrapper(llm_options[judge])

        # Run evaluation
        results = ragas_eval(
            dataset=langsmith_ragas_eval,
            metrics=[
                metrics.ContextPrecision(),
                metrics.ContextRecall(),
                metrics.AnswerRelevancy(),
                metrics.Faithfulness(),
                metrics.FactualCorrectness(),
            ],
            llm=evaluator_llm,
        )

        return {"results": self.process_scores(results.scores)}  # type: ignore

    def multi_judge_evaluation(
        self,
        question_answer_pairs: dict,
        judges: list,
        course_name: str,
        temperature: float,
        model_config: dict,
        log: bool = True,
    ) -> dict:
        """
        Evaluates RAG performance for a set of question-answer pairs using multiple LLM judges.
        This function processes test cases once and evaluates them with each judge in the list.

        Args:
            question_answer_pairs (dict): A dictionary containing question-answer pairs for evaluation.
            judges (list, optional): A list of strings representing the LLM models to use for evaluation.
            log (bool, optional): Whether to log errors. Defaults to True.

        Returns:
            dict: A dictionary containing the evaluation results for all judges and the path to the markdown report.
        """
        # Create test cases and preprocess them - only done once for all judges
        test_cases = self.create_test_cases(question_answer_pairs, course_name)
        processed_test_cases = self.preprocess_test_cases(test_cases)
        evaluation_dict, errors = self.create_dataset(processed_test_cases)

        # Log errors
        if errors:
            if log:
                logging.error(f"errors in dataset creation: {errors}")

        # Convert dataset to LangSmith format - only done once
        langsmith_ragas_eval = EvaluationDataset.from_list(
            self.convert_dict_to_list(evaluation_dict)
        )

        # Initialize Langchain LLM wrapper options
        llm_options = self.get_llm_options(judges, model_config, temperature)

        # Dictionary to store results for each judge
        all_results = {}

        # Process each judge
        for judge_name in judges:
            # Check if the judge model is in the available options
            evaluator_llm = LangchainLLMWrapper(llm_options[judge_name])

            # Run evaluation for this judge
            results = ragas_eval(
                dataset=langsmith_ragas_eval,
                metrics=[
                    metrics.ContextPrecision(),
                    metrics.ContextRecall(),
                    metrics.AnswerRelevancy(),
                    metrics.Faithfulness(),
                    metrics.FactualCorrectness(),
                ],
                llm=evaluator_llm,
                return_executor=False,
            )

            # Store results for this judge
            all_results[judge_name] = self.process_scores(results.scores)  # type: ignore

        return {"results": all_results}

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

    def evaluate(
        self,
        dataset,
        test_judge: list,
        course_name: str,
        temperature: float,
        model_config: dict,
    ) -> dict:

        # llm_options = self.get_llm_options(temperature)

        # if all(item in llm_options for item in test_judge):
        if len(test_judge) == 1:
            result = self.single_judge_evaluation(
                dataset, test_judge[0], course_name, temperature, model_config
            )
            return result
        elif len(test_judge) > 1:
            result = self.multi_judge_evaluation(
                dataset, test_judge, course_name, temperature, model_config
            )
            return result

        return {}
