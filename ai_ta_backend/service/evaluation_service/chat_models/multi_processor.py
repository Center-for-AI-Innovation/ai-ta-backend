import json
import multiprocessing
import os
import shutil
from abc import ABC, abstractmethod
from tqdm import tqdm
from typing import List, Dict, Any, Set, Optional


class BaseMultiProcessor(ABC):
    """
    An abstract base class for processing data in parallel.

    This class provides a reusable template for tasks that involve:
    1. Reading items from a source file.
    2. Processing each item using multiple processes.
    3. Supporting resumable progress (checkpointing).
    4. Writing results to an output file safely.
    5. Cleaning up the final output.

    To use this, create a subclass and implement the abstract methods:
    - _process_item_logic
    - _is_item_processed
    """

    def __init__(
        self, input_path: str, output_path: str, num_processes: Optional[int] = None
    ):
        """
        Initializes the base processor.

        Args:
            input_path (str): Path to the source data file. Must be a JSON or JSONL file.
            output_path (str): Path for the output JSONL file.
            num_processes (Optional[int]): Number of worker processes. Defaults to CPU count.
        """
        self.input_path = input_path
        self.output_path = output_path
        self.num_processes = (
            num_processes if num_processes is not None else os.cpu_count()
        )
        print(f"Initializing processor with {self.num_processes} processes.")

    # --- Methods to be implemented by subclasses ---

    @abstractmethod
    def _process_item_logic(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        The core logic for processing a single item.
        This is where you call your model, run calculations, etc.

        Args:
            item (Dict[str, Any]): The single data item to process.

        Returns:
            Optional[Dict[str, Any]]: The processed item with results.
                                      Return None or raise an exception on failure.
        """
        pass

    @abstractmethod
    def _is_item_processed(self, item: Dict[str, Any]) -> bool:
        """
        Checks if an item from the output file is considered successfully processed.

        Args:
            item (Dict[str, Any]): An item loaded from a line in the output file.

        Returns:
            bool: True if the item is complete, False otherwise.
        """
        pass

    # --- Core framework methods (usually not overridden) ---

    def _load_source_data(self) -> List[Dict[str, Any]]:
        """Loads data from the source JSON file."""
        print(f"Loading source data from {self.input_path}...")

        if self.input_path.endswith(".jsonl"):
            with open(self.input_path, "r", encoding="utf-8") as f:
                return [json.loads(line) for line in f]
        elif self.input_path.endswith(".json"):
            with open(self.input_path, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            raise ValueError(f"Unsupported file extension: {self.input_path}")

    def _get_processed_ids(self) -> Set[str]:
        """Gets the set of IDs from already processed items for checkpointing."""
        processed_ids: Set[str] = set()
        if not os.path.exists(self.output_path):
            return processed_ids

        with open(self.output_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    processed_item = json.loads(line)
                    if "id" in processed_item and self._is_item_processed(
                        processed_item
                    ):
                        processed_ids.add(processed_item["id"])
                except (json.JSONDecodeError, KeyError):
                    continue
        return processed_ids

    def _worker_task(self, args: tuple):
        """
        A wrapper function executed by each worker process.
        It calls the user-defined logic and handles file writing.
        """
        item, lock = args
        try:
            # The core custom logic is called here
            processed_item = self._process_item_logic(item)

            if processed_item:
                # Use a lock for safe concurrent writing
                with lock:
                    with open(self.output_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(processed_item, ensure_ascii=False) + "\n")
        except Exception as e:
            item_id = item.get("id", "unknown_id")
            print(
                f"An unhandled exception occurred while processing item {item_id}: {e}"
            )
            # Optionally write failed items to a separate log
            # with lock: ...

    def run(self):
        """
        Main method to orchestrate the entire data processing workflow.
        """
        source_data = self._load_source_data()
        processed_ids = self._get_processed_ids()

        items_to_process = [
            item for item in source_data if item.get("id") not in processed_ids
        ]

        if not items_to_process:
            print("All items have already been processed.")
        else:
            print(
                f"Total items: {len(source_data)} | Already processed: {len(processed_ids)} | To process: {len(items_to_process)}"
            )

            manager = multiprocessing.Manager()
            lock = manager.Lock()

            with multiprocessing.Pool(processes=self.num_processes) as pool:
                args_list = [(item, lock) for item in items_to_process]

                for _ in tqdm(
                    pool.imap_unordered(self._worker_task, args_list),
                    total=len(args_list),
                    desc="Processing items",
                ):
                    pass

        print("Initial processing run completed.")
        self._cleanup_and_summarize_output(len(source_data))

    def _cleanup_and_summarize_output(self, total_source_count: int):
        """Memory-efficiently cleans the output file and prints a summary."""
        if not os.path.exists(self.output_path):
            print("Output file not found. Nothing to clean.")
            return

        temp_path = self.output_path + ".tmp"
        valid_items_by_id: Dict[str, Dict[str, Any]] = {}

        with open(self.output_path, "r", encoding="utf-8") as f_in:
            for line in f_in:
                try:
                    item = json.loads(line)
                    if "id" in item and self._is_item_processed(item):
                        valid_items_by_id[item["id"]] = item
                except (json.JSONDecodeError, KeyError):
                    continue

        with open(temp_path, "w", encoding="utf-8") as f_out:
            for item in valid_items_by_id.values():
                f_out.write(json.dumps(item, ensure_ascii=False) + "\n")

        shutil.move(temp_path, self.output_path)

        num_successful = len(valid_items_by_id)
        print(
            f"\n--- Processing Summary ---\nCleanup complete. Final valid items: {num_successful}\nRemaining items to process: {total_source_count - num_successful}\n--------------------------\n"
        )
