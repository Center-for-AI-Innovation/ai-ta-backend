"""Pipeline state logging and management."""
import json
import os
import logging
from pathlib import Path
from datetime import datetime


class ProcessStateLogger:
    """
    Manages a JSON log file to track the current state of the pipeline.
    States: RUNNING, COMPLETED, FAILED
    This allows cron jobs to check if the script should run.
    """
    
    def __init__(self, log_file_path: str):
        self.log_file_path = Path(log_file_path)
        self._log = logging.getLogger("pipeline.state")
        self.log_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    def _read_state(self) -> dict:
        """Read current state from file, return empty dict if doesn't exist."""
        if not self.log_file_path.exists():
            return {}
        try:
            with open(self.log_file_path, "r") as f:
                return json.load(f)
        except Exception as e:
            self._log.warning("Failed to read state file: %s", e)
            return {}
    
    def _write_state(self, state_data: dict) -> None:
        """Write state to file atomically."""
        try:
            # Write to temp file first, then rename (atomic on POSIX)
            temp_path = self.log_file_path.with_suffix(".tmp")
            with open(temp_path, "w") as f:
                json.dump(state_data, f, indent=2, default=str)
            temp_path.replace(self.log_file_path)
        except Exception as e:
            self._log.error("Failed to write state file: %s", e)
    
    def mark_running(self, pid: int) -> None:
        """Mark pipeline as running."""
        state = {
            "status": "RUNNING",
            "pid": pid,
            "start_time": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "error_message": None
        }
        self._write_state(state)
        self._log.info("Pipeline state: RUNNING (PID: %d)", pid)
    
    def mark_completed(self, processed_count: int, metrics: dict | None = None) -> None:
        """Mark pipeline as completed successfully."""
        state = self._read_state()
        state.update({
            "status": "COMPLETED",
            "end_time": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "processed_count": processed_count,
            "error_message": None
        })
        if metrics is not None:
            state["metrics"] = metrics
        self._write_state(state)
        self._log.info("Pipeline state: COMPLETED (processed: %d)", processed_count)
    
    def mark_failed(self, error_message: str) -> None:
        """Mark pipeline as failed with error message and save backup."""
        state = self._read_state()
        state.update({
            "status": "FAILED",
            "end_time": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "error_message": error_message
        })
        self._write_state(state)
        
        # Save a timestamped backup of the failed state
        try:
            backup_dir = self.log_file_path.parent
            backup_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = backup_dir / f"pipeline_state.failed.{timestamp}.json"
            with open(backup_file, "w") as f:
                json.dump(state, f, indent=2, default=str)
            self._log.info("Failed state backed up to: %s", backup_file)
        except Exception as e:
            self._log.warning("Failed to save backup state file: %s", e)
        
        self._log.error("Pipeline state: FAILED - %s", error_message)
    
    def get_state(self) -> dict:
        """Get current state for external inspection."""
        return self._read_state()
    
    def is_running(self) -> bool:
        """Check if pipeline is currently running."""
        state = self._read_state()
        if state.get("status") != "RUNNING":
            return False
        
        # Check if the PID is still alive
        pid = state.get("pid")
        if pid:
            try:
                # Send signal 0 to check if process exists (doesn't actually send a signal)
                os.kill(pid, 0)
                return True
            except OSError:
                return False
        
        return True
