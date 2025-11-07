#!/usr/bin/env python3
"""
Utility script to check the current state of the pipeline.
Can be used by cron jobs to decide whether to run the pipeline.

Exit codes:
  0 - Pipeline is not running (safe to start)
  1 - Pipeline is currently running
  2 - Pipeline failed (check log for details)
  3 - Error reading state file
"""

import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def main():
    state_log_path = os.getenv("PIPELINE_STATE_LOG", "pipeline_state/pipeline_state.json")
    state_file = Path(state_log_path)
    
    if not state_file.exists():
        print(f"No state file found at {state_log_path}")
        print("Status: READY (never run or state file deleted)")
        sys.exit(0)
    
    try:
        with open(state_file, "r") as f:
            state = json.load(f)
    except Exception as e:
        print(f"Error reading state file: {e}")
        sys.exit(3)
    
    status = state.get("status", "UNKNOWN")
    pid = state.get("pid")
    start_time = state.get("start_time")
    end_time = state.get("end_time")
    error_message = state.get("error_message")
    processed_count = state.get("processed_count", 0)
    
    print(f"Pipeline State: {status}")
    print(f"State file: {state_log_path}")
    
    if status == "RUNNING":
        print(f"PID: {pid}")
        print(f"Started: {start_time}")
        
        # Check if process is actually running
        if pid:
            try:
                os.kill(pid, 0)
                print("Process is alive")
                sys.exit(1)  # Running
            except (OSError, ProcessLookupError):
                print("WARNING: Process is not running (crashed or killed)")
                print("You may want to delete the state file and restart")
                sys.exit(2)  # Failed (crashed)
        else:
            sys.exit(1)
    
    elif status == "COMPLETED":
        print(f"Started: {start_time}")
        print(f"Completed: {end_time}")
        print(f"Processed: {processed_count} items")
        sys.exit(0)  # Safe to run again
    
    elif status == "FAILED":
        print(f"Started: {start_time}")
        print(f"Failed: {end_time}")
        if error_message:
            print(f"Error: {error_message}")
        sys.exit(2)  # Failed
    
    else:
        print(f"Unknown status: {status}")
        sys.exit(3)

if __name__ == "__main__":
    main()
