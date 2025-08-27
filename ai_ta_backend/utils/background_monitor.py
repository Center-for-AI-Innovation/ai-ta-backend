import threading
import time
import logging
from ai_ta_backend.utils.thread_monitor import log_thread_metrics

# Set up a specific logger for thread monitoring
thread_logger = logging.getLogger('thread_monitor')
thread_logger.setLevel(logging.INFO)

# Create a handler that writes to a specific file
handler = logging.FileHandler('/tmp/thread_monitor.log')
handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
thread_logger.addHandler(handler)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
thread_logger.addHandler(console_handler)


def start_background_monitor(interval_seconds=60):
    """Start a background thread that logs thread metrics periodically."""
    def monitor_loop():
        while True:
            try:
                log_thread_metrics(thread_logger)
            except Exception as e:
                thread_logger.error(f"Error in thread monitor: {e}")
            time.sleep(interval_seconds)
    
    monitor_thread = threading.Thread(target=monitor_loop, daemon=True, name="ThreadMonitor")
    monitor_thread.start()
    thread_logger.info("Background thread monitor started")