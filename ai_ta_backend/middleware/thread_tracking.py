from flask import g, request
import threading
import time
import logging

logger = logging.getLogger(__name__)


def track_thread_usage():
    """Middleware to track thread count before and after requests."""
    def before_request():
        g.start_time = time.time()
        g.start_thread_count = threading.active_count()
    
    def after_request(response):
        if hasattr(g, 'start_time'):
            duration = time.time() - g.start_time
            end_thread_count = threading.active_count()
            thread_delta = end_thread_count - g.start_thread_count
            
            # Log if thread count increased significantly
            if thread_delta > 5 or end_thread_count > 100:
                logger.warning(
                    f"High thread usage - Path: {request.path} | "
                    f"Duration: {duration:.2f}s | "
                    f"Threads: {g.start_thread_count} -> {end_thread_count} "
                    f"(delta: {thread_delta})"
                )
            
            # Add headers for debugging (optional)
            response.headers['X-Thread-Count'] = str(end_thread_count)
            response.headers['X-Thread-Delta'] = str(thread_delta)
            response.headers['X-Response-Time'] = f"{duration:.3f}"
        
        return response
    
    return before_request, after_request