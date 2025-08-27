import threading
import traceback
import psutil
import os
from datetime import datetime


def get_thread_info():
    """Get detailed information about all threads in the current process."""
    current_process = psutil.Process(os.getpid())
    
    # Get system-level thread info
    system_info = {
        "timestamp": datetime.utcnow().isoformat(),
        "pid": current_process.pid,
        "num_threads": current_process.num_threads(),
        "memory_percent": current_process.memory_percent(),
        "memory_info_mb": current_process.memory_info().rss / 1024 / 1024,
        "cpu_percent": current_process.cpu_percent(interval=0.1),
    }
    
    # Get Python thread details
    threads = []
    for thread in threading.enumerate():
        thread_info = {
            "name": thread.name,
            "ident": thread.ident,
            "daemon": thread.daemon,
            "is_alive": thread.is_alive(),
        }
        
        # Try to get stack trace for each thread
        if thread.ident:
            try:
                frame = sys._current_frames().get(thread.ident)
                if frame:
                    stack = traceback.format_stack(frame, limit=5)
                    thread_info["stack_trace"] = stack[-3:]  # Last 3 frames
            except:
                pass
                
        threads.append(thread_info)
    
    return {
        "system": system_info,
        "threads": threads,
        "thread_count": len(threads),
        "executor_states": get_executor_states()
    }


def get_executor_states():
    """Check the state of known executors in the app."""
    states = {}
    
    try:
        from flask import current_app
        
        # Check Flask-Executor
        if hasattr(current_app, 'extensions') and 'executor' in current_app.extensions:
            executor = current_app.extensions['executor']
            if hasattr(executor, '_executor'):
                pool = executor._executor
                states['flask_executor'] = {
                    'class': pool.__class__.__name__,
                    'shutdown': pool._shutdown if hasattr(pool, '_shutdown') else 'unknown',
                    'threads': pool._threads if hasattr(pool, '_threads') else 'unknown',
                    'max_workers': pool._max_workers if hasattr(pool, '_max_workers') else 'unknown',
                }
    except:
        pass
    
    return states


def log_thread_metrics(logger=None):
    """Log thread metrics for monitoring."""
    info = get_thread_info()
    
    message = (
        f"Thread Monitor - "
        f"PID: {info['system']['pid']} | "
        f"Threads: {info['system']['num_threads']} | "
        f"Memory: {info['system']['memory_info_mb']:.1f}MB | "
        f"CPU: {info['system']['cpu_percent']:.1f}% | "
        f"Python threads: {info['thread_count']}"
    )
    
    if logger:
        logger.info(message)
    else:
        print(message)
    
    return info


import sys