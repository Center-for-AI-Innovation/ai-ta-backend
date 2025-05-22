from concurrent.futures import ThreadPoolExecutor


class ThreadPoolExecutorInterface:

  def submit(self, fn, *args, **kwargs):
    raise NotImplementedError


class ThreadPoolExecutorAdapter(ThreadPoolExecutorInterface):
  """
	Adapter for Python's ThreadPoolExecutor, suitable for I/O-bound tasks that can be performed concurrently.
	Use this executor for tasks that are largely waiting on I/O operations, such as database queries or file reads,
	where the GIL (Global Interpreter Lock) does not become a bottleneck.

	Not for CPU-bound tasks like heavy computation, as the GIL would prevent true parallel execution.
	
	This executor is particularly useful when you want more control over the number of concurrent threads
	than what Flask Executor provides, or when you're not working within a Flask application context.
	"""

  def __init__(self, max_workers=None):
    self.executor = ThreadPoolExecutor(max_workers=max_workers)

  def submit(self, fn, *args, **kwargs):
    return self.executor.submit(fn, *args, **kwargs)

  def map(self, fn, *iterables, timeout=None, chunksize=1):
    return self.executor.map(fn, *iterables, timeout=timeout, chunksize=chunksize)

  def __enter__(self):
    return self.executor

  def __exit__(self, exc_type, exc_value, traceback):
    self.executor.shutdown()
