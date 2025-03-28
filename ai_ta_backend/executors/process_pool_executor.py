from concurrent.futures import ProcessPoolExecutor


class ProcessPoolExecutorInterface:

  def submit(self, fn, *args, **kwargs):
    raise NotImplementedError


class ProcessPoolExecutorAdapter(ProcessPoolExecutorInterface):
  """
	Adapter for Python's ProcessPoolExecutor, suitable for CPU-bound tasks that benefit from parallel execution.
	Use this executor for tasks that require significant computation and can be efficiently parallelized across multiple CPUs.
	Not for I/O-bound tasks like database queries, file I/O, or network requests, as the overhead of creating and managing processes can outweigh the benefits.
	
	This executor is ideal for scenarios where the task execution time would significantly benefit from being distributed
	across multiple processes, thereby bypassing the GIL (Global Interpreter Lock) and utilizing multiple CPU cores.
	
	Note: ProcessPoolExecutor is best used with tasks that are relatively heavy and can be executed independently of each other.
	"""

  def __init__(self, max_workers=None):
    self.executor = ProcessPoolExecutor(max_workers=max_workers)

  def submit(self, fn, *args, **kwargs):
    return self.executor.submit(fn, *args, **kwargs)

  def map(self, fn, *iterables, timeout=None, chunksize=1):
    return self.executor.map(fn, *iterables, timeout=timeout, chunksize=chunksize)
