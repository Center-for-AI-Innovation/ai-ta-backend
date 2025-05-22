from flask_executor import Executor
from injector import inject


class ExecutorInterface:

  def submit(self, fn, *args, **kwargs):
    raise NotImplementedError


class FlaskExecutorAdapter(ExecutorInterface):
  """
	Adapter for Flask Executor, suitable for I/O-bound tasks that benefit from asynchronous execution.
	Use this executor for tasks that involve waiting for I/O operations (e.g., network requests, file I/O),
	where the overhead of creating new threads or processes is justified by the time spent waiting.
	"""

  @inject
  def __init__(self, executor: Executor):
    self.executor = executor

  def submit(self, fn, *args, **kwargs):
    return self.executor.submit(fn, *args, **kwargs)
