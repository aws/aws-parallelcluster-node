import inspect
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Semaphore

logger = logging.getLogger(__name__)


class TaskExecutor:
    class MaximumBacklogExceededException(RuntimeError):
        """Exception raised when a task can't be queued due to backlog."""

        def __init__(self, task, maximum_backlog):
            self.failed_task = task
            self.maximum_backlog = maximum_backlog

    """Class for managing execution of asynchronous tasks."""

    def __init__(self, worker_pool_size, max_backlog):
        self._max_backlog = max_backlog
        self._executor_limit = Semaphore(max_backlog)
        self._executor_pool = ThreadPoolExecutor(max_workers=worker_pool_size)

    def queue_executor_task(self, task):
        def queue_executor_task_callback(*args):
            semaphore.release()

        semaphore = self._executor_limit
        if task:
            if semaphore.acquire(blocking=False):
                future = self._executor_pool.submit(task)
                future.add_done_callback(queue_executor_task_callback)

                return future
            else:
                logging.error(
                    "Unable to queue task due to exceeding backlog limit of %d",
                    self._max_backlog,
                )
                raise TaskExecutor.MaximumBacklogExceededException(task=task, maximum_backlog=self._max_backlog)

        return None

    def shutdown(self):
        if self._executor_pool:
            # `cancel_futures` parameter does not exist in python pre-3.9
            can_cancel = "cancel_futures" in inspect.getfullargspec(self._executor_pool.shutdown).kwonlyargs
            self._executor_pool.shutdown(
                wait=False, cancel_futures=True
            ) if can_cancel else self._executor_pool.shutdown(wait=False)
            self._executor_pool = None
