# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import inspect
import logging
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from threading import Event, Semaphore
from typing import Callable, Optional

from slurm_plugin.common import TaskController

logger = logging.getLogger(__name__)


class TaskExecutor:
    """Class for managing execution of asynchronous tasks."""

    class MaximumBacklogExceededError(RuntimeError):
        """Exception raised when a task can't be queued due to backlog."""

        def __init__(self, task, maximum_backlog):
            self.failed_task = task
            self.maximum_backlog = maximum_backlog

    def __init__(self, worker_pool_size, max_backlog):
        self._max_backlog = max_backlog
        self._executor_limit = Semaphore(max_backlog)
        self._shutdown_event = Event()
        self._executor_pool = ThreadPoolExecutor(max_workers=worker_pool_size)

    def is_shutdown(self) -> bool:
        return self._shutdown_event.is_set()

    def raise_if_shutdown(self) -> None:
        if self.is_shutdown():
            raise TaskController.TaskShutdownError()

    def wait_unless_shutdown(self, seconds_to_wait: float) -> None:
        shutdown = self._shutdown_event.wait(seconds_to_wait)
        if shutdown:
            raise TaskController.TaskShutdownError()

    def queue_task(self, task: Callable[[], None]) -> Optional[Future]:
        def queue_executor_task_callback(semaphore, *args):
            semaphore.release()

        if task:
            self.raise_if_shutdown()

            if self._executor_limit.acquire(blocking=False):
                future = self._executor_pool.submit(task)
                future.add_done_callback(partial(queue_executor_task_callback, self._executor_limit))

                return future
            else:
                logging.error(
                    "Unable to queue task due to exceeding backlog limit of %d",
                    self._max_backlog,
                )
                raise TaskExecutor.MaximumBacklogExceededError(task=task, maximum_backlog=self._max_backlog)

        return None

    def shutdown(self, wait: bool = False, cancel_futures: bool = True) -> None:
        if self._executor_pool:
            # Notify any waiters that we are shutting down
            self._shutdown_event.set()

            # `cancel_futures` parameter does not exist in python pre-3.9
            can_cancel = "cancel_futures" in inspect.getfullargspec(self._executor_pool.shutdown).kwonlyargs
            self._executor_pool.shutdown(
                wait=wait, cancel_futures=cancel_futures
            ) if can_cancel else self._executor_pool.shutdown(wait=False)
            self._executor_pool = None
