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

import time
from datetime import datetime, timezone

from assertpy import assert_that, soft_assertions
from slurm_plugin.common import TaskController
from slurm_plugin.task_executor import TaskExecutor


def test_task_executor():
    def get_task(value):
        def task():
            return value + 1

        return task

    task_executor = TaskExecutor(worker_pool_size=3, max_backlog=10)

    futures = {value: task_executor.queue_task(get_task(value)) for value in range(10, 20)}

    with soft_assertions():
        for value, future in futures.items():
            assert_that(future.result()).is_equal_to(value + 1)

    task_executor.shutdown()


def test_exceeding_max_backlog():
    def get_task(value):
        def task():
            time.sleep(value)
            return value + 1

        return task

    task_executor = TaskExecutor(worker_pool_size=1, max_backlog=1)

    future = task_executor.queue_task(get_task(10))
    assert_that(task_executor.queue_task).raises(TaskExecutor.MaximumBacklogExceededError).when_called_with(
        get_task(20)
    )

    assert_that(future.result()).is_equal_to(11)

    task_executor.shutdown()


def test_that_shutdown_does_not_block():
    def get_task(value):
        def task():
            task_executor.wait_unless_shutdown(value)
            return value + 1

        return task

    def callback(*args):
        nonlocal callback_called
        callback_called = True

    task_executor = TaskExecutor(worker_pool_size=1, max_backlog=1)

    callback_called = False
    start_wait = datetime.now(tz=timezone.utc)
    future = task_executor.queue_task(get_task(600))
    future.add_done_callback(callback)

    task_executor.shutdown(wait=True)

    delta = (datetime.now(tz=timezone.utc) - start_wait).total_seconds()
    assert_that(delta).is_less_than(300)

    assert_that(future.exception).raises(TaskController.TaskShutdownError)
    assert_that(callback_called).is_true()
