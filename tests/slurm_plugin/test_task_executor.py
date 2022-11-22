import time

from assertpy import assert_that, soft_assertions
from slurm_plugin.task_executor import TaskExecutor


def test_task_executor():
    def get_task(value):
        def task():
            return value + 1

        return task

    task_executor = TaskExecutor(worker_pool_size=3, max_backlog=10)

    futures = {value: task_executor.queue_executor_task(get_task(value)) for value in range(10, 20)}

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

    future = task_executor.queue_executor_task(get_task(10))
    assert_that(task_executor.queue_executor_task).raises(
        TaskExecutor.MaximumBacklogExceededException
    ).when_called_with(get_task(20))

    assert_that(future.result()).is_equal_to(11)

    task_executor.shutdown()
