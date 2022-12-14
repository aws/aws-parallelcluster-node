import logging
import re
from datetime import datetime, timezone
from typing import Callable, Iterable

import boto3
from common.utils import sleep_remaining_loop_time
from slurm_plugin.common import ComputeInstanceDescriptor

logger = logging.getLogger(__name__)


class ConsoleLogger:
    """Class for retrieving and logging instance console output."""

    def __init__(self, enabled, region, console_output_consumer, console_output_wait_time):
        self._region = region
        self._console_logging_enabled = enabled
        self._console_output_consumer = console_output_consumer
        self._console_output_wait_time = console_output_wait_time
        self._boto3_client_factory = lambda service_name: boto3.session.Session().client(
            service_name, region_name=region
        )

    def report_console_output_from_nodes(
        self,
        compute_instances: Iterable[ComputeInstanceDescriptor],
        task_executor: Callable[[Callable[[], None]], None],
    ):
        """Queue a task that will retrieve the console output for failed compute nodes."""
        if not self._console_logging_enabled:
            return None

        start_time = datetime.now(tz=timezone.utc)

        # Only schedule a task if we have any compute_instances to query. We also need to realize any lazy instance ID
        # lookups before we schedule the task since the instance ID mapping may change after we return from this
        # call but before the task is executed.
        compute_instances = tuple(compute_instances)
        if len(compute_instances) < 1:
            return None

        task = self._get_console_output_task(
            start_time=start_time,
            wait_time=self._console_output_wait_time,
            client_factory=self._boto3_client_factory,
            compute_instances=compute_instances,
        )

        return task_executor(task)

    def _get_console_output_task(self, start_time, wait_time, client_factory, compute_instances):
        def console_collector():
            try:
                # Sleep the remainder of the given wait time in order to allow EC2 time to publish the console
                # output after the node terminates.
                sleep_remaining_loop_time(wait_time, start_time)
                ec2client = client_factory("ec2")

                for output in ConsoleLogger._get_console_output_from_nodes(ec2client, compute_instances):
                    self._console_output_consumer(
                        output.get("Name"),
                        output.get("InstanceId"),
                        output.get("ConsoleOutput"),
                    )
            except Exception as e:
                logger.error("Encountered exception while retrieving compute console output: %s", e)
                raise

        return console_collector

    @staticmethod
    def _get_console_output_from_nodes(ec2client, compute_instances):
        pattern = re.compile(r"\r\n|\n")
        for instance in compute_instances:
            instance_name = instance.get("Name")
            instance_id = instance.get("InstanceId")
            logger.info("Retrieving Console Output for node %s (%s)", instance_id, instance_name)
            response = ec2client.get_console_output(InstanceId=instance_id)
            output = response.get("Output")
            yield {
                "Name": instance_name,
                "InstanceId": instance_id,
                "ConsoleOutput": pattern.sub("\r", output) if output else None,
            }
