import logging
import re
from datetime import datetime, timezone

import boto3
from common.utils import sleep_remaining_loop_time

logger = logging.getLogger(__name__)


class ConsoleLogger:
    """Class for retrieving and logging instance console output."""

    def __init__(self, enabled, region, console_output_consumer, max_sample_size, console_output_wait_time):
        self._region = region
        self._console_logging_enabled = enabled
        self._console_output_consumer = console_output_consumer
        self._max_sample_size = max_sample_size
        self._console_output_wait_time = console_output_wait_time
        self._boto3_client_factory = lambda service_name: boto3.session.Session().client(
            service_name, region_name=region
        )

    def report_console_output_from_nodes(self, compute_nodes, instance_supplier, task_executor):
        """
        Queue a task that will retrieve the console output for failed compute nodes.

        The console output of up to self._max_sample_size nodes that have backing instances in `compute_nodes` will be
        retrieved.
        """
        if not self._console_logging_enabled:
            return None

        start_time = datetime.now(tz=timezone.utc)

        # Fetch instance IDs now since the node to instance mapping will likely change after returning from this call.
        instances = tuple(instance for instance in instance_supplier(compute_nodes) if instance.get("InstanceId"))

        if self._max_sample_size > 0:
            instances = instances[: self._max_sample_size]

        if len(instances) < 1:
            return None

        task = self._get_console_output_task(
            start_time=start_time,
            wait_time=self._console_output_wait_time,
            instance_supplier=lambda: instances,
        )

        return task_executor(task)

    def _get_console_output_task(self, start_time, wait_time, instance_supplier):
        def get_console_output_from_nodes(ec2client, instances):
            pattern = re.compile(r"\r\n|\n")
            for instance in instances:
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

        def console_collector():
            try:
                sleep_remaining_loop_time(wait_time, start_time)
                ec2client = self._boto3_client_factory("ec2")

                for output in get_console_output_from_nodes(ec2client, instance_supplier()):
                    self._console_output_consumer(
                        output.get("Name"),
                        output.get("InstanceId"),
                        output.get("ConsoleOutput"),
                    )
            except Exception as e:
                logger.error("Encountered exception while retrieving compute console output: %s", e)
                raise

        return console_collector
