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

import logging
import re
from typing import Any, Callable, Iterable

import boto3
from slurm_plugin.common import ComputeInstanceDescriptor, TaskController

logger = logging.getLogger(__name__)


class ConsoleLogger:
    """Class for retrieving and logging instance console output."""

    def __init__(self, enabled: bool, region: str, console_output_consumer: Callable[[str, str, str], None]):
        self._region = region
        self._console_logging_enabled = enabled
        self._console_output_consumer = console_output_consumer
        self._boto3_client_factory = lambda service_name: boto3.session.Session().client(
            service_name, region_name=region
        )

    def report_console_output_from_nodes(
        self,
        compute_instances: Iterable[ComputeInstanceDescriptor],
        task_controller: TaskController,
        task_wait_function: Callable[[], None],
    ):
        """Queue a task that will retrieve the console output for failed compute nodes."""
        if not self._console_logging_enabled:
            return None

        # Only schedule a task if we have any compute_instances to query. We also need to realize any lazy instance ID
        # lookups before we schedule the task since the instance ID mapping may change after we return from this
        # call but before the task is executed.
        compute_instances = tuple(compute_instances)
        if len(compute_instances) < 1:
            return None

        task = self._get_console_output_task(
            raise_if_shutdown=task_controller.raise_if_shutdown,
            task_wait_function=task_wait_function,
            client_factory=self._boto3_client_factory,
            compute_instances=compute_instances,
        )

        return task_controller.queue_task(task)

    def _get_console_output_task(
        self,
        task_wait_function: Callable[[], None],
        raise_if_shutdown: Callable[[], None],
        client_factory: Callable[[str], Any],
        compute_instances: Iterable[ComputeInstanceDescriptor],
    ):
        def console_collector():
            try:
                # Sleep to allow EC2 time to publish the console output after the node terminates.
                task_wait_function()
                ec2client = client_factory("ec2")

                for output in ConsoleLogger._get_console_output_from_nodes(ec2client, compute_instances):
                    # If shutdown, raise an exception so that any interested threads will know
                    # this task was not completed.
                    raise_if_shutdown()
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
