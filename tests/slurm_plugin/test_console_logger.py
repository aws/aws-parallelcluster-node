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

import base64
import os
import re
from concurrent.futures import Future
from typing import Callable, Optional

import boto3
import pytest
from assertpy import assert_that
from botocore.stub import Stubber
from slurm_plugin.common import TaskController
from slurm_plugin.console_logger import ConsoleLogger

from tests.common import MockedBoto3Request


class _TestController(TaskController):
    def __init__(self):
        self.tasks_queued: int = 0
        self._shutdown = False

    def queue_task(self, task: Callable[[], None]) -> Optional[Future]:
        self.tasks_queued += 1
        task()
        return None

    def is_shutdown(self) -> bool:
        return self._shutdown

    def raise_if_shutdown(self) -> None:
        if self._shutdown:
            raise TaskController.TaskShutdownError()

    def wait_unless_shutdown(self, seconds_to_wait: float) -> None:
        self.raise_if_shutdown()

    def shutdown(self, wait: bool = False, cancel_futures: bool = True) -> None:
        self._shutdown = True


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
    return "slurm_plugin.instance_manager.boto3"  # FIXME


@pytest.mark.parametrize(
    "compute_instances",
    [
        [
            {
                "Name": "node-0",
                "InstanceId": "i-005457f0c2beb9ad2",
            },
            {
                "Name": "node-1",
                "InstanceId": "i-105457f0c2beb9ad2",
            },
        ],
        [],
    ],
)
def test_get_console_output_from_nodes(compute_instances):
    def console_callback(name, instance_id, output):
        actual_results.update({instance_id: output})

    expected_instances = tuple(node.get("InstanceId") for node in compute_instances if node.get("InstanceId"))
    expected_results = {instance: f"{instance}:\rConsole output for you too." for instance in expected_instances}

    mocked_ec2_requests = [
        MockedBoto3Request(
            method="get_console_output",
            response={
                "InstanceId": instance,
                "Output": str(base64.b64encode(re.sub(r"\r", "\r\n", output).encode("utf-8")), "latin-1"),
                "Timestamp": "2022-11-18T23:37:25.000Z",
            },
            expected_params={
                "InstanceId": instance,
            },
            generate_error=False,
        )
        for instance, output in expected_results.items()
    ]

    actual_results = {}

    console_logger = ConsoleLogger(
        enabled=True,
        region="us-east-2",
        console_output_consumer=console_callback,
    )

    ec2client = boto3.session.Session().client("ec2", "us-east-2")

    task_controller = _TestController()

    with Stubber(ec2client) as ec2_stub:
        for request in mocked_ec2_requests:
            ec2_stub.add_response(request.method, request.response, expected_params=request.expected_params)
        console_logger._boto3_client_factory = lambda service_name: ec2client
        console_logger.report_console_output_from_nodes(
            compute_instances=compute_instances,
            task_controller=task_controller,
            task_wait_function=lambda: None,
        )
        ec2_stub.assert_no_pending_responses()

    (
        assert_that(task_controller.tasks_queued).is_equal_to(1)
        if len(compute_instances) > 0
        else assert_that(task_controller.tasks_queued).is_zero()
    )
    assert_that(actual_results).is_length(len(mocked_ec2_requests))

    for instance, actual_output in actual_results.items():
        assert_that(actual_output).is_equal_to(expected_results.get(instance))


def test_exception_handling():
    class EC2Client:
        def get_console_output(*args, **kwargs):
            test_controller.shutdown()

            instance_id = kwargs.get("InstanceId")
            return {"Output": f"Output for {instance_id}"}

    def boto3_factory(*args, **kwargs):
        return EC2Client()

    def callback(*args):
        nonlocal call_count
        call_count += 1

    call_count = 0

    console_logger = ConsoleLogger(enabled=True, region="us-east-2", console_output_consumer=callback)

    console_logger._boto3_client_factory = boto3_factory

    test_controller = _TestController()

    assert_that(console_logger.report_console_output_from_nodes).raises(
        TaskController.TaskShutdownError
    ).when_called_with(
        compute_instances=[{"Name": "hello", "InstanceId": "instance-id"}],
        task_controller=test_controller,
        task_wait_function=lambda: None,
    )

    assert_that(test_controller.tasks_queued).is_equal_to(1)
    assert_that(call_count).is_zero()
