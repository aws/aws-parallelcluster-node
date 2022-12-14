import base64
import os
import re

import boto3
import pytest
from assertpy import assert_that
from botocore.stub import Stubber
from slurm_plugin.console_logger import ConsoleLogger

from tests.common import MockedBoto3Request


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
        console_output_wait_time=0,
    )

    ec2client = boto3.session.Session().client("ec2", "us-east-2")

    with Stubber(ec2client) as ec2_stub:
        for request in mocked_ec2_requests:
            ec2_stub.add_response(request.method, request.response, expected_params=request.expected_params)
        console_logger._boto3_client_factory = lambda service_name: ec2client
        task_executed = console_logger.report_console_output_from_nodes(
            compute_instances=compute_instances,
            task_executor=lambda task: task() or True,
        )
        ec2_stub.assert_no_pending_responses()

    assert_that(task_executed).is_true() if len(compute_instances) > 0 else assert_that(task_executed).is_none()
    assert_that(actual_results).is_length(len(mocked_ec2_requests))

    for instance, actual_output in actual_results.items():
        assert_that(actual_output).is_equal_to(expected_results.get(instance))


def test_exception_handling():
    def boto3_factory(service_name):
        raise Exception(service_name)

    console_logger = ConsoleLogger(
        enabled=True,
        region="us-east-2",
        console_output_consumer=lambda *args: None,
        console_output_wait_time=0,
    )

    console_logger._boto3_client_factory = boto3_factory

    assert_that(console_logger.report_console_output_from_nodes).raises(Exception).when_called_with(
        compute_nodes=["hello"], task_executor=lambda task: task()
    )
