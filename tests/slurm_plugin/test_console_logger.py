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
    "compute_nodes,sample_size",
    [
        (
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
            -1,
        ),
        (
            [
                {
                    "Name": "node-0",
                },
                {
                    "Name": "node-1",
                },
                {
                    "Name": "node-2",
                    "InstanceId": "i-205457f0c2beb9ad2",
                },
                {
                    "Name": "node-3",
                },
                {
                    "Name": "node-4",
                },
                {
                    "Name": "node-5",
                    "InstanceId": "i-505457f0c2beb9ad2",
                },
                {
                    "Name": "node-6",
                    "InstanceId": "i-605457f0c2beb9ad2",
                },
            ],
            2,
        ),
    ],
)
def test_get_console_output_from_nodes(compute_nodes, sample_size):
    def console_callback(name, instance_id, output):
        actual_results.update({instance_id: output})

    def instance_supplier(compute_nodes):
        return ({"Name": node.get("Name"), "InstanceId": node.get("InstanceId")} for node in compute_nodes)

    def task_executor(task):
        with Stubber(ec2client) as ec2_stub:
            for request in mocked_ec2_requests:
                ec2_stub.add_response(request.method, request.response, expected_params=request.expected_params)
            console_logger._boto3_client_factory = lambda service_name: ec2client
            task()
            ec2_stub.assert_no_pending_responses()

    expected_instances = tuple(node.get("InstanceId") for node in compute_nodes if node.get("InstanceId"))
    if sample_size > 0:
        expected_instances = expected_instances[:sample_size]

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
        max_sample_size=sample_size,
        console_output_wait_time=0,
    )

    ec2client = boto3.session.Session().client("ec2", "us-east-2")

    console_logger.report_console_output_from_nodes(
        compute_nodes=compute_nodes, instance_supplier=instance_supplier, task_executor=task_executor
    )

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
        max_sample_size=1,
        console_output_wait_time=0,
    )

    console_logger._boto3_client_factory = boto3_factory

    assert_that(console_logger.report_console_output_from_nodes).raises(Exception).when_called_with(
        compute_nodes=["hello"], instance_supplier=lambda nodes: None, task_executor=lambda task: task()
    )
