# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.


import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import call

import botocore
import pytest
import slurm_plugin
from assertpy import assert_that
from slurm_plugin.resume import SlurmResumeConfig, _resume
from slurm_plugin.slurm_resources import EC2Instance

from tests.common import MockedBoto3Request


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
    return "slurm_plugin.instance_manager.boto3"


@pytest.mark.parametrize(
    ("config_file", "expected_attributes"),
    [
        (
            "default.conf",
            {
                "cluster_name": "hit",
                "region": "us-east-2",
                "max_batch_size": 500,
                "update_node_address": True,
                "_boto3_config": {"retries": {"max_attempts": 1, "mode": "standard"}},
                "logging_config": os.path.join(
                    os.path.dirname(slurm_plugin.__file__), "logging", "parallelcluster_resume_logging.conf"
                ),
                "all_or_nothing_batch": False,
                "clustermgtd_timeout": 300,
                "clustermgtd_heartbeat_file_path": "/home/ec2-user/clustermgtd_heartbeat",
            },
        ),
        (
            "all_options.conf",
            {
                "cluster_name": "hit",
                "region": "us-east-2",
                "max_batch_size": 50,
                "update_node_address": False,
                "_boto3_config": {
                    "retries": {"max_attempts": 10, "mode": "standard"},
                    "proxies": {"https": "my.resume.proxy"},
                },
                "logging_config": "/path/to/resume_logging/config",
                "all_or_nothing_batch": True,
                "clustermgtd_timeout": 5,
                "clustermgtd_heartbeat_file_path": "alternate/clustermgtd_heartbeat",
            },
        ),
    ],
)
def test_resume_config(config_file, expected_attributes, test_datadir, mocker):
    mocker.patch("slurm_plugin.resume.retrieve_instance_type_mapping", return_value={"c5xlarge": "c5.xlarge"})
    resume_config = SlurmResumeConfig(test_datadir / config_file)
    for key in expected_attributes:
        assert_that(resume_config.__dict__.get(key)).is_equal_to(expected_attributes.get(key))


@pytest.mark.parametrize(
    (
        "mock_node_lists",
        "batch_size",
        "all_or_nothing_batch",
        "mock_boto3_response",
        "expected_failed_nodes",
        "expected_update_node_calls",
        "expected_assigned_nodes",
        "is_heartbeat_valid",
    ),
    [
        # all_or_nothing_batch
        (
            [
                SimpleNamespace(name="queue1-dy-c5xlarge-1"),
                SimpleNamespace(name="queue1-dy-c5xlarge-2"),
                SimpleNamespace(name="queue1-st-c5xlarge-1"),
                SimpleNamespace(name="queue1-st-c5xlarge-2"),
            ],
            3,
            True,
            [
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-11111",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.1",
                                "PrivateDnsName": "ip-1-0-0-1",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            },
                            {
                                "InstanceId": "i-22222",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.2",
                                "PrivateDnsName": "ip-1-0-0-2",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            },
                            {
                                "InstanceId": "i-33333",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.3",
                                "PrivateDnsName": "ip-1-0-0-3",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            },
                        ]
                    },
                    expected_params={
                        "InstanceInitiatedShutdownBehavior": "terminate",
                        "MinCount": 3,
                        "MaxCount": 3,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5xlarge", "Version": "$Latest"},
                    },
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={},
                    expected_params={
                        "InstanceInitiatedShutdownBehavior": "terminate",
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5xlarge", "Version": "$Latest"},
                    },
                    generate_error=True,
                ),
            ],
            ["queue1-st-c5xlarge-2"],
            [
                call(
                    ["queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-2", "queue1-st-c5xlarge-1"],
                    nodeaddrs=["ip.1.0.0.1", "ip.1.0.0.2", "ip.1.0.0.3"],
                    nodehostnames=None,
                )
            ],
            dict(
                zip(
                    ["queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-2", "queue1-st-c5xlarge-1"],
                    [
                        EC2Instance("i-11111", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                        EC2Instance("i-22222", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                        EC2Instance("i-33333", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                    ],
                )
            ),
            True,
        ),
        # best_effort
        (
            [
                SimpleNamespace(name="queue1-dy-c5xlarge-1"),
                SimpleNamespace(name="queue1-dy-c5xlarge-2"),
                SimpleNamespace(name="queue1-st-c5xlarge-1"),
                SimpleNamespace(name="queue1-st-c5xlarge-2"),
            ],
            3,
            False,
            [
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-11111",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.1",
                                "PrivateDnsName": "ip-1-0-0-1",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            },
                        ]
                    },
                    expected_params={
                        "InstanceInitiatedShutdownBehavior": "terminate",
                        "MinCount": 1,
                        "MaxCount": 3,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5xlarge", "Version": "$Latest"},
                    },
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={},
                    expected_params={
                        "InstanceInitiatedShutdownBehavior": "terminate",
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5xlarge", "Version": "$Latest"},
                    },
                    generate_error=True,
                ),
            ],
            ["queue1-dy-c5xlarge-2", "queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2"],
            [call(["queue1-dy-c5xlarge-1"], nodeaddrs=["ip.1.0.0.1"], nodehostnames=None)],
            dict(
                zip(
                    ["queue1-dy-c5xlarge-1"],
                    [
                        EC2Instance("i-11111", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                    ],
                )
            ),
            True,
        ),
        (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            False,
        ),
    ],
    ids=["all_or_nothing", "best_effort", "invalid_heartbeat"],
)
def test_resume_launch(
    mock_node_lists,
    batch_size,
    all_or_nothing_batch,
    mock_boto3_response,
    expected_failed_nodes,
    expected_update_node_calls,
    expected_assigned_nodes,
    is_heartbeat_valid,
    mocker,
    boto3_stubber,
):
    # Test that all or nothing batch settings are working correctly
    mock_resume_config = SimpleNamespace(
        max_batch_size=batch_size,
        update_node_address=True,
        all_or_nothing_batch=all_or_nothing_batch,
        dynamodb_table="some_table",
        region="us-east-2",
        cluster_name="hit",
        head_node_private_ip="some_ip",
        head_node_hostname="some_hostname",
        instance_name_type_mapping={"queue1": {"c5xlarge": "c5.xlarge"}, "queue2": {"c52xlarge": "c5.2xlarge"}},
        clustermgtd_heartbeat_file_path="some_path",
        clustermgtd_timeout=600,
        boto3_config=botocore.config.Config(),
        hosted_zone=None,
        dns_domain=None,
        use_private_hostname=False,
    )
    mocker.patch("slurm_plugin.resume.is_clustermgtd_heartbeat_valid", auto_spec=True, return_value=is_heartbeat_valid)
    mock_handle_failed_nodes = mocker.patch("slurm_plugin.resume._handle_failed_nodes", auto_spec=True)
    # patch slurm calls
    mock_update_nodes = mocker.patch("slurm_plugin.instance_manager.update_nodes", auto_spec=True)
    mock_get_node_info = mocker.patch(
        "slurm_plugin.resume.get_nodes_info", return_value=mock_node_lists, auto_spec=True
    )
    # patch DNS related functions
    mock_store_hostname = mocker.patch.object(
        slurm_plugin.instance_manager.InstanceManager, "_store_assigned_hostnames", auto_spec=True
    )
    mock_update_dns = mocker.patch.object(
        slurm_plugin.instance_manager.InstanceManager, "_update_dns_hostnames", auto_spec=True
    )
    # Only mock boto3 if testing case of valid clustermgtd heartbeat
    if is_heartbeat_valid:
        # patch boto3 call
        boto3_stubber("ec2", mock_boto3_response)

    _resume("some_arg_nodes", mock_resume_config)
    if not is_heartbeat_valid:
        mock_handle_failed_nodes.assert_called_with("some_arg_nodes")
        mock_update_nodes.assert_not_called()
        mock_get_node_info.assert_not_called()
        mock_store_hostname.assert_not_called()
        mock_update_dns.assert_not_called()
    else:
        if expected_failed_nodes:
            mock_handle_failed_nodes.assert_called_with(expected_failed_nodes)
        if expected_update_node_calls:
            mock_update_nodes.assert_has_calls(expected_update_node_calls)
        if expected_assigned_nodes:
            mock_store_hostname.assert_called_with(expected_assigned_nodes)
            mock_update_dns.assert_called_with(expected_assigned_nodes)
