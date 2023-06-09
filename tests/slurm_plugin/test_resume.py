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


import logging
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import ANY, call

import botocore
import pytest
import slurm_plugin
from assertpy import assert_that
from slurm_plugin.fleet_manager import EC2Instance
from slurm_plugin.resume import SlurmResumeConfig, _get_slurm_resume, _resume

from tests.common import FLEET_CONFIG, LAUNCH_OVERRIDES, client_error


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
    return "slurm_plugin.instance_manager.boto3"


# todo add tests with fleet


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
                "job_level_scaling": True,
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
                "job_level_scaling": False,
            },
        ),
    ],
)
def test_resume_config(config_file, expected_attributes, test_datadir, mocker):
    mocker.patch("slurm_plugin.resume.read_json", side_effect=[FLEET_CONFIG, LAUNCH_OVERRIDES, LAUNCH_OVERRIDES])
    resume_config = SlurmResumeConfig(test_datadir / config_file)
    for key in expected_attributes:
        assert_that(resume_config.__dict__.get(key)).is_equal_to(expected_attributes.get(key))


@pytest.mark.parametrize(
    (
        "mock_node_lists",
        "batch_size",
        "all_or_nothing_batch",
        "launched_instances",
        "expected_failed_nodes",
        "expected_update_node_calls",
        "expected_assigned_nodes",
        "is_heartbeat_valid",
    ),
    [
        # all_or_nothing_batch without ice error
        (
            [
                SimpleNamespace(name="queue1-dy-c5xlarge-1", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-dy-c5xlarge-2", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-st-c5xlarge-1", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-st-c5xlarge-2", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
            ],
            3,
            True,
            [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-11111",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.1",
                            "PrivateDnsName": "ip-1-0-0-1",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip.1.0.0.1",
                                },
                            ],
                        },
                        {
                            "InstanceId": "i-22222",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.2",
                            "PrivateDnsName": "ip-1-0-0-2",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip.1.0.0.2",
                                },
                            ],
                        },
                        {
                            "InstanceId": "i-33333",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.3",
                            "PrivateDnsName": "ip-1-0-0-3",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip.1.0.0.3",
                                },
                            ],
                        },
                    ]
                },
                client_error("RequestLimitExceeded"),
            ],
            {"RequestLimitExceeded": {"queue1-st-c5xlarge-2"}},
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
                        EC2Instance(
                            "i-11111",
                            "ip.1.0.0.1",
                            "ip-1-0-0-1",
                            datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "c5.xlarge",
                            None,
                        ),
                        EC2Instance(
                            "i-22222",
                            "ip.1.0.0.2",
                            "ip-1-0-0-2",
                            datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "c5.xlarge",
                            None,
                        ),
                        EC2Instance(
                            "i-33333",
                            "ip.1.0.0.3",
                            "ip-1-0-0-3",
                            datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "c5.xlarge",
                            None,
                        ),
                    ],
                )
            ),
            True,
        ),
        # all_or_nothing_batch with ice error
        (
            [
                SimpleNamespace(name="queue1-dy-c5xlarge-1", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-dy-c5xlarge-2", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-st-c5xlarge-1", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-st-c5xlarge-2", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
            ],
            3,
            True,
            [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-11111",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.1",
                            "PrivateDnsName": "ip-1-0-0-1",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip.1.0.0.1",
                                },
                            ],
                        },
                        {
                            "InstanceId": "i-22222",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.2",
                            "PrivateDnsName": "ip-1-0-0-2",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip.1.0.0.2",
                                },
                            ],
                        },
                        {
                            "InstanceId": "i-33333",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.3",
                            "PrivateDnsName": "ip-1-0-0-3",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip.1.0.0.3",
                                },
                            ],
                        },
                    ]
                },
                client_error("InsufficientInstanceCapacity"),
            ],
            {"InsufficientInstanceCapacity": {"queue1-st-c5xlarge-2"}},
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
                        EC2Instance(
                            "i-11111",
                            "ip.1.0.0.1",
                            "ip-1-0-0-1",
                            datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "c5.xlarge",
                            None,
                        ),
                        EC2Instance(
                            "i-22222",
                            "ip.1.0.0.2",
                            "ip-1-0-0-2",
                            datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "c5.xlarge",
                            None,
                        ),
                        EC2Instance(
                            "i-33333",
                            "ip.1.0.0.3",
                            "ip-1-0-0-3",
                            datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "c5.xlarge",
                            None,
                        ),
                    ],
                )
            ),
            True,
        ),
        # best_effort without ice error
        (
            [
                SimpleNamespace(name="queue1-dy-c5xlarge-1", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-dy-c5xlarge-2", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-st-c5xlarge-1", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-st-c5xlarge-2", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
            ],
            3,
            False,
            [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-11111",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.1",
                            "PrivateDnsName": "ip-1-0-0-1",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip.1.0.0.1",
                                },
                            ],
                        },
                    ]
                },
                client_error("ServiceUnavailable"),
            ],
            {
                "LimitedInstanceCapacity": {"queue1-dy-c5xlarge-2", "queue1-st-c5xlarge-1"},
                "ServiceUnavailable": {"queue1-st-c5xlarge-2"},
            },
            [call(["queue1-dy-c5xlarge-1"], nodeaddrs=["ip.1.0.0.1"], nodehostnames=None)],
            dict(
                zip(
                    ["queue1-dy-c5xlarge-1"],
                    [
                        EC2Instance(
                            "i-11111",
                            "ip.1.0.0.1",
                            "ip-1-0-0-1",
                            datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "c5.xlarge",
                            None,
                        ),
                    ],
                )
            ),
            True,
        ),
        # best_effort wit ice error
        (
            [
                SimpleNamespace(name="queue1-dy-c5xlarge-1", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-dy-c5xlarge-2", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-st-c5xlarge-1", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
                SimpleNamespace(name="queue1-st-c5xlarge-2", state_string="ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP"),
            ],
            3,
            False,
            [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-11111",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.1",
                            "PrivateDnsName": "ip-1-0-0-1",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip.1.0.0.1",
                                },
                            ],
                        },
                    ]
                },
                client_error("InsufficientReservedInstanceCapacity"),
            ],
            {"InsufficientReservedInstanceCapacity": {"queue1-st-c5xlarge-2"}},
            [call(["queue1-dy-c5xlarge-1"], nodeaddrs=["ip.1.0.0.1"], nodehostnames=None)],
            dict(
                zip(
                    ["queue1-dy-c5xlarge-1"],
                    [
                        EC2Instance(
                            "i-11111",
                            "ip.1.0.0.1",
                            "ip-1-0-0-1",
                            datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "c5.xlarge",
                            None,
                        ),
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
            {},
            None,
            None,
            False,
        ),
    ],
    ids=[
        "all_or_nothing without ICE error",
        "all_or_nothing with ICE error",
        "best_effort without ICE error",
        "best_effort with ICE error",
        "invalid_heartbeat",
    ],
)
def test_resume_launch(
    mock_node_lists,
    batch_size,
    all_or_nothing_batch,
    launched_instances,
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
        run_instances_overrides={},
        create_fleet_overrides={},
        fleet_config=FLEET_CONFIG,
        clustermgtd_heartbeat_file_path="some_path",
        clustermgtd_timeout=600,
        boto3_config=botocore.config.Config(),
        hosted_zone=None,
        dns_domain=None,
        use_private_hostname=False,
        head_node_instance_id="i-headnode",
        job_level_scaling=True,
    )
    mocker.patch("slurm_plugin.resume.is_clustermgtd_heartbeat_valid", autospec=True, return_value=is_heartbeat_valid)
    mock_handle_failed_nodes = mocker.patch("slurm_plugin.resume._handle_failed_nodes", autospec=True)
    # patch slurm calls
    mock_update_nodes = mocker.patch("slurm_plugin.instance_manager.update_nodes", autospec=True)
    mock_get_node_info = mocker.patch("slurm_plugin.resume.get_nodes_info", return_value=mock_node_lists, autospec=True)
    # patch DNS related functions
    mock_store_hostname = mocker.patch.object(
        slurm_plugin.instance_manager.InstanceManager, "_store_assigned_hostnames", autospec=True
    )
    mock_update_dns = mocker.patch.object(
        slurm_plugin.instance_manager.InstanceManager, "_update_dns_hostnames", autospec=True
    )

    # Only mock fleet manager if testing case of valid clustermgtd heartbeat
    if is_heartbeat_valid:
        # patch fleet manager calls
        mocker.patch.object(
            slurm_plugin.fleet_manager.Ec2RunInstancesManager,
            "_launch_instances",
            side_effect=launched_instances,
        )

    _resume("some_arg_nodes", mock_resume_config, {})
    if not is_heartbeat_valid:
        mock_handle_failed_nodes.assert_called_with("some_arg_nodes")
        mock_update_nodes.assert_not_called()
        mock_get_node_info.assert_not_called()
        mock_store_hostname.assert_not_called()
        mock_update_dns.assert_not_called()
    else:
        mock_handle_failed_nodes_calls = []
        if expected_failed_nodes:
            for error_code, nodeset in expected_failed_nodes.items():
                mock_handle_failed_nodes_calls.append(
                    call(nodeset, reason=f"(Code:{error_code})Failure when resuming nodes")
                )
            mock_handle_failed_nodes.assert_has_calls(mock_handle_failed_nodes_calls)
        if expected_update_node_calls:
            mock_update_nodes.assert_has_calls(expected_update_node_calls)
        if expected_assigned_nodes:
            mock_store_hostname.assert_called_with(ANY, expected_assigned_nodes)
            mock_update_dns.assert_called_with(ANY, expected_assigned_nodes)


@pytest.mark.parametrize(
    ("config_file", "expected_slurm_resume"),
    [
        (None, {}),
        ("no_existent", {}),
        ("malformed.json", {}),
        (
            "resume.json",
            {
                "all_nodes_resume": "cloud[1-3]",
                "jobs": [
                    {
                        "extra": "An arbitrary string from --extra",
                        "features": "c1,c2",
                        "job_id": 140814,
                        "nodes_alloc": "cloud[1-4]",
                        "nodes_resume": "cloud[1-3]",
                        "oversubscribe": "OK",
                        "partition": "cloud",
                        "reservation": "resv_1234",
                    }
                ],
            },
        ),
    ],
)
def test_get_slurm_resume(config_file, expected_slurm_resume, test_datadir, caplog):
    caplog.set_level(logging.INFO)
    if config_file:
        os.environ["SLURM_RESUME_FILE"] = str(test_datadir / config_file)
    slurm_resume = _get_slurm_resume()
    assert_that(slurm_resume).is_equal_to(expected_slurm_resume)

    if not expected_slurm_resume:
        if config_file == "no_existent":
            assert_that(caplog.records).is_length(1)
            assert_that(caplog.records[0].levelname).is_equal_to("ERROR")
            assert_that(caplog.records[0].message).contains(f"Slurm Resume File content: {expected_slurm_resume}")
        else:
            assert_that(caplog.records).is_length(2)
            assert_that(caplog.records[0].levelname).is_equal_to("INFO")
            assert_that(caplog.records[0].message).contains(
                f"Unable to read file '{test_datadir / config_file if config_file else config_file}'"
            )
            assert_that(caplog.records[1].levelname).is_equal_to("ERROR")
            assert_that(caplog.records[1].message).contains(f"Slurm Resume File content: {expected_slurm_resume}")
    else:
        assert_that(caplog.records).is_length(1)
        assert_that(caplog.records[0].levelname).is_equal_to("INFO")
        assert_that(caplog.records[0].message).contains("Slurm Resume File content")
