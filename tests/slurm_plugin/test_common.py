# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.


import os
from datetime import datetime, timedelta, timezone
from unittest.mock import call

import pytest
from assertpy import assert_that

from slurm_plugin.common import (
    EC2_HEALTH_STATUS_UNHEALTHY_STATES,
    EC2_INSTANCE_ALIVE_STATES,
    EC2_SCHEDULED_EVENT_CODES,
    EC2Instance,
    EC2InstanceHealthState,
    InstanceManager,
    time_is_up,
)
from tests.common import MockedBoto3Request


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
    return "slurm_plugin.common.boto3"


@pytest.mark.parametrize(
    (
        "instances_to_launch",
        "instance_manager",
        "launch_batch_size",
        "update_node_address",
        "mocked_boto3_request",
        "expected_failed_nodes",
        "expected_update_nodes_calls",
    ),
    [
        # normal
        (
            {
                "queue1": {"c5.xlarge": ["queue1-static-c5.xlarge-2"], "c5.2xlarge": ["queue1-static-c5.2xlarge-1"]},
                "queue2": {"c5.xlarge": ["queue2-static-c5.xlarge-1", "queue2-dynamic-c5.xlarge-1"]},
            },
            InstanceManager(region="us-east-2", cluster_name="hit", boto3_config="some_boto3_config",),
            10,
            True,
            [
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.1",
                                "PrivateDnsName": "ip-1-0-0-1",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            }
                        ]
                    },
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5.xlarge"},
                    },
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-23456",
                                "InstanceType": "c5.2xlarge",
                                "PrivateIpAddress": "ip.1.0.0.2",
                                "PrivateDnsName": "ip-1-0-0-2",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            }
                        ]
                    },
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5.2xlarge"},
                    },
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-34567",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.3",
                                "PrivateDnsName": "ip-1-0-0-3",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            },
                            {
                                "InstanceId": "i-45678",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.4",
                                "PrivateDnsName": "ip-1-0-0-4",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            },
                        ]
                    },
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 2,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    },
                ),
            ],
            None,
            [
                call(
                    ["queue1-static-c5.xlarge-2"],
                    [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                ),
                call(
                    ["queue1-static-c5.2xlarge-1"],
                    [EC2Instance("i-23456", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                ),
                call(
                    ["queue2-static-c5.xlarge-1", "queue2-dynamic-c5.xlarge-1"],
                    [
                        EC2Instance("i-34567", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                        EC2Instance("i-45678", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                    ],
                ),
            ],
        ),
        # client_error
        (
            {
                "queue1": {"c5.xlarge": ["queue1-static-c5.xlarge-2"], "c5.2xlarge": ["queue1-static-c5.2xlarge-1"]},
                "queue2": {"c5.xlarge": ["queue2-static-c5.xlarge-1", "queue2-dynamic-c5.xlarge-1"]},
            },
            InstanceManager(region="us-east-2", cluster_name="hit", boto3_config="some_boto3_config",),
            10,
            True,
            [
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.1",
                                "PrivateDnsName": "ip-1-0-0-1",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            }
                        ]
                    },
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5.xlarge"},
                    },
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={},
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5.2xlarge"},
                    },
                    generate_error=True,
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-34567",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.3",
                                "PrivateDnsName": "ip-1-0-0-3",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            },
                            {
                                "InstanceId": "i-45678",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.4",
                                "PrivateDnsName": "ip-1-0-0-4",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            },
                        ]
                    },
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 2,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    },
                ),
            ],
            ["queue1-static-c5.2xlarge-1"],
            [
                call(
                    ["queue1-static-c5.xlarge-2"],
                    [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                ),
                call(
                    ["queue2-static-c5.xlarge-1", "queue2-dynamic-c5.xlarge-1"],
                    [
                        EC2Instance("i-34567", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                        EC2Instance("i-45678", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                    ],
                ),
            ],
        ),
        # no_update
        (
            {"queue1": {"c5.xlarge": ["queue1-static-c5.xlarge-2"]}},
            InstanceManager(region="us-east-2", cluster_name="hit", boto3_config="some_boto3_config",),
            10,
            False,
            [
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.1",
                                "PrivateDnsName": "ip-1-0-0-1",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            }
                        ]
                    },
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5.xlarge"},
                    },
                ),
            ],
            None,
            None,
        ),
        # batch_size1
        (
            {
                "queue1": {"c5.xlarge": ["queue1-static-c5.xlarge-2"], "c5.2xlarge": ["queue1-static-c5.2xlarge-1"]},
                "queue2": {
                    "c5.xlarge": [
                        "queue2-static-c5.xlarge-1",
                        "queue2-static-c5.xlarge-2",
                        "queue2-dynamic-c5.xlarge-1",
                    ],
                },
            },
            InstanceManager(region="us-east-2", cluster_name="hit", boto3_config="some_boto3_config",),
            3,
            True,
            [
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.1",
                                "PrivateDnsName": "ip-1-0-0-1",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            }
                        ]
                    },
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5.xlarge"},
                    },
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={},
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5.2xlarge"},
                    },
                    generate_error=True,
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={},
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 3,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    },
                    generate_error=True,
                ),
            ],
            [
                "queue1-static-c5.2xlarge-1",
                "queue2-static-c5.xlarge-1",
                "queue2-static-c5.xlarge-2",
                "queue2-dynamic-c5.xlarge-1",
            ],
            [
                call(
                    ["queue1-static-c5.xlarge-2"],
                    [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                )
            ],
        ),
        # batch_size2
        (
            {
                "queue1": {"c5.xlarge": ["queue1-static-c5.xlarge-2"], "c5.2xlarge": ["queue1-static-c5.2xlarge-1"]},
                "queue2": {
                    "c5.xlarge": [
                        "queue2-static-c5.xlarge-1",
                        "queue2-static-c5.xlarge-2",
                        "queue2-dynamic-c5.xlarge-1",
                    ],
                },
            },
            InstanceManager(region="us-east-2", cluster_name="hit", boto3_config="some_boto3_config",),
            1,
            True,
            [
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.1",
                                "PrivateDnsName": "ip-1-0-0-1",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            }
                        ]
                    },
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5.xlarge"},
                    },
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={},
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue1-c5.2xlarge"},
                    },
                    generate_error=True,
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-34567",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.3",
                                "PrivateDnsName": "ip-1-0-0-3",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            }
                        ]
                    },
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    },
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={},
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    },
                    generate_error=True,
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-45678",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.4",
                                "PrivateDnsName": "ip-1-0-0-4",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            }
                        ]
                    },
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    },
                ),
            ],
            ["queue1-static-c5.2xlarge-1", "queue2-static-c5.xlarge-2"],
            [
                call(
                    ["queue1-static-c5.xlarge-2"],
                    [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                ),
                call(
                    ["queue2-static-c5.xlarge-1"],
                    [EC2Instance("i-34567", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                ),
                call(
                    ["queue2-dynamic-c5.xlarge-1"],
                    [EC2Instance("i-45678", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                ),
            ],
        ),
        (
            {
                "queue2": {
                    "c5.xlarge": [
                        "queue2-static-c5.xlarge-1",
                        "queue2-static-c5.xlarge-2",
                        "queue2-dynamic-c5.xlarge-1",
                    ],
                },
            },
            InstanceManager(region="us-east-2", cluster_name="hit", boto3_config="some_boto3_config",),
            10,
            True,
            # Simulate the case that only a part of the requested capacity is launched
            MockedBoto3Request(
                method="run_instances",
                response={
                    "Instances": [
                        {
                            "InstanceId": "i-45678",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.4",
                            "PrivateDnsName": "ip-1-0-0-4",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                        }
                    ]
                },
                expected_params={
                    "MinCount": 1,
                    "MaxCount": 3,
                    "LaunchTemplate": {"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                },
            ),
            ["queue2-static-c5.xlarge-2", "queue2-dynamic-c5.xlarge-1"],
            [
                call(
                    ["queue2-static-c5.xlarge-1", "queue2-static-c5.xlarge-2", "queue2-dynamic-c5.xlarge-1"],
                    [EC2Instance("i-45678", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                )
            ],
        ),
    ],
    ids=["normal", "client_error", "no_update", "batch_size1", "batch_size2", "partial_launch"],
)
def test_add_instances(
    boto3_stubber,
    instances_to_launch,
    instance_manager,
    launch_batch_size,
    update_node_address,
    mocked_boto3_request,
    expected_failed_nodes,
    expected_update_nodes_calls,
    mocker,
):
    mocker.patch("slurm_plugin.common.update_nodes")
    # patch internal functions
    # Mock _update_slurm_node_addrs but still allow original code to execute
    original_update_func = instance_manager._update_slurm_node_addrs
    instance_manager._update_slurm_node_addrs = mocker.MagicMock(side_effect=original_update_func)
    instance_manager._parse_requested_instances = mocker.MagicMock(return_value=instances_to_launch)
    # patch boto3 call
    boto3_stubber("ec2", mocked_boto3_request)
    # run test
    instance_manager.add_instances_for_nodes(
        node_list=["placeholder_node_list"],
        launch_batch_size=launch_batch_size,
        update_node_address=update_node_address,
    )
    if expected_update_nodes_calls:
        instance_manager._update_slurm_node_addrs.assert_has_calls(expected_update_nodes_calls)
    else:
        instance_manager._update_slurm_node_addrs.assert_not_called()
    if expected_failed_nodes:
        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)
    else:
        assert_that(instance_manager.failed_nodes).is_empty()


@pytest.mark.parametrize(
    "instance_manager, node_lists, launched_nodes, expected_update_nodes_call, expected_failed_nodes",
    [
        (
            InstanceManager(region="us-east-2", cluster_name="hit", boto3_config="some_boto3_config",),
            ["node-1"],
            [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
            call(["node-1"], nodeaddrs=["ip-1"], nodehostnames=["hostname-1"], raise_on_error=True),
            [],
        ),
        (
            InstanceManager(region="us-east-2", cluster_name="hit", boto3_config="some_boto3_config",),
            ["node-1"],
            [],
            None,
            ["node-1"],
        ),
        (
            InstanceManager(region="us-east-2", cluster_name="hit", boto3_config="some_boto3_config",),
            ["node-1", "node-2", "node-3", "node-4"],
            [
                EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
            ],
            call(
                ["node-1", "node-2"],
                nodeaddrs=["ip-1", "ip-2"],
                nodehostnames=["hostname-1", "hostname-2"],
                raise_on_error=True,
            ),
            ["node-3", "node-4"],
        ),
    ],
    ids=("all_launched", "nothing_launched", "partial_launched"),
)
def test_update_slurm_node_addrs(
    instance_manager, node_lists, launched_nodes, expected_update_nodes_call, expected_failed_nodes, mocker
):
    mock_update_nodes = mocker.patch("slurm_plugin.common.update_nodes")
    instance_manager._update_slurm_node_addrs(node_lists, launched_nodes)
    if expected_update_nodes_call:
        mock_update_nodes.assert_called_once()
        mock_update_nodes.assert_has_calls([expected_update_nodes_call])
    else:
        mock_update_nodes.assert_not_called()
    assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)


@pytest.mark.parametrize(
    ("node_list", "expected_results", "expected_failed_nodes"),
    [
        (
            [
                "queue1-static-c5.xlarge-1",
                "queue1-static-c5.xlarge-2",
                "queue1-dynamic-c5.xlarge-201",
                "queue2-static-g3.4xlarge-1",
                "in-valid/queue.name-static-c5.xlarge-2",
                "noBrackets-static-c5.xlarge-[1-2]",
                "queue2-dynamic-g3.8xlarge-1",
                "queue2-static-i3en.metal-2tb-1",
                "queue2-static-i3en.metal_2tb-1",
                "queue2-invalidnodetype-c5.xlarge-12",
                "queuename-with-dash-and_underscore-static-i3en.metal-2tb-1",
            ],
            {
                "queue1": {
                    "c5.xlarge": [
                        "queue1-static-c5.xlarge-1",
                        "queue1-static-c5.xlarge-2",
                        "queue1-dynamic-c5.xlarge-201",
                    ]
                },
                "queue2": {
                    "g3.4xlarge": ["queue2-static-g3.4xlarge-1"],
                    "g3.8xlarge": ["queue2-dynamic-g3.8xlarge-1"],
                    "i3en.metal-2tb": ["queue2-static-i3en.metal-2tb-1"],
                },
                "queuename-with-dash-and_underscore": {
                    "i3en.metal-2tb": ["queuename-with-dash-and_underscore-static-i3en.metal-2tb-1"]
                },
            },
            [
                "in-valid/queue.name-static-c5.xlarge-2",
                "noBrackets-static-c5.xlarge-[1-2]",
                "queue2-static-i3en.metal_2tb-1",
                "queue2-invalidnodetype-c5.xlarge-12",
            ],
        ),
    ],
)
def test_parse_requested_instances(node_list, expected_results, expected_failed_nodes):
    mock_instance_manager = InstanceManager(region="us-east-2", cluster_name="hit", boto3_config="some_boto3_config",)
    assert_that(mock_instance_manager._parse_requested_instances(node_list)).is_equal_to(expected_results)
    assert_that(mock_instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)


@pytest.mark.parametrize(
    ("instance_ids_to_name", "batch_size", "mocked_boto3_request"),
    [
        # normal
        (
            ["i-12345", "i-23456", "i-34567"],
            10,
            [
                MockedBoto3Request(
                    method="terminate_instances",
                    response={},
                    expected_params={"InstanceIds": ["i-12345", "i-23456", "i-34567"]},
                ),
            ],
        ),
        # ClientError
        (
            ["i-12345", "i-23456", "i-34567"],
            1,
            [
                MockedBoto3Request(
                    method="terminate_instances",
                    response={},
                    expected_params={"InstanceIds": ["i-12345"]},
                    generate_error=True,
                ),
                MockedBoto3Request(
                    method="terminate_instances",
                    response={},
                    expected_params={"InstanceIds": ["i-23456"]},
                    generate_error=True,
                ),
                MockedBoto3Request(
                    method="terminate_instances",
                    response={},
                    expected_params={"InstanceIds": ["i-34567"]},
                    generate_error=True,
                ),
            ],
        ),
    ],
    ids=["normal", "client_error"],
)
def test_delete_instances(boto3_stubber, instance_ids_to_name, batch_size, mocked_boto3_request, mocker):
    # patch boto3 call
    boto3_stubber("ec2", mocked_boto3_request)
    mock_instance_manager = InstanceManager(region="us-east-2", cluster_name="hit", boto3_config="some_boto3_config",)
    # run test
    mock_instance_manager.delete_instances(instance_ids_to_name, batch_size)


@pytest.mark.parametrize(
    "instance_ids, mocked_boto3_request, expected_parsed_result",
    [
        (
            ["i-1", "i-2"],
            [
                MockedBoto3Request(
                    method="describe_instance_status",
                    response={
                        "InstanceStatuses": [
                            {
                                "InstanceId": "i-1",
                                "InstanceState": {"Name": "running"},
                                "InstanceStatus": {"Status": "impaired"},
                                "SystemStatus": {"Status": "ok"},
                            },
                            {
                                "InstanceId": "i-2",
                                "InstanceState": {"Name": "pending"},
                                "InstanceStatus": {"Status": "initializing"},
                                "SystemStatus": {"Status": "impaired"},
                                "Events": [{"InstanceEventId": "event-id-1"}],
                            },
                        ]
                    },
                    expected_params={
                        "Filters": [
                            {"Name": "instance-status.status", "Values": list(EC2_HEALTH_STATUS_UNHEALTHY_STATES)}
                        ],
                        "MaxResults": 1000,
                    },
                    generate_error=False,
                ),
                MockedBoto3Request(
                    method="describe_instance_status",
                    response={
                        "InstanceStatuses": [
                            {
                                "InstanceId": "i-1",
                                "InstanceState": {"Name": "running"},
                                "InstanceStatus": {"Status": "impaired"},
                                "SystemStatus": {"Status": "ok"},
                            },
                        ]
                    },
                    expected_params={
                        "Filters": [
                            {"Name": "instance-status.status", "Values": list(EC2_HEALTH_STATUS_UNHEALTHY_STATES)}
                        ],
                        "MaxResults": 1000,
                    },
                    generate_error=False,
                ),
                MockedBoto3Request(
                    method="describe_instance_status",
                    response={
                        "InstanceStatuses": [
                            {
                                "InstanceId": "i-2",
                                "InstanceState": {"Name": "pending"},
                                "InstanceStatus": {"Status": "initializing"},
                                "SystemStatus": {"Status": "impaired"},
                                "Events": [{"InstanceEventId": "event-id-1"}],
                            },
                        ]
                    },
                    expected_params={
                        "Filters": [
                            {"Name": "system-status.status", "Values": list(EC2_HEALTH_STATUS_UNHEALTHY_STATES)}
                        ],
                        "MaxResults": 1000,
                    },
                    generate_error=False,
                ),
                MockedBoto3Request(
                    method="describe_instance_status",
                    response={
                        "InstanceStatuses": [
                            {
                                "InstanceId": "i-2",
                                "InstanceState": {"Name": "pending"},
                                "InstanceStatus": {"Status": "initializing"},
                                "SystemStatus": {"Status": "impaired"},
                                "Events": [{"InstanceEventId": "event-id-1"}],
                            },
                        ]
                    },
                    expected_params={
                        "Filters": [{"Name": "event.code", "Values": EC2_SCHEDULED_EVENT_CODES}],
                        "MaxResults": 1000,
                    },
                    generate_error=False,
                ),
            ],
            [
                EC2InstanceHealthState("i-1", "running", {"Status": "ok"}, {"Status": "ok"}, None),
                EC2InstanceHealthState(
                    "i-2",
                    "pending",
                    {"Status": "initializing"},
                    {"Status": "initializing"},
                    [{"InstanceEventId": "event-id-1"}],
                ),
            ],
        )
    ],
)
def get_unhealthy_cluster_instance_status(instance_ids, mocked_boto3_request, expected_parsed_result, boto3_stubber):
    # patch boto3 call
    boto3_stubber("ec2", mocked_boto3_request)
    # run test
    instance_manager = InstanceManager("us-east-1", "hit-test", "some_boto3_config")
    result = instance_manager.get_unhealthy_cluster_instance_status(instance_ids)
    assert_that(result).is_equal_to(expected_parsed_result)


@pytest.mark.parametrize(
    "mock_kwargs, mocked_boto3_request, expected_parsed_result",
    [
        (
            {"include_master": False, "alive_states_only": True},
            MockedBoto3Request(
                method="describe_instances",
                response={
                    "Reservations": [
                        {
                            "Instances": [
                                {
                                    "InstanceId": "i-1",
                                    "PrivateIpAddress": "ip-1",
                                    "PrivateDnsName": "hostname",
                                    "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                },
                                {
                                    "InstanceId": "i-2",
                                    "PrivateIpAddress": "ip-2",
                                    "PrivateDnsName": "hostname",
                                    "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                },
                            ]
                        }
                    ]
                },
                expected_params={
                    "Filters": [
                        {"Name": "tag:ClusterName", "Values": ["hit-test"]},
                        {"Name": "instance-state-name", "Values": list(EC2_INSTANCE_ALIVE_STATES)},
                        {"Name": "tag:aws-parallelcluster-node-type", "Values": ["Compute"]},
                    ],
                    "MaxResults": 1000,
                },
                generate_error=False,
            ),
            [
                EC2Instance("i-1", "ip-1", "hostname", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                EC2Instance("i-2", "ip-2", "hostname", datetime(2020, 1, 1, tzinfo=timezone.utc)),
            ],
        ),
        (
            {"include_master": False, "alive_states_only": True},
            MockedBoto3Request(
                method="describe_instances",
                response={"Reservations": []},
                expected_params={
                    "Filters": [
                        {"Name": "tag:ClusterName", "Values": ["hit-test"]},
                        {"Name": "instance-state-name", "Values": list(EC2_INSTANCE_ALIVE_STATES)},
                        {"Name": "tag:aws-parallelcluster-node-type", "Values": ["Compute"]},
                    ],
                    "MaxResults": 1000,
                },
                generate_error=False,
            ),
            [],
        ),
        (
            {"include_master": True, "alive_states_only": False},
            MockedBoto3Request(
                method="describe_instances",
                response={
                    "Reservations": [
                        {
                            "Instances": [
                                {
                                    "InstanceId": "i-1",
                                    "PrivateIpAddress": "ip-1",
                                    "PrivateDnsName": "hostname",
                                    "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                },
                            ]
                        }
                    ]
                },
                expected_params={"Filters": [{"Name": "tag:ClusterName", "Values": ["hit-test"]}], "MaxResults": 1000},
                generate_error=False,
            ),
            [EC2Instance("i-1", "ip-1", "hostname", datetime(2020, 1, 1, tzinfo=timezone.utc))],
        ),
    ],
    ids=["default", "empty_response", "custom_args"],
)
def test_get_cluster_instances(mock_kwargs, mocked_boto3_request, expected_parsed_result, boto3_stubber):
    # patch boto3 call
    boto3_stubber("ec2", mocked_boto3_request)
    # run test
    instance_manager = InstanceManager("us-east-1", "hit-test", "some_boto3_config")
    result = instance_manager.get_cluster_instances(**mock_kwargs)
    assert_that(result).is_equal_to(expected_parsed_result)


@pytest.mark.parametrize(
    "initial_time, current_time, grace_time, expected_result",
    [
        (datetime(2020, 1, 1, 0, 0, 0), datetime(2020, 1, 1, 0, 0, 29), 30, False),
        (datetime(2020, 1, 1, 0, 0, 0), datetime(2020, 1, 1, 0, 0, 30), 30, True),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours ahead of UTC, so this time stamp is actually 30 mins before initial_time
            datetime(2020, 1, 1, 0, 30, 0, tzinfo=timezone(timedelta(hours=1))),
            30 * 60,
            False,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours ahead of UTC, so this time stamp is actually 30 mins after initial_time
            datetime(2020, 1, 1, 1, 30, 0, tzinfo=timezone(timedelta(hours=1))),
            30 * 60,
            True,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours behind of UTC, so this time stamp is actually 1.5 hrs after initial_time
            datetime(2020, 1, 1, 0, 30, 0, tzinfo=timezone(-timedelta(hours=1))),
            90 * 60,
            True,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours behind of UTC, so this time stamp is actually 1 hrs after initial_time
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone(-timedelta(hours=1))),
            90 * 60,
            False,
        ),
    ],
)
def test_time_is_up(initial_time, current_time, grace_time, expected_result):
    assert_that(time_is_up(initial_time, current_time, grace_time)).is_equal_to(expected_result)
