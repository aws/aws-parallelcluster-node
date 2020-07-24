import os
from unittest.mock import MagicMock, call

import pytest
from assertpy import assert_that

from common.schedulers.slurm_commands import SlurmNode
from slurm_plugin.common import InstanceManager
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
                            },
                            {
                                "InstanceId": "i-45678",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.4",
                                "PrivateDnsName": "ip-1-0-0-4",
                            },
                        ]
                    },
                    expected_params={
                        "MinCount": 2,
                        "MaxCount": 2,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    },
                ),
            ],
            None,
            [
                call(["queue1-static-c5.xlarge-2"], ["i-12345"], ["ip.1.0.0.1"], ["ip-1-0-0-1"]),
                call(["queue1-static-c5.2xlarge-1"], ["i-23456"], ["ip.1.0.0.2"], ["ip-1-0-0-2"]),
                call(
                    ["queue2-static-c5.xlarge-1", "queue2-dynamic-c5.xlarge-1"],
                    ["i-34567", "i-45678"],
                    ["ip.1.0.0.3", "ip.1.0.0.4"],
                    ["ip-1-0-0-3", "ip-1-0-0-4"],
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
                            },
                            {
                                "InstanceId": "i-45678",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.4",
                                "PrivateDnsName": "ip-1-0-0-4",
                            },
                        ]
                    },
                    expected_params={
                        "MinCount": 2,
                        "MaxCount": 2,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    },
                ),
            ],
            ["queue1-static-c5.2xlarge-1"],
            [
                call(["queue1-static-c5.xlarge-2"], ["i-12345"], ["ip.1.0.0.1"], ["ip-1-0-0-1"]),
                call(
                    ["queue2-static-c5.xlarge-1", "queue2-dynamic-c5.xlarge-1"],
                    ["i-34567", "i-45678"],
                    ["ip.1.0.0.3", "ip.1.0.0.4"],
                    ["ip-1-0-0-3", "ip-1-0-0-4"],
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
                        "MinCount": 3,
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
            [call(["queue1-static-c5.xlarge-2"], ["i-12345"], ["ip.1.0.0.1"], ["ip-1-0-0-1"])],
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
                call(["queue1-static-c5.xlarge-2"], ["i-12345"], ["ip.1.0.0.1"], ["ip-1-0-0-1"]),
                call(["queue2-static-c5.xlarge-1"], ["i-34567"], ["ip.1.0.0.3"], ["ip-1-0-0-3"],),
                call(["queue2-dynamic-c5.xlarge-1"], ["i-45678"], ["ip.1.0.0.4"], ["ip-1-0-0-4"]),
            ],
        ),
    ],
    ids=["normal", "client_error", "no_update", "batch_size1", "batch_size2"],
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
    # patch internal functions
    instance_manager._update_slurm_node_addrs = MagicMock()
    # update_node_mocker = mocker.patch("slurm_plugin.common.InstanceLaunch._update_slurm_node_addrs", autospec=True)
    instance_manager._parse_requested_instances = MagicMock(return_value=instances_to_launch)
    # patch boto3 call
    boto3_stubber("ec2", mocked_boto3_request)
    # run test
    instance_manager.add_instances_for_nodes(
        node_list=["placeholder_node_list"],
        launch_batch_size=launch_batch_size,
        update_node_address=update_node_address,
    )
    if expected_failed_nodes:
        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)
    else:
        assert_that(instance_manager.failed_nodes).is_empty()
    if expected_update_nodes_calls:
        instance_manager._update_slurm_node_addrs.assert_has_calls(expected_update_nodes_calls)
    else:
        instance_manager._update_slurm_node_addrs.assert_not_called()


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
    # run test
    InstanceManager.delete_instances(instance_ids_to_name, "us-east-1", "some_boto_3_config", batch_size)


@pytest.mark.parametrize(
    ("slurm_nodes", "mocked_boto3_request", "expected_results"),
    [
        (
            [
                SlurmNode("queue1-static-c5.xlarge-2", "ip.1", "ip-1", "some_state"),
                SlurmNode("queue1-static-c5.2xlarge-1", "ip.2", "ip-2", "some_state"),
                SlurmNode("queue2-static-c5.xlarge-1", "ip.3", "ip-3", "some_state"),
            ],
            [
                MockedBoto3Request(
                    method="describe_instances",
                    response={
                        "Reservations": [
                            {
                                "Instances": [
                                    {"InstanceId": "i-12345", "PrivateIpAddress": "ip.1"},
                                    {"InstanceId": "i-23456", "PrivateIpAddress": "ip.2"},
                                    {"InstanceId": "i-34567", "PrivateIpAddress": "ip.3"},
                                ]
                            }
                        ]
                    },
                    expected_params={
                        "Filters": [
                            {"Name": "private-ip-address", "Values": ["ip.1", "ip.2", "ip.3"]},
                            {"Name": "tag:ClusterName", "Values": ["hit-test"]},
                        ],
                    },
                ),
            ],
            {
                "i-12345": "queue1-static-c5.xlarge-2",
                "i-23456": "queue1-static-c5.2xlarge-1",
                "i-34567": "queue2-static-c5.xlarge-1",
            },
        ),
        (
            [
                SlurmNode("queue1-static-c5.xlarge-2", "ip.1", "ip-1", "some_state"),
                SlurmNode("queue1-static-c5.2xlarge-1", "ip.2", "ip-2", "some_state"),
                SlurmNode("queue2-static-c5.xlarge-1", "ip.3", "ip-3", "some_state"),
            ],
            [
                MockedBoto3Request(
                    method="describe_instances",
                    response={
                        "Reservations": [
                            {
                                "Instances": [
                                    {"InstanceId": "i-12345", "PrivateIpAddress": "ip.1"},
                                    {"InstanceId": "i-23456", "PrivateIpAddress": "ip.2"},
                                ]
                            }
                        ]
                    },
                    expected_params={
                        "Filters": [
                            {"Name": "private-ip-address", "Values": ["ip.1", "ip.2", "ip.3"]},
                            {"Name": "tag:ClusterName", "Values": ["hit-test"]},
                        ],
                    },
                ),
            ],
            {"i-12345": "queue1-static-c5.xlarge-2", "i-23456": "queue1-static-c5.2xlarge-1"},
        ),
    ],
    ids=["default", "missing_instance"],
)
def test_get_instance_ids_to_nodename(slurm_nodes, mocked_boto3_request, expected_results, mocker, boto3_stubber):
    # patch boto3 call
    boto3_stubber("ec2", mocked_boto3_request)
    # run test
    instance_manager = InstanceManager("us-east-1", "hit-test", "some_boto3_config")
    result = instance_manager.get_instance_ids_to_nodename(slurm_nodes)
    assert_that(result).is_equal_to(expected_results)
