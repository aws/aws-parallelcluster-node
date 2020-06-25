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
from types import SimpleNamespace
from unittest.mock import call

import pytest

import slurm_plugin
from assertpy import assert_that
from slurm_plugin.resume import SlurmResumeConfig, _add_instances, _parse_requested_instances
from tests.common import MockedBoto3Request


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
    return "slurm_plugin.resume.boto3"


@pytest.mark.parametrize(
    ("instances_to_launch, resume_config, mocked_boto3_request, " "expected_failed_nodes, expected_update_nodes_calls"),
    [
        # normal
        (
            {
                "queue1": {"c5.xlarge": ["queue1-static-c5.xlarge-2"], "c5.2xlarge": ["queue1-static-c5.2xlarge-1"]},
                "queue2": {"c5.xlarge": ["queue2-static-c5.xlarge-1", "queue2-dynamic-c5.xlarge-1"]},
            },
            SimpleNamespace(
                region="us-east-2",
                cluster_name="hit",
                boto3_config="some_boto3_config",
                max_batch_size=10,
                update_node_address=True,
            ),
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
            SimpleNamespace(
                region="us-east-2",
                cluster_name="hit",
                boto3_config="some_boto3_config",
                max_batch_size=10,
                update_node_address=True,
            ),
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
            SimpleNamespace(
                region="us-east-2",
                cluster_name="hit",
                boto3_config="some_boto3_config",
                max_batch_size=10,
                update_node_address=False,
            ),
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
            SimpleNamespace(
                region="us-east-2",
                cluster_name="hit",
                boto3_config="some_boto3_config",
                max_batch_size=3,
                update_node_address=True,
            ),
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
            SimpleNamespace(
                region="us-east-2",
                cluster_name="hit",
                boto3_config="some_boto3_config",
                max_batch_size=1,
                update_node_address=True,
            ),
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
    resume_config,
    mocked_boto3_request,
    expected_failed_nodes,
    expected_update_nodes_calls,
    mocker,
):
    # reset failed_nodes to empty list
    failed_node_mocker = mocker.patch("slurm_plugin.resume.failed_nodes", [])
    # patch internal functions
    update_node_mocker = mocker.patch("slurm_plugin.resume._update_slurm_node_addrs", autospec=True)
    mocker.patch("slurm_plugin.resume._parse_requested_instances", return_value=instances_to_launch)
    # patch boto3 call
    boto3_stubber("ec2", mocked_boto3_request)
    # run test
    _add_instances(["placeholder_node_lists"], resume_config)
    if expected_failed_nodes:
        assert_that(failed_node_mocker).is_equal_to(expected_failed_nodes)
    else:
        assert_that(failed_node_mocker).is_empty()
    if expected_update_nodes_calls:
        update_node_mocker.assert_has_calls(expected_update_nodes_calls)
    else:
        update_node_mocker.assert_not_called()


@pytest.mark.parametrize(
    ("node_list", "expected_results"),
    [
        (
            [
                "queue1-static-c5.xlarge-1",
                "queue1-static-c5.xlarge-2",
                "queue1-dynamic-c5.xlarge-201",
                "queue2-static-g3.4xlarge-1",
                "in-valid-queue-static-c5.xlarge-2",
                "noBrackets-static-c5.xlarge-[1-2]",
                "queue2-dynamic-g3.8xlarge-1",
            ],
            {
                "queue1": {
                    "c5.xlarge": [
                        "queue1-static-c5.xlarge-1",
                        "queue1-static-c5.xlarge-2",
                        "queue1-dynamic-c5.xlarge-201",
                    ]
                },
                "queue2": {"g3.4xlarge": ["queue2-static-g3.4xlarge-1"], "g3.8xlarge": ["queue2-dynamic-g3.8xlarge-1"]},
            },
        ),
    ],
)
def test_parse_requested_instances(node_list, expected_results):
    assert_that(_parse_requested_instances(node_list)).is_equal_to(expected_results)


@pytest.mark.parametrize(
    ("config_file", "expected_attributes"),
    [
        (
            "default.conf",
            {
                "cluster_name": "hit",
                "region": "us-east-2",
                "max_batch_size": 100,
                "update_node_address": True,
                "_boto3_config": {"retries": {"max_attempts": 5, "mode": "standard"}},
                "logging_config": os.path.join(
                    os.path.dirname(slurm_plugin.__file__), "logging", "parallelcluster_resume_logging.conf"
                ),
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
                    "retries": {"max_attempts": 5, "mode": "standard"},
                    "proxies": {"https": "my.resume.proxy"},
                },
                "logging_config": "/path/to/resume_logging/config",
            },
        ),
    ],
)
def test_resume_config(config_file, expected_attributes, test_datadir):
    resume_config = SlurmResumeConfig(test_datadir / config_file)
    for key in expected_attributes:
        assert_that(resume_config.__dict__.get(key)).is_equal_to(expected_attributes.get(key))
