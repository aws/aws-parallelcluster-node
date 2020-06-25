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

import pytest

import slurm_plugin
from assertpy import assert_that
from common.schedulers.slurm_commands import SlurmNode
from slurm_plugin.suspend import SlurmSuspendConfig, _delete_instances, _get_instance_ids_to_nodename
from tests.common import MockedBoto3Request


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    return "slurm_plugin.suspend.boto3"


@pytest.mark.parametrize(
    "instance_ids_to_name, batch_size, mocked_boto3_request",
    [
        # normal
        (
            {
                "i-12345": "queue1-static-c5.xlarge-2",
                "i-23456": "queue1-static-c5.2xlarge-1",
                "i-34567": "queue2-static-c5.xlarge-1",
            },
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
            {
                "i-12345": "queue1-static-c5.xlarge-2",
                "i-23456": "queue1-static-c5.2xlarge-1",
                "i-34567": "queue2-static-c5.xlarge-1",
            },
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
    _delete_instances(instance_ids_to_name, "us-east-1", "some_boto_3_config", batch_size)


@pytest.mark.parametrize(
    ("slurm_nodes", "expected_paginator_call", "paginator_response", "expected_results"),
    [
        (
            [
                SlurmNode("queue1-static-c5.xlarge-2", "ip.1", "ip-1", "some_state"),
                SlurmNode("queue1-static-c5.2xlarge-1", "ip.2", "ip-2", "some_state"),
                SlurmNode("queue2-static-c5.xlarge-1", "ip.3", "ip-3", "some_state"),
            ],
            {
                "Filters": [
                    {"Name": "private-ip-address", "Values": ["ip.1", "ip.2", "ip.3"]},
                    {"Name": "tag:ClusterName", "Values": ["hit-test"]},
                ],
            },
            [
                {"InstanceId": "i-12345", "PrivateIpAddress": "ip.1"},
                {"InstanceId": "i-23456", "PrivateIpAddress": "ip.2"},
                {"InstanceId": "i-34567", "PrivateIpAddress": "ip.3"},
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
            {
                "Filters": [
                    {"Name": "private-ip-address", "Values": ["ip.1", "ip.2", "ip.3"]},
                    {"Name": "tag:ClusterName", "Values": ["hit-test"]},
                ],
            },
            [
                {"InstanceId": "i-12345", "PrivateIpAddress": "ip.1"},
                {"InstanceId": "i-23456", "PrivateIpAddress": "ip.2"},
            ],
            {"i-12345": "queue1-static-c5.xlarge-2", "i-23456": "queue1-static-c5.2xlarge-1"},
        ),
    ],
    ids=["default", "missing_instance"],
)
def test_get_instance_ids_to_nodename(
    slurm_nodes, expected_paginator_call, paginator_response, expected_results, mocker
):
    # TO-DO: use stubber to mock paginator
    # patch paginator
    mocked_client = mocker.patch("slurm_plugin.suspend.boto3.client")
    mocked_paginator = mocked_client.return_value.get_paginator.return_value.paginate
    mocked_paginator.return_value.search.return_value = paginator_response
    # run test
    result = _get_instance_ids_to_nodename(slurm_nodes, "us-east-1", "hit-test", "some_boto3_config")
    mocked_paginator.assert_called_with(**expected_paginator_call)
    assert_that(result).is_equal_to(expected_results)


@pytest.mark.parametrize(
    ("config_file", "expected_attributes"),
    [
        (
            "default.conf",
            {
                "cluster_name": "hit2",
                "region": "us-east-1",
                "max_batch_size": 1000,
                "_boto3_config": {"retries": {"max_attempts": 5, "mode": "standard"}},
                "logging_config": os.path.join(
                    os.path.dirname(slurm_plugin.__file__), "logging", "parallelcluster_suspend_logging.conf"
                ),
            },
        ),
        (
            "all_options.conf",
            {
                "cluster_name": "hit2",
                "region": "us-east-1",
                "max_batch_size": 200,
                "_boto3_config": {
                    "retries": {"max_attempts": 5, "mode": "standard"},
                    "proxies": {"https": "my.suspend.proxy"},
                },
                "logging_config": "/path/to/suspend_logging/config",
            },
        ),
    ],
)
def test_suspend_config(config_file, expected_attributes, test_datadir):
    suspend_config = SlurmSuspendConfig(test_datadir / config_file)
    for key in expected_attributes:
        assert_that(suspend_config.__dict__.get(key)).is_equal_to(expected_attributes.get(key))
