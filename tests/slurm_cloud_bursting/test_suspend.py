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


from unittest.mock import call

import pytest
from botocore.exceptions import ClientError

from assertpy import assert_that
from common.schedulers.slurm_commands import SlurmNode
from slurm_cloud_bursting.suspend import SlurmSuspendConfig, _suspend


@pytest.mark.parametrize(
    "instance_ids_to_name, batch_size, terminate_instances_side_effect, expected_idle_nodes_calls",
    [
        # normal
        (
            {
                "i-12345": "queue1-static-c5.xlarge-2",
                "i-23456": "queue1-static-c5.2xlarge-1",
                "i-34567": "queue2-static-c5.xlarge-1",
            },
            10,
            None,
            [call(["queue1-static-c5.xlarge-2", "queue1-static-c5.2xlarge-1", "queue2-static-c5.xlarge-1"])],
        ),
        # ClientError
        (
            {
                "i-12345": "queue1-static-c5.xlarge-2",
                "i-23456": "queue1-static-c5.2xlarge-1",
                "i-34567": "queue2-static-c5.xlarge-1",
            },
            10,
            ClientError(
                error_response={
                    "Error": {
                        "Code": "SomeServiceException",
                        "Message": "Details/context around the exception or error",
                    },
                },
                operation_name="random operation name",
            ),
            [call(["queue1-static-c5.xlarge-2", "queue1-static-c5.2xlarge-1", "queue2-static-c5.xlarge-1"])],
        ),
    ],
    ids=["normal", "client_error"],
)
def test_suspend(instance_ids_to_name, batch_size, terminate_instances_side_effect, expected_idle_nodes_calls, mocker):
    # initialize test
    arg_nodes = "queue1-static-c5.xlarge-2,queue1-static-c5.2xlarge-1,queue2-static-c5.xlarge-1"
    suspend_config = SlurmSuspendConfig(
        region="us-east-2", cluster_name="hit-test", max_batch_size=batch_size, boto3_config="some_boto3_config",
    )
    # patch internal functions
    mocker.patch(
        "slurm_cloud_bursting.suspend.get_nodes_info",
        return_value=[
            SlurmNode("queue1-static-c5.xlarge-2", "some_ip", "some_hostname", "some_state"),
            SlurmNode("queue1-static-c5.2xlarge-1", "some_ip", "some_hostname", "some_state"),
            SlurmNode("queue2-static-c5.xlarge-1", "some_ip", "some_hostname", "some_state"),
        ],
        autospec=True,
    )
    mocker.patch(
        "slurm_cloud_bursting.suspend._get_instance_ids_to_nodename", return_value=instance_ids_to_name, autospec=True
    )
    node_down_mocker = mocker.patch("slurm_cloud_bursting.suspend._set_nodes_idle", autospec=True)
    # patch boto3 call
    mock_boto_client = mocker.patch("slurm_cloud_bursting.suspend.boto3.client")
    mock_boto_client.return_value.terminate_instances.side_effect = terminate_instances_side_effect
    # run test
    _suspend(arg_nodes, suspend_config)
    if terminate_instances_side_effect:
        assert_that(mock_boto_client.return_value.terminate_instances.call_count).is_equal_to(3)
    if expected_idle_nodes_calls:
        node_down_mocker.assert_has_calls(expected_idle_nodes_calls)
