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

from common.schedulers.slurm_commands import SlurmNode
from slurm_cloud_bursting.resume import TAG_SPECIFICATIONS, EC2Instance, SlurmResumeConfig, _resume


@pytest.mark.parametrize(
    (
        "node_info, resume_config, run_instance_calls, run_instance_side_effects, "
        "expected_failed_nodes, expected_launched_nodes_calls"
    ),
    [
        # normal
        (
            [
                SlurmNode("queue1-static-c5.xlarge-2", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue1-static-c5.2xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue2-static-c5.xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue2-dynamic-c5.xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
            ],
            SlurmResumeConfig(
                region="us-east-2",
                cluster_name="hit",
                boto3_config="some_boto3_config",
                max_batch_size=10,
                update_node_address=True,
            ),
            [
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue1-c5.xlarge"},
                    InstanceType="c5.xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue1-c5.2xlarge"},
                    InstanceType="c5.2xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
                call(
                    MinCount=2,
                    MaxCount=2,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    InstanceType="c5.xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
            ],
            [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-12345",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.1",
                            "PrivateDnsName": "ip-1-0-0-1",
                        }
                    ]
                },
                {
                    "Instances": [
                        {
                            "InstanceId": "i-23456",
                            "InstanceType": "c5.2xlarge",
                            "PrivateIpAddress": "ip.1.0.0.2",
                            "PrivateDnsName": "ip-1-0-0-2",
                        }
                    ]
                },
                {
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
            ],
            None,
            [
                call([(EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1"), "queue1-static-c5.xlarge-2",)]),
                call([(EC2Instance("i-23456", "ip.1.0.0.2", "ip-1-0-0-2"), "queue1-static-c5.2xlarge-1",)]),
                call(
                    [
                        (EC2Instance("i-34567", "ip.1.0.0.3", "ip-1-0-0-3"), "queue2-static-c5.xlarge-1",),
                        (EC2Instance("i-45678", "ip.1.0.0.4", "ip-1-0-0-4"), "queue2-dynamic-c5.xlarge-1",),
                    ]
                ),
            ],
        ),
        # client_error
        (
            [
                SlurmNode("queue1-static-c5.xlarge-2", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue1-static-c5.2xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue2-static-c5.xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue2-dynamic-c5.xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
            ],
            SlurmResumeConfig(
                region="us-east-2",
                cluster_name="hit",
                boto3_config="some_boto3_config",
                max_batch_size=10,
                update_node_address=True,
            ),
            [
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue1-c5.xlarge"},
                    InstanceType="c5.xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue1-c5.2xlarge"},
                    InstanceType="c5.2xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
                call(
                    MinCount=2,
                    MaxCount=2,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    InstanceType="c5.xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
            ],
            [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-12345",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.1",
                            "PrivateDnsName": "ip-1-0-0-1",
                        }
                    ]
                },
                ClientError(
                    error_response={
                        "Error": {
                            "Code": "SomeServiceException",
                            "Message": "Details/context around the exception or error",
                        },
                    },
                    operation_name="random operation name",
                ),
                {
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
            ],
            ["queue1-static-c5.2xlarge-1"],
            [
                call([(EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1"), "queue1-static-c5.xlarge-2",)]),
                call(
                    [
                        (EC2Instance("i-34567", "ip.1.0.0.3", "ip-1-0-0-3"), "queue2-static-c5.xlarge-1",),
                        (EC2Instance("i-45678", "ip.1.0.0.4", "ip-1-0-0-4"), "queue2-dynamic-c5.xlarge-1",),
                    ]
                ),
            ],
        ),
        # no_update
        (
            [SlurmNode("queue1-static-c5.xlarge-2", "random_nodeaddr", "random_hostname", "random_state")],
            SlurmResumeConfig(
                region="us-east-2",
                cluster_name="hit",
                boto3_config="some_boto3_config",
                max_batch_size=10,
                update_node_address=False,
            ),
            [
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue1-c5.xlarge"},
                    InstanceType="c5.xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
            ],
            [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-12345",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.1",
                            "PrivateDnsName": "ip-1-0-0-1",
                        }
                    ]
                },
            ],
            None,
            None,
        ),
        # batch_size1
        (
            [
                SlurmNode("queue1-static-c5.xlarge-2", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue1-static-c5.2xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue2-static-c5.xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue2-static-c5.xlarge-2", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue2-dynamic-c5.xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
            ],
            SlurmResumeConfig(
                region="us-east-2",
                cluster_name="hit",
                boto3_config="some_boto3_config",
                max_batch_size=3,
                update_node_address=True,
            ),
            [
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue1-c5.xlarge"},
                    InstanceType="c5.xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue1-c5.2xlarge"},
                    InstanceType="c5.2xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
                call(
                    MinCount=3,
                    MaxCount=3,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    InstanceType="c5.xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
            ],
            [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-12345",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.1",
                            "PrivateDnsName": "ip-1-0-0-1",
                        }
                    ]
                },
                ClientError(
                    error_response={
                        "Error": {
                            "Code": "SomeServiceException",
                            "Message": "Details/context around the exception or error",
                        },
                    },
                    operation_name="random operation name",
                ),
                ClientError(
                    error_response={
                        "Error": {
                            "Code": "SomeServiceException",
                            "Message": "Details/context around the exception or error",
                        },
                    },
                    operation_name="random operation name",
                ),
            ],
            [
                "queue1-static-c5.2xlarge-1",
                "queue2-static-c5.xlarge-1",
                "queue2-static-c5.xlarge-2",
                "queue2-dynamic-c5.xlarge-1",
            ],
            [call([(EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1"), "queue1-static-c5.xlarge-2",)])],
        ),
        # batch_size2
        (
            [
                SlurmNode("queue1-static-c5.xlarge-2", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue1-static-c5.2xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue2-static-c5.xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue2-static-c5.xlarge-2", "random_nodeaddr", "random_hostname", "random_state"),
                SlurmNode("queue2-dynamic-c5.xlarge-1", "random_nodeaddr", "random_hostname", "random_state"),
            ],
            SlurmResumeConfig(
                region="us-east-2",
                cluster_name="hit",
                boto3_config="some_boto3_config",
                max_batch_size=1,
                update_node_address=True,
            ),
            [
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue1-c5.xlarge"},
                    InstanceType="c5.xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue1-c5.2xlarge"},
                    InstanceType="c5.2xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    InstanceType="c5.xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    InstanceType="c5.xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
                call(
                    MinCount=1,
                    MaxCount=1,
                    LaunchTemplate={"LaunchTemplateName": "hit-queue2-c5.xlarge"},
                    InstanceType="c5.xlarge",
                    TagSpecifications=TAG_SPECIFICATIONS,
                ),
            ],
            [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-12345",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.1",
                            "PrivateDnsName": "ip-1-0-0-1",
                        }
                    ]
                },
                ClientError(
                    error_response={
                        "Error": {
                            "Code": "SomeServiceException",
                            "Message": "Details/context around the exception or error",
                        },
                    },
                    operation_name="random operation name",
                ),
                {
                    "Instances": [
                        {
                            "InstanceId": "i-34567",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.3",
                            "PrivateDnsName": "ip-1-0-0-3",
                        },
                    ]
                },
                ClientError(
                    error_response={
                        "Error": {
                            "Code": "SomeServiceException",
                            "Message": "Details/context around the exception or error",
                        },
                    },
                    operation_name="random operation name",
                ),
                {
                    "Instances": [
                        {
                            "InstanceId": "i-45678",
                            "InstanceType": "c5.xlarge",
                            "PrivateIpAddress": "ip.1.0.0.4",
                            "PrivateDnsName": "ip-1-0-0-4",
                        },
                    ]
                },
            ],
            ["queue1-static-c5.2xlarge-1", "queue2-static-c5.xlarge-2"],
            [
                call([(EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1"), "queue1-static-c5.xlarge-2",)]),
                call([(EC2Instance("i-34567", "ip.1.0.0.3", "ip-1-0-0-3"), "queue2-static-c5.xlarge-1",)]),
                call([(EC2Instance("i-45678", "ip.1.0.0.4", "ip-1-0-0-4"), "queue2-dynamic-c5.xlarge-1",)]),
            ],
        ),
    ],
    ids=["normal", "client_error", "no_update", "batch_size1", "batch_size2"],
)
def test_resume(
    node_info,
    resume_config,
    run_instance_calls,
    run_instance_side_effects,
    expected_failed_nodes,
    expected_launched_nodes_calls,
    mocker,
):
    # reset failed_nodes to empty list
    mocker.patch("slurm_cloud_bursting.resume.failed_nodes", [])
    # patch internal functions
    handle_failure_mocker = mocker.patch("slurm_cloud_bursting.resume._handle_failed_nodes", autospec=True)
    update_node_mocker = mocker.patch("slurm_cloud_bursting.resume._update_slurm_node_addrs", autospec=True)
    mocker.patch("slurm_cloud_bursting.resume.get_nodes_info", return_value=node_info)
    # patch boto3 call
    mock_boto_client = mocker.patch("slurm_cloud_bursting.resume.boto3.client")
    mock_boto_client.return_value.run_instances.side_effect = run_instance_side_effects
    # run test
    _resume("placeholder_arg_nodes", resume_config)
    mock_boto_client.return_value.run_instances.assert_has_calls(run_instance_calls)
    if expected_failed_nodes:
        handle_failure_mocker.assert_called_with(expected_failed_nodes)
    if expected_launched_nodes_calls:
        update_node_mocker.assert_has_calls(expected_launched_nodes_calls)
