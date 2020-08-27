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


import logging
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import call

import botocore
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


class TestInstanceManager:
    class BatchWriterMock:
        """Utility class to mock batch_writer."""

        def __enter__(self, *args):
            return self

        def __exit__(self, *args):
            pass

    @pytest.fixture
    def instance_manager(self, mocker):
        instance_manager = InstanceManager(
            region="us-east-2",
            cluster_name="hit",
            boto3_config=botocore.config.Config(),
            table_name="table_name",
            master_private_ip="master.ip",
            master_hostname="master-hostname",
            hosted_zone="hosted_zone",
            dns_domain="dns.domain",
            use_private_hostname=False,
        )
        mocker.patch.object(instance_manager, "_table")
        return instance_manager

    @pytest.mark.parametrize(
        (
            "instances_to_launch",
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
                    "queue1": {
                        "c5.xlarge": ["queue1-static-c5-xlarge-2"],
                        "c5.2xlarge": ["queue1-static-c5-2xlarge-1"],
                    },
                    "queue2": {"c5.xlarge": ["queue2-static-c5-xlarge-1", "queue2-dynamic-c5-xlarge-1"]},
                },
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
                        ["queue1-static-c5-xlarge-2"],
                        [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue1-static-c5-2xlarge-1"],
                        [EC2Instance("i-23456", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue2-static-c5-xlarge-1", "queue2-dynamic-c5-xlarge-1"],
                        [
                            EC2Instance(
                                "i-34567", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-45678", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    ),
                ],
            ),
            # client_error
            (
                {
                    "queue1": {
                        "c5.xlarge": ["queue1-static-c5-xlarge-2"],
                        "c5.2xlarge": ["queue1-static-c5-2xlarge-1"],
                    },
                    "queue2": {"c5.xlarge": ["queue2-static-c5-xlarge-1", "queue2-dynamic-c5-xlarge-1"]},
                },
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
                ["queue1-static-c5-2xlarge-1"],
                [
                    call(
                        ["queue1-static-c5-xlarge-2"],
                        [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue2-static-c5-xlarge-1", "queue2-dynamic-c5-xlarge-1"],
                        [
                            EC2Instance(
                                "i-34567", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-45678", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    ),
                ],
            ),
            # no_update
            (
                {"queue1": {"c5.xlarge": ["queue1-static-c5-xlarge-2"]}},
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
                    "queue1": {
                        "c5.xlarge": ["queue1-static-c5-xlarge-2"],
                        "c5.2xlarge": ["queue1-static-c5-2xlarge-1"],
                    },
                    "queue2": {
                        "c5.xlarge": [
                            "queue2-static-c5-xlarge-1",
                            "queue2-static-c5-xlarge-2",
                            "queue2-dynamic-c5-xlarge-1",
                        ],
                    },
                },
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
                    "queue1-static-c5-2xlarge-1",
                    "queue2-static-c5-xlarge-1",
                    "queue2-static-c5-xlarge-2",
                    "queue2-dynamic-c5-xlarge-1",
                ],
                [
                    call(
                        ["queue1-static-c5-xlarge-2"],
                        [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    )
                ],
            ),
            # batch_size2
            (
                {
                    "queue1": {
                        "c5.xlarge": ["queue1-static-c5-xlarge-2"],
                        "c5.2xlarge": ["queue1-static-c5-2xlarge-1"],
                    },
                    "queue2": {
                        "c5.xlarge": [
                            "queue2-static-c5-xlarge-1",
                            "queue2-static-c5-xlarge-2",
                            "queue2-dynamic-c5-xlarge-1",
                        ],
                    },
                },
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
                ["queue1-static-c5-2xlarge-1", "queue2-static-c5-xlarge-2"],
                [
                    call(
                        ["queue1-static-c5-xlarge-2"],
                        [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue2-static-c5-xlarge-1"],
                        [EC2Instance("i-34567", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue2-dynamic-c5-xlarge-1"],
                        [EC2Instance("i-45678", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                ],
            ),
            (
                {
                    "queue2": {
                        "c5.xlarge": [
                            "queue2-static-c5-xlarge-1",
                            "queue2-static-c5-xlarge-2",
                            "queue2-dynamic-c5-xlarge-1",
                        ],
                    },
                },
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
                ["queue2-static-c5-xlarge-2", "queue2-dynamic-c5-xlarge-1"],
                [
                    call(
                        ["queue2-static-c5-xlarge-1", "queue2-static-c5-xlarge-2", "queue2-dynamic-c5-xlarge-1"],
                        [EC2Instance("i-45678", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    )
                ],
            ),
        ],
        ids=["normal", "client_error", "no_update", "batch_size1", "batch_size2", "partial_launch"],
    )
    def test_add_instances(
        self,
        boto3_stubber,
        instances_to_launch,
        launch_batch_size,
        update_node_address,
        mocked_boto3_request,
        expected_failed_nodes,
        expected_update_nodes_calls,
        mocker,
        instance_manager,
    ):
        mocker.patch("slurm_plugin.common.update_nodes")
        # patch internal functions
        instance_manager._store_assigned_hostnames = mocker.MagicMock()
        instance_manager._update_dns_hostnames = mocker.MagicMock()

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
        (
            "node_list, launched_nodes, expected_update_nodes_call, "
            "expected_failed_nodes, use_private_hostname, dns_domain"
        ),
        [
            (
                ["node-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                call(["node-1"], nodeaddrs=["ip-1"], nodehostnames=None),
                [],
                False,
                "dns.domain",
            ),
            (["node-1"], [], None, ["node-1"], False, "dns.domain"),
            (
                ["node-1", "node-2", "node-3", "node-4"],
                [
                    EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                    EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
                ],
                call(["node-1", "node-2"], nodeaddrs=["ip-1", "ip-2"], nodehostnames=None),
                ["node-3", "node-4"],
                False,
                "dns.domain",
            ),
            (
                ["node-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                call(["node-1"], nodeaddrs=["ip-1"], nodehostnames=["hostname-1"]),
                [],
                True,
                "dns.domain",
            ),
            (
                ["node-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                call(["node-1"], nodeaddrs=["ip-1"], nodehostnames=None),
                [],
                False,
                "",
            ),
        ],
        ids=("all_launched", "nothing_launched", "partial_launched", "forced_private_hostname", "no_dns_domain"),
    )
    def test_update_slurm_node_addrs(
        self,
        node_list,
        launched_nodes,
        expected_update_nodes_call,
        expected_failed_nodes,
        use_private_hostname,
        dns_domain,
        instance_manager,
        mocker,
    ):
        mock_update_nodes = mocker.patch("slurm_plugin.common.update_nodes")
        instance_manager._use_private_hostname = use_private_hostname
        instance_manager._dns_domain = dns_domain

        instance_manager._update_slurm_node_addrs(node_list, launched_nodes)
        if expected_update_nodes_call:
            mock_update_nodes.assert_called_once()
            mock_update_nodes.assert_has_calls([expected_update_nodes_call])
        else:
            mock_update_nodes.assert_not_called()
        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)

    @pytest.mark.parametrize(
        "table_name, node_list, slurm_nodes, expected_put_item_calls, expected_message",
        [
            (
                None,
                ["node-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                None,
                "Empty table name configuration parameter",
            ),
            ("table_name", ["node-1"], [], None, None,),
            (
                "table_name",
                ["node-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                [
                    call(
                        Item={
                            "Id": "node-1",
                            "InstanceId": "id-1",
                            "MasterPrivateIp": "master.ip",
                            "MasterHostname": "master-hostname",
                        }
                    )
                ],
                None,
            ),
            (
                "table_name",
                ["node-1", "node-2"],
                [
                    EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                    EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
                ],
                [
                    call(
                        Item={
                            "Id": "node-1",
                            "InstanceId": "id-1",
                            "MasterPrivateIp": "master.ip",
                            "MasterHostname": "master-hostname",
                        }
                    ),
                    call(
                        Item={
                            "Id": "node-2",
                            "InstanceId": "id-2",
                            "MasterPrivateIp": "master.ip",
                            "MasterHostname": "master-hostname",
                        }
                    ),
                ],
                None,
            ),
        ],
        ids=("empty_table", "nothing_stored", "single_store", "multiple_store"),
    )
    def test_store_assigned_hostnames(
        self, table_name, node_list, slurm_nodes, expected_put_item_calls, expected_message, mocker, instance_manager,
    ):
        # Mock other methods
        instance_manager._update_dns_hostnames = mocker.MagicMock()
        instance_manager._update_slurm_node_addrs = mocker.MagicMock()

        # mock inputs
        launched_nodes = node_list[: len(slurm_nodes)]
        assigned_nodes = dict(zip(launched_nodes, slurm_nodes))

        if not table_name:
            instance_manager._table = None
            with pytest.raises(Exception, match=expected_message):
                instance_manager._store_assigned_hostnames(assigned_nodes)
        else:
            # mock batch_writer
            batch_writer_mock = self.BatchWriterMock()
            batch_writer_mock.put_item = mocker.MagicMock()
            mocker.patch.object(instance_manager._table, "batch_writer", return_value=batch_writer_mock)

            # call function and verify execution
            instance_manager._store_assigned_hostnames(assigned_nodes)
            if expected_put_item_calls:
                batch_writer_mock.put_item.assert_has_calls(expected_put_item_calls)
            else:
                batch_writer_mock.put_item.assert_not_called()

    @pytest.mark.parametrize(
        "hosted_zone, dns_domain, node_list, slurm_nodes, mocked_boto3_request, expected_message, expected_failure",
        [
            (
                None,
                "dns.domain",
                ["node-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                None,
                "Empty table name configuration parameter",
                False,
            ),
            ("hosted_zone", None, ["node-1"], [], None, "Empty table name configuration parameter", False),
            ("hosted_zone", "dns.domain", ["node-1"], [], None, None, False),
            (
                "hosted_zone",
                "dns.domain",
                ["node-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                MockedBoto3Request(
                    method="change_resource_record_sets",
                    response={
                        "ChangeInfo": {
                            "Id": "string",
                            "Status": "PENDING",
                            "SubmittedAt": datetime(2020, 1, 1, tzinfo=timezone.utc),
                        }
                    },
                    expected_params={
                        "HostedZoneId": "hosted_zone",
                        "ChangeBatch": {
                            "Changes": [
                                {
                                    "Action": "UPSERT",
                                    "ResourceRecordSet": {
                                        "Name": "node-1.dns.domain",
                                        "ResourceRecords": [{"Value": "ip-1"}],
                                        "Type": "A",
                                        "TTL": 120,
                                    },
                                }
                            ]
                        },
                    },
                ),
                None,
                False,
            ),
            (
                "hosted_zone",
                "dns.domain",
                ["node-1", "node-2"],
                [
                    EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                    EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
                ],
                [
                    MockedBoto3Request(
                        method="change_resource_record_sets",
                        response={
                            "ChangeInfo": {
                                "Id": "string",
                                "Status": "PENDING",
                                "SubmittedAt": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            }
                        },
                        expected_params={
                            "HostedZoneId": "hosted_zone",
                            "ChangeBatch": {
                                "Changes": [
                                    {
                                        "Action": "UPSERT",
                                        "ResourceRecordSet": {
                                            "Name": "node-1.dns.domain",
                                            "ResourceRecords": [{"Value": "ip-1"}],
                                            "Type": "A",
                                            "TTL": 120,
                                        },
                                    },
                                    {
                                        "Action": "UPSERT",
                                        "ResourceRecordSet": {
                                            "Name": "node-2.dns.domain",
                                            "ResourceRecords": [{"Value": "ip-2"}],
                                            "Type": "A",
                                            "TTL": 120,
                                        },
                                    },
                                ],
                            },
                        },
                    )
                ],
                None,
                False,
            ),
            (
                "hosted_zone",
                "dns.domain",
                ["node-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                MockedBoto3Request(
                    method="change_resource_record_sets",
                    response={},
                    expected_params={
                        "HostedZoneId": "hosted_zone",
                        "ChangeBatch": {
                            "Changes": [
                                {
                                    "Action": "UPSERT",
                                    "ResourceRecordSet": {
                                        "Name": "node-1.dns.domain",
                                        "ResourceRecords": [{"Value": "ip-1"}],
                                        "Type": "A",
                                        "TTL": 120,
                                    },
                                }
                            ]
                        },
                    },
                    generate_error=True,
                ),
                None,
                True,
            ),
        ],
        ids=("no_hosted_zone", "no_domain_name", "nothing_stored", "single_store", "multiple_store", "client_error",),
    )
    def test_update_dns_hostnames(
        self,
        hosted_zone,
        dns_domain,
        node_list,
        slurm_nodes,
        mocked_boto3_request,
        expected_message,
        expected_failure,
        mocker,
        boto3_stubber,
        instance_manager,
        caplog,
    ):
        # Mock other methods
        instance_manager._update_slurm_node_addrs = mocker.MagicMock()
        instance_manager._store_assigned_hostnames = mocker.MagicMock()

        if mocked_boto3_request:
            boto3_stubber("route53", mocked_boto3_request)

        # mock inputs
        instance_manager._hosted_zone = hosted_zone
        instance_manager._dns_domain = dns_domain
        launched_nodes = node_list[: len(slurm_nodes)]
        assigned_nodes = dict(zip(launched_nodes, slurm_nodes))
        if expected_message:
            with caplog.at_level(logging.INFO):
                instance_manager._update_dns_hostnames(assigned_nodes)
                assert_that(caplog.text).contains("Empty DNS domain name or hosted zone configuration parameter")
        if expected_failure:
            with pytest.raises(Exception, match="calling the ChangeResourceRecordSets"):
                instance_manager._update_dns_hostnames(assigned_nodes)
        else:
            instance_manager._update_dns_hostnames(assigned_nodes)

    @pytest.mark.parametrize(
        ("nodename", "expected_queue", "expected_instance_type", "expected_failure"),
        [
            ("queue1-static-c5-xlarge-1", "queue1", "c5.xlarge", False),
            ("queue-1-static-c5-xlarge-1", "queue-1", "c5.xlarge", False),
            # ("queue1-static-i3en-metal-2tb-1", "queue1", "i3en.metal-2tb", False), not supported for now
            ("queue1-static-u-6tb1-metal-1", "queue1", "u-6tb1.metal", False),
            ("queue1-static-c5.xlarge-1", "queue1", "c5.xlarge", True),
            ("queue_1-static-c5-xlarge-1", "queue_1", "c5.xlarge", True),
        ],
    )
    def test_parse_nodename(self, nodename, expected_queue, expected_instance_type, expected_failure, instance_manager):
        if expected_failure:
            with pytest.raises(Exception):
                instance_manager._parse_nodename(nodename)
        else:
            queue_name, instance_type = instance_manager._parse_nodename(nodename)
            assert_that(expected_queue).is_equal_to(queue_name)
            assert_that(expected_instance_type).is_equal_to(instance_type)

    @pytest.mark.parametrize(
        ("node_list", "expected_results", "expected_failed_nodes"),
        [
            (
                [
                    "queue1-static-c5-xlarge-1",
                    "queue1-static-c5-xlarge-2",
                    "queue1-dynamic-c5-xlarge-201",
                    "queue2-static-g3-4xlarge-1",
                    "in-valid/queue.name-static-c5-xlarge-2",
                    "noBrackets-static-c5-xlarge-[1-2]",
                    "queue2-dynamic-g3-8xlarge-1",
                    "queue2-static-u-6tb1-metal-1",
                    "queue2-invalidnodetype-c5-xlarge-12",
                    "queuename-with-dash-and_underscore-static-i3en-metal-2tb-1",
                ],
                {
                    "queue1": {
                        "c5.xlarge": [
                            "queue1-static-c5-xlarge-1",
                            "queue1-static-c5-xlarge-2",
                            "queue1-dynamic-c5-xlarge-201",
                        ]
                    },
                    "queue2": {
                        "g3.4xlarge": ["queue2-static-g3-4xlarge-1"],
                        "g3.8xlarge": ["queue2-dynamic-g3-8xlarge-1"],
                        "u-6tb1.metal": ["queue2-static-u-6tb1-metal-1"],
                    },
                },
                [
                    "in-valid/queue.name-static-c5-xlarge-2",
                    "noBrackets-static-c5-xlarge-[1-2]",
                    "queue2-invalidnodetype-c5-xlarge-12",
                    "queuename-with-dash-and_underscore-static-i3en-metal-2tb-1",
                ],
            ),
        ],
    )
    def test_parse_requested_instances(self, node_list, expected_results, expected_failed_nodes, instance_manager):
        assert_that(instance_manager._parse_requested_instances(node_list)).is_equal_to(expected_results)
        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)

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
    def test_delete_instances(
        self, boto3_stubber, instance_ids_to_name, batch_size, mocked_boto3_request, instance_manager
    ):
        # patch boto3 call
        boto3_stubber("ec2", mocked_boto3_request)
        # run test
        instance_manager.delete_instances(instance_ids_to_name, batch_size)

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
    def get_unhealthy_cluster_instance_status(
        self, instance_ids, mocked_boto3_request, expected_parsed_result, boto3_stubber, instance_manager
    ):
        # patch boto3 call
        boto3_stubber("ec2", mocked_boto3_request)
        # run test
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
                            {"Name": "tag:ClusterName", "Values": ["hit"]},
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
                            {"Name": "tag:ClusterName", "Values": ["hit"]},
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
                    expected_params={"Filters": [{"Name": "tag:ClusterName", "Values": ["hit"]}], "MaxResults": 1000},
                    generate_error=False,
                ),
                [EC2Instance("i-1", "ip-1", "hostname", datetime(2020, 1, 1, tzinfo=timezone.utc))],
            ),
        ],
        ids=["default", "empty_response", "custom_args"],
    )
    def test_get_cluster_instances(
        self, mock_kwargs, mocked_boto3_request, expected_parsed_result, instance_manager, boto3_stubber
    ):
        # patch boto3 call
        boto3_stubber("ec2", mocked_boto3_request)
        # run test
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
