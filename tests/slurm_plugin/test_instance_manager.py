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
from datetime import datetime, timezone
from unittest.mock import call

import botocore
import pytest
import slurm_plugin
from assertpy import assert_that
from slurm_plugin.fleet_manager import EC2Instance
from slurm_plugin.instance_manager import InstanceManager
from slurm_plugin.slurm_resources import (
    EC2_HEALTH_STATUS_UNHEALTHY_STATES,
    EC2_INSTANCE_ALIVE_STATES,
    EC2_SCHEDULED_EVENT_CODES,
    EC2InstanceHealthState,
)

from tests.common import MockedBoto3Request, client_error


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
    return "slurm_plugin.instance_manager.boto3"  # FIXME


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
            head_node_private_ip="head.node.ip",
            head_node_hostname="head-node-hostname",
            hosted_zone="hosted_zone",
            dns_domain="dns.domain",
            use_private_hostname=False,
            instance_name_type_mapping={"c5xlarge": "c5.xlarge"},
            launch_overrides={
                "queue3": {
                    "p4d.24xlarge": {
                        "CapacityReservationSpecification": {
                            "CapacityReservationTarget": {"CapacityReservationId": "cr-123"}
                        }
                    },
                    "c5.xlarge": {
                        "CapacityReservationSpecification": {
                            "CapacityReservationTarget": {"CapacityReservationId": "cr-456"}
                        }
                    },
                },
            },
        )
        mocker.patch.object(instance_manager, "_table")
        return instance_manager

    @pytest.mark.parametrize(
        (
            "instances_to_launch",
            "launch_batch_size",
            "update_node_address",
            "all_or_nothing_batch",
            "launched_instances",
            "expected_failed_nodes",
            "expected_update_nodes_calls",
        ),
        [
            # normal
            (
                {
                    "queue1": {
                        "c5xlarge": ["queue1-st-c5xlarge-2"],
                        "c52xlarge": ["queue1-st-c52xlarge-1"],
                    },
                    "queue2": {"c5xlarge": ["queue2-st-c5xlarge-1", "queue2-dy-c5xlarge-1"]},
                },
                10,
                True,
                False,
                [
                    {
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
                    {
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
                    {
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
                ],
                {},
                [
                    call(
                        ["queue1-st-c5xlarge-2"],
                        [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue1-st-c52xlarge-1"],
                        [EC2Instance("i-23456", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue2-st-c5xlarge-1", "queue2-dy-c5xlarge-1"],
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
                        "c5xlarge": ["queue1-st-c5xlarge-2"],
                        "c52xlarge": ["queue1-st-c52xlarge-1"],
                    },
                    "queue2": {"c5xlarge": ["queue2-st-c5xlarge-1", "queue2-dy-c5xlarge-1"]},
                },
                10,
                True,
                False,
                [
                    {
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
                    client_error("some_error_code"),
                    {
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
                ],
                {"some_error_code": {"queue1-st-c52xlarge-1"}},
                [
                    call(
                        ["queue1-st-c5xlarge-2"],
                        [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue2-st-c5xlarge-1", "queue2-dy-c5xlarge-1"],
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
                {"queue1": {"c5xlarge": ["queue1-st-c5xlarge-2"]}},
                10,
                False,
                False,
                [
                    {
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
                ],
                {},
                None,
            ),
            # batch_size1
            (
                {
                    "queue1": {
                        "c5xlarge": ["queue1-st-c5xlarge-2"],
                        "c52xlarge": ["queue1-st-c52xlarge-1"],
                    },
                    "queue2": {
                        "c5xlarge": [
                            "queue2-st-c5xlarge-1",
                            "queue2-st-c5xlarge-2",
                            "queue2-dy-c5xlarge-1",
                        ],
                    },
                },
                3,
                True,
                False,
                [
                    {
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
                    client_error("InsufficientHostCapacity"),
                    client_error("ServiceUnavailable"),
                ],
                {
                    "InsufficientHostCapacity": {"queue1-st-c52xlarge-1"},
                    "ServiceUnavailable": {"queue2-st-c5xlarge-1", "queue2-dy-c5xlarge-1", "queue2-st-c5xlarge-2"},
                },
                [
                    call(
                        ["queue1-st-c5xlarge-2"],
                        [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    )
                ],
            ),
            # batch_size2
            (
                {
                    "queue1": {
                        "c5xlarge": ["queue1-st-c5xlarge-2"],
                        "c52xlarge": ["queue1-st-c52xlarge-1"],
                    },
                    "queue2": {
                        "c5xlarge": [
                            "queue2-st-c5xlarge-1",
                            "queue2-st-c5xlarge-2",
                            "queue2-dy-c5xlarge-1",
                        ],
                    },
                },
                1,
                True,
                False,
                [
                    {
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
                    client_error("InsufficientVolumeCapacity"),
                    {
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
                    client_error("InternalError"),
                    {
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
                ],
                {"InsufficientVolumeCapacity": {"queue1-st-c52xlarge-1"}, "InternalError": {"queue2-st-c5xlarge-2"}},
                [
                    call(
                        ["queue1-st-c5xlarge-2"],
                        [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue2-st-c5xlarge-1"],
                        [EC2Instance("i-34567", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue2-dy-c5xlarge-1"],
                        [EC2Instance("i-45678", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                ],
            ),
            # partial_launch
            (
                {
                    "queue2": {
                        "c5xlarge": [
                            "queue2-st-c5xlarge-1",
                            "queue2-st-c5xlarge-2",
                            "queue2-dy-c5xlarge-1",
                        ],
                    },
                },
                10,
                True,
                False,
                # Simulate the case that only a part of the requested capacity is launched
                [
                    {
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
                    client_error("LimitedInstanceCapacity"),
                ],
                {"LimitedInstanceCapacity": {"queue2-st-c5xlarge-2", "queue2-dy-c5xlarge-1"}},
                [
                    call(
                        ["queue2-st-c5xlarge-1", "queue2-st-c5xlarge-2", "queue2-dy-c5xlarge-1"],
                        [EC2Instance("i-45678", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    )
                ],
            ),
            # all_or_nothing
            (
                {
                    "queue2": {
                        "c5xlarge": [
                            "queue2-st-c5xlarge-1",
                            "queue2-st-c5xlarge-2",
                            "queue2-dy-c5xlarge-1",
                            "queue2-dy-c5xlarge-2",
                            "queue2-dy-c5xlarge-3",
                        ],
                    },
                },
                3,
                True,
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
                    client_error("InsufficientInstanceCapacity"),
                ],
                {"InsufficientInstanceCapacity": {"queue2-dy-c5xlarge-2", "queue2-dy-c5xlarge-3"}},
                [
                    call(
                        ["queue2-st-c5xlarge-1", "queue2-st-c5xlarge-2", "queue2-dy-c5xlarge-1"],
                        [
                            EC2Instance(
                                "i-11111", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-22222", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-33333", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    )
                ],
            ),
            # override_runinstances
            (
                {
                    "queue3": {
                        "c5xlarge": ["queue3-st-c5xlarge-2"],
                        "c52xlarge": ["queue3-st-c52xlarge-1"],
                        "p4d24xlarge": ["queue3-st-p4d24xlarge-1"],
                    },
                    "queue2": {"c5xlarge": ["queue2-st-c5xlarge-1", "queue2-dy-c5xlarge-1"]},
                },
                10,
                True,
                False,
                [
                    {
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
                    {
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
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12346",
                                "InstanceType": "p4d.24xlarge",
                                "PrivateIpAddress": "ip.1.0.0.5",
                                "PrivateDnsName": "ip-1-0-0-5",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
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
                ],
                {},
                [
                    call(
                        ["queue3-st-c5xlarge-2"],
                        [EC2Instance("i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue3-st-c52xlarge-1"],
                        [EC2Instance("i-23456", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue3-st-p4d24xlarge-1"],
                        [EC2Instance("i-12346", "ip.1.0.0.5", "ip-1-0-0-5", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                    ),
                    call(
                        ["queue2-st-c5xlarge-1", "queue2-dy-c5xlarge-1"],
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
        ],
        ids=[
            "normal",
            "client_error",
            "no_update",
            "batch_size1",
            "batch_size2",
            "partial_launch",
            "all_or_nothing",
            "override_runinstances",
        ],
    )
    def test_add_instances(
        self,
        boto3_stubber,
        instances_to_launch,
        launch_batch_size,
        update_node_address,
        all_or_nothing_batch,
        launched_instances,
        expected_failed_nodes,
        expected_update_nodes_calls,
        mocker,
        instance_manager,
        fleet_manager_factory,
    ):
        mocker.patch("slurm_plugin.instance_manager.update_nodes")

        # patch internal functions
        instance_manager._store_assigned_hostnames = mocker.MagicMock()
        instance_manager._update_dns_hostnames = mocker.MagicMock()

        # Mock _update_slurm_node_addrs but still allow original code to execute
        original_update_func = instance_manager._update_slurm_node_addrs
        instance_manager._update_slurm_node_addrs = mocker.MagicMock(side_effect=original_update_func)
        instance_manager._parse_requested_instances = mocker.MagicMock(return_value=instances_to_launch)
        # patch fleet manager calls
        mocker.patch.object(
            slurm_plugin.fleet_manager.Ec2RunInstancesManager,
            "_launch_instances",
            side_effect=launched_instances,
        )

        # run test
        instance_manager.add_instances_for_nodes(
            node_list=["placeholder_node_list"],
            launch_batch_size=launch_batch_size,
            update_node_address=update_node_address,
            all_or_nothing_batch=all_or_nothing_batch,
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
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                call(["queue1-st-c5xlarge-1"], nodeaddrs=["ip-1"], nodehostnames=None),
                {},
                False,
                "dns.domain",
            ),
            (
                ["queue1-st-c5xlarge-1"],
                {},
                None,
                {"LimitedInstanceCapacity": {"queue1-st-c5xlarge-1"}},
                False,
                "dns.domain",
            ),
            (
                ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3", "queue1-st-c5xlarge-4"],
                [
                    EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                    EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
                ],
                call(["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2"], nodeaddrs=["ip-1", "ip-2"], nodehostnames=None),
                {"LimitedInstanceCapacity": {"queue1-st-c5xlarge-4", "queue1-st-c5xlarge-3"}},
                False,
                "dns.domain",
            ),
            (
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                call(["queue1-st-c5xlarge-1"], nodeaddrs=["ip-1"], nodehostnames=["hostname-1"]),
                {},
                True,
                "dns.domain",
            ),
            (
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                call(["queue1-st-c5xlarge-1"], nodeaddrs=["ip-1"], nodehostnames=None),
                {},
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
        mock_update_nodes = mocker.patch("slurm_plugin.instance_manager.update_nodes")
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
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                None,
                "Empty table name configuration parameter",
            ),
            (
                "table_name",
                ["queue1-st-c5xlarge-1"],
                [],
                None,
                None,
            ),
            (
                "table_name",
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                [
                    call(
                        Item={
                            "Id": "queue1-st-c5xlarge-1",
                            "InstanceId": "id-1",
                            "HeadNodePrivateIp": "head.node.ip",
                            "HeadNodeHostname": "head-node-hostname",
                        }
                    )
                ],
                None,
            ),
            (
                "table_name",
                ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2"],
                [
                    EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                    EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
                ],
                [
                    call(
                        Item={
                            "Id": "queue1-st-c5xlarge-1",
                            "InstanceId": "id-1",
                            "HeadNodePrivateIp": "head.node.ip",
                            "HeadNodeHostname": "head-node-hostname",
                        }
                    ),
                    call(
                        Item={
                            "Id": "queue1-st-c5xlarge-2",
                            "InstanceId": "id-2",
                            "HeadNodePrivateIp": "head.node.ip",
                            "HeadNodeHostname": "head-node-hostname",
                        }
                    ),
                ],
                None,
            ),
        ],
        ids=("empty_table", "nothing_stored", "single_store", "multiple_store"),
    )
    def test_store_assigned_hostnames(
        self,
        table_name,
        node_list,
        slurm_nodes,
        expected_put_item_calls,
        expected_message,
        mocker,
        instance_manager,
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
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                None,
                "Empty table name configuration parameter",
                False,
            ),
            (
                "hosted_zone",
                None,
                ["queue1-st-c5xlarge-1"],
                [],
                None,
                "Empty table name configuration parameter",
                False,
            ),
            ("hosted_zone", "dns.domain", ["queue1-st-c5xlarge-1"], [], None, None, False),
            (
                "hosted_zone",
                "dns.domain",
                ["queue1-st-c5xlarge-1"],
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
                                        "Name": "queue1-st-c5xlarge-1.dns.domain",
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
                ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2"],
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
                                            "Name": "queue1-st-c5xlarge-1.dns.domain",
                                            "ResourceRecords": [{"Value": "ip-1"}],
                                            "Type": "A",
                                            "TTL": 120,
                                        },
                                    },
                                    {
                                        "Action": "UPSERT",
                                        "ResourceRecordSet": {
                                            "Name": "queue1-st-c5xlarge-2.dns.domain",
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
                ["queue1-st-c5xlarge-1"],
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
                                        "Name": "queue1-st-c5xlarge-1.dns.domain",
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
        ids=(
            "no_hosted_zone",
            "no_domain_name",
            "nothing_stored",
            "single_store",
            "multiple_store",
            "client_error",
        ),
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
        ("node_list", "expected_results", "expected_failed_nodes"),
        [
            (
                [
                    "queue1-st-c5xlarge-1",
                    "queue1-st-c5xlarge-2",
                    "queue1-dy-c5xlarge-201",
                    "queue2-st-g34xlarge-1",
                    "in-valid/queue.name-st-c5xlarge-2",
                    "noBrackets-st-c5xlarge-[1-2]",
                    "queue2-dy-g38xlarge-1",
                    "queue2-st-u6tb1metal-1",
                    "queue2-invalidnodetype-c5xlarge-12",
                    "queuename-with-dash-and_underscore-st-i3enmetal2tb-1",
                ],
                {
                    "queue1": {
                        "c5xlarge": [
                            "queue1-st-c5xlarge-1",
                            "queue1-st-c5xlarge-2",
                            "queue1-dy-c5xlarge-201",
                        ]
                    },
                    "queue2": {
                        "g34xlarge": ["queue2-st-g34xlarge-1"],
                        "g38xlarge": ["queue2-dy-g38xlarge-1"],
                        "u6tb1metal": ["queue2-st-u6tb1metal-1"],
                    },
                },
                {
                    "InvalidNodenameError": {
                        "queue2-invalidnodetype-c5xlarge-12",
                        "noBrackets-st-c5xlarge-[1-2]",
                        "queuename-with-dash-and_underscore-st-i3enmetal2tb-1",
                        "in-valid/queue.name-st-c5xlarge-2",
                    }
                },
            ),
        ],
    )
    def test_parse_requested_instances(
        self, node_list, expected_results, expected_failed_nodes, instance_manager, mocker
    ):
        # Mock instance name/type mapping
        instance_name_type_mapping = {
            "queue1": {"c5xlarge": "c5.xlarge"},
            "queue2": {"g34xlarge": "g3.4xlarge", "g38xlarge": "g3.8xlarge", "u6tb1metal": "u-6tb1.metal"},
            "queuename-with-dash-and_underscore": {
                "i3enmetal2tb": "i3en.metal-2tb",
            },
        }
        instance_manager._instance_name_type_mapping = instance_name_type_mapping

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
            pytest.param(
                {"include_head_node": False, "alive_states_only": True},
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
                            {"Name": "tag:parallelcluster:cluster-name", "Values": ["hit"]},
                            {"Name": "instance-state-name", "Values": list(EC2_INSTANCE_ALIVE_STATES)},
                            {"Name": "tag:parallelcluster:node-type", "Values": ["Compute"]},
                        ],
                        "MaxResults": 1000,
                    },
                    generate_error=False,
                ),
                [
                    EC2Instance("i-1", "ip-1", "hostname", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                    EC2Instance("i-2", "ip-2", "hostname", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                ],
                id="default",
            ),
            pytest.param(
                {"include_head_node": False, "alive_states_only": True},
                MockedBoto3Request(
                    method="describe_instances",
                    response={"Reservations": []},
                    expected_params={
                        "Filters": [
                            {"Name": "tag:parallelcluster:cluster-name", "Values": ["hit"]},
                            {"Name": "instance-state-name", "Values": list(EC2_INSTANCE_ALIVE_STATES)},
                            {"Name": "tag:parallelcluster:node-type", "Values": ["Compute"]},
                        ],
                        "MaxResults": 1000,
                    },
                    generate_error=False,
                ),
                [],
                id="empty_response",
            ),
            pytest.param(
                {"include_head_node": True, "alive_states_only": False},
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
                    expected_params={
                        "Filters": [{"Name": "tag:parallelcluster:cluster-name", "Values": ["hit"]}],
                        "MaxResults": 1000,
                    },
                    generate_error=False,
                ),
                [EC2Instance("i-1", "ip-1", "hostname", datetime(2020, 1, 1, tzinfo=timezone.utc))],
                id="custom_args",
            ),
            pytest.param(
                {"include_head_node": False, "alive_states_only": True},
                MockedBoto3Request(
                    method="describe_instances",
                    response={
                        "Reservations": [
                            {
                                "Instances": [
                                    {
                                        "InstanceId": "i-1",
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
                            {"Name": "tag:parallelcluster:cluster-name", "Values": ["hit"]},
                            {"Name": "instance-state-name", "Values": list(EC2_INSTANCE_ALIVE_STATES)},
                            {"Name": "tag:parallelcluster:node-type", "Values": ["Compute"]},
                        ],
                        "MaxResults": 1000,
                    },
                    generate_error=False,
                ),
                [
                    EC2Instance("i-2", "ip-2", "hostname", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                ],
                id="no_ec2_info",
            ),
        ],
    )
    def test_get_cluster_instances(
        self, mock_kwargs, mocked_boto3_request, expected_parsed_result, instance_manager, boto3_stubber, mocker
    ):
        # patch boto3 call
        boto3_stubber("ec2", mocked_boto3_request)
        # run test
        result = instance_manager.get_cluster_instances(**mock_kwargs)
        assert_that(result).is_equal_to(expected_parsed_result)
