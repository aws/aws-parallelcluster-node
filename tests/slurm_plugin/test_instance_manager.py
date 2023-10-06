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
import collections
import logging
import os
import re
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable
from unittest.mock import call

import botocore
import pytest
import slurm_plugin
from assertpy import assert_that
from slurm_plugin.common import ScalingStrategy
from slurm_plugin.fleet_manager import EC2Instance
from slurm_plugin.instance_manager import (
    HostnameDnsStoreError,
    InstanceManager,
    InstanceManagerFactory,
    InstanceToNodeAssignmentError,
    NodeAddrUpdateError,
)
from slurm_plugin.slurm_resources import (
    EC2_HEALTH_STATUS_UNHEALTHY_STATES,
    EC2_INSTANCE_ALIVE_STATES,
    EC2_SCHEDULED_EVENT_CODES,
    EC2InstanceHealthState,
    SlurmNode,
    SlurmResumeJob,
    StaticNode,
)

from tests.common import FLEET_CONFIG, MockedBoto3Request, client_error


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
        instance_manager = InstanceManagerFactory.get_manager(
            region="us-east-2",
            cluster_name="hit",
            boto3_config=botocore.config.Config(),
            table_name="table_name",
            head_node_private_ip="head.node.ip",
            head_node_hostname="head-node-hostname",
            hosted_zone="hosted_zone",
            dns_domain="dns.domain",
            use_private_hostname=False,
            fleet_config=FLEET_CONFIG,
            run_instances_overrides={},
            create_fleet_overrides={},
            job_level_scaling=True,
        )
        table_mock = mocker.patch.object(instance_manager, "_table")
        table_mock.table_name = "table_name"
        return instance_manager

    @pytest.mark.parametrize(
        "mock_compute_nodes, terminate_batch_size, expected_return_code, expected_instance_ids",
        [
            (
                [
                    [],
                ],
                1,
                True,
                [],
            ),
            (
                [
                    [
                        EC2Instance("i-2", "ip-2", "hostname", datetime(2020, 1, 1, tzinfo=timezone.utc)),
                    ],
                ],
                10,
                True,
                ["i-2"],
            ),
            (
                Exception(),
                10,
                False,
                [],
            ),
        ],
    )
    def test_terminate_all_compute_nodes(
        self,
        mocker,
        instance_manager,
        mock_compute_nodes,
        terminate_batch_size,
        expected_return_code,
        expected_instance_ids,
    ):
        # patch internal functions
        instance_manager.get_cluster_instances = mocker.MagicMock(side_effect=mock_compute_nodes)
        instance_manager.delete_instances = mocker.MagicMock()

        return_code = instance_manager.terminate_all_compute_nodes(terminate_batch_size)
        assert_that(return_code).is_equal_to(expected_return_code)

        if not isinstance(mock_compute_nodes, Exception):
            instance_manager.delete_instances.assert_called_once_with(
                instance_ids_to_terminate=expected_instance_ids,
                terminate_batch_size=terminate_batch_size,
            )
        else:
            instance_manager.delete_instances.assert_not_called()

    @pytest.mark.parametrize(
        "initial_failed_nodes, nodeset, errorcode, override, expected_failed_nodes",
        [
            (
                {},
                {},
                "",
                True,
                {},
            ),
            ({}, {"node1", "node2"}, "error", True, {"error": {"node1", "node2"}}),
            (
                {"error": {"node1", "node2"}},
                {"node1"},
                "another_error",
                True,
                {"error": {"node1", "node2"}, "another_error": {"node1"}},
            ),
            (
                {"error": {"node1", "node2"}},
                {"node1", "node3"},
                "another_error",
                False,
                {"error": {"node1", "node2"}, "another_error": {"node3"}},
            ),
        ],
    )
    def test_update_failed_nodes(
        self,
        instance_manager,
        initial_failed_nodes,
        nodeset,
        errorcode,
        override,
        expected_failed_nodes,
    ):
        instance_manager.failed_nodes = initial_failed_nodes
        instance_manager._update_failed_nodes(nodeset, errorcode, override)

        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)

    @pytest.mark.parametrize(
        "table_name, node_list, slurm_nodes, expected_put_item_calls, expected_message, job_level_scaling",
        [
            (
                None,
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                None,
                "Empty table name configuration parameter",
                False,
            ),
            (
                "table_name",
                ["queue1-st-c5xlarge-1"],
                [],
                None,
                None,
                False,
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
                False,
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
                False,
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
        job_level_scaling,
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
        "hosted_zone, dns_domain, node_list, slurm_nodes, mocked_boto3_request, expected_message, "
        "expected_failure, job_level_scaling",
        [
            (
                None,
                "dns.domain",
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                None,
                "Empty table name configuration parameter",
                False,
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
                False,
            ),
            ("hosted_zone", "dns.domain", ["queue1-st-c5xlarge-1"], [], None, None, False, False),
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
                False,
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
        job_level_scaling,
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
            with pytest.raises(Exception, match=""):
                instance_manager._update_dns_hostnames(assigned_nodes)
        else:
            instance_manager._update_dns_hostnames(assigned_nodes)

    @pytest.mark.parametrize(
        ("node_list", "expected_results", "expected_failed_nodes", "nodes_assigned_to_instances"),
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
                {},
            ),
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
                {},
                {
                    "InvalidNodenameError": {
                        "queue2-invalidnodetype-c5xlarge-12",
                        "noBrackets-st-c5xlarge-[1-2]",
                        "queuename-with-dash-and_underscore-st-i3enmetal2tb-1",
                        "in-valid/queue.name-st-c5xlarge-2",
                    }
                },
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
            ),
            (
                [
                    "queue1-st-c5xlarge-1",
                    "queue1-st-c5xlarge-2",
                ],
                {
                    "queue1": {
                        "c5xlarge": [
                            "queue1-st-c5xlarge-1",
                        ]
                    },
                },
                {},
                {
                    "queue1": {
                        "c5xlarge": [
                            "queue1-st-c5xlarge-2",
                        ]
                    },
                },
            ),
        ],
    )
    def test_parse_nodes_resume_list(
        self, node_list, expected_results, expected_failed_nodes, instance_manager, nodes_assigned_to_instances
    ):
        instance_manager.nodes_assigned_to_instances = nodes_assigned_to_instances
        assert_that(instance_manager._parse_nodes_resume_list(node_list)).is_equal_to(expected_results)
        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)

    @pytest.mark.parametrize(
        ("instance_ids_to_terminate", "batch_size", "mocked_boto3_request", "job_level_scaling"),
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
                False,
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
                False,
            ),
        ],
        ids=["normal", "client_error"],
    )
    def test_delete_instances(
        self,
        boto3_stubber,
        instance_ids_to_terminate,
        batch_size,
        mocked_boto3_request,
        instance_manager,
        job_level_scaling,
    ):
        # patch boto3 call
        boto3_stubber("ec2", mocked_boto3_request)
        # run test
        instance_manager.delete_instances(instance_ids_to_terminate, batch_size)

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
                False,
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
        "mock_kwargs, mocked_boto3_request, expected_parsed_result, job_level_scaling",
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
                                        "NetworkInterfaces": [
                                            {
                                                "Attachment": {
                                                    "DeviceIndex": 0,
                                                    "NetworkCardIndex": 0,
                                                },
                                                "PrivateIpAddress": "ip-1",
                                            },
                                        ],
                                    },
                                    {
                                        "InstanceId": "i-2",
                                        "PrivateIpAddress": "ip-2",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                        "NetworkInterfaces": [
                                            {
                                                "Attachment": {
                                                    "DeviceIndex": 0,
                                                    "NetworkCardIndex": 0,
                                                },
                                                "PrivateIpAddress": "ip-2",
                                            },
                                        ],
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
                False,
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
                False,
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
                                        "NetworkInterfaces": [
                                            {
                                                "Attachment": {
                                                    "DeviceIndex": 0,
                                                    "NetworkCardIndex": 0,
                                                },
                                                "PrivateIpAddress": "ip-1",
                                            },
                                        ],
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
                False,
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
                                        "NetworkInterfaces": [
                                            {
                                                "Attachment": {
                                                    "DeviceIndex": 0,
                                                    "NetworkCardIndex": 0,
                                                },
                                                "PrivateIpAddress": "ip-2",
                                            },
                                        ],
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
                False,
                id="no_ec2_info",
            ),
        ],
    )
    def test_get_cluster_instances(
        self,
        mock_kwargs,
        mocked_boto3_request,
        expected_parsed_result,
        instance_manager,
        boto3_stubber,
        job_level_scaling,
    ):
        # patch boto3 call
        boto3_stubber("ec2", mocked_boto3_request)
        # run test
        result = instance_manager.get_cluster_instances(**mock_kwargs)
        assert_that(result).is_equal_to(expected_parsed_result)

    class DdbResource:
        """Test class to mimic DynamoDb resource."""

        def __init__(self, nodes: Iterable[SlurmNode]):
            self.requested_key_count = 0
            self.call_count = 0
            self.node_lookup = {node.name: node for node in nodes}
            self.request_counts = []

        def batch_get_item(self, *args, **kwargs):
            self.call_count += 1
            responses = {}
            for table_name, keys in kwargs.get("RequestItems").items():
                table_response = [
                    {
                        "Id": key.get("Id"),
                        "InstanceId": "instance-" + self.node_lookup.get(key.get("Id")).name,
                    }
                    for key in keys.get("Keys")
                ]
                self.request_counts.append(len(table_response))
                self.requested_key_count += len(table_response)
                responses.update({table_name: table_response})
            return {
                "Responses": responses,
            }

    @pytest.mark.parametrize(
        "nodes, max_count, expected_key_requests, job_level_scaling",
        [
            (
                [
                    StaticNode(
                        "queue1-st-c5xlarge-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-1", "1.2.3.1", "instance-1-host", "12:45am"),
                    ),
                    StaticNode(
                        "queue1-st-c5xlarge-2",
                        "ip-2",
                        "hostname-2",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-2", "1.2.3.2", "instance-2-host", "12:45am"),
                    ),
                    StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname-3", "some_state", "queue1"),
                    StaticNode("queue1-st-c5xlarge-4", "ip-4", "hostname-4", "some_state", "queue1"),
                    StaticNode(
                        "queue1-st-c5xlarge-5",
                        "ip-5",
                        "hostname-5",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-5", "1.2.3.5", "instance-5-host", "12:45am"),
                    ),
                    StaticNode("queue1-st-c5xlarge-6", "ip-6", "hostname-6", "some_state", "queue1"),
                ],
                0,
                3,
                False,
            ),
            (
                [
                    StaticNode(
                        "queue1-st-c5xlarge-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-1", "1.2.3.1", "instance-1-host", "12:45am"),
                    ),
                    StaticNode(
                        "queue1-st-c5xlarge-2",
                        "ip-2",
                        "hostname-2",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-2", "1.2.3.2", "instance-2-host", "12:45am"),
                    ),
                    StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname-3", "some_state", "queue1"),
                    StaticNode("queue1-st-c5xlarge-4", "ip-4", "hostname-4", "some_state", "queue1"),
                    StaticNode(
                        "queue1-st-c5xlarge-5",
                        "ip-5",
                        "hostname-5",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-5", "1.2.3.5", "instance-5-host", "12:45am"),
                    ),
                    StaticNode("queue1-st-c5xlarge-6", "ip-6", "hostname-6", "some_state", "queue1"),
                ],
                2,
                0,
                False,
            ),
            (
                [
                    StaticNode(
                        "queue1-st-c5xlarge-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-1", "1.2.3.1", "instance-1-host", "12:45am"),
                    ),
                    StaticNode(
                        "queue1-st-c5xlarge-2",
                        "ip-2",
                        "hostname-2",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-2", "1.2.3.2", "instance-2-host", "12:45am"),
                    ),
                    StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname-3", "some_state", "queue1"),
                    StaticNode("queue1-st-c5xlarge-4", "ip-4", "hostname-4", "some_state", "queue1"),
                    StaticNode(
                        "queue1-st-c5xlarge-5",
                        "ip-5",
                        "hostname-5",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-5", "1.2.3.5", "instance-5-host", "12:45am"),
                    ),
                    StaticNode("queue1-st-c5xlarge-6", "ip-6", "hostname-6", "some_state", "queue1"),
                ],
                5,
                2,
                False,
            ),
            (
                [
                    StaticNode(
                        "queue1-st-c5xlarge-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-1", "1.2.3.1", "instance-1-host", "12:45am"),
                    ),
                    StaticNode(
                        "queue1-st-c5xlarge-2",
                        "ip-2",
                        "hostname-2",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-2", "1.2.3.2", "instance-2-host", "12:45am"),
                    ),
                    StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname-3", "some_state", "queue1"),
                    StaticNode("queue1-st-c5xlarge-4", "ip-4", "hostname-4", "some_state", "queue1"),
                    StaticNode(
                        "queue1-st-c5xlarge-5",
                        "ip-5",
                        "hostname-5",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-5", "1.2.3.5", "instance-5-host", "12:45am"),
                    ),
                    StaticNode("queue1-st-c5xlarge-6", "ip-6", "hostname-6", "some_state", "queue1"),
                ],
                6,
                3,
                False,
            ),
            (
                [
                    StaticNode(
                        "queue1-st-c5xlarge-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-1", "1.2.3.1", "instance-1-host", "12:45am"),
                    ),
                    StaticNode(
                        "queue1-st-c5xlarge-2",
                        "ip-2",
                        "hostname-2",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-2", "1.2.3.2", "instance-2-host", "12:45am"),
                    ),
                    StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname-3", "some_state", "queue1"),
                    StaticNode("queue1-st-c5xlarge-4", "ip-4", "hostname-4", "some_state", "queue1"),
                    StaticNode(
                        "queue1-st-c5xlarge-5",
                        "ip-5",
                        "hostname-5",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-5", "1.2.3.5", "instance-5-host", "12:45am"),
                    ),
                    StaticNode("queue1-st-c5xlarge-6", "ip-6", "hostname-6", "some_state", "queue1"),
                ],
                20,
                3,
                False,
            ),
            (
                [
                    StaticNode(
                        "queue1-st-c5xlarge-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-1", "1.2.3.1", "instance-1-host", "12:45am"),
                    ),
                    StaticNode(
                        "queue1-st-c5xlarge-2",
                        "ip-2",
                        "hostname-2",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-2", "1.2.3.2", "instance-2-host", "12:45am"),
                    ),
                    StaticNode(
                        "queue1-st-c5xlarge-5",
                        "ip-5",
                        "hostname-5",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-5", "1.2.3.5", "instance-5-host", "12:45am"),
                    ),
                ],
                0,
                0,
                False,
            ),
            (
                [
                    StaticNode(
                        "queue1-st-c5xlarge-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-1", "1.2.3.1", "instance-1-host", "12:45am"),
                    ),
                    StaticNode(
                        "queue1-st-c5xlarge-2",
                        "ip-2",
                        "hostname-2",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-2", "1.2.3.2", "instance-2-host", "12:45am"),
                    ),
                    StaticNode(
                        "queue1-st-c5xlarge-5",
                        "ip-5",
                        "hostname-5",
                        "some_state",
                        "queue1",
                        instance=EC2Instance("instance-5", "1.2.3.5", "instance-5-host", "12:45am"),
                    ),
                ],
                2,
                0,
                False,
            ),
        ],
    )
    def test_get_console_output(
        self,
        nodes,
        max_count,
        expected_key_requests,
        instance_manager,
        job_level_scaling,
    ):
        ddb_resource = TestInstanceManager.DdbResource(nodes)
        instance_manager._boto3_resource_factory = lambda resource_name: ddb_resource
        instances = instance_manager.get_compute_node_instances(nodes, max_count)
        results = list(instances)

        expected_count = min(max_count, len(nodes)) if max_count > 0 else len(nodes)

        assert_that(results).is_length(expected_count)
        assert_that(ddb_resource.requested_key_count).is_equal_to(expected_key_requests)
        for result in results:
            node = ddb_resource.node_lookup.get(result.get("Name"))
            if node.instance:
                assert_that(result.get("InstanceId")).is_equal_to(node.instance.id)
            else:
                assert_that(result.get("InstanceId")).is_equal_to("instance-" + node.name)

    @pytest.mark.parametrize(
        "node_count,max_retrieval_count,expected_request_counts",
        [
            (200, 200, [50, 50, 50, 50]),
            (200, 400, [50, 50, 50, 50]),
            (200, 139, [50, 50, 39]),
            (201, 400, [50, 50, 50, 50, 1]),
        ],
    )
    def test_instance_retrieval_partitioning(self, node_count, max_retrieval_count, expected_request_counts):
        nodes = [
            StaticNode(
                f"queue1-st-c5xlarge-{item_id}",
                f"ip-{item_id}",
                f"hostname-{item_id}",
                "some_state",
                "queue1",
            )
            for item_id in range(node_count)
        ]

        compute_nodes = ({"Name": node.name, "InstanceId": None} for node in nodes)

        ddb_resource = TestInstanceManager.DdbResource(nodes)

        results = list(
            InstanceManager._retrieve_instance_ids_from_dynamo(
                ddb_resource=ddb_resource,
                table_name="the_table",
                compute_nodes=compute_nodes,
                max_retrieval_count=max_retrieval_count,
            )
        )

        expected_node_count = min(len(nodes), max_retrieval_count)
        assert_that(results).is_length(expected_node_count)
        assert_that(ddb_resource.requested_key_count).is_equal_to(expected_node_count)
        assert_that(ddb_resource.call_count).is_equal_to(len(expected_request_counts))
        assert_that(ddb_resource.request_counts).is_equal_to(expected_request_counts)

    @pytest.mark.parametrize(
        (
            "slurm_resume",
            "node_list",
            "launch_batch_size",
            "assign_node_batch_size",
            "terminate_batch_size",
            "update_node_address",
            "scaling_strategy",
        ),
        [
            (
                {
                    "jobs": [
                        {
                            "job_id": 140817,
                            "nodes_alloc": "node-1",
                            "nodes_resume": "node-1",
                            "oversubscribe": "NO",
                        },
                    ],
                },
                ["node-1"],
                10,
                30,
                40,
                True,
                ScalingStrategy.BEST_EFFORT,
            ),
            (
                {
                    "jobs": [
                        {
                            "job_id": 140817,
                            "nodes_alloc": "node-1",
                            "nodes_resume": "node-1",
                            "oversubscribe": "NO",
                        },
                    ],
                },
                ["node-1"],
                10,
                20,
                30,
                True,
                ScalingStrategy.BEST_EFFORT,
            ),
            (
                {},
                ["node-1"],
                10,
                40,
                20,
                True,
                ScalingStrategy.BEST_EFFORT,
            ),
            (
                {},
                ["node-1, node-2"],
                10,
                40,
                20,
                True,
                ScalingStrategy.BEST_EFFORT,
            ),
        ],
    )
    def test_add_instances(
        self,
        slurm_resume,
        node_list,
        launch_batch_size,
        assign_node_batch_size,
        terminate_batch_size,
        update_node_address,
        scaling_strategy,
        instance_manager,
        mocker,
    ):
        instance_manager._add_instances_for_resume_file = mocker.MagicMock()
        instance_manager._add_instances_for_nodes = mocker.MagicMock()

        instance_manager.add_instances(
            slurm_resume=slurm_resume,
            node_list=node_list,
            launch_batch_size=launch_batch_size,
            assign_node_batch_size=assign_node_batch_size,
            terminate_batch_size=terminate_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
        )

        assert_that(instance_manager.failed_nodes).is_empty()

        if slurm_resume:
            assert_that(instance_manager.unused_launched_instances).is_empty()
            instance_manager._add_instances_for_resume_file.assert_called_once_with(
                slurm_resume=slurm_resume,
                node_list=node_list,
                launch_batch_size=launch_batch_size,
                assign_node_batch_size=assign_node_batch_size,
                update_node_address=update_node_address,
                scaling_strategy=scaling_strategy,
            )
        else:
            instance_manager._add_instances_for_nodes.assert_called_once_with(
                node_list=node_list,
                launch_batch_size=launch_batch_size,
                assign_node_batch_size=assign_node_batch_size,
                update_node_address=update_node_address,
                scaling_strategy=scaling_strategy,
            )


class TestJobLevelScalingInstanceManager:
    @pytest.fixture
    def instance_manager(self, mocker):
        instance_manager = InstanceManagerFactory.get_manager(
            region="us-east-2",
            cluster_name="hit",
            boto3_config=botocore.config.Config(),
            table_name="table_name",
            head_node_private_ip="head.node.ip",
            head_node_hostname="head-node-hostname",
            hosted_zone="hosted_zone",
            dns_domain="dns.domain",
            use_private_hostname=False,
            fleet_config=FLEET_CONFIG,
            run_instances_overrides={},
            create_fleet_overrides={},
            job_level_scaling=True,
        )
        table_mock = mocker.patch.object(instance_manager, "_table")
        table_mock.table_name = "table_name"
        return instance_manager

    @pytest.mark.parametrize(
        (
            "node_list, launch_batch_size, update_node_address, scaling_strategy, slurm_resume, "
            "assign_node_batch_size, terminate_batch_size"
        ),
        [
            (
                ["queue1-st-c5xlarge-2", "queue2-dy-c5xlarge-10"],
                10,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                {},
                30,
                40,
            ),
            (
                ["queue1-st-c5xlarge-2", "queue2-dy-c5xlarge-10"],
                40,
                True,
                ScalingStrategy.BEST_EFFORT,
                {
                    "jobs": [
                        {
                            "job_id": 140817,
                            "nodes_alloc": "node-1",
                            "nodes_resume": "node-1",
                            "oversubscribe": "NO",
                        },
                    ],
                },
                20,
                10,
            ),
            (
                ["queue1-st-c5xlarge-2"],
                50,
                True,
                None,
                None,
                None,
                None,
            ),
        ],
    )
    def test_add_instances(
        self,
        instance_manager,
        mocker,
        node_list,
        launch_batch_size,
        update_node_address,
        scaling_strategy,
        slurm_resume,
        assign_node_batch_size,
        terminate_batch_size,
        caplog,
    ):
        # patch internal functions
        instance_manager._add_instances_for_resume_file = mocker.MagicMock()
        instance_manager._add_instances_for_nodes = mocker.MagicMock()

        instance_manager.add_instances(
            node_list=node_list,
            launch_batch_size=launch_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
            slurm_resume=slurm_resume,
            assign_node_batch_size=assign_node_batch_size,
            terminate_batch_size=terminate_batch_size,
        )

        assert_that(instance_manager.failed_nodes).is_empty()
        assert_that(instance_manager.unused_launched_instances).is_empty()

        if not slurm_resume:
            instance_manager._add_instances_for_resume_file.assert_not_called()
            instance_manager._add_instances_for_nodes.assert_called_with(
                node_list=node_list,
                launch_batch_size=launch_batch_size,
                assign_node_batch_size=assign_node_batch_size,
                update_node_address=update_node_address,
                scaling_strategy=scaling_strategy,
            )
            assert_that(caplog.text).contains(
                "Not possible to perform job level scaling " "because Slurm resume file content is empty."
            )
        else:
            instance_manager._add_instances_for_nodes.assert_not_called()
            instance_manager._add_instances_for_resume_file.assert_called_with(
                slurm_resume=slurm_resume,
                node_list=node_list,
                launch_batch_size=launch_batch_size,
                assign_node_batch_size=assign_node_batch_size,
                update_node_address=update_node_address,
                scaling_strategy=scaling_strategy,
            )

    @pytest.mark.parametrize(
        (
            "slurm_resume",
            "node_list",
            "launch_batch_size",
            "assign_node_batch_size",
            "update_node_address",
            "scaling_strategy",
            "expected_jobs_multi_node_oversubscribe",
            "expected_multi_node_oversubscribe",
            "expected_jobs_single_node_oversubscribe",
            "expected_jobs_multi_node_no_oversubscribe",
            "expected_multi_node_no_oversubscribe",
            "expected_jobs_single_node_no_oversubscribe",
        ),
        [
            (
                {
                    "all_nodes_resume": "cloud[1-3,7-8]",
                    "jobs": [
                        {
                            "job_id": 140814,
                            "nodes_alloc": "queue1-st-c5xlarge-[1-4]",
                            "nodes_resume": "queue1-st-c5xlarge-[1-3]",
                            "oversubscribe": "YES",
                        },
                        {
                            "job_id": 140815,
                            "nodes_alloc": "queue2-st-c5xlarge-[1-3]",
                            "nodes_resume": "queue2-st-c5xlarge-[1-3]",
                            "oversubscribe": "NO",
                        },
                        {
                            "job_id": 140816,
                            "nodes_alloc": "queue3-st-c5xlarge-[7-10]",
                            "nodes_resume": "queue3-st-c5xlarge-[7-9]",
                            "oversubscribe": "NO",
                        },
                        {
                            "job_id": 140817,
                            "nodes_alloc": "broken",
                            "nodes_resume": "broken",
                            "oversubscribe": "NO",
                        },
                        {
                            "job_id": 140818,
                            "nodes_alloc": "queue1-st-c5xlarge-[1-3], queue4-st-c5xlarge-11",
                            "nodes_resume": "queue1-st-c5xlarge-[1-3], queue4-st-c5xlarge-11",
                            "oversubscribe": "UNKNOWN",
                        },
                    ],
                },
                [
                    "queue1-st-c5xlarge-1",
                    "queue1-st-c5xlarge-2",
                    "queue1-st-c5xlarge-3",
                    "queue2-st-c5xlarge-1",
                    "queue2-st-c5xlarge-2",
                    "queue2-st-c5xlarge-3",
                    "queue3-st-c5xlarge-7",
                    "queue3-st-c5xlarge-8",
                    "queue3-st-c5xlarge-9",
                    "broken",
                    "queue4-st-c5xlarge-11",
                ],
                10,
                30,
                True,
                ScalingStrategy.BEST_EFFORT,
                [
                    SlurmResumeJob(
                        job_id=140814,
                        nodes_alloc="queue1-st-c5xlarge-[1-4]",
                        nodes_resume="queue1-st-c5xlarge-[1-3]",
                        oversubscribe="YES",
                    ),
                    SlurmResumeJob(
                        job_id=140818,
                        nodes_alloc="queue1-st-c5xlarge-[1-3], queue4-st-c5xlarge-11",
                        nodes_resume="queue1-st-c5xlarge-[1-3], queue4-st-c5xlarge-11",
                        oversubscribe="OK",
                    ),
                ],
                [
                    "queue1-st-c5xlarge-1",
                    "queue1-st-c5xlarge-2",
                    "queue1-st-c5xlarge-3",
                    "queue4-st-c5xlarge-11",
                ],
                [],
                [
                    SlurmResumeJob(
                        job_id=140815,
                        nodes_alloc="queue2-st-c5xlarge-[1-3]",
                        nodes_resume="queue2-st-c5xlarge-[1-3]",
                        oversubscribe="NO",
                    ),
                    SlurmResumeJob(
                        job_id=140816,
                        nodes_alloc="queue3-st-c5xlarge-[7-10]",
                        nodes_resume="queue3-st-c5xlarge-[7-9]",
                        oversubscribe="NO",
                    ),
                ],
                [
                    "queue2-st-c5xlarge-1",
                    "queue2-st-c5xlarge-2",
                    "queue2-st-c5xlarge-3",
                    "queue3-st-c5xlarge-7",
                    "queue3-st-c5xlarge-8",
                    "queue3-st-c5xlarge-9",
                ],
                [],
            ),
            (
                {
                    "all_nodes_resume": "anything",
                    "jobs": [
                        {
                            "job_id": 140814,
                            "nodes_alloc": "queue1-st-c5xlarge-[1-4]",
                            "nodes_resume": "queue1-st-c5xlarge-[1-3]",
                            "oversubscribe": "FORCE",
                        },
                    ],
                },
                [
                    "queue1-st-c5xlarge-1",
                    "queue1-st-c5xlarge-2",
                    "queue1-st-c5xlarge-3",
                ],
                5,
                25,
                False,
                ScalingStrategy.BEST_EFFORT,
                [
                    SlurmResumeJob(
                        job_id=140814,
                        nodes_alloc="queue1-st-c5xlarge-[1-4]",
                        nodes_resume="queue1-st-c5xlarge-[1-3]",
                        oversubscribe="FORCE",
                    ),
                ],
                [
                    "queue1-st-c5xlarge-1",
                    "queue1-st-c5xlarge-2",
                    "queue1-st-c5xlarge-3",
                ],
                [],
                [],
                [],
                [],
            ),
            (
                {
                    "all_nodes_resume": "anything",
                    "jobs": [
                        {
                            "job_id": 140814,
                            "nodes_alloc": "queue1-st-c5xlarge-[1-2]",
                            "nodes_resume": "queue1-st-c5xlarge-1",
                            "oversubscribe": "NO",
                        },
                    ],
                },
                [],
                8,
                28,
                True,
                ScalingStrategy.BEST_EFFORT,
                [],
                [],
                [],
                [],
                [],
                [
                    SlurmResumeJob(
                        job_id=140814,
                        nodes_alloc="queue1-st-c5xlarge-[1-2]",
                        nodes_resume="queue1-st-c5xlarge-1",
                        oversubscribe="NO",
                    ),
                ],
            ),
            (
                {
                    "all_nodes_resume": "anything",
                    "jobs": [
                        {
                            "job_id": 140814,
                            "nodes_alloc": "queue1-st-c5xlarge-[1-2]",
                            "nodes_resume": "queue1-st-c5xlarge-1",
                            "oversubscribe": "NO",
                        },
                        {
                            "job_id": 140815,
                            "nodes_alloc": "queue2-st-c5xlarge-[1-2]",
                            "nodes_resume": "queue2-st-c5xlarge-[1-2]",
                            "oversubscribe": "NO",
                        },
                        {
                            "job_id": 140816,
                            "nodes_alloc": "queue3-st-c5xlarge-1",
                            "nodes_resume": "queue3-st-c5xlarge-1",
                            "oversubscribe": "YES",
                        },
                    ],
                },
                [
                    "queue1-st-c5xlarge-1",
                    "queue2-st-c5xlarge-1",
                    "queue2-st-c5xlarge-2",
                    "queue3-st-c5xlarge-1",
                ],
                8,
                28,
                True,
                ScalingStrategy.BEST_EFFORT,
                [],
                [],
                [
                    SlurmResumeJob(
                        job_id=140816,
                        nodes_alloc="queue3-st-c5xlarge-1",
                        nodes_resume="queue3-st-c5xlarge-1",
                        oversubscribe="YES",
                    ),
                ],
                [
                    SlurmResumeJob(
                        job_id=140815,
                        nodes_alloc="queue2-st-c5xlarge-[1-2]",
                        nodes_resume="queue2-st-c5xlarge-[1-2]",
                        oversubscribe="NO",
                    ),
                ],
                ["queue2-st-c5xlarge-1", "queue2-st-c5xlarge-2"],
                [
                    SlurmResumeJob(
                        job_id=140814,
                        nodes_alloc="queue1-st-c5xlarge-[1-2]",
                        nodes_resume="queue1-st-c5xlarge-1",
                        oversubscribe="NO",
                    ),
                ],
            ),
        ],
    )
    def test_add_instances_for_resume_file(
        self,
        slurm_resume,
        node_list,
        launch_batch_size,
        assign_node_batch_size,
        update_node_address,
        scaling_strategy,
        expected_jobs_multi_node_oversubscribe,
        expected_multi_node_oversubscribe,
        expected_jobs_single_node_oversubscribe,
        expected_jobs_multi_node_no_oversubscribe,
        expected_multi_node_no_oversubscribe,
        expected_jobs_single_node_no_oversubscribe,
        instance_manager,
        mocker,
    ):
        mocker.patch("slurm_plugin.instance_manager.get_nodes_info", autospec=True)
        instance_manager._scaling_for_jobs_single_node = mocker.MagicMock()
        instance_manager._scaling_for_jobs_multi_node = mocker.MagicMock()
        instance_manager._add_instances_for_nodes = mocker.MagicMock()

        instance_manager._add_instances_for_resume_file(
            slurm_resume=slurm_resume,
            node_list=node_list,
            launch_batch_size=launch_batch_size,
            assign_node_batch_size=assign_node_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
        )

        instance_manager._scaling_for_jobs_single_node.assert_any_call(
            job_list=expected_jobs_single_node_no_oversubscribe + expected_jobs_single_node_oversubscribe,
            launch_batch_size=launch_batch_size,
            assign_node_batch_size=assign_node_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
        )
        instance_manager._scaling_for_jobs_multi_node.assert_any_call(
            job_list=expected_jobs_multi_node_no_oversubscribe + expected_jobs_multi_node_oversubscribe,
            node_list=expected_multi_node_no_oversubscribe + expected_multi_node_oversubscribe,
            launch_batch_size=launch_batch_size,
            assign_node_batch_size=assign_node_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
        )
        assert_that(instance_manager.unused_launched_instances).is_empty()
        assert_that(instance_manager._scaling_for_jobs_single_node.call_count).is_equal_to(1)
        assert_that(instance_manager._scaling_for_jobs_multi_node.call_count).is_equal_to(1)

    @pytest.mark.parametrize(
        "slurm_resume, node_list, expected_single_node_oversubscribe, expected_multi_node_oversubscribe, "
        "expected_jobs_single_node_oversubscribe, expected_jobs_multi_node_oversubscribe, "
        "expected_single_node_no_oversubscribe, expected_multi_node_no_oversubscribe, "
        "expected_jobs_single_node_no_oversubscribe, expected_jobs_multi_node_no_oversubscribe, "
        "expected_nodes_difference",
        [
            (
                {
                    "all_nodes_resume": "cloud[1-3,7-8]",
                    "jobs": [
                        {
                            "job_id": 140814,
                            "nodes_alloc": "queue1-st-c5xlarge-[1-4]",
                            "nodes_resume": "queue1-st-c5xlarge-[1-3]",
                            "oversubscribe": "YES",
                        },
                        {
                            "job_id": 140815,
                            "nodes_alloc": "queue2-st-c5xlarge-[1-3]",
                            "nodes_resume": "queue2-st-c5xlarge-[1-3]",
                            "oversubscribe": "NO",
                        },
                        {
                            "job_id": 140816,
                            "nodes_alloc": "queue3-st-c5xlarge-[7-10]",
                            "nodes_resume": "queue3-st-c5xlarge-[7-9]",
                            "oversubscribe": "NO",
                        },
                        {
                            "job_id": 140817,
                            "nodes_alloc": "broken",
                            "nodes_resume": "broken",
                            "oversubscribe": "NO",
                        },
                        {
                            "job_id": 140818,
                            "nodes_alloc": "queue1-st-c5xlarge-[1-3], queue4-st-c5xlarge-11",
                            "nodes_resume": "queue1-st-c5xlarge-[1-3], queue4-st-c5xlarge-11",
                            "oversubscribe": "UNKNOWN",
                        },
                        {
                            "job_id": 140819,
                            "nodes_alloc": "queue4-st-c5xlarge-1",
                            "nodes_resume": "queue4-st-c5xlarge-1",
                            "oversubscribe": "NO",
                        },
                        {
                            "job_id": 140820,
                            "nodes_alloc": "queue5-st-c5xlarge-[1-2]",
                            "nodes_resume": "queue5-st-c5xlarge-1",
                            "oversubscribe": "NO",
                        },
                        {
                            "job_id": 140821,
                            "nodes_alloc": "queue6-st-c5xlarge-[1-2]",
                            "nodes_resume": "queue6-st-c5xlarge-1",
                            "oversubscribe": "YES",
                        },
                    ],
                },
                [
                    "queue1-st-c5xlarge-1",
                    "queue1-st-c5xlarge-2",
                    "queue1-st-c5xlarge-3",
                    "queue2-st-c5xlarge-1",
                    "queue2-st-c5xlarge-2",
                    "queue2-st-c5xlarge-3",
                    "queue3-st-c5xlarge-7",
                    "queue3-st-c5xlarge-8",
                    "queue3-st-c5xlarge-9",
                    "broken",
                    "queue4-st-c5xlarge-11",
                    "queue5-st-c5xlarge-1",
                    "queue6-st-c5xlarge-1",
                ],
                ["queue6-st-c5xlarge-1"],
                ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3", "queue4-st-c5xlarge-11"],
                [
                    SlurmResumeJob(140821, "queue6-st-c5xlarge-[1-2]", "queue6-st-c5xlarge-1", "YES"),
                ],
                [
                    SlurmResumeJob(140814, "queue1-st-c5xlarge-[1-4]", "queue1-st-c5xlarge-[1-3]", "YES"),
                    SlurmResumeJob(
                        140818,
                        "queue1-st-c5xlarge-[1-3], queue4-st-c5xlarge-11",
                        "queue1-st-c5xlarge-[1-3], queue4-st-c5xlarge-11",
                        "OK",
                    ),
                ],
                [
                    "queue5-st-c5xlarge-1",
                ],
                [
                    "queue2-st-c5xlarge-1",
                    "queue2-st-c5xlarge-2",
                    "queue2-st-c5xlarge-3",
                    "queue3-st-c5xlarge-7",
                    "queue3-st-c5xlarge-8",
                    "queue3-st-c5xlarge-9",
                ],
                [
                    SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                    SlurmResumeJob(140820, "queue5-st-c5xlarge-[1-2]", "queue5-st-c5xlarge-1", "NO"),
                ],
                [
                    SlurmResumeJob(140815, "queue2-st-c5xlarge-[1-3]", "queue2-st-c5xlarge-[1-3]", "NO"),
                    SlurmResumeJob(140816, "queue3-st-c5xlarge-[7-10]", "queue3-st-c5xlarge-[7-9]", "NO"),
                ],
                ["broken"],
            ),
        ],
    )
    def test_get_slurm_resume_data(
        self,
        slurm_resume,
        node_list,
        expected_single_node_oversubscribe,
        expected_multi_node_oversubscribe,
        expected_jobs_single_node_oversubscribe,
        expected_jobs_multi_node_oversubscribe,
        expected_single_node_no_oversubscribe,
        expected_multi_node_no_oversubscribe,
        expected_jobs_single_node_no_oversubscribe,
        expected_jobs_multi_node_no_oversubscribe,
        instance_manager,
        expected_nodes_difference,
        mocker,
        caplog,
    ):
        mocker.patch("slurm_plugin.instance_manager.get_nodes_info", autospec=True)
        slurm_resume = instance_manager._get_slurm_resume_data(slurm_resume, node_list)
        assert_that(slurm_resume.single_node_oversubscribe).contains(*expected_single_node_oversubscribe)
        assert_that(slurm_resume.multi_node_oversubscribe).contains(*expected_multi_node_oversubscribe)
        assert_that(slurm_resume.jobs_single_node_oversubscribe).is_equal_to(expected_jobs_single_node_oversubscribe)
        assert_that(slurm_resume.jobs_multi_node_oversubscribe).is_equal_to(expected_jobs_multi_node_oversubscribe)
        assert_that(slurm_resume.single_node_no_oversubscribe).contains(*expected_single_node_no_oversubscribe)
        assert_that(slurm_resume.multi_node_no_oversubscribe).contains(*expected_multi_node_no_oversubscribe)
        assert_that(slurm_resume.jobs_single_node_no_oversubscribe).is_equal_to(
            expected_jobs_single_node_no_oversubscribe
        )
        assert_that(slurm_resume.jobs_multi_node_no_oversubscribe).is_equal_to(
            expected_jobs_multi_node_no_oversubscribe
        )
        if expected_nodes_difference:
            assert_that(caplog.text).contains(
                "Discarding NodeNames because of mismatch in Slurm Resume File Vs Nodes passed to Resume Program: "
                f"{', '.join(expected_nodes_difference)}"
            )
            assert_that(len(instance_manager.failed_nodes["InvalidNodenameError"])).is_equal_to(
                len(expected_nodes_difference)
            )
        else:
            assert_that("InvalidNodenameError").is_not_in(instance_manager.failed_nodes)

    @pytest.mark.parametrize(
        "slurm_resume, expected_job_list_length, expected_discarded_nodes_alloc, expected_discarded_nodes_resume, "
        "broken_nodenames, expected_failed_nodes",
        [
            (
                {
                    "all_nodes_resume": "cloud[1-3,7-8]",
                    "jobs": [
                        {
                            "extra": "An arbitrary string from --extra",
                            "features": "c1,c2",
                            "job_id": 140814,
                            "nodes_alloc": "queue1-st-c5xlarge-[4-5]",
                            "nodes_resume": "queue1-st-c5xlarge-[1,3]",
                            "oversubscribe": "OK",
                            "partition": "cloud",
                            "reservation": "resv_1234",
                        },
                        {
                            "extra": None,
                            "features": "c1,c2",
                            "job_id": 140815,
                            "nodes_alloc": "queue2-st-c5xlarge-[1-2]",
                            "nodes_resume": "queue2-st-c5xlarge-[1-2]",
                            "oversubscribe": "OK",
                            "partition": "cloud",
                            "reservation": None,
                        },
                        {
                            "extra": None,
                            "features": None,
                            "job_id": 140816,
                            "nodes_alloc": "queue2-st-c5xlarge-[7,8]",
                            "nodes_resume": "queue2-st-c5xlarge-[7,8]",
                            "oversubscribe": "NO",
                            "partition": "cloud_exclusive",
                            "reservation": None,
                        },
                    ],
                },
                3,
                None,
                None,
                None,
                0,
            ),
            (
                {
                    "all_nodes_resume": "cloud[1-3,7-8]",
                    "jobs": [
                        {
                            "extra": "An arbitrary string from --extra",
                            "features": "c1,c2",
                            "job_id": 140814,
                            "nodes_alloc": "queue1-st-c5xlarge-[4-5]",
                            "nodes_resume": "queue1-st-c5xlarge-[1,3]",
                            "oversubscribe": "OK",
                            "partition": "cloud",
                            "reservation": "resv_1234",
                        },
                        {
                            "extra": None,
                            "features": "c1,c2",
                            "job_id": 140815,
                            "nodes_alloc": "queue2-st-c5xlarge-[1-2]",
                            "nodes_resume": "broken",
                            "oversubscribe": "OK",
                            "partition": "cloud",
                            "reservation": None,
                        },
                    ],
                },
                1,
                "queue2-st-c5xlarge-[1-2]",
                "broken",
                [
                    StaticNode(
                        "queue-st-large-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                    ),
                ],
                1,
            ),
            (
                {
                    "all_nodes_resume": "cloud[1-3,7-8]",
                    "jobs": [
                        {
                            "extra": "An arbitrary string from --extra",
                            "features": "c1,c2",
                            "job_id": 140814,
                            "nodes_alloc": "queue1-st-c5xlarge-[4-5]",
                            "nodes_resume": "queue1-st-c5xlarge-[1,3]",
                            "oversubscribe": "OK",
                            "partition": "cloud",
                            "reservation": "resv_1234",
                        },
                        {
                            "extra": None,
                            "features": "c1,c2",
                            "job_id": 140815,
                            "nodes_alloc": "broken",
                            "nodes_resume": "queue3-st-c5xlarge-3, queue2-st-c5xlarge-2",
                            "oversubscribe": "OK",
                            "partition": "cloud",
                            "reservation": None,
                        },
                    ],
                },
                1,
                "broken",
                "queue3-st-c5xlarge-3, queue2-st-c5xlarge-2",
                [
                    StaticNode(
                        "queue3-st-c5xlarge-3",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                    ),
                    StaticNode(
                        "queue2-st-c5xlarge-2",
                        "ip-2",
                        "hostname-2",
                        "some_state",
                    ),
                ],
                2,
            ),
            (
                {
                    "all_nodes_resume": "cloud-1",
                    "jobs": [
                        {
                            "extra": "An arbitrary string from --extra",
                            "features": "c1,c2",
                            "job_id": 140814,
                            "nodes_alloc": "broken-1, broken-2",
                            "nodes_resume": "broken-1, broken-2",
                            "oversubscribe": "OK",
                            "partition": "cloud",
                            "reservation": "resv_1234",
                        },
                    ],
                },
                0,
                "broken-1, broken-2",
                "broken-1, broken-2",
                [
                    StaticNode(
                        "queue2-st-c5xlarge-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                    ),
                    StaticNode(
                        "queue2-st-c5xlarge-2",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                    ),
                ],
                2,
            ),
        ],
    )
    def test_parse_slurm_resume(
        self,
        slurm_resume,
        instance_manager,
        expected_job_list_length,
        expected_discarded_nodes_alloc,
        expected_discarded_nodes_resume,
        broken_nodenames,
        expected_failed_nodes,
        mocker,
        caplog,
    ):
        mocker.patch("slurm_plugin.instance_manager.get_nodes_info", return_value=broken_nodenames, autospec=True)
        slurm_resume_jobs = instance_manager._parse_slurm_resume(slurm_resume)
        assert_that(len(slurm_resume_jobs)).is_equal_to(expected_job_list_length)
        if expected_failed_nodes > 0:
            assert_that(caplog.text).matches(
                rf"Discarding NodeNames with invalid format for Job Id (.*):"
                rf" nodes_alloc \({re.escape(expected_discarded_nodes_alloc)}\),"
                rf" nodes_resume \({re.escape(expected_discarded_nodes_resume)}\)"
            )
            assert_that(len(instance_manager.failed_nodes["InvalidNodenameError"])).is_equal_to(expected_failed_nodes)
        else:
            assert_that("InvalidNodenameError").is_not_in(instance_manager.failed_nodes)

    @pytest.mark.parametrize(
        "update_node_address, nodes_to_launch, instances_launched, assign_node_batch_size, "
        "raise_on_error, expected_update_slurm_node_addrs_calls, expected_exception, "
        "failed_nodes, expected_failed_nodes",
        [
            (
                False,
                {},
                {},
                0,
                False,
                None,
                None,
                {"Error": {"queueX-st-cxlarge-1"}},
                {"Error": {"queueX-st-cxlarge-1"}},
            ),
            (
                False,
                {},
                {},
                0,
                True,
                None,
                None,
                {"Error": {"queueX-st-cxlarge-1"}},
                {"Error": {"queueX-st-cxlarge-1"}},
            ),
            (
                True,
                {
                    "q1": {
                        "q1c1": ["q1-q1c1-st-large-1"],
                    },
                },
                {
                    "q1": {
                        "q1c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    },
                },
                1,
                False,
                [
                    call(
                        slurm_nodes=["q1-q1c1-st-large-1"],
                        launched_instances=(
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                ],
                None,
                {"Error": {"queueX-st-cxlarge-1"}},
                {"Error": {"queueX-st-cxlarge-1"}},
            ),
            (
                True,
                {
                    "q1": {
                        "q1c1": ["q1-q1c1-st-large-1", "q1-q1c1-st-large-2"],
                        "q1c2": ["q1-q1c2-st-small-1"],
                    },
                    "q2": {
                        "q2c1": ["q2-q2c1-st-large-1", "q2-q2c1-st-large-2", "q2-q2c1-st-large-3"],
                    },
                },
                {
                    "q1": {
                        "q1c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-54321", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                        "q1c2": [
                            EC2Instance(
                                "i-123456", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ],
                    },
                    "q2": {
                        "q2c1": [
                            EC2Instance(
                                "i-12347", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12348", "ip.1.0.0.5", "ip-1-0-0-5", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12349", "ip.1.0.0.6", "ip-1-0-0-6", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    },
                },
                10,
                False,
                [
                    call(
                        slurm_nodes=["q1-q1c1-st-large-1", "q1-q1c1-st-large-2"],
                        launched_instances=(
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-54321", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                    call(
                        slurm_nodes=["q1-q1c2-st-small-1"],
                        launched_instances=(
                            EC2Instance(
                                "i-123456", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                    call(
                        slurm_nodes=["q2-q2c1-st-large-1", "q2-q2c1-st-large-2", "q2-q2c1-st-large-3"],
                        launched_instances=(
                            EC2Instance(
                                "i-12347", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12348", "ip.1.0.0.5", "ip-1-0-0-5", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12349", "ip.1.0.0.6", "ip-1-0-0-6", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                ],
                None,
                {"Error": {"queueX-st-cxlarge-1"}},
                {"Error": {"queueX-st-cxlarge-1"}},
            ),
            (
                True,
                {
                    "q1": {
                        "q1c1": ["q1-q1c1-st-large-1"],
                    },
                },
                {
                    "q1": {
                        "q1c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    },
                },
                1,
                False,
                [
                    call(
                        slurm_nodes=["q1-q1c1-st-large-1"],
                        launched_instances=(
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                ],
                [HostnameDnsStoreError()],
                {"Error": {"queueX-st-cxlarge-1"}},
                {"Error": {"queueX-st-cxlarge-1"}, "Exception": {"q1-q1c1-st-large-1"}},
            ),
            (
                True,
                {
                    "q1": {
                        "q1c1": ["q1-q1c1-st-large-1", "q1-q1c1-st-large-2"],
                    },
                },
                {
                    "q1": {
                        "q1c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    },
                },
                1,
                False,
                [
                    call(
                        slurm_nodes=["q1-q1c1-st-large-1"],
                        launched_instances=(
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                    call(
                        slurm_nodes=["q1-q1c1-st-large-2"],
                        launched_instances=(
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                ],
                [None, HostnameDnsStoreError()],
                {"Error": {"queueX-st-cxlarge-1"}},
                {"Error": {"queueX-st-cxlarge-1"}, "Exception": {"q1-q1c1-st-large-2"}},
            ),
            (
                True,
                {
                    "q1": {
                        "q1c1": ["q1-q1c1-st-large-1", "q1-q1c1-st-large-2"],
                    },
                },
                {
                    "q1": {
                        "q1c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    },
                },
                1,
                False,
                [
                    call(
                        slurm_nodes=["q1-q1c1-st-large-1"],
                        launched_instances=(
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                    call(
                        slurm_nodes=["q1-q1c1-st-large-2"],
                        launched_instances=(
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                ],
                [HostnameDnsStoreError(), None],
                {"Error": {"queueX-st-cxlarge-1"}},
                {"Error": {"queueX-st-cxlarge-1"}, "Exception": {"q1-q1c1-st-large-1"}},
            ),
            (
                True,
                {
                    "q1": {
                        "q1c1": ["q1-q1c1-st-large-1"],
                    },
                },
                {
                    "q1": {
                        "q1c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    },
                },
                1,
                True,
                [
                    call(
                        slurm_nodes=["q1-q1c1-st-large-1"],
                        launched_instances=(
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                ],
                HostnameDnsStoreError(),
                {"Error": {"queueX-st-cxlarge-1"}},
                {"Error": {"queueX-st-cxlarge-1"}},
            ),
            (
                True,
                {
                    "q1": {
                        "q1c1": ["q1-q1c1-st-large-1", "q1-q1c1-st-large-2"],
                    },
                },
                {
                    "q1": {
                        "q1c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    },
                },
                1,
                True,
                [
                    call(
                        slurm_nodes=["q1-q1c1-st-large-1"],
                        launched_instances=(
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                    call(
                        slurm_nodes=["q1-q1c1-st-large-2"],
                        launched_instances=(
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                ],
                [None, HostnameDnsStoreError()],
                {"Error": {"queueX-st-cxlarge-1"}},
                {"Error": {"queueX-st-cxlarge-1"}},
            ),
            (
                True,
                {
                    "q1": {
                        "q1c1": ["q1-q1c1-st-large-1", "q1-q1c1-st-large-2"],
                    },
                },
                {
                    "q1": {
                        "q1c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    },
                },
                1,
                True,
                [
                    call(
                        slurm_nodes=["q1-q1c1-st-large-1"],
                        launched_instances=(
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ),
                    ),
                ],
                [HostnameDnsStoreError(), None],
                {"Error": {"queueX-st-cxlarge-1"}},
                {"Error": {"queueX-st-cxlarge-1"}},
            ),
        ],
    )
    def test_assign_instances_to_nodes(
        self,
        mocker,
        instance_manager,
        update_node_address,
        nodes_to_launch,
        instances_launched,
        assign_node_batch_size,
        raise_on_error,
        expected_update_slurm_node_addrs_calls,
        expected_exception,
        failed_nodes,
        expected_failed_nodes,
    ):
        # patch internal functions
        instance_manager._update_slurm_node_addrs = mocker.MagicMock()
        instance_manager._store_assigned_hostnames = mocker.MagicMock()
        instance_manager._update_dns_hostnames = mocker.MagicMock(side_effect=expected_exception)
        instance_manager.failed_nodes = failed_nodes

        if update_node_address and raise_on_error:
            with pytest.raises(InstanceToNodeAssignmentError):
                instance_manager._assign_instances_to_nodes(
                    update_node_address=update_node_address,
                    nodes_to_launch=nodes_to_launch,
                    instances_launched=instances_launched,
                    assign_node_batch_size=assign_node_batch_size,
                    raise_on_error=raise_on_error,
                )
        else:
            instance_manager._assign_instances_to_nodes(
                update_node_address=update_node_address,
                nodes_to_launch=nodes_to_launch,
                instances_launched=instances_launched,
                assign_node_batch_size=assign_node_batch_size,
                raise_on_error=raise_on_error,
            )

        if not update_node_address:
            instance_manager._update_slurm_node_addrs.assert_not_called()
            instance_manager._store_assigned_hostnames.assert_not_called()
            instance_manager._update_dns_hostnames.assert_not_called()
        else:
            instance_manager._update_slurm_node_addrs.assert_has_calls(expected_update_slurm_node_addrs_calls)
            assert_that(instance_manager._store_assigned_hostnames.call_count).is_equal_to(
                len(expected_update_slurm_node_addrs_calls)
            )
            assert_that(instance_manager._update_dns_hostnames.call_count).is_equal_to(
                len(expected_update_slurm_node_addrs_calls)
            )

        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)

    @pytest.mark.parametrize(
        "node_list, launched_instances, use_private_hostname, expected_update_nodes, "
        "expected_update_nodes_call, expected_return",
        [
            (
                ["queue1-st-c5xlarge-1"],
                [],
                False,
                None,
                call(["queue1-st-c5xlarge-1"], nodeaddrs=[], nodehostnames=None),
                {},
            ),
            (
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                False,
                None,
                call(["queue1-st-c5xlarge-1"], nodeaddrs=["ip-1"], nodehostnames=None),
                {
                    "queue1-st-c5xlarge-1": EC2Instance(
                        id="id-1", private_ip="ip-1", hostname="hostname-1", launch_time="some_launch_time"
                    )
                },
            ),
            (
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                True,
                None,
                call(["queue1-st-c5xlarge-1"], nodeaddrs=["ip-1"], nodehostnames=["hostname-1"]),
                {
                    "queue1-st-c5xlarge-1": EC2Instance(
                        id="id-1", private_ip="ip-1", hostname="hostname-1", launch_time="some_launch_time"
                    )
                },
            ),
            (
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                True,
                subprocess.CalledProcessError(1, "command"),
                call(["queue1-st-c5xlarge-1"], nodeaddrs=["ip-1"], nodehostnames=["hostname-1"]),
                NodeAddrUpdateError(),
            ),
            (
                ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2"],
                [
                    EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                    EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
                ],
                False,
                None,
                call(["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2"], nodeaddrs=["ip-1", "ip-2"], nodehostnames=None),
                {
                    "queue1-st-c5xlarge-1": EC2Instance(
                        id="id-1", private_ip="ip-1", hostname="hostname-1", launch_time="some_launch_time"
                    ),
                    "queue1-st-c5xlarge-2": EC2Instance(
                        id="id-2", private_ip="ip-2", hostname="hostname-2", launch_time="some_launch_time"
                    ),
                },
            ),
        ],
    )
    def test_update_slurm_node_addrs(
        self,
        node_list,
        launched_instances,
        use_private_hostname,
        expected_update_nodes,
        expected_update_nodes_call,
        expected_return,
        instance_manager,
        mocker,
    ):
        mock_update_nodes = mocker.patch(
            "slurm_plugin.instance_manager.update_nodes", side_effect=expected_update_nodes
        )
        instance_manager._use_private_hostname = use_private_hostname

        if isinstance(expected_return, Exception):
            with pytest.raises(expected_return.__class__):
                instance_manager._update_slurm_node_addrs(node_list, launched_instances)
        else:
            function_return = instance_manager._update_slurm_node_addrs(node_list, launched_instances)
            assert_that(function_return).is_equal_to(expected_return)

        if expected_update_nodes_call:
            mock_update_nodes.assert_called_once()
            mock_update_nodes.assert_has_calls([expected_update_nodes_call])
        else:
            mock_update_nodes.assert_not_called()

        assert_that(instance_manager.failed_nodes).is_empty()

    @pytest.mark.parametrize(
        "job, launch_batch_size, assign_node_batch_size, update_node_address, scaling_strategy, skip_launch, "
        "expected_nodes_to_launch, mock_instances_launched, initial_unused_launched_instances, "
        "expected_unused_launched_instances, expect_assign_instances_to_nodes_called, "
        "expect_assign_instances_to_nodes_failure, expected_failed_nodes",
        [
            (
                SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                1,
                2,
                False,
                ScalingStrategy.BEST_EFFORT,
                False,
                {"queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                {},
                {},
                {},
                False,
                None,
                {"InsufficientInstanceCapacity": {"queue4-st-c5xlarge-1"}},
            ),
            (
                SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                1,
                2,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                False,
                {"queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                {},
                {},
                {},
                False,
                None,
                {"InsufficientInstanceCapacity": {"queue4-st-c5xlarge-1"}},
            ),
            (
                SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                1,
                2,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                False,
                {"queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                {},
                {},
                True,
                None,
                {},
            ),
            (
                SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                1,
                2,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                False,
                {"queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                {},
                {},
                True,
                InstanceToNodeAssignmentError(),
                {"Exception": {"queue4-st-c5xlarge-1"}},
            ),
            (
                SlurmResumeJob(
                    140819,
                    "queue1-st-c5xlarge-1, queue4-st-c5xlarge-1",
                    "queue1-st-c5xlarge-1, queue4-st-c5xlarge-1",
                    "NO",
                ),
                1,
                2,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                False,
                {"queue1": {"c5xlarge": ["queue1-st-c5xlarge-1"]}, "queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                {},
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                False,
                None,
                {"LimitedInstanceCapacity": {"queue1-st-c5xlarge-1", "queue4-st-c5xlarge-1"}},
            ),
            (
                SlurmResumeJob(
                    140819,
                    "queue1-st-c5xlarge-1, queue4-st-c5xlarge-1",
                    "queue1-st-c5xlarge-1, queue4-st-c5xlarge-1",
                    "NO",
                ),
                1,
                2,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                False,
                {"queue1": {"c5xlarge": ["queue1-st-c5xlarge-1"]}, "queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                {},
                {},
                {},
                False,
                None,
                {"InsufficientInstanceCapacity": {"queue1-st-c5xlarge-1", "queue4-st-c5xlarge-1"}},
            ),
            (
                SlurmResumeJob(
                    140819,
                    "queue1-st-c5xlarge-1, queue4-st-c5xlarge-1",
                    "queue1-st-c5xlarge-1",
                    "NO",
                ),
                1,
                2,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                False,
                {"queue1": {"c5xlarge": ["queue1-st-c5xlarge-1"]}},
                {},
                {},
                {},
                False,
                None,
                {"InsufficientInstanceCapacity": {"queue1-st-c5xlarge-1"}},
            ),
            (
                SlurmResumeJob(
                    140819,
                    "queue1-st-c5xlarge-1, queue4-st-c5xlarge-1",
                    "queue1-st-c5xlarge-1",
                    "NO",
                ),
                1,
                2,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                False,
                {"queue1": {"c5xlarge": ["queue1-st-c5xlarge-1"]}},
                {
                    "queue1": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                {},
                {},
                True,
                None,
                {},
            ),
            (
                SlurmResumeJob(
                    140819,
                    "queue1-st-c5xlarge-1, queue4-st-c5xlarge-1",
                    "queue1-st-c5xlarge-1, queue4-st-c5xlarge-1",
                    "NO",
                ),
                1,
                2,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                False,
                {"queue1": {"c5xlarge": ["queue1-st-c5xlarge-1"]}, "queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                {
                    "queue10": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    },
                    "queue10": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    },
                },
                False,
                None,
                {"LimitedInstanceCapacity": {"queue1-st-c5xlarge-1", "queue4-st-c5xlarge-1"}},
            ),
            (
                SlurmResumeJob(
                    140819,
                    "queue1-st-c5xlarge-1, queue4-st-c5xlarge-1",
                    "queue1-st-c5xlarge-1, queue4-st-c5xlarge-1",
                    "NO",
                ),
                1,
                2,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                False,
                {"queue1": {"c5xlarge": ["queue1-st-c5xlarge-1"]}, "queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12346", "ip.1.0.0.6", "ip-1-0-0-6", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12346", "ip.1.0.0.6", "ip-1-0-0-6", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                False,
                None,
                {"LimitedInstanceCapacity": {"queue1-st-c5xlarge-1", "queue4-st-c5xlarge-1"}},
            ),
        ],
    )
    def test_add_instances_for_nodes(
        self,
        mocker,
        instance_manager,
        job,
        launch_batch_size,
        assign_node_batch_size,
        update_node_address,
        scaling_strategy,
        skip_launch,
        expected_nodes_to_launch,
        mock_instances_launched,
        initial_unused_launched_instances,
        expected_unused_launched_instances,
        expect_assign_instances_to_nodes_called,
        expect_assign_instances_to_nodes_failure,
        expected_failed_nodes,
    ):
        # patch internal functions and data
        instance_manager._launch_instances = mocker.MagicMock(return_value=mock_instances_launched)
        instance_manager._assign_instances_to_nodes = mocker.MagicMock(
            side_effect=expect_assign_instances_to_nodes_failure
        )
        instance_manager.unused_launched_instances = initial_unused_launched_instances

        instance_manager._add_instances_for_nodes(
            node_list=job.nodes_resume,
            job=job,
            launch_batch_size=launch_batch_size,
            assign_node_batch_size=assign_node_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
            skip_launch=skip_launch,
        )

        instance_manager._launch_instances.assert_called_once_with(
            job=job,
            nodes_to_launch=expected_nodes_to_launch,
            launch_batch_size=launch_batch_size,
            scaling_strategy=scaling_strategy,
            skip_launch=skip_launch,
        )

        assert_that(instance_manager.unused_launched_instances).is_equal_to(expected_unused_launched_instances)
        if expect_assign_instances_to_nodes_called:
            instance_manager._assign_instances_to_nodes.assert_called_once()
            if expect_assign_instances_to_nodes_failure:
                assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)
            else:
                assert_that(instance_manager.failed_nodes).is_empty()
        else:
            instance_manager._assign_instances_to_nodes.assert_not_called()
            assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)

    @pytest.mark.parametrize(
        "job, nodes_to_launch, launch_batch_size, unused_launched_instances, launched_instances, "
        "expected_instances_launched, expected_failed_nodes",
        [
            (
                SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                {"queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                10,
                {},
                [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
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
                            }
                        ]
                    },
                ],
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {},
            ),
            (
                SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                {"queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                10,
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                [],
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {},
            ),
            (
                SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                {"queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                10,
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-654321", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                [],
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {},
            ),
            (
                SlurmResumeJob(140819, "queue4-st-c5xlarge-[1-3]", "queue4-st-c5xlarge-[1-2]", "NO"),
                {"queue4": {"c5xlarge": ["queue4-st-c5xlarge-1", "queue4-st-c5xlarge-2"]}},
                10,
                {},
                [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
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
                            }
                        ],
                    },
                ],
                {
                    "queue4": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {},
            ),
            (
                SlurmResumeJob(140819, "queue4-st-c5xlarge-[1-3]", "queue4-st-c5xlarge-[1-2]", "NO"),
                {"queue4": {"c5xlarge": ["queue4-st-c5xlarge-1", "queue4-st-c5xlarge-2"]}},
                10,
                {},
                client_error("some_error_code"),
                {},
                {"some_error_code": {"queue4-st-c5xlarge-1", "queue4-st-c5xlarge-2"}},
            ),
            (
                SlurmResumeJob(140819, "queue4-st-c5xlarge-[1-3]", "queue4-st-c5xlarge-[1-2]", "NO"),
                {"queue4": {"c5xlarge": ["queue4-st-c5xlarge-1", "queue4-st-c5xlarge-2"]}},
                10,
                {},
                Exception(),
                {},
                {"Exception": {"queue4-st-c5xlarge-1", "queue4-st-c5xlarge-2"}},
            ),
            (
                SlurmResumeJob(
                    140819,
                    "queue1-st-c5xlarge-1,queue2-st-c5xlarge-1,queue3-st-c5xlarge-1",
                    "queue1-st-c5xlarge-1,queue2-st-c5xlarge-1,queue3-st-c5xlarge-1",
                    "NO",
                ),
                {
                    "queue1": {"c5xlarge": ["queue1-st-c5xlarge-1"]},
                    "queue2": {"c5xlarge": ["queue2-st-c5xlarge-1"]},
                    "queue3": {"c5xlarge": ["queue3-st-c5xlarge-1"]},
                },
                10,
                {},
                [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
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
                            }
                        ],
                    },
                    client_error("some_error_code"),
                ],
                {
                    "queue1": {
                        "c5xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {"some_error_code": {"queue2-st-c5xlarge-1"}},
            ),
            (
                None,
                {
                    "queue1": {"c52xlarge": ["queue1-st-c52xlarge-1"]},
                    "queue2": {"c5xlarge": ["queue2-st-c5xlarge-1"]},
                    "queue3": {"p4d24xlarge": ["queue3-st-p4d24xlarge-1"]},
                },
                15,
                {},
                [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
                                "InstanceType": "c5.2xlarge",
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
                            }
                        ],
                    },
                    client_error("some_error_code"),
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-123456",
                                "InstanceType": "p4d24xlarge",
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
                            }
                        ],
                    },
                ],
                {
                    "queue1": {
                        "c52xlarge": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    },
                    "queue3": {
                        "p4d24xlarge": [
                            EC2Instance(
                                "i-123456", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    },
                },
                {"some_error_code": {"queue2-st-c5xlarge-1"}},
            ),
            (
                SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                {"queue4": {"c5xlarge": ["queue4-st-c5xlarge-1"]}},
                10,
                {},
                [{"Instances": []}],
                {},
                {"InsufficientInstanceCapacity": {"queue4-st-c5xlarge-1"}},
            ),
        ],
    )
    def test_launch_instances(
        self,
        mocker,
        instance_manager,
        job,
        nodes_to_launch,
        launch_batch_size,
        unused_launched_instances,
        launched_instances,
        expected_instances_launched,
        expected_failed_nodes,
    ):
        # patch fleet manager calls
        mocker.patch.object(
            slurm_plugin.fleet_manager.Ec2RunInstancesManager,
            "_launch_instances",
            side_effect=launched_instances,
        )
        instance_manager.unused_launched_instances = unused_launched_instances

        instances_launched = instance_manager._launch_instances(
            job=job,
            nodes_to_launch=nodes_to_launch,
            launch_batch_size=launch_batch_size,
            scaling_strategy=ScalingStrategy.ALL_OR_NOTHING,
        )

        assert_that(instances_launched).is_equal_to(expected_instances_launched)
        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)

    @pytest.mark.parametrize(
        "job_list, launch_batch_size, assign_node_batch_size, update_node_address, "
        "expected_single_nodes_no_oversubscribe, scaling_strategy",
        [
            (
                [],
                1,
                2,
                True,
                [],
                ScalingStrategy.ALL_OR_NOTHING,
            ),
            (
                [
                    SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                ],
                1,
                2,
                True,
                [],
                ScalingStrategy.BEST_EFFORT,
            ),
            (
                [
                    SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                    SlurmResumeJob(140820, "queue4-st-c5xlarge-2", "queue4-st-c5xlarge-2", "NO"),
                ],
                1,
                2,
                True,
                ["queue4-st-c5xlarge-1", "queue4-st-c5xlarge-2"],
                ScalingStrategy.BEST_EFFORT,
            ),
            (
                [
                    SlurmResumeJob(140819, "queue4-st-c5xlarge-1", "queue4-st-c5xlarge-1", "NO"),
                    SlurmResumeJob(140820, "queue4-st-c5xlarge-2", "queue4-st-c5xlarge-2", "NO"),
                ],
                1,
                2,
                True,
                ["queue4-st-c5xlarge-1", "queue4-st-c5xlarge-2"],
                ScalingStrategy.ALL_OR_NOTHING,
            ),
        ],
    )
    def test_scaling_for_jobs_single_node(
        self,
        mocker,
        instance_manager,
        job_list,
        launch_batch_size,
        assign_node_batch_size,
        update_node_address,
        expected_single_nodes_no_oversubscribe,
        scaling_strategy,
    ):
        # patch internal functions
        instance_manager._scaling_for_jobs = mocker.MagicMock()
        instance_manager._add_instances_for_nodes = mocker.MagicMock()

        instance_manager._scaling_for_jobs_single_node(
            job_list=job_list,
            launch_batch_size=launch_batch_size,
            assign_node_batch_size=assign_node_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
        )
        if not job_list:
            instance_manager._scaling_for_jobs.assert_not_called()
            instance_manager._add_instances_for_nodes.assert_not_called()
        if len(job_list) == 1:
            instance_manager._scaling_for_jobs.assert_called_once_with(
                job_list=job_list,
                launch_batch_size=launch_batch_size,
                assign_node_batch_size=assign_node_batch_size,
                update_node_address=update_node_address,
                scaling_strategy=scaling_strategy,
            )
            instance_manager._add_instances_for_nodes.assert_not_called()
        if len(job_list) > 1:
            instance_manager._scaling_for_jobs.assert_not_called()
            instance_manager._add_instances_for_nodes.assert_called_once_with(
                node_list=expected_single_nodes_no_oversubscribe,
                launch_batch_size=launch_batch_size,
                assign_node_batch_size=assign_node_batch_size,
                update_node_address=update_node_address,
                scaling_strategy=ScalingStrategy.BEST_EFFORT,
            )

    @pytest.mark.parametrize(
        "job_list, launch_batch_size, assign_node_batch_size, update_node_address, scaling_strategy, skip_launch",
        [
            ([], 1, 2, True, False, False),
            (
                [
                    SlurmResumeJob(
                        job_id=140816,
                        nodes_alloc="queue3-st-c5xlarge-[7-10]",
                        nodes_resume="queue3-st-c5xlarge-[7-9]",
                        oversubscribe="NO",
                    ),
                ],
                3,
                2,
                True,
                ScalingStrategy.ALL_OR_NOTHING,
                False,
            ),
            (
                [
                    SlurmResumeJob(
                        job_id=140816,
                        nodes_alloc="queue3-st-c5xlarge-[7-10]",
                        nodes_resume="queue3-st-c5xlarge-[7-9]",
                        oversubscribe="NO",
                    ),
                    SlurmResumeJob(
                        job_id=140817,
                        nodes_alloc="queue1-st-c5xlarge-1",
                        nodes_resume="queue1-st-c5xlarge-1",
                        oversubscribe="NO",
                    ),
                ],
                2,
                1,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                False,
            ),
        ],
    )
    def test_scaling_for_jobs(
        self,
        mocker,
        instance_manager,
        job_list,
        launch_batch_size,
        assign_node_batch_size,
        update_node_address,
        scaling_strategy,
        skip_launch,
    ):
        # patch internal functions
        instance_manager._terminate_unassigned_launched_instances = mocker.MagicMock()
        instance_manager._add_instances_for_nodes = mocker.MagicMock()
        setup_logging_filter = mocker.patch(
            "slurm_plugin.instance_manager.setup_logging_filter", return_value=mocker.MagicMock()
        )

        instance_manager._scaling_for_jobs(
            job_list=job_list,
            launch_batch_size=launch_batch_size,
            assign_node_batch_size=assign_node_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
            skip_launch=skip_launch,
        )

        if not job_list:
            instance_manager._add_instances_for_nodes.assert_not_called()
        else:
            for job in job_list:
                instance_manager._add_instances_for_nodes.assert_any_call(
                    job=job,
                    node_list=job.nodes_resume,
                    launch_batch_size=launch_batch_size,
                    assign_node_batch_size=assign_node_batch_size,
                    update_node_address=update_node_address,
                    scaling_strategy=scaling_strategy,
                    skip_launch=skip_launch,
                )
                setup_logging_filter.return_value.__enter__.return_value.set_custom_value.assert_any_call(job.job_id)
            assert_that(
                setup_logging_filter.return_value.__enter__.return_value.set_custom_value.call_count
            ).is_equal_to(len(job_list))
            assert_that(instance_manager._add_instances_for_nodes.call_count).is_equal_to(len(job_list))

        instance_manager._terminate_unassigned_launched_instances.assert_not_called()
        setup_logging_filter.assert_called_once()

    @pytest.mark.parametrize(
        "terminate_batch_size, mock_unassigned_launched_instances, expect_delete_instances_list",
        [
            (
                0,
                {},
                [],
            ),
            (
                10,
                {
                    "q1": {
                        "q1c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    },
                },
                ["i-12345"],
            ),
            (
                20,
                {
                    "q1": {
                        "q1c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-54321", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                        "q1c2": [
                            EC2Instance(
                                "i-123456", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ],
                    },
                    "q2": {
                        "q2c1": [
                            EC2Instance(
                                "i-12347", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12348", "ip.1.0.0.5", "ip-1-0-0-5", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12349", "ip.1.0.0.6", "ip-1-0-0-6", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    },
                },
                ["i-12345", "i-54321", "i-123456", "i-12347", "i-12348", "i-12349"],
            ),
        ],
    )
    def test_terminate_unassigned_launched_instances(
        self,
        mocker,
        instance_manager,
        terminate_batch_size,
        mock_unassigned_launched_instances,
        expect_delete_instances_list,
    ):
        # patch internal functions
        instance_manager.delete_instances = mocker.MagicMock()
        instance_manager.unused_launched_instances = mock_unassigned_launched_instances

        instance_manager._terminate_unassigned_launched_instances(terminate_batch_size)

        if mock_unassigned_launched_instances:
            instance_manager.delete_instances.assert_called_once_with(
                expect_delete_instances_list, terminate_batch_size
            )
        assert_that(instance_manager.unused_launched_instances).is_empty()

    @pytest.mark.parametrize(
        "queue, compute_resource, slurm_node_list, instances_launched, "
        "unused_launched_instances, expected_slurm_node_list, expected_instances_launched",
        [
            (
                "q1",
                "c1",
                [],
                {},
                {},
                [],
                {},
            ),
            (
                "q1",
                "c1",
                ["q1-st-c1-1"],
                {},
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12347", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                [],
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12347", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
            ),
            (
                "q1",
                "c1",
                ["q1-st-c1-1"],
                {},
                {
                    "q1": {
                        "c2": [
                            EC2Instance(
                                "i-12347", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                ["q1-st-c1-1"],
                {},
            ),
            (
                "q1",
                "c1",
                ["q1-st-c1-1", "q1-st-c1-2"],
                {
                    "q2": {
                        "c2": [
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {
                    "q1": {
                        "c2": [
                            EC2Instance(
                                "i-12347", "ip.1.0.0.4", "ip-1-0-0-4", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                ["q1-st-c1-1", "q1-st-c1-2"],
                {
                    "q2": {
                        "c2": [
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
            ),
        ],
    )
    def test_resize_slurm_node_list(
        self,
        instance_manager,
        queue,
        compute_resource,
        slurm_node_list,
        instances_launched,
        unused_launched_instances,
        expected_slurm_node_list,
        expected_instances_launched,
    ):
        if not instances_launched:
            instances_launched = collections.defaultdict(lambda: collections.defaultdict(list))

        instance_manager.unused_launched_instances = unused_launched_instances
        new_slurm_node_list = instance_manager._resize_slurm_node_list(
            queue=queue,
            compute_resource=compute_resource,
            slurm_node_list=slurm_node_list,
            instances_launched=instances_launched,
        )

        assert_that(new_slurm_node_list).is_equal_to(expected_slurm_node_list)
        assert_that(instances_launched).is_equal_to(expected_instances_launched)

    @pytest.mark.parametrize(
        "target_dict, update, expected_dict",
        [
            (
                {},
                {},
                {},
            ),
            (
                {"q1": {"c1": ["a", "b"]}},
                {},
                {"q1": {"c1": ["a", "b"]}},
            ),
            (
                {"q1": {"c1": ["a", "b"]}},
                {"q1": {"c1": ["c"]}},
                {"q1": {"c1": ["a", "b", "c"]}},
            ),
            (
                {},
                {"q1": {"c1": ["a", "b"]}},
                {"q1": {"c1": ["a", "b"]}},
            ),
            (
                {"q1": {"c1": {"a", "b"}}},
                {},
                {"q1": {"c1": {"a", "b"}}},
            ),
            (
                {"q1": {"c1": {"a", "b"}}},
                {"q1": {"c1": {"c"}}},
                {"q1": {"c1": {"a", "b", "c"}}},
            ),
            (
                {},
                {"q1": {"c1": {"a", "b"}}},
                {"q1": {"c1": {"a", "b"}}},
            ),
            (
                {"q1": {"c1": 1}},
                {},
                {"q1": {"c1": 1}},
            ),
            (
                {"q1": {"c1": 1}},
                {"q1": {"c1": 2}},
                {"q1": {"c1": 2}},
            ),
            (
                {},
                {"q1": {"c1": 3}},
                {"q1": {"c1": 3}},
            ),
            (
                {"q1": {"c1": ["a", "b"], "c2": ["c"]}, "q2": {"c1": ["d"]}},
                {"q2": {"c1": ["k"]}, "q3": {"c1": ["y"]}},
                {"q1": {"c1": ["a", "b"], "c2": ["c"]}, "q2": {"c1": ["d", "k"]}, "q3": {"c1": ["y"]}},
            ),
            (
                defaultdict(lambda: defaultdict(list)),
                defaultdict(lambda: defaultdict(list)),
                defaultdict(lambda: defaultdict(list)),
            ),
            (
                defaultdict(lambda: defaultdict(list)),
                {"q1": {"c1": ["a", "b"]}},
                {"q1": {"c1": ["a", "b"]}},
            ),
            (
                {"q1": {"c1": ["a", "b", "c"]}},
                defaultdict(lambda: defaultdict(list)),
                {"q1": {"c1": ["a", "b", "c"]}},
            ),
            (
                {
                    "q1": {
                        "c1": [
                            EC2Instance("q1c1-1", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))
                        ],
                        "c2": [
                            EC2Instance("q1c2-1", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc))
                        ],
                    }
                },
                {},
                {
                    "q1": {
                        "c1": [
                            EC2Instance("q1c1-1", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))
                        ],
                        "c2": [
                            EC2Instance("q1c2-1", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc))
                        ],
                    }
                },
            ),
            (
                {
                    "q1": {
                        "c1": [
                            EC2Instance("q1c1-1", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))
                        ],
                        "c2": [
                            EC2Instance("q1c2-1", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc))
                        ],
                    }
                },
                {
                    "q1": {
                        "c2": [
                            EC2Instance("q1c2-2", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc))
                        ]
                    }
                },
                {
                    "q1": {
                        "c1": [
                            EC2Instance("q1c1-1", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc))
                        ],
                        "c2": [
                            EC2Instance(
                                "q1c2-1", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "q1c2-2", "ip.1.0.0.3", "ip-1-0-0-3", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ],
                    }
                },
            ),
        ],
    )
    def test_update_dict(self, instance_manager, target_dict, update, expected_dict):
        actual_dict = instance_manager._update_dict(target_dict, update)
        assert_that(actual_dict).is_equal_to(expected_dict)

    @pytest.mark.parametrize(
        "assign_node_batch_size, failed_launch_nodes, instances_launched, nodes_resume_list, nodes_resume_mapping, "
        "successful_launched_nodes, update_node_address, expected_failed_nodes",
        [
            (
                1,
                [],
                {},
                [],
                {},
                [],
                False,
                {},
            ),
            (
                2,
                [],
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ],
                        "c2": [
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ],
                    }
                },
                ["q1-dy-c1-1", "q1-dy-c2-1"],
                {"q1": {"c1": ["q1-dy-c1-1"], "c2": ["q1-dy-c2-1"]}},
                ["q1-dy-c1-1", "q1-dy-c2-1"],
                True,
                {},
            ),
            (
                3,
                ["q1-dy-c2-1"],
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ],
                    }
                },
                ["q1-dy-c1-1", "q1-dy-c2-1"],
                {"q1": {"c1": ["q1-dy-c1-1"], "c2": ["q1-dy-c2-1"]}},
                ["q1-dy-c1-1"],
                True,
                {"LimitedInstanceCapacity": {"q1-dy-c2-1"}},
            ),
            (
                4,
                ["q1-dy-c1-1", "q1-dy-c2-1"],
                {},
                ["q1-dy-c1-1", "q1-dy-c2-1"],
                {"q1": {"c1": ["q1-dy-c1-1"], "c2": ["q1-dy-c2-1"]}},
                [],
                True,
                {"InsufficientInstanceCapacity": {"q1-dy-c1-1", "q1-dy-c2-1"}},
            ),
        ],
    )
    def test_best_effort_node_assignment(
        self,
        mocker,
        instance_manager,
        assign_node_batch_size,
        failed_launch_nodes,
        instances_launched,
        nodes_resume_list,
        nodes_resume_mapping,
        successful_launched_nodes,
        update_node_address,
        expected_failed_nodes,
    ):
        # patch internal functions
        instance_manager._assign_instances_to_nodes = mocker.MagicMock()

        instance_manager.best_effort_node_assignment(
            assign_node_batch_size=assign_node_batch_size,
            failed_launch_nodes=failed_launch_nodes,
            instances_launched=instances_launched,
            nodes_resume_list=nodes_resume_list,
            nodes_resume_mapping=nodes_resume_mapping,
            successful_launched_nodes=successful_launched_nodes,
            update_node_address=update_node_address,
        )

        if len(successful_launched_nodes) == 0:
            instance_manager._assign_instances_to_nodes.assert_not_called()
        else:
            instance_manager._assign_instances_to_nodes.assert_called_with(
                update_node_address=update_node_address,
                nodes_to_launch=nodes_resume_mapping,
                instances_launched=instances_launched,
                assign_node_batch_size=assign_node_batch_size,
                raise_on_error=False,
            )

        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)

    @pytest.mark.parametrize(
        "assign_node_batch_size, instances_launched, nodes_resume_list, nodes_resume_mapping, "
        "successful_launched_nodes, update_node_address, expected_failed_nodes, "
        "expect_assign_instances_to_nodes_failure, unused_launched_instances, expected_unused_launched_instances",
        [
            (
                1,
                {},
                [],
                {},
                [],
                False,
                {},
                None,
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
            ),
            (
                2,
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ],
                        "c2": [
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ],
                    }
                },
                ["q1-dy-c1-1", "q1-dy-c2-1"],
                {"q1": {"c1": ["q1-dy-c1-1"], "c2": ["q1-dy-c2-1"]}},
                ["q1-dy-c1-1", "q1-dy-c2-1"],
                True,
                {},
                None,
                {},
                {},
            ),
            (
                3,
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ],
                    }
                },
                ["q1-dy-c1-1", "q1-dy-c2-1"],
                {"q1": {"c1": ["q1-dy-c1-1"], "c2": ["q1-dy-c2-1"]}},
                ["q1-dy-c1-1"],
                True,
                {"LimitedInstanceCapacity": {"q1-dy-c1-1", "q1-dy-c2-1"}},
                None,
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
            ),
            (
                4,
                {},
                ["q1-dy-c1-1", "q1-dy-c2-1"],
                {"q1": {"c1": ["q1-dy-c1-1"], "c2": ["q1-dy-c2-1"]}},
                [],
                True,
                {"InsufficientInstanceCapacity": {"q1-dy-c1-1", "q1-dy-c2-1"}},
                None,
                {},
                {},
            ),
            (
                5,
                {},
                [],
                {},
                [],
                False,
                {},
                InstanceToNodeAssignmentError(),
                {},
                {},
            ),
            (
                6,
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ],
                        "c2": [
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ],
                    }
                },
                ["q1-dy-c1-1", "q1-dy-c2-1"],
                {"q1": {"c1": ["q1-dy-c1-1"], "c2": ["q1-dy-c2-1"]}},
                ["q1-dy-c1-1", "q1-dy-c2-1"],
                True,
                {"Exception": {"q1-dy-c1-1", "q1-dy-c2-1"}},
                InstanceToNodeAssignmentError(),
                {},
                {},
            ),
        ],
    )
    def test_all_or_nothing_node_assignment(
        self,
        mocker,
        instance_manager,
        assign_node_batch_size,
        instances_launched,
        nodes_resume_list,
        nodes_resume_mapping,
        successful_launched_nodes,
        update_node_address,
        expected_failed_nodes,
        expect_assign_instances_to_nodes_failure,
        unused_launched_instances,
        expected_unused_launched_instances,
    ):
        # patch internal functions
        instance_manager._assign_instances_to_nodes = mocker.MagicMock(
            side_effect=expect_assign_instances_to_nodes_failure
        )
        instance_manager.unused_launched_instances = unused_launched_instances

        instance_manager.all_or_nothing_node_assignment(
            assign_node_batch_size=assign_node_batch_size,
            instances_launched=instances_launched,
            nodes_resume_list=nodes_resume_list,
            nodes_resume_mapping=nodes_resume_mapping,
            successful_launched_nodes=successful_launched_nodes,
            update_node_address=update_node_address,
        )

        if len(successful_launched_nodes) < len(nodes_resume_list):
            instance_manager._assign_instances_to_nodes.assert_not_called()
        else:
            instance_manager._assign_instances_to_nodes.assert_called_with(
                update_node_address=update_node_address,
                nodes_to_launch=nodes_resume_mapping,
                instances_launched=instances_launched,
                assign_node_batch_size=assign_node_batch_size,
                raise_on_error=True,
            )

        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)
        assert_that(instance_manager.unused_launched_instances).is_equal_to(expected_unused_launched_instances)

    @pytest.mark.parametrize(
        "job_list, "
        "node_list, "
        "launch_batch_size, "
        "assign_node_batch_size, "
        "update_node_address, "
        "scaling_strategy, "
        "unused_launched_instances, "
        "mock_launch_instances, "
        "expected_unused_launched_instances, "
        "expect_to_skip_job_level_launch",
        [
            (
                [],
                ["queue4-st-c5xlarge-1"],
                1,
                2,
                False,
                ScalingStrategy.BEST_EFFORT,
                {},
                {},
                {},
                True,
            ),
            (
                [],
                [],
                1,
                2,
                True,
                ScalingStrategy.BEST_EFFORT,
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {},
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                True,
            ),
            (
                [],
                [],
                1,
                2,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
                {},
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                False,
            ),
            (
                [],
                [],
                1,
                2,
                True,
                ScalingStrategy.ALL_OR_NOTHING,
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    }
                },
                False,
            ),
            (
                [
                    SlurmResumeJob(
                        job_id=140816,
                        nodes_alloc="queue3-st-c5xlarge-[7-10]",
                        nodes_resume="queue3-st-c5xlarge-[7-9]",
                        oversubscribe="NO",
                    ),
                ],
                ["queue4-st-c5xlarge-1"],
                3,
                2,
                True,
                ScalingStrategy.ALL_OR_NOTHING,
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {
                    "q2": {
                        "c2": [
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            )
                        ]
                    }
                },
                {
                    "q1": {
                        "c1": [
                            EC2Instance(
                                "i-12345", "ip.1.0.0.1", "ip-1-0-0-1", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    },
                    "q2": {
                        "c2": [
                            EC2Instance(
                                "i-12346", "ip.1.0.0.2", "ip-1-0-0-2", datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ),
                        ]
                    },
                },
                False,
            ),
        ],
    )
    def test_scaling_for_jobs_multi_node(
        self,
        mocker,
        instance_manager,
        job_list,
        node_list,
        launch_batch_size,
        assign_node_batch_size,
        update_node_address,
        scaling_strategy,
        unused_launched_instances,
        mock_launch_instances,
        expected_unused_launched_instances,
        expect_to_skip_job_level_launch,
    ):
        # patch internal functions
        instance_manager._launch_instances = mocker.MagicMock(return_value=mock_launch_instances)
        instance_manager._scaling_for_jobs = mocker.MagicMock()

        instance_manager.unused_launched_instances = unused_launched_instances

        instance_manager._scaling_for_jobs_multi_node(
            job_list=job_list,
            node_list=node_list,
            launch_batch_size=launch_batch_size,
            assign_node_batch_size=assign_node_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
        )

        instance_manager._scaling_for_jobs.assert_called_once_with(
            job_list=job_list,
            launch_batch_size=launch_batch_size,
            assign_node_batch_size=assign_node_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
            skip_launch=expect_to_skip_job_level_launch,
        )

        assert_that(instance_manager.unused_launched_instances).is_equal_to(expected_unused_launched_instances)

    @pytest.mark.parametrize(
        "nodeset, mock_failed_nodes, expected_failed_nodes",
        [
            (
                {},
                {},
                {},
            ),
            (
                {},
                {
                    "Exception": {"queue2-dy-c5xlarge-1", "queue1-st-c5xlarge-2", "queue2-st-c5xlarge-1"},
                    "some_error_code": {"queue1-st-c52xlarge-1"},
                },
                {
                    "Exception": {"queue2-dy-c5xlarge-1", "queue1-st-c5xlarge-2", "queue2-st-c5xlarge-1"},
                    "some_error_code": {"queue1-st-c52xlarge-1"},
                },
            ),
            (
                {"queue1-st-c5xlarge-2"},
                {
                    "Exception": {"queue2-dy-c5xlarge-1", "queue1-st-c5xlarge-2", "queue2-st-c5xlarge-1"},
                    "some_error_code": {"queue1-st-c52xlarge-1"},
                },
                {
                    "Exception": {"queue2-dy-c5xlarge-1", "queue2-st-c5xlarge-1"},
                    "some_error_code": {"queue1-st-c52xlarge-1"},
                },
            ),
            (
                {"queue2-dy-c5xlarge-1"},
                {
                    "Exception": {"queue2-dy-c5xlarge-1", "queue1-st-c5xlarge-2", "queue2-st-c5xlarge-1"},
                    "some_error_code": {"queue2-dy-c5xlarge-1"},
                },
                {
                    "Exception": {"queue1-st-c5xlarge-2", "queue2-st-c5xlarge-1"},
                    "some_error_code": set(),
                },
            ),
        ],
    )
    def test_reset_failed_nodes(self, instance_manager, nodeset, mock_failed_nodes, expected_failed_nodes):
        instance_manager.failed_nodes = mock_failed_nodes
        instance_manager._reset_failed_nodes(nodeset)
        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)


class TestNodeListScalingInstanceManager:
    @pytest.fixture
    def instance_manager(self, mocker):
        instance_manager = InstanceManagerFactory.get_manager(
            region="us-east-2",
            cluster_name="hit",
            boto3_config=botocore.config.Config(),
            table_name="table_name",
            head_node_private_ip="head.node.ip",
            head_node_hostname="head-node-hostname",
            hosted_zone="hosted_zone",
            dns_domain="dns.domain",
            use_private_hostname=False,
            fleet_config=FLEET_CONFIG,
            run_instances_overrides={},
            create_fleet_overrides={},
            job_level_scaling=False,
        )
        table_mock = mocker.patch.object(instance_manager, "_table")
        table_mock.table_name = "table_name"
        return instance_manager

    @pytest.mark.parametrize(
        "node_list, launch_batch_size, update_node_address, scaling_strategy",
        [
            (
                ["queue1-st-c5xlarge-2", "queue2-dy-c5xlarge-10"],
                10,
                False,
                ScalingStrategy.ALL_OR_NOTHING,
            )
        ],
    )
    def test_add_instances(
        self, instance_manager, mocker, node_list, launch_batch_size, update_node_address, scaling_strategy
    ):
        # patch internal functions
        instance_manager._add_instances_for_nodes = mocker.MagicMock()

        instance_manager.add_instances(
            node_list=node_list,
            launch_batch_size=launch_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
        )

        assert_that(instance_manager.failed_nodes).is_empty()
        instance_manager._add_instances_for_nodes.assert_called_with(
            node_list=node_list,
            launch_batch_size=launch_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
        )

    @pytest.mark.parametrize(
        (
            "nodes_to_launch",
            "launch_batch_size",
            "update_node_address",
            "scaling_strategy",
            "launched_instances",
            "expected_failed_nodes",
            "expected_update_nodes_calls",
            "dns_or_table_exception",
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
                ScalingStrategy.BEST_EFFORT,
                [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
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
                                "NetworkInterfaces": [
                                    {
                                        "Attachment": {
                                            "DeviceIndex": 0,
                                            "NetworkCardIndex": 0,
                                        },
                                        "PrivateIpAddress": "ip.1.0.0.2",
                                    },
                                ],
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
                            {
                                "InstanceId": "i-45678",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.4",
                                "PrivateDnsName": "ip-1-0-0-4",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                "NetworkInterfaces": [
                                    {
                                        "Attachment": {
                                            "DeviceIndex": 0,
                                            "NetworkCardIndex": 0,
                                        },
                                        "PrivateIpAddress": "ip.1.0.0.4",
                                    },
                                ],
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
                None,
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
                ScalingStrategy.BEST_EFFORT,
                [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
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
                            {
                                "InstanceId": "i-45678",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.4",
                                "PrivateDnsName": "ip-1-0-0-4",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                "NetworkInterfaces": [
                                    {
                                        "Attachment": {
                                            "DeviceIndex": 0,
                                            "NetworkCardIndex": 0,
                                        },
                                        "PrivateIpAddress": "ip.1.0.0.4",
                                    },
                                ],
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
                None,
            ),
            # no_update
            (
                {"queue1": {"c5xlarge": ["queue1-st-c5xlarge-2"]}},
                10,
                False,
                ScalingStrategy.BEST_EFFORT,
                [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
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
                            }
                        ]
                    },
                ],
                {},
                None,
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
                ScalingStrategy.BEST_EFFORT,
                [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
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
                None,
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
                ScalingStrategy.BEST_EFFORT,
                [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
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
                                "NetworkInterfaces": [
                                    {
                                        "Attachment": {
                                            "DeviceIndex": 0,
                                            "NetworkCardIndex": 0,
                                        },
                                        "PrivateIpAddress": "ip.1.0.0.3",
                                    },
                                ],
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
                                "NetworkInterfaces": [
                                    {
                                        "Attachment": {
                                            "DeviceIndex": 0,
                                            "NetworkCardIndex": 0,
                                        },
                                        "PrivateIpAddress": "ip.1.0.0.4",
                                    },
                                ],
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
                None,
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
                ScalingStrategy.BEST_EFFORT,
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
                                "NetworkInterfaces": [
                                    {
                                        "Attachment": {
                                            "DeviceIndex": 0,
                                            "NetworkCardIndex": 0,
                                        },
                                        "PrivateIpAddress": "ip.1.0.0.4",
                                    },
                                ],
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
                None,
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
                ScalingStrategy.ALL_OR_NOTHING,
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
                None,
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
                ScalingStrategy.BEST_EFFORT,
                [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
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
                                "NetworkInterfaces": [
                                    {
                                        "Attachment": {
                                            "DeviceIndex": 0,
                                            "NetworkCardIndex": 0,
                                        },
                                        "PrivateIpAddress": "ip.1.0.0.2",
                                    },
                                ],
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
                                "NetworkInterfaces": [
                                    {
                                        "Attachment": {
                                            "DeviceIndex": 0,
                                            "NetworkCardIndex": 0,
                                        },
                                        "PrivateIpAddress": "ip.1.0.0.5",
                                    },
                                ],
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
                            {
                                "InstanceId": "i-45678",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.4",
                                "PrivateDnsName": "ip-1-0-0-4",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                "NetworkInterfaces": [
                                    {
                                        "Attachment": {
                                            "DeviceIndex": 0,
                                            "NetworkCardIndex": 0,
                                        },
                                        "PrivateIpAddress": "ip.1.0.0.4",
                                    },
                                ],
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
                None,
            ),
            # launch_exception
            (
                {
                    "queue1": {
                        "c5xlarge": ["queue1-st-c5xlarge-2"],
                        "c52xlarge": ["queue1-st-c52xlarge-1"],
                    },
                    "queue2": {"c5xlarge": ["queue2-st-c5xlarge-1", "queue2-dy-c5xlarge-1"]},
                },
                18,
                True,
                ScalingStrategy.BEST_EFFORT,
                [Exception()],
                {
                    "Exception": {
                        "queue1-st-c5xlarge-2",
                        "queue1-st-c52xlarge-1",
                        "queue2-st-c5xlarge-1",
                        "queue2-dy-c5xlarge-1",
                    }
                },
                [],
                None,
            ),
            # dns_or_table_exception
            (
                {
                    "queue1": {
                        "c5xlarge": ["queue1-st-c5xlarge-2"],
                        "c52xlarge": ["queue1-st-c52xlarge-1"],
                    },
                    "queue2": {"c5xlarge": ["queue2-st-c5xlarge-1", "queue2-dy-c5xlarge-1"]},
                },
                18,
                True,
                ScalingStrategy.BEST_EFFORT,
                [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-12345",
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
                            {
                                "InstanceId": "i-45678",
                                "InstanceType": "c5.xlarge",
                                "PrivateIpAddress": "ip.1.0.0.4",
                                "PrivateDnsName": "ip-1-0-0-4",
                                "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                "NetworkInterfaces": [
                                    {
                                        "Attachment": {
                                            "DeviceIndex": 0,
                                            "NetworkCardIndex": 0,
                                        },
                                        "PrivateIpAddress": "ip.1.0.0.4",
                                    },
                                ],
                            },
                        ]
                    },
                ],
                {
                    "Exception": {"queue2-dy-c5xlarge-1", "queue1-st-c5xlarge-2", "queue2-st-c5xlarge-1"},
                    "some_error_code": {"queue1-st-c52xlarge-1"},
                },
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
                HostnameDnsStoreError(),
            ),
        ],
        ids=[
            "normal",
            "client_error",
            "no_update",
            "batch_size1",
            "batch_size2",
            "partial_launch",
            "scaling_strategy",
            "override_runinstances",
            "launch_exception",
            "dns_or_table_exception",
        ],
    )
    def test_add_instances_for_nodes(
        self,
        nodes_to_launch,
        launch_batch_size,
        update_node_address,
        scaling_strategy,
        launched_instances,
        expected_failed_nodes,
        expected_update_nodes_calls,
        dns_or_table_exception,
        mocker,
        instance_manager,
    ):
        mocker.patch("slurm_plugin.instance_manager.update_nodes")

        # patch internal functions
        instance_manager._store_assigned_hostnames = mocker.MagicMock(side_effect=dns_or_table_exception)
        instance_manager._update_dns_hostnames = mocker.MagicMock(side_effect=dns_or_table_exception)

        # Mock _update_slurm_node_addrs_and_failed_nodes but still allow original code to execute
        original_update_func = instance_manager._update_slurm_node_addrs_and_failed_nodes
        instance_manager._update_slurm_node_addrs_and_failed_nodes = mocker.MagicMock(side_effect=original_update_func)
        instance_manager._parse_nodes_resume_list = mocker.MagicMock(return_value=nodes_to_launch)
        # patch fleet manager calls
        mocker.patch.object(
            slurm_plugin.fleet_manager.Ec2RunInstancesManager,
            "_launch_instances",
            side_effect=launched_instances,
        )

        # run test
        instance_manager._add_instances_for_nodes(
            node_list=["placeholder_node_list"],
            launch_batch_size=launch_batch_size,
            update_node_address=update_node_address,
            scaling_strategy=scaling_strategy,
        )
        if expected_update_nodes_calls:
            instance_manager._update_slurm_node_addrs_and_failed_nodes.assert_has_calls(expected_update_nodes_calls)
        else:
            instance_manager._update_slurm_node_addrs_and_failed_nodes.assert_not_called()
        if expected_failed_nodes:
            assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)
        else:
            assert_that(instance_manager.failed_nodes).is_empty()

    @pytest.mark.parametrize(
        (
            "node_list, launched_nodes, expected_update_nodes_call, "
            "expected_failed_nodes, use_private_hostname, dns_domain, job_level_scaling, "
            "expected_update_nodes_output, update_nodes_exception"
        ),
        [
            (
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                call(["queue1-st-c5xlarge-1"], nodeaddrs=["ip-1"], nodehostnames=None),
                {},
                False,
                "dns.domain",
                False,
                {
                    "queue1-st-c5xlarge-1": EC2Instance(
                        id="id-1", private_ip="ip-1", hostname="hostname-1", launch_time="some_launch_time"
                    )
                },
                None,
            ),
            (
                ["queue1-st-c5xlarge-1"],
                {},
                None,
                {"InsufficientInstanceCapacity": {"queue1-st-c5xlarge-1"}},
                False,
                "dns.domain",
                False,
                {},
                None,
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
                False,
                {
                    "queue1-st-c5xlarge-1": EC2Instance(
                        id="id-1", private_ip="ip-1", hostname="hostname-1", launch_time="some_launch_time"
                    ),
                    "queue1-st-c5xlarge-2": EC2Instance(
                        id="id-2", private_ip="ip-2", hostname="hostname-2", launch_time="some_launch_time"
                    ),
                },
                None,
            ),
            (
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                call(["queue1-st-c5xlarge-1"], nodeaddrs=["ip-1"], nodehostnames=["hostname-1"]),
                {},
                True,
                "dns.domain",
                False,
                {
                    "queue1-st-c5xlarge-1": EC2Instance(
                        id="id-1", private_ip="ip-1", hostname="hostname-1", launch_time="some_launch_time"
                    )
                },
                None,
            ),
            (
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                call(["queue1-st-c5xlarge-1"], nodeaddrs=["ip-1"], nodehostnames=None),
                {},
                False,
                "",
                False,
                {
                    "queue1-st-c5xlarge-1": EC2Instance(
                        id="id-1", private_ip="ip-1", hostname="hostname-1", launch_time="some_launch_time"
                    )
                },
                None,
            ),
            (
                ["queue1-st-c5xlarge-1"],
                [EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time")],
                call(["queue1-st-c5xlarge-1"], nodeaddrs=["ip-1"], nodehostnames=None),
                {"Exception": {"queue1-st-c5xlarge-1"}},
                False,
                "",
                False,
                {},
                subprocess.CalledProcessError(1, "command"),
            ),
        ],
        ids=(
            "all_launched",
            "nothing_launched",
            "partial_launched",
            "forced_private_hostname",
            "no_dns_domain",
            "update_nodes_exception",
        ),
    )
    def test_update_slurm_node_addrs_and_failed_nodes(
        self,
        node_list,
        launched_nodes,
        expected_update_nodes_call,
        expected_failed_nodes,
        use_private_hostname,
        dns_domain,
        instance_manager,
        mocker,
        job_level_scaling,
        expected_update_nodes_output,
        update_nodes_exception,
    ):
        mock_update_nodes = mocker.patch(
            "slurm_plugin.instance_manager.update_nodes", side_effect=update_nodes_exception
        )
        instance_manager._use_private_hostname = use_private_hostname
        instance_manager._dns_domain = dns_domain

        update_slurm_node_addrs_and_failed_nodes_output = instance_manager._update_slurm_node_addrs_and_failed_nodes(
            node_list, launched_nodes
        )
        if expected_update_nodes_call:
            mock_update_nodes.assert_called_once()
            mock_update_nodes.assert_has_calls([expected_update_nodes_call])
        else:
            mock_update_nodes.assert_not_called()
        assert_that(instance_manager.failed_nodes).is_equal_to(expected_failed_nodes)

        assert_that(update_slurm_node_addrs_and_failed_nodes_output).is_equal_to(expected_update_nodes_output)
