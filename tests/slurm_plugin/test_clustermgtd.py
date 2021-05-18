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
from types import SimpleNamespace
from unittest.mock import call

import boto3
import botocore
import pytest
import slurm_plugin
from assertpy import assert_that
from common.schedulers.slurm_commands import PartitionStatus, SlurmNode, SlurmPartition, update_all_partitions
from slurm_plugin.clustermgtd import ClusterManager, ClustermgtdConfig, ComputeFleetStatus, ComputeFleetStatusManager
from slurm_plugin.common import (
    EC2_HEALTH_STATUS_UNHEALTHY_STATES,
    EC2_INSTANCE_ALIVE_STATES,
    EC2_SCHEDULED_EVENT_CODES,
    EC2Instance,
    EC2InstanceHealthState,
)

from tests.common import MockedBoto3Request


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
    return "slurm_plugin.common.boto3"


class TestClustermgtdConfig:
    @pytest.mark.parametrize(
        ("config_file", "expected_attributes"),
        [
            (
                "default.conf",
                {
                    # basic configs
                    "cluster_name": "hit",
                    "region": "us-east-2",
                    "_boto3_config": {"retries": {"max_attempts": 1, "mode": "standard"}},
                    "loop_time": 60,
                    "disable_all_cluster_management": False,
                    "heartbeat_file_path": "/home/ec2-user/clustermgtd_heartbeat",
                    "logging_config": os.path.join(
                        os.path.dirname(slurm_plugin.__file__), "logging", "parallelcluster_clustermgtd_logging.conf"
                    ),
                    "dynamodb_table": "table-name",
                    # launch configs
                    "update_node_address": True,
                    "launch_max_batch_size": 500,
                    # terminate configs
                    "terminate_max_batch_size": 1000,
                    "node_replacement_timeout": 3600,
                    "terminate_drain_nodes": True,
                    "terminate_down_nodes": True,
                    "orphaned_instance_timeout": 120,
                    # health check configs
                    "disable_ec2_health_check": False,
                    "disable_scheduled_event_health_check": False,
                    "disable_all_health_checks": False,
                    "health_check_timeout": 180,
                },
            ),
            (
                "all_options.conf",
                {
                    # basic configs
                    "cluster_name": "hit",
                    "region": "us-east-1",
                    "_boto3_config": {
                        "retries": {"max_attempts": 10, "mode": "standard"},
                        "proxies": {"https": "https://fake.proxy"},
                    },
                    "loop_time": 30,
                    "disable_all_cluster_management": True,
                    "heartbeat_file_path": "/home/ubuntu/clustermgtd_heartbeat",
                    "logging_config": "/my/logging/config",
                    "dynamodb_table": "table-name",
                    # launch configs
                    "update_node_address": False,
                    "launch_max_batch_size": 1,
                    # terminate configs
                    "terminate_max_batch_size": 500,
                    "node_replacement_timeout": 10,
                    "terminate_drain_nodes": False,
                    "terminate_down_nodes": False,
                    "orphaned_instance_timeout": 60,
                    # health check configs
                    "disable_ec2_health_check": True,
                    "disable_scheduled_event_health_check": True,
                    "disable_all_health_checks": False,
                    "health_check_timeout": 10,
                },
            ),
            (
                "health_check.conf",
                {
                    # basic configs
                    "cluster_name": "hit",
                    "region": "us-east-1",
                    "_boto3_config": {
                        "retries": {"max_attempts": 1, "mode": "standard"},
                        "proxies": {"https": "https://fake.proxy"},
                    },
                    "loop_time": 30,
                    "disable_all_cluster_management": True,
                    "heartbeat_file_path": "/home/ubuntu/clustermgtd_heartbeat",
                    "logging_config": "/my/logging/config",
                    "dynamodb_table": "table-name",
                    "head_node_private_ip": "head.node.ip",
                    "head_node_hostname": "head-node-hostname",
                    # launch configs
                    "update_node_address": False,
                    "launch_max_batch_size": 1,
                    # terminate configs
                    "terminate_max_batch_size": 500,
                    "node_replacement_timeout": 10,
                    "terminate_drain_nodes": False,
                    "terminate_down_nodes": False,
                    "orphaned_instance_timeout": 60,
                    # health check configs
                    "disable_ec2_health_check": True,
                    "disable_scheduled_event_health_check": True,
                    "disable_all_health_checks": True,
                    "health_check_timeout": 10,
                },
            ),
        ],
        ids=["default", "all_options", "health_check"],
    )
    def test_config_parsing(self, config_file, expected_attributes, test_datadir, mocker):
        mocker.patch("slurm_plugin.clustermgtd.retrieve_instance_type_mapping", return_value={"c5xlarge": "c5.xlarge"})
        sync_config = ClustermgtdConfig(test_datadir / config_file)
        for key in expected_attributes:
            assert_that(sync_config.__dict__.get(key)).is_equal_to(expected_attributes.get(key))

    def test_config_comparison(self, test_datadir, mocker):
        mocker.patch("slurm_plugin.clustermgtd.retrieve_instance_type_mapping", return_value={"c5xlarge": "c5.xlarge"})
        config = test_datadir / "config.conf"
        config_modified = test_datadir / "config_modified.conf"

        assert_that(ClustermgtdConfig(config)).is_equal_to(ClustermgtdConfig(config))
        assert_that(ClustermgtdConfig(config)).is_not_equal_to(ClustermgtdConfig(config_modified))


def test_set_config(initialize_instance_manager_mock, initialize_compute_fleet_status_manager_mock):
    initial_config = SimpleNamespace(some_key_1="some_value_1", some_key_2="some_value_2")
    updated_config = SimpleNamespace(some_key_1="some_value_1", some_key_2="some_value_2_changed")

    cluster_manager = ClusterManager(initial_config)
    assert_that(cluster_manager._config).is_equal_to(initial_config)
    cluster_manager.set_config(initial_config)
    assert_that(cluster_manager._config).is_equal_to(initial_config)
    cluster_manager.set_config(updated_config)
    assert_that(cluster_manager._config).is_equal_to(updated_config)

    assert_that(initialize_instance_manager_mock.call_count).is_equal_to(2)
    assert_that(initialize_compute_fleet_status_manager_mock.call_count).is_equal_to(2)


@pytest.mark.parametrize(
    "partitions, nodes, expected_inactive_nodes, expected_active_nodes",
    [
        (
            [
                SlurmPartition("partition1", "placeholder_nodes", "UP"),
                SlurmPartition("partition2", "placeholder_nodes", "INACTIVE"),
                SlurmPartition("partition3", "placeholder_nodes", "DRAIN"),
                SlurmPartition("partition4", "placeholder_nodes", "INACTIVE"),
            ],
            [
                SlurmNode("queue1-st-c5xlarge-1", "nodeaddr", "nodeaddr", "DOWN", "partition1"),
                SlurmNode("queue1-st-c5xlarge-2", "nodeaddr", "nodeaddr", "IDLE", "partition1"),
                SlurmNode("queue1-st-c5xlarge-3", "nodeaddr", "nodeaddr", "IDLE", "partition2"),
                SlurmNode("queue1-st-c5xlarge-4", "nodeaddr", "nodeaddr", "IDLE", "partition2"),
                SlurmNode("queue1-st-c5xlarge-5", "nodeaddr", "nodeaddr", "DRAIN", "partition3"),
                SlurmNode("queue1-st-c5xlarge-6", "nodeaddr", "nodeaddr", "DRAIN", "partition1,partition2"),
                SlurmNode("queue1-st-c5xlarge-7", "nodeaddr", "nodeaddr", "DRAIN", "partition2,partition3"),
                SlurmNode("queue1-st-c5xlarge-8", "nodeaddr", "nodeaddr", "DRAIN", "partition2,partition4"),
                SlurmNode("queue1-st-c5xlarge-8", "nodeaddr", "nodeaddr", "DRAIN", None),
            ],
            [
                SlurmNode("queue1-st-c5xlarge-3", "nodeaddr", "nodeaddr", "IDLE", "partition2"),
                SlurmNode("queue1-st-c5xlarge-4", "nodeaddr", "nodeaddr", "IDLE", "partition2"),
                SlurmNode("queue1-st-c5xlarge-8", "nodeaddr", "nodeaddr", "DRAIN", "partition2,partition4"),
            ],
            [
                SlurmNode("queue1-st-c5xlarge-1", "nodeaddr", "nodeaddr", "DOWN", "partition1"),
                SlurmNode("queue1-st-c5xlarge-2", "nodeaddr", "nodeaddr", "IDLE", "partition1"),
                SlurmNode("queue1-st-c5xlarge-5", "nodeaddr", "nodeaddr", "DRAIN", "partition3"),
                SlurmNode("queue1-st-c5xlarge-6", "nodeaddr", "nodeaddr", "DRAIN", "partition1,partition2"),
                SlurmNode("queue1-st-c5xlarge-7", "nodeaddr", "nodeaddr", "DRAIN", "partition2,partition3"),
            ],
        ),
    ],
    ids=["mixed"],
)
def test_get_node_info_from_partition(partitions, nodes, expected_inactive_nodes, expected_active_nodes, mocker):
    get_partition_info_with_retry_mock = mocker.patch(
        "slurm_plugin.clustermgtd.ClusterManager._get_partition_info_with_retry", return_value=partitions
    )
    get_node_info_with_retry_mock = mocker.patch(
        "slurm_plugin.clustermgtd.ClusterManager._get_node_info_with_retry", return_value=nodes
    )
    active_nodes, inactive_nodes = ClusterManager._get_node_info_from_partition()
    assert_that(active_nodes).is_equal_to(expected_active_nodes)
    assert_that(inactive_nodes).is_equal_to(expected_inactive_nodes)
    get_partition_info_with_retry_mock.assert_called_once()
    get_node_info_with_retry_mock.assert_called_once_with()


@pytest.mark.usefixtures("initialize_instance_manager_mock", "initialize_compute_fleet_status_manager_mock")
@pytest.mark.parametrize(
    (
        "mock_cluster_instances",
        "mock_backing_instances",
        "expected_result",
        "slurm_inactive_nodes",
        "expected_reset",
        "delete_instances_side_effect",
        "reset_nodes_side_effect",
    ),
    [
        (
            [
                EC2Instance("id-1", "ip-1", "hostname", "some_time"),
                EC2Instance("id-2", "ip-2", "hostname", "some_time"),
                EC2Instance("id-3", "ip-3", "hostname", "some_time"),
            ],
            ["id-3"],
            [
                EC2Instance("id-1", "ip-1", "hostname", "some_time"),
                EC2Instance("id-2", "ip-2", "hostname", "some_time"),
            ],
            [
                SlurmNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "some_state", "queue1"),
                SlurmNode(
                    "queue1-st-c5xlarge-5", "queue1-st-c5xlarge-5", "queue1-st-c5xlarge-5", "some_state", "queue1"
                ),
                SlurmNode(
                    "queue1-dy-c5xlarge-4", "queue1-dy-c5xlarge-4", "queue1-dy-c5xlarge-4", "some_state", "queue1"
                ),
                SlurmNode("queue1-st-c5xlarge-6", "ip-6", "ip-6", "IDLE", "queue1"),
                SlurmNode(
                    "queue1-st-c5xlarge-7", "queue1-st-c5xlarge-7", "queue1-st-c5xlarge-7", "POWERING_DOWN", "queue1"
                ),
                SlurmNode("queue1-dy-c5xlarge-8", "queue1-dy-c5xlarge-8", "queue1-dy-c5xlarge-8", "IDLE*", "queue1"),
                SlurmNode("queue1-st-c5xlarge-9", "queue1-st-c5xlarge-9", "queue1-st-c5xlarge-9", "IDLE*", "queue1"),
            ],
            ["queue1-st-c5xlarge-3", "queue1-dy-c5xlarge-4", "queue1-st-c5xlarge-6", "queue1-dy-c5xlarge-8"],
            None,
            None,
        ),
        (
            [
                EC2Instance("id-1", "ip-1", "hostname", "some_time"),
                EC2Instance("id-2", "ip-2", "hostname", "some_time"),
                EC2Instance("id-3", "ip-3", "hostname", "some_time"),
            ],
            ["id-3"],
            [
                EC2Instance("id-1", "ip-1", "hostname", "some_time"),
                EC2Instance("id-2", "ip-2", "hostname", "some_time"),
                EC2Instance("id-3", "ip-3", "hostname", "some_time"),
            ],
            [SlurmNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "some_state", "queue1")],
            None,
            Exception,
            None,
        ),
        (
            [
                EC2Instance("id-1", "ip-1", "hostname", "some_time"),
                EC2Instance("id-2", "ip-2", "hostname", "some_time"),
                EC2Instance("id-3", "ip-3", "hostname", "some_time"),
            ],
            ["id-3"],
            [
                EC2Instance("id-1", "ip-1", "hostname", "some_time"),
                EC2Instance("id-2", "ip-2", "hostname", "some_time"),
            ],
            [SlurmNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "some_state", "queue1")],
            ["queue1-st-c5xlarge-3"],
            None,
            Exception,
        ),
    ],
    ids=["normal", "delete_exception", "reset_exception"],
)
def test_clean_up_inactive_parititon(
    mock_cluster_instances,
    mock_backing_instances,
    slurm_inactive_nodes,
    expected_reset,
    expected_result,
    delete_instances_side_effect,
    reset_nodes_side_effect,
    mocker,
):
    # Test setup
    mock_sync_config = SimpleNamespace(terminate_max_batch_size=4)
    cluster_manager = ClusterManager(mock_sync_config)
    mock_instance_manager = mocker.patch.object(cluster_manager, "_instance_manager", auto_spec=True)
    if delete_instances_side_effect:
        mock_instance_manager.delete_instances = mocker.patch.object(
            mock_instance_manager, "delete_instances", side_effect=delete_instances_side_effect, auto_spec=True
        )
    if reset_nodes_side_effect:
        mock_reset_node = mocker.patch(
            "slurm_plugin.clustermgtd.reset_nodes", side_effect=reset_nodes_side_effect, auto_spec=True
        )
    else:
        mock_reset_node = mocker.patch("slurm_plugin.clustermgtd.reset_nodes", auto_spec=True)
    result = cluster_manager._clean_up_inactive_partition(slurm_inactive_nodes, mock_cluster_instances)
    mock_instance_manager.delete_instances.assert_called_with(mock_backing_instances, terminate_batch_size=4)
    if expected_reset:
        mock_reset_node.assert_called_with(
            expected_reset, raise_on_error=False, reason="inactive partition", state="down"
        )
    else:
        mock_reset_node.assert_not_called()
    assert_that(result).is_equal_to(expected_result)


@pytest.mark.usefixtures("initialize_compute_fleet_status_manager_mock")
def test_get_ec2_instances(mocker):
    # Test setup
    mock_sync_config = SimpleNamespace(
        region="us-east-2",
        cluster_name="hit-test",
        boto3_config=botocore.config.Config(),
        dynamodb_table="table_name",
        head_node_private_ip="head.node.ip",
        head_node_hostname="head-node-hostname",
        hosted_zone="hosted_zone",
        dns_domain="dns.domain",
        use_private_hostname=False,
        instance_name_type_mapping={"c5xlarge": "c5.xlarge"},
    )
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._instance_manager.get_cluster_instances = mocker.MagicMock()
    # Run test
    cluster_manager._get_ec2_instances()
    # Assert calls
    cluster_manager._instance_manager.get_cluster_instances.assert_called_with(
        include_head_node=False, alive_states_only=True
    )


@pytest.mark.parametrize(
    (
        "mock_instance_health_states",
        "disable_ec2_health_check",
        "disable_scheduled_event_health_check",
        "expected_handle_health_check_calls",
    ),
    [
        (
            ["some_instance_health_states"],
            False,
            False,
            [
                call(
                    ["some_instance_health_states"],
                    {
                        "id-1": EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
                        "id-2": EC2Instance("id-2", "ip-2", "hostname", "launch_time"),
                    },
                    {"ip-1": "some_slurm_node1", "ip-2": "some_slurm_node2"},
                    health_check_type=ClusterManager.HealthCheckTypes.ec2_health,
                ),
                call(
                    ["some_instance_health_states"],
                    {
                        "id-1": EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
                        "id-2": EC2Instance("id-2", "ip-2", "hostname", "launch_time"),
                    },
                    {"ip-1": "some_slurm_node1", "ip-2": "some_slurm_node2"},
                    health_check_type=ClusterManager.HealthCheckTypes.scheduled_event,
                ),
            ],
        ),
        (
            ["some_instance_health_states"],
            True,
            False,
            [
                call(
                    ["some_instance_health_states"],
                    {
                        "id-1": EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
                        "id-2": EC2Instance("id-2", "ip-2", "hostname", "launch_time"),
                    },
                    {"ip-1": "some_slurm_node1", "ip-2": "some_slurm_node2"},
                    health_check_type=ClusterManager.HealthCheckTypes.scheduled_event,
                )
            ],
        ),
        (["some_instance_health_states"], True, True, []),
        (
            ["some_instance_health_states"],
            False,
            True,
            [
                call(
                    ["some_instance_health_states"],
                    {
                        "id-1": EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
                        "id-2": EC2Instance("id-2", "ip-2", "hostname", "launch_time"),
                    },
                    {"ip-1": "some_slurm_node1", "ip-2": "some_slurm_node2"},
                    health_check_type=ClusterManager.HealthCheckTypes.ec2_health,
                )
            ],
        ),
        (
            [],
            False,
            False,
            [],
        ),
    ],
    ids=["basic", "disable_ec2", "disable_all", "disable_scheduled", "no_unhealthy_instance"],
)
@pytest.mark.usefixtures("initialize_compute_fleet_status_manager_mock")
def test_perform_health_check_actions(
    mock_instance_health_states,
    disable_ec2_health_check,
    disable_scheduled_event_health_check,
    expected_handle_health_check_calls,
    mocker,
):
    mock_cluster_instances = [
        EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
        EC2Instance("id-2", "ip-2", "hostname", "launch_time"),
    ]
    ip_to_slurm_node_map = {"ip-1": "some_slurm_node1", "ip-2": "some_slurm_node2"}
    mock_sync_config = SimpleNamespace(
        disable_ec2_health_check=disable_ec2_health_check,
        disable_scheduled_event_health_check=disable_scheduled_event_health_check,
        region="us-east-2",
        cluster_name="hit-test",
        boto3_config=botocore.config.Config(),
        dynamodb_table="table_name",
        head_node_private_ip="head.node.ip",
        head_node_hostname="head-node-hostname",
        hosted_zone="hosted_zone",
        dns_domain="dns.domain",
        use_private_hostname=False,
        instance_name_type_mapping={"c5xlarge": "c5.xlarge"},
    )
    # Mock functions
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._instance_manager.get_unhealthy_cluster_instance_status = mocker.MagicMock(
        return_value=mock_instance_health_states
    )
    cluster_manager._handle_health_check = mocker.MagicMock().patch()

    # Run test
    cluster_manager._perform_health_check_actions(mock_cluster_instances, ip_to_slurm_node_map)
    # Check function calls
    if expected_handle_health_check_calls:
        cluster_manager._handle_health_check.assert_has_calls(expected_handle_health_check_calls)
    else:
        cluster_manager._handle_health_check.assert_not_called()


@pytest.mark.parametrize(
    "instance_health_state, current_time, expected_result",
    [
        (
            EC2InstanceHealthState(
                "id-12345",
                "running",
                {"Details": [{}], "Status": "ok"},
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 0)}], "Status": "ok"},
                None,
            ),
            datetime(2020, 1, 1, 0, 0, 30),
            False,
        ),
        (
            EC2InstanceHealthState(
                "id-12345",
                "stopped",
                {"Details": [{}], "Status": "initializing"},
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 0)}], "Status": "initializing"},
                None,
            ),
            datetime(2020, 1, 1, 0, 0, 30),
            False,
        ),
        (
            EC2InstanceHealthState(
                "id-12345",
                "stopped",
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 0)}], "Status": "not-applicable"},
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 0)}], "Status": "not-applicable"},
                None,
            ),
            datetime(2020, 1, 1, 0, 0, 30),
            False,
        ),
        (
            EC2InstanceHealthState(
                "id-12345",
                "stopped",
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 0)}], "Status": "insufficient-data"},
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 0)}], "Status": "insufficient-data"},
                None,
            ),
            datetime(2020, 1, 1, 0, 0, 30),
            False,
        ),
        (
            EC2InstanceHealthState(
                "id-12345",
                "stopped",
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 15)}], "Status": "initializing"},
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 0)}], "Status": "impaired"},
                None,
            ),
            datetime(2020, 1, 1, 0, 0, 30),
            True,
        ),
        (
            EC2InstanceHealthState(
                "id-12345",
                "stopped",
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 15)}], "Status": "initializing"},
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 0)}], "Status": "impaired"},
                None,
            ),
            datetime(2020, 1, 1, 0, 0, 29),
            False,
        ),
    ],
    ids=["ok", "initializing", "not-applicable", "insufficient-data", "impaired", "timeout"],
)
def test_fail_ec2_health_check(instance_health_state, current_time, expected_result):
    assert_that(
        ClusterManager._fail_ec2_health_check(instance_health_state, current_time, health_check_timeout=30)
    ).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "instance_health_state, expected_result",
    [
        (
            EC2InstanceHealthState(
                "id-12345",
                "running",
                {"Details": [{}], "Status": "ok"},
                {"Details": [{}], "Status": "ok"},
                [],
            ),
            False,
        ),
        (
            EC2InstanceHealthState(
                "id-12345",
                "running",
                {"Details": [{}], "Status": "ok"},
                {"Details": [{}], "Status": "ok"},
                [{"InstanceEventId": "someid"}],
            ),
            True,
        ),
    ],
    ids=["no_event", "has_event"],
)
def test_fail_scheduled_events_health_check(instance_health_state, expected_result):
    assert_that(ClusterManager._fail_scheduled_events_check(instance_health_state)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    (
        "health_check_type",
        "mock_fail_ec2_side_effect",
        "mock_fail_scheduled_events_side_effect",
        "expected_failed_nodes",
        "current_node_in_replacement",
        "expected_node_in_replacement",
    ),
    [
        (
            ClusterManager.HealthCheckTypes.scheduled_event,
            [True, False],
            [False, True],
            ["queue1-st-c5xlarge-2"],
            {"some_node_in_replacement1"},
            {"some_node_in_replacement1"},
        ),
        (
            ClusterManager.HealthCheckTypes.ec2_health,
            [True, False],
            [False, True],
            ["queue1-st-c5xlarge-1"],
            {"some_node_in_replacement1", "queue1-st-c5xlarge-1"},
            {"some_node_in_replacement1"},
        ),
        (ClusterManager.HealthCheckTypes.ec2_health, [False, False], [False, True], [], {}, {}),
    ],
    ids=["scheduled_event", "ec2_health", "all_healthy"],
)
@pytest.mark.usefixtures("initialize_compute_fleet_status_manager_mock", "initialize_instance_manager_mock")
def test_handle_health_check(
    health_check_type,
    mock_fail_ec2_side_effect,
    mock_fail_scheduled_events_side_effect,
    expected_failed_nodes,
    current_node_in_replacement,
    expected_node_in_replacement,
    mocker,
):
    # Define variable that will be used for all tests
    health_state_1 = EC2InstanceHealthState("id-1", "some_state", "some_status", "some_status", "some_event")
    health_state_2 = EC2InstanceHealthState("id-2", "some_state", "some_status", "some_status", "some_event")
    placeholder_states = [health_state_1, health_state_2]
    id_to_instance_map = {
        "id-1": EC2Instance("id-1", "ip-1", "host-1", "some_launch_time"),
        "id-2": EC2Instance("id-2", "ip-2", "host-2", "some_launch_time"),
    }
    ip_to_slurm_node_map = {
        "ip-1": SlurmNode("queue1-st-c5xlarge-1", "ip-1", "host-1", "some_states", "queue1"),
        "ip-2": SlurmNode("queue1-st-c5xlarge-2", "ip-2", "host-2", "some_states", "queue1"),
    }
    mock_ec2_health_check = mocker.patch(
        "slurm_plugin.clustermgtd.ClusterManager._fail_ec2_health_check",
        side_effect=mock_fail_ec2_side_effect,
    )
    mock_scheduled_health_check = mocker.patch(
        "slurm_plugin.clustermgtd.ClusterManager._fail_scheduled_events_check",
        side_effect=mock_fail_scheduled_events_side_effect,
    )
    # Setup mocking
    mock_sync_config = SimpleNamespace(health_check_timeout=10)
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._current_time = "some_current_time"
    cluster_manager._static_nodes_in_replacement = current_node_in_replacement
    drain_node_mock = mocker.patch("slurm_plugin.clustermgtd.set_nodes_drain", auto_spec=True)
    # Run tests
    cluster_manager._handle_health_check(
        placeholder_states, id_to_instance_map, ip_to_slurm_node_map, health_check_type
    )
    # Assert on calls
    if health_check_type == ClusterManager.HealthCheckTypes.scheduled_event:
        mock_scheduled_health_check.assert_has_calls(
            [call(instance_health_state=health_state_1), call(instance_health_state=health_state_2)]
        )
    else:
        mock_ec2_health_check.assert_has_calls(
            [
                call(instance_health_state=health_state_1, current_time="some_current_time", health_check_timeout=10),
                call(instance_health_state=health_state_2, current_time="some_current_time", health_check_timeout=10),
            ]
        )
    if expected_failed_nodes:
        drain_node_mock.assert_called_with(expected_failed_nodes, reason=f"Node failing {health_check_type}")
    else:
        drain_node_mock.assert_not_called()
    assert_that(cluster_manager._static_nodes_in_replacement).is_equal_to(expected_node_in_replacement)


@pytest.mark.parametrize(
    "current_replacing_nodes, slurm_nodes, expected_replacing_nodes",
    [
        (
            {"queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-4"},
            [
                SlurmNode("queue1-st-c5xlarge-1", "ip", "hostname", "IDLE+CLOUD", "queue1"),
                SlurmNode("queue1-st-c5xlarge-2", "ip", "hostname", "DOWN+CLOUD", "queue1"),
                SlurmNode("queue1-st-c5xlarge-3", "ip", "hostname", "IDLE+CLOUD", "queue1"),
            ],
            {"queue1-st-c5xlarge-2"},
        )
    ],
    ids=["mixed"],
)
@pytest.mark.usefixtures("initialize_instance_manager_mock", "initialize_compute_fleet_status_manager_mock")
def test_update_static_nodes_in_replacement(current_replacing_nodes, slurm_nodes, expected_replacing_nodes, mocker):
    cluster_manager = ClusterManager(mocker.MagicMock())
    cluster_manager._static_nodes_in_replacement = current_replacing_nodes
    cluster_manager._update_static_nodes_in_replacement(slurm_nodes)
    assert_that(cluster_manager._static_nodes_in_replacement).is_equal_to(expected_replacing_nodes)


@pytest.mark.parametrize(
    "current_replacing_nodes, node, private_ip_to_instance_map, current_time, expected_result",
    [
        (
            set(),
            SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
            {"ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0))},
            datetime(2020, 1, 1, 0, 0, 29),
            False,
        ),
        (
            {"queue1-st-c5xlarge-1"},
            SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
            {},
            datetime(2020, 1, 1, 0, 0, 29),
            False,
        ),
        (
            {"queue1-st-c5xlarge-1"},
            SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD", "queue1"),
            {"ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0))},
            datetime(2020, 1, 1, 0, 0, 29),
            True,
        ),
        (
            {"queue1-st-c5xlarge-1"},
            SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
            {"ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0))},
            datetime(2020, 1, 1, 0, 0, 30),
            False,
        ),
    ],
    ids=["not_in_replacement", "no-backing-instance", "in_replacement", "timeout"],
)
@pytest.mark.usefixtures("initialize_instance_manager_mock", "initialize_compute_fleet_status_manager_mock")
def test_is_node_being_replaced(
    current_replacing_nodes, node, private_ip_to_instance_map, current_time, expected_result
):
    mock_sync_config = SimpleNamespace(node_replacement_timeout=30)
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._current_time = current_time
    cluster_manager._static_nodes_in_replacement = current_replacing_nodes
    assert_that(cluster_manager._is_node_being_replaced(node, private_ip_to_instance_map)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, expected_result",
    [
        (SlurmNode("queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"), True),
    ],
    ids=["static_addr_not_set", "static_valid"],
)
def test_is_static_node_configuration_valid(node, expected_result):
    assert_that(ClusterManager._is_static_node_configuration_valid(node)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, instances_ips_in_cluster, expected_result",
    [
        (
            SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
            ["ip-2"],
            False,
        ),
        (
            SlurmNode("node-dy-c5xlarge-1", "node-dy-c5xlarge-1", "hostname", "IDLE+CLOUD+POWER", "node"),
            ["ip-2"],
            True,
        ),
        (
            SlurmNode("node-dy-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+POWER", "node"),
            ["ip-2"],
            False,
        ),
        (
            SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+POWER", "queue1"),
            ["ip-1"],
            True,
        ),
    ],
    ids=["static_no_backing", "dynamic_power_save", "dynamic_no_backing", "static_valid"],
)
def test_is_backing_instance_valid(node, instances_ips_in_cluster, expected_result):
    assert_that(ClusterManager._is_backing_instance_valid(node, instances_ips_in_cluster)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, mock_sync_config, mock_is_node_being_replaced, expected_result",
    [
        (
            SlurmNode("queue-st-c5xlarge-1", "some_ip", "hostname", "MIXED+CLOUD", "queue"),
            SimpleNamespace(terminate_drain_nodes=True, terminate_down_nodes=True),
            None,
            True,
        ),
        (
            SlurmNode("queue-st-c5xlarge-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN", "queue"),
            SimpleNamespace(terminate_drain_nodes=True, terminate_down_nodes=True),
            False,
            False,
        ),
        (
            SlurmNode("queue-st-c5xlarge-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN", "queue"),
            SimpleNamespace(terminate_drain_nodes=True, terminate_down_nodes=True),
            True,
            True,
        ),
        (
            SlurmNode("queue-st-c5xlarge-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN", "queue"),
            SimpleNamespace(terminate_drain_nodes=False, terminate_down_nodes=True),
            False,
            True,
        ),
        (
            SlurmNode("queue-st-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD", "queue"),
            SimpleNamespace(terminate_drain_nodes=True, terminate_down_nodes=True),
            False,
            False,
        ),
        (
            SlurmNode("queue-st-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD", "queue"),
            SimpleNamespace(terminate_drain_nodes=True, terminate_down_nodes=True),
            True,
            True,
        ),
        (
            SlurmNode("queue-st-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD", "queue"),
            SimpleNamespace(terminate_drain_nodes=True, terminate_down_nodes=False),
            False,
            True,
        ),
    ],
    ids=[
        "healthy_node",
        "drained_not_in_replacement",
        "drained_in_replacement",
        "drain_not_term",
        "down_not_in_replacement",
        "down_in_replacement",
        "down_not_term",
    ],
)
@pytest.mark.usefixtures("initialize_instance_manager_mock", "initialize_compute_fleet_status_manager_mock")
def test_is_node_state_healthy(node, mock_sync_config, mock_is_node_being_replaced, expected_result, mocker):
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._is_node_being_replaced = mocker.MagicMock(return_value=mock_is_node_being_replaced)
    assert_that(
        cluster_manager._is_node_state_healthy(node, private_ip_to_instance_map={"placeholder phonebook"})
    ).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, private_ip_to_instance_map, expected_result",
    [
        (
            SlurmNode("queue-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue"),
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                "ip-2": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            },
            True,
        ),
        (
            SlurmNode("queue-st-c5xlarge-1", "queue-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue"),
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                "ip-2": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            },
            False,
        ),
        (
            SlurmNode("queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue"),
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                "ip-2": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            },
            True,
        ),
        (
            SlurmNode("queue-dy-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                "ip-2": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            },
            False,
        ),
        (
            SlurmNode("queue-st-c5xlarge-1", "ip-2", "hostname", "DOWN+CLOUD", "queue"),
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                "ip-2": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            },
            False,
        ),
        # Powering_down nodes are handled separately, always considered healthy by this workflow
        (
            SlurmNode("queue-dy-c5xlarge-1", "ip-2", "hostname", "DOWN+CLOUD+POWERING_DOWN", "queue"),
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                "ip-2": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            },
            True,
        ),
        # Node in POWER_SAVE, but still has ip associated should be considered unhealthy
        (
            SlurmNode("queue-dy-c5xlarge-1", "ip-2", "hostname", "IDLE+CLOUD+POWER", "queue"),
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            },
            False,
        ),
        # Node in POWER_SAVE, but also in DOWN should be considered unhealthy
        (
            SlurmNode("queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "hostname", "DOWN+CLOUD+POWER", "queue"),
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                "ip-2": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            },
            False,
        ),
        (
            SlurmNode("queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "IDLE+CLOUD+POWER", "queue"),
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            },
            True,
        ),
    ],
    ids=[
        "basic",
        "static_nodeaddr_not_set",
        "dynamic_nodeaddr_not_set",
        "dynamic_unhealthy",
        "static_unhealthy",
        "powering_down",
        "power_unhealthy1",
        "power_unhealthy2",
        "power_healthy",
    ],
)
@pytest.mark.usefixtures("initialize_instance_manager_mock", "initialize_compute_fleet_status_manager_mock")
def test_is_node_healthy(node, private_ip_to_instance_map, expected_result, mocker):
    mock_sync_config = SimpleNamespace(terminate_down_nodes=True)
    cluster_manager = ClusterManager(mock_sync_config)
    assert_that(cluster_manager._is_node_healthy(node, private_ip_to_instance_map)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "unhealthy_dynamic_nodes, mock_backing_instances, expected_power_save_node_list",
    [
        (
            [
                SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
                SlurmNode("queue1-st-c5xlarge-2", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
            ],
            ["id-1", "id-2"],
            ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2"],
        )
    ],
    ids=["basic"],
)
@pytest.mark.usefixtures("initialize_instance_manager_mock", "initialize_compute_fleet_status_manager_mock")
def test_handle_unhealthy_dynamic_nodes(
    unhealthy_dynamic_nodes, mock_backing_instances, expected_power_save_node_list, mocker
):
    mock_sync_config = SimpleNamespace(terminate_max_batch_size=4)
    cluster_manager = ClusterManager(mock_sync_config)
    mock_instance_manager = mocker.patch.object(cluster_manager, "_instance_manager", auto_spec=True)
    mocker.patch(
        "slurm_plugin.clustermgtd.ClusterManager._get_backing_instance_ids",
        return_value=mock_backing_instances,
        auto_spec=True,
    )
    power_save_mock = mocker.patch("slurm_plugin.clustermgtd.set_nodes_down_and_power_save", auto_spec=True)
    cluster_manager._handle_unhealthy_dynamic_nodes(unhealthy_dynamic_nodes, {"placeholder": "map"})
    mock_instance_manager.delete_instances.assert_called_with(["id-1", "id-2"], terminate_batch_size=4)
    power_save_mock.assert_called_with(expected_power_save_node_list, reason="Scheduler health check failed")


@pytest.mark.parametrize(
    "slurm_nodes, mock_backing_instances, expected_powering_down_nodes",
    [
        (
            [
                SlurmNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
                SlurmNode("queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"),
                SlurmNode("queue1-dy-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD+POWER", "queue1"),
                SlurmNode("queue1-dy-c5xlarge-4", "ip-4", "hostname", "IDLE+CLOUD+POWER_", "queue1"),
                SlurmNode(
                    "queue1-dy-c5xlarge-5", "queue1-dy-c5xlarge-5", "queue1-dy-c5xlarge-5", "POWERING_DOWN", "queue1"
                ),
                SlurmNode("queue1-st-c5xlarge-6", "ip-6", "hostname", "POWERING_DOWN", "queue1"),
            ],
            ["id-1", "id-2"],
            [
                SlurmNode("queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"),
                SlurmNode("queue1-dy-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD+POWER", "queue1"),
            ],
        )
    ],
    ids=["basic"],
)
@pytest.mark.usefixtures("initialize_instance_manager_mock", "initialize_compute_fleet_status_manager_mock")
def test_handle_powering_down_nodes(slurm_nodes, mock_backing_instances, expected_powering_down_nodes, mocker):
    mock_sync_config = SimpleNamespace(terminate_max_batch_size=4)
    cluster_manager = ClusterManager(mock_sync_config)
    mock_instance_manager = mocker.patch.object(cluster_manager, "_instance_manager", auto_spec=True)
    get_backing_instance_mock = mocker.patch(
        "slurm_plugin.clustermgtd.ClusterManager._get_backing_instance_ids",
        return_value=mock_backing_instances,
        auto_spec=True,
    )
    reset_nodes_mock = mocker.patch("slurm_plugin.clustermgtd.reset_nodes", auto_spec=True)
    cluster_manager._handle_powering_down_nodes(slurm_nodes, {"placeholder": "map"})
    get_backing_instance_mock.assert_called_with(expected_powering_down_nodes, {"placeholder": "map"})
    mock_instance_manager.delete_instances.assert_called_with(["id-1", "id-2"], terminate_batch_size=4)
    # We don't need to reset nodes manually because cloud_reg_addrs option is specified
    reset_nodes_mock.assert_not_called()


@pytest.mark.parametrize(
    (
        "current_replacing_nodes",
        "unhealthy_static_nodes",
        "private_ip_to_instance_map",
        "launched_instances",
        "expected_replacing_nodes",
        "delete_instance_list",
        "add_node_list",
    ),
    [
        (
            {"current-queue1-st-c5xlarge-6"},
            [
                SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
                SlurmNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD", "queue1"),
                SlurmNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue1"),
            ],
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", "some_launch_time"),
                "ip-2": EC2Instance("id-2", "ip-2", "hostname", "some_launch_time"),
            },
            [
                EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
                EC2Instance("id-3", "ip-3", "hostname-3", "some_launch_time"),
            ],
            {
                "current-queue1-st-c5xlarge-6",
                "queue1-st-c5xlarge-1",
                "queue1-st-c5xlarge-2",
                "queue1-st-c5xlarge-3",
            },
            list({"id-1", "id-2"}),
            ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3"],
        ),
        (
            {"current-queue1-st-c5xlarge-6"},
            [
                SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
                SlurmNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD", "queue1"),
                SlurmNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue1"),
            ],
            {
                "ip-4": EC2Instance("id-1", "ip-4", "hostname", "some_launch_time"),
                "ip-5": EC2Instance("id-2", "ip-5", "hostname", "some_launch_time"),
            },
            [
                EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
                EC2Instance("id-3", "ip-3", "hostname-3", "some_launch_time"),
            ],
            {
                "current-queue1-st-c5xlarge-6",
                "queue1-st-c5xlarge-1",
                "queue1-st-c5xlarge-2",
                "queue1-st-c5xlarge-3",
            },
            [],
            ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3"],
        ),
        (
            {"current-queue1-st-c5xlarge-6"},
            [
                SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
                SlurmNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD", "queue1"),
                SlurmNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue1"),
            ],
            {
                "ip-4": EC2Instance("id-1", "ip-4", "hostname", "some_launch_time"),
                "ip-5": EC2Instance("id-2", "ip-5", "hostname", "some_launch_time"),
            },
            [
                EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
            ],
            {"current-queue1-st-c5xlarge-6", "queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2"},
            [],
            ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3"],
        ),
    ],
    ids=["basic", "no_associated_instances", "partial_launch"],
)
@pytest.mark.usefixtures("initialize_compute_fleet_status_manager_mock")
def test_handle_unhealthy_static_nodes(
    current_replacing_nodes,
    unhealthy_static_nodes,
    private_ip_to_instance_map,
    launched_instances,
    expected_replacing_nodes,
    delete_instance_list,
    add_node_list,
    mocker,
    caplog,
    request,
):
    # Test setup
    mock_sync_config = SimpleNamespace(
        terminate_max_batch_size=1,
        launch_max_batch_size=5,
        update_node_address=True,
        region="us-east-2",
        cluster_name="hit-test",
        boto3_config=botocore.config.Config(),
        dynamodb_table="table_name",
        head_node_private_ip="head.node.ip",
        head_node_hostname="head-node-hostname",
        hosted_zone="hosted_zone",
        dns_domain="dns.domain",
        use_private_hostname=False,
        instance_name_type_mapping={"c5xlarge": "c5.xlarge"},
    )
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._static_nodes_in_replacement = current_replacing_nodes

    # Mock associated function
    cluster_manager._instance_manager.delete_instances = mocker.MagicMock()
    cluster_manager._instance_manager._parse_requested_instances = mocker.MagicMock(
        return_value={
            "some_queue": {
                "some_instance_type": ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3"]
            }
        }
    )
    cluster_manager._instance_manager._launch_ec2_instances = mocker.MagicMock(return_value=launched_instances)
    mocker.patch("slurm_plugin.common.update_nodes")
    cluster_manager._instance_manager._store_assigned_hostnames = mocker.MagicMock()
    cluster_manager._instance_manager._update_dns_hostnames = mocker.MagicMock()
    # Mock add_instances_for_nodes but still try to execute original code
    original_add_instances = cluster_manager._instance_manager.add_instances_for_nodes
    cluster_manager._instance_manager.add_instances_for_nodes = mocker.MagicMock(side_effect=original_add_instances)
    update_mock = mocker.patch("slurm_plugin.clustermgtd.set_nodes_down", return_value=None, auto_spec=True)
    # Run test
    cluster_manager._handle_unhealthy_static_nodes(unhealthy_static_nodes, private_ip_to_instance_map)
    # Assert calls
    update_mock.assert_called_with(add_node_list, reason="Static node maintenance: unhealthy node is being replaced")
    if delete_instance_list:
        cluster_manager._instance_manager.delete_instances.assert_called_with(
            delete_instance_list, terminate_batch_size=1
        )
    else:
        cluster_manager._instance_manager.delete_instances.assert_not_called()
    cluster_manager._instance_manager.add_instances_for_nodes.assert_called_with(add_node_list, 5, True)
    if "partial_launch" not in request.node.name:
        assert_that(caplog.text).is_empty()
    assert_that(cluster_manager._static_nodes_in_replacement).is_equal_to(expected_replacing_nodes)


@pytest.mark.parametrize(
    "slurm_nodes, private_ip_to_instance_map, expected_result",
    [
        (
            [
                SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "some_state", "queue1"),
                SlurmNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "some_state", "queue1"),
                SlurmNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "some_state", "queue1"),
                SlurmNode("queue1-st-c5xlarge-999", "ip-1", "hostname", "some_state", "queue1"),
            ],
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
                "ip-2": EC2Instance("id-2", "ip-2", "hostname", "launch_time"),
            },
            list({"id-1", "id-2"}),
        )
    ],
    ids=["basic"],
)
@pytest.mark.usefixtures("initialize_instance_manager_mock", "initialize_compute_fleet_status_manager_mock")
def test_get_backing_instance_ids(slurm_nodes, private_ip_to_instance_map, expected_result):
    assert_that(ClusterManager._get_backing_instance_ids(slurm_nodes, private_ip_to_instance_map)).is_equal_to(
        expected_result
    )


@pytest.mark.parametrize(
    "private_ip_to_instance_map, active_nodes, mock_unhealthy_nodes",
    [
        (
            {"ip-1", EC2Instance("id-1", "ip-1", "hostname", "launch_time")},
            [
                SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "some_state", "queue1"),
                SlurmNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "some_state", "queue1"),
            ],
            (["queue1-st-c5xlarge-1"], ["queue1-st-c5xlarge-2"]),
        ),
        (
            {"ip-1", EC2Instance("id-1", "ip-1", "hostname", "launch_time")},
            [
                SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "some_state", "queue1"),
                SlurmNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "some_state", "queue1"),
                SlurmNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "some_state", "queue1"),
            ],
            (["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1"], ["queue1-st-c5xlarge-2"]),
        ),
    ],
    ids=["basic", "repetitive_ip"],
)
@pytest.mark.usefixtures("initialize_instance_manager_mock", "initialize_compute_fleet_status_manager_mock")
def test_maintain_nodes(private_ip_to_instance_map, active_nodes, mock_unhealthy_nodes, mocker):
    # Mock functions
    cluster_manager = ClusterManager(mocker.MagicMock())
    mock_update_replacement = mocker.patch.object(
        cluster_manager, "_update_static_nodes_in_replacement", auto_spec=True
    )
    mock_find_unhealthy = mocker.patch.object(
        cluster_manager, "_find_unhealthy_slurm_nodes", return_value=mock_unhealthy_nodes, auto_spec=True
    )
    mock_handle_dynamic = mocker.patch.object(cluster_manager, "_handle_unhealthy_dynamic_nodes", auto_spec=True)
    mock_handle_static = mocker.patch.object(cluster_manager, "_handle_unhealthy_static_nodes", auto_spec=True)
    mock_handle_powering_down_nodes = mocker.patch.object(
        cluster_manager, "_handle_powering_down_nodes", auto_spec=True
    )
    # Run test
    cluster_manager._maintain_nodes(private_ip_to_instance_map, active_nodes)
    # Check function calls
    mock_update_replacement.assert_called_with(active_nodes)
    mock_find_unhealthy.assert_called_with(active_nodes, private_ip_to_instance_map)
    mock_handle_dynamic.assert_called_with(mock_unhealthy_nodes[0], private_ip_to_instance_map)
    mock_handle_static.assert_called_with(mock_unhealthy_nodes[1], private_ip_to_instance_map)
    mock_handle_powering_down_nodes.assert_called_with(active_nodes, private_ip_to_instance_map)


@pytest.mark.parametrize(
    "cluster_instances, private_ip_to_instance_map, current_time, expected_instance_to_terminate",
    [
        (
            [
                EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                EC2Instance("id-2", "ip-2", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            ],
            {"ip-1": "some_slurm_node1", "ip-2": "some_slurm_node2"},
            datetime(2020, 1, 1, 0, 0, 30),
            [],
        ),
        (
            [
                EC2Instance("id-3", "ip-3", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                EC2Instance("id-2", "ip-2", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            ],
            {"ip-1": "some_slurm_node1", "ip-2": "some_slurm_node2"},
            datetime(2020, 1, 1, 0, 0, 30),
            ["id-3"],
        ),
        (
            [
                EC2Instance("id-3", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                EC2Instance("id-2", "ip-2", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            ],
            {"ip-1": "some_slurm_node1", "ip-2": "some_slurm_node2"},
            datetime(2020, 1, 1, 0, 0, 29),
            [],
        ),
    ],
    ids=["all_good", "orphaned", "orphaned_timeout"],
)
@pytest.mark.usefixtures("initialize_compute_fleet_status_manager_mock")
def test_terminate_orphaned_instances(
    cluster_instances, private_ip_to_instance_map, current_time, expected_instance_to_terminate, mocker
):
    # Mock functions
    mock_sync_config = SimpleNamespace(
        orphaned_instance_timeout=30,
        terminate_max_batch_size=4,
        region="us-east-2",
        cluster_name="hit-test",
        boto3_config=botocore.config.Config(),
        dynamodb_table="table_name",
        head_node_private_ip="head.node.ip",
        head_node_hostname="head-node-hostname",
        hosted_zone="hosted_zone",
        dns_domain="dns.domain",
        use_private_hostname=False,
        instance_name_type_mapping={"c5xlarge": "c5.xlarge"},
    )
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._current_time = current_time
    cluster_manager._instance_manager.delete_instances = mocker.MagicMock()
    # Run test
    cluster_manager._terminate_orphaned_instances(cluster_instances, private_ip_to_instance_map)
    # Check function calls
    if expected_instance_to_terminate:
        cluster_manager._instance_manager.delete_instances.assert_called_with(
            expected_instance_to_terminate, terminate_batch_size=4
        )


@pytest.mark.parametrize(
    "disable_cluster_management, disable_health_check, mock_cluster_instances, mock_active_nodes, mock_inactive_nodes",
    [
        (
            False,
            False,
            [
                EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
                EC2Instance("id-2", "ip-2", "hostname", "launch_time"),
            ],
            [
                SlurmNode("queue1-st-c5xlarge-1", "ip", "hostname", "some_state", "queue1"),
                SlurmNode("queue1-st-c5xlarge-2", "ip", "hostname", "some_state", "queue1"),
            ],
            [],
        ),
        (
            True,
            False,
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [
                SlurmNode("queue1-st-c5xlarge-1", "ip", "hostname", "some_state", "queue1"),
                SlurmNode("queue1-st-c5xlarge-2", "ip", "hostname", "some_state", "queue1"),
            ],
            [],
        ),
        (
            False,
            True,
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [
                SlurmNode("queue1-st-c5xlarge-1", "ip", "hostname", "some_state", "queue1"),
                SlurmNode("queue1-st-c5xlarge-2", "ip", "hostname", "some_state", "queue1"),
            ],
            [],
        ),
        (
            False,
            True,
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [],
            [
                SlurmNode("inactive-queue1-st-c5xlarge-1", "ip", "hostname", "some_state", "inactive-queue1"),
                SlurmNode("inactive-queue1-st-c5xlarge-2", "ip", "hostname", "some_state", "inactive-queue1"),
            ],
        ),
        (
            False,
            True,
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [],
            [],
        ),
    ],
    ids=["all_enabled", "disable_all", "disable_health_check", "no_active", "no_node"],
)
def test_manage_cluster(
    disable_cluster_management,
    disable_health_check,
    mock_cluster_instances,
    mock_active_nodes,
    mock_inactive_nodes,
    mocker,
    initialize_instance_manager_mock,
    initialize_compute_fleet_status_manager_mock,
    caplog,
):
    caplog.set_level(logging.ERROR)
    mock_sync_config = SimpleNamespace(
        disable_all_cluster_management=disable_cluster_management,
        disable_all_health_checks=disable_health_check,
        region="us-east-2",
        cluster_name="hit-test",
        boto3_config=botocore.config.Config(),
        dynamodb_table="table_name",
        head_node_private_ip="head.node.ip",
        head_node_hostname="head-node-hostname",
        hosted_zone="hosted_zone",
        dns_domain="dns.domain",
        use_private_hostname=False,
    )
    mocker.patch("time.sleep")
    ip_to_slurm_node_map = {node.nodeaddr: node for node in mock_active_nodes}
    cluster_manager = ClusterManager(mock_sync_config)
    # Set up function mocks
    mocker.patch("slurm_plugin.clustermgtd.datetime").now.return_value = datetime(2020, 1, 1, 0, 0, 0)
    compute_fleet_status_manager_mock = mocker.patch.object(
        cluster_manager, "_compute_fleet_status_manager", spec=ComputeFleetStatusManager
    )
    compute_fleet_status_manager_mock.get_status.return_value = ComputeFleetStatus.RUNNING
    write_timestamp_to_file_mock = mocker.patch.object(ClusterManager, "_write_timestamp_to_file", auto_spec=True)
    perform_health_check_actions_mock = mocker.patch.object(
        ClusterManager, "_perform_health_check_actions", auto_spec=True
    )
    clean_up_inactive_partition_mock = mocker.patch.object(
        ClusterManager, "_clean_up_inactive_partition", return_value=mock_cluster_instances, auto_spec=True
    )
    terminate_orphaned_instances_mock = mocker.patch.object(
        ClusterManager, "_terminate_orphaned_instances", auto_spec=True
    )
    maintain_nodes_mock = mocker.patch.object(ClusterManager, "_maintain_nodes", auto_spec=True)
    get_ec2_instances_mock = mocker.patch.object(
        ClusterManager, "_get_ec2_instances", auto_spec=True, return_value=mock_cluster_instances
    )
    get_node_info_from_partition_mock = mocker.patch.object(
        ClusterManager,
        "_get_node_info_from_partition",
        auto_spec=True,
        return_value=(mock_active_nodes, mock_inactive_nodes),
    )

    # Run test
    cluster_manager.manage_cluster()
    # Assert function calls
    initialize_instance_manager_mock.assert_called_once()
    initialize_compute_fleet_status_manager_mock.assert_called_once()
    write_timestamp_to_file_mock.assert_called_once()
    compute_fleet_status_manager_mock.get_status.assert_called_once()
    if disable_cluster_management:
        perform_health_check_actions_mock.assert_not_called()
        clean_up_inactive_partition_mock.assert_not_called()
        terminate_orphaned_instances_mock.assert_not_called()
        maintain_nodes_mock.assert_not_called()
        get_node_info_from_partition_mock.assert_not_called()
        get_ec2_instances_mock.assert_not_called()
        return
    if mock_inactive_nodes:
        clean_up_inactive_partition_mock.assert_called_with(mock_inactive_nodes, mock_cluster_instances)
    get_ec2_instances_mock.assert_called_once()
    if not mock_active_nodes:
        terminate_orphaned_instances_mock.assert_called_with(mock_cluster_instances, ips_used_by_slurm=[])
        perform_health_check_actions_mock.assert_not_called()
        maintain_nodes_mock.assert_not_called()
        return
    if disable_health_check:
        perform_health_check_actions_mock.assert_not_called()
    else:
        perform_health_check_actions_mock.assert_called_with(mock_cluster_instances, ip_to_slurm_node_map)
    maintain_nodes_mock.assert_called_with(
        {instance.private_ip: instance for instance in mock_cluster_instances}, mock_active_nodes
    )
    terminate_orphaned_instances_mock.assert_called_with(
        mock_cluster_instances, ips_used_by_slurm=list(ip_to_slurm_node_map.keys())
    )

    assert_that(caplog.text).is_empty()


@pytest.mark.parametrize(
    "config_file, mocked_active_nodes, mocked_inactive_nodes, mocked_boto3_request, expected_error_messages",
    [
        (
            # basic: This is the most comprehensive case in manage_cluster with max number of boto3 calls
            "default.conf",
            [
                # This node fail scheduler state check and corresponding instance will be terminated and replaced
                SlurmNode("queue-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+DRAIN", "queue"),
                # This node fail scheduler state check and node will be power_down
                SlurmNode("queue-dy-c5xlarge-2", "ip-2", "hostname", "DOWN+CLOUD", "queue"),
                # This node is good and should not be touched by clustermgtd
                SlurmNode("queue-dy-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue"),
                # This node is in power_saving state but still has running backing instance, it should be terminated
                SlurmNode("queue-dy-c5xlarge-6", "ip-6", "hostname", "IDLE+CLOUD+POWER", "queue"),
                # This node is in powering_down but still has no valid backing instance, no boto3 call
                SlurmNode("queue-dy-c5xlarge-8", "ip-8", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue"),
            ],
            [
                SlurmNode("queue-st-c5xlarge-4", "ip-4", "hostname", "IDLE+CLOUD", "queue"),
                SlurmNode("queue-dy-c5xlarge-5", "ip-5", "hostname", "DOWN+CLOUD", "queue"),
            ],
            [
                # _get_ec2_instances: get all cluster instances by tags
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
                                    {
                                        "InstanceId": "i-3",
                                        "PrivateIpAddress": "ip-3",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                    },
                                    {
                                        "InstanceId": "i-4",
                                        "PrivateIpAddress": "ip-4",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                    },
                                    {
                                        "InstanceId": "i-6",
                                        "PrivateIpAddress": "ip-6",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                    },
                                    # Return an orphaned instance
                                    {
                                        "InstanceId": "i-999",
                                        "PrivateIpAddress": "ip-999",
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
                # _clean_up_inactive_partition/terminate_associated_instances: delete inactive instances
                MockedBoto3Request(
                    method="terminate_instances",
                    response={},
                    expected_params={"InstanceIds": ["i-4"]},
                    generate_error=False,
                ),
                # _perform_health_check_actions: get unhealthy instance status by instance status filter
                MockedBoto3Request(
                    method="describe_instance_status",
                    response={"InstanceStatuses": []},
                    expected_params={
                        "Filters": [
                            {"Name": "instance-status.status", "Values": list(EC2_HEALTH_STATUS_UNHEALTHY_STATES)}
                        ],
                        "MaxResults": 1000,
                    },
                    generate_error=False,
                ),
                # _perform_health_check_actions: get unhealthy instance status by system status filter
                MockedBoto3Request(
                    method="describe_instance_status",
                    response={"InstanceStatuses": []},
                    expected_params={
                        "Filters": [
                            {"Name": "system-status.status", "Values": list(EC2_HEALTH_STATUS_UNHEALTHY_STATES)}
                        ],
                        "MaxResults": 1000,
                    },
                    generate_error=False,
                ),
                # _perform_health_check_actions: get unhealthy instance status by schedule event filter
                MockedBoto3Request(
                    method="describe_instance_status",
                    response={"InstanceStatuses": []},
                    expected_params={
                        "Filters": [{"Name": "event.code", "Values": EC2_SCHEDULED_EVENT_CODES}],
                        "MaxResults": 1000,
                    },
                    generate_error=False,
                ),
                # _maintain_nodes: _handle_powering_down_nodes
                MockedBoto3Request(
                    method="terminate_instances",
                    response={},
                    expected_params={"InstanceIds": ["i-6"]},
                    generate_error=False,
                ),
                # _maintain_nodes/delete_instances: terminate dynamic down nodes
                MockedBoto3Request(
                    method="terminate_instances",
                    response={},
                    expected_params={"InstanceIds": ["i-2"]},
                    generate_error=False,
                ),
                # _maintain_nodes/delete_instances: terminate static down nodes
                MockedBoto3Request(
                    method="terminate_instances",
                    response={},
                    expected_params={"InstanceIds": ["i-1"]},
                    generate_error=False,
                ),
                # _maintain_nodes/add_instances_for_nodes: launch new instance for static node
                MockedBoto3Request(
                    method="run_instances",
                    response={
                        "Instances": [
                            {
                                "InstanceId": "i-1234",
                                "PrivateIpAddress": "ip-1234",
                                "PrivateDnsName": "hostname-1234",
                                "LaunchTime": datetime(2020, 1, 1, 0, 0, 0),
                            }
                        ]
                    },
                    expected_params={
                        "MinCount": 1,
                        "MaxCount": 1,
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue-c5.xlarge", "Version": "$Latest"},
                    },
                    generate_error=False,
                ),
                # _terminate_orphaned_instances: terminate orphaned instances
                MockedBoto3Request(
                    method="terminate_instances",
                    response={},
                    expected_params={"InstanceIds": ["i-999"]},
                    generate_error=False,
                ),
            ],
            [],
        ),
        (
            # failures: All failure tolerant module will have an exception, but the program should not crash
            "default.conf",
            [
                SlurmNode("queue-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD", "queue"),
                SlurmNode("queue-dy-c5xlarge-2", "ip-2", "hostname", "DOWN+CLOUD", "queue"),
                SlurmNode("queue-dy-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue"),
            ],
            [
                SlurmNode("queue-st-c5xlarge-4", "ip-4", "hostname", "IDLE+CLOUD", "queue"),
                SlurmNode("queue-dy-c5xlarge-5", "ip-5", "hostname", "DOWN+CLOUD", "queue"),
            ],
            [
                # _get_ec2_instances: get all cluster instances by tags
                # Not producing failure here so logic after can be executed
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
                                    {
                                        "InstanceId": "i-3",
                                        "PrivateIpAddress": "ip-3",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                    },
                                    {
                                        "InstanceId": "i-4",
                                        "PrivateIpAddress": "ip-4",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                    },
                                    # Return an orphaned instance
                                    {
                                        "InstanceId": "i-999",
                                        "PrivateIpAddress": "ip-999",
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
                # _clean_up_inactive_partition/terminate_associated_instances: delete inactive instances
                # Produce an error, cluster should be able to handle exception and move on
                MockedBoto3Request(
                    method="terminate_instances",
                    response={"error for i-4"},
                    expected_params={"InstanceIds": ["i-4"]},
                    generate_error=True,
                ),
                # _perform_health_check_actions: get unhealthy instance status by instance status filter
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
                # _perform_health_check_actions: get unhealthy instance status by system status filter
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
                # _perform_health_check_actions: get unhealthy instance status by schedule event filter
                # Produce an error, cluster should be able to handle exception and move on
                MockedBoto3Request(
                    method="describe_instance_status",
                    response={"InstanceStatuses": []},
                    expected_params={
                        "Filters": [{"Name": "event.code", "Values": EC2_SCHEDULED_EVENT_CODES}],
                        "MaxResults": 1000,
                    },
                    generate_error=True,
                ),
                # _maintain_nodes/delete_instances: terminate dynamic down nodes
                # Produce an error, cluster should be able to handle exception and move on
                MockedBoto3Request(
                    method="terminate_instances",
                    response={"error for i-2"},
                    expected_params={"InstanceIds": ["i-2"]},
                    generate_error=True,
                ),
                # _maintain_nodes/delete_instances: terminate static down nodes
                # Produce an error, cluster should be able to handle exception and move on
                MockedBoto3Request(
                    method="terminate_instances",
                    response={"error for i-1"},
                    expected_params={"InstanceIds": ["i-1"]},
                    generate_error=True,
                ),
                MockedBoto3Request(
                    method="run_instances",
                    response={"some run_instances error"},
                    expected_params={
                        "LaunchTemplate": {"LaunchTemplateName": "hit-queue-c5.xlarge", "Version": "$Latest"},
                        "MaxCount": 1,
                        "MinCount": 1,
                    },
                    generate_error=True,
                ),
                # _terminate_orphaned_instances: terminate orphaned instances
                # Produce an error, cluster should be able to handle exception and move on
                MockedBoto3Request(
                    method="terminate_instances",
                    response={"error for i-999"},
                    expected_params={"InstanceIds": ["i-999"]},
                    generate_error=True,
                ),
            ],
            [
                r"Failed TerminateInstances request:",
                r"Failed when terminating instances \(x1\) \['i-4'\].*{'error for i-4'}",
                r"Failed when getting health status for unhealthy EC2 instances",
                r"Failed when performing health check action with exception",
                r"Failed TerminateInstances request:",
                r"Failed when terminating instances \(x1\) \['i-2'\].*{'error for i-2'}",
                r"Failed TerminateInstances request:",
                r"Failed when terminating instances \(x1\) \['i-1'\].*{'error for i-1'}",
                r"Failed RunInstances request:",
                (
                    r"Encountered exception when launching instances for nodes \(x1\) \['queue-st-c5xlarge-1'\].*"
                    r"{'some run_instances error'}"
                ),
                r"Failed TerminateInstances request:",
                r"Failed when terminating instances \(x1\) \['i-999'\].*{'error for i-999'}",
            ],
        ),
        (
            # critical_failure_1: _get_ec2_instances will have an exception, but the program should not crash
            "default.conf",
            [
                SlurmNode("queue-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD", "queue"),
                SlurmNode("queue-dy-c5xlarge-2", "ip-2", "hostname", "DOWN+CLOUD", "queue"),
                SlurmNode("queue-dy-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue"),
            ],
            [
                SlurmNode("queue-st-c5xlarge-4", "ip-4", "hostname", "IDLE+CLOUD", "queue"),
                SlurmNode("queue-dy-c5xlarge-5", "ip-5", "hostname", "DOWN+CLOUD", "queue"),
            ],
            [
                # _get_ec2_instances: get all cluster instances by tags
                # Produce an error, cluster should be able to handle exception and skip other actions
                MockedBoto3Request(
                    method="describe_instances",
                    response={},
                    expected_params={
                        "Filters": [
                            {"Name": "tag:parallelcluster:cluster-name", "Values": ["hit"]},
                            {"Name": "instance-state-name", "Values": list(EC2_INSTANCE_ALIVE_STATES)},
                            {"Name": "tag:parallelcluster:node-type", "Values": ["Compute"]},
                        ],
                        "MaxResults": 1000,
                    },
                    generate_error=True,
                ),
            ],
            [
                r"Failed when getting cluster instances from EC2 with exception",
                r"Failed when getting instance info from EC2 with exception",
                r"Unable to get instances info from EC2, no other action can be performed.",
            ],
        ),
        (
            # critical_failure_2: _get_node_info_from_partition will have an exception, but the program should not crash
            "default.conf",
            Exception,
            Exception,
            [],
            ["Unable to get partition/node info from slurm, no other action can be performed"],
        ),
    ],
    ids=["basic", "failures", "critical_failure_1", "critical_failure_2"],
)
def test_manage_cluster_boto3(
    boto3_stubber,
    config_file,
    mocked_active_nodes,
    mocked_inactive_nodes,
    mocked_boto3_request,
    expected_error_messages,
    test_datadir,
    mocker,
    caplog,
):
    caplog.set_level(logging.ERROR)
    # This test only patches I/O and boto3 calls to ensure that all boto3 calls are expected
    mocker.patch("subprocess.run")
    mocker.patch("time.sleep")
    # patch boto3 call
    boto3_stubber("ec2", mocked_boto3_request)
    mocker.patch("slurm_plugin.clustermgtd.datetime").now.return_value = datetime(2020, 1, 2, 0, 0, 0)
    mocker.patch("slurm_plugin.clustermgtd.retrieve_instance_type_mapping", return_value={"c5xlarge": "c5.xlarge"})
    sync_config = ClustermgtdConfig(test_datadir / config_file)
    cluster_manager = ClusterManager(sync_config)
    dynamodb_table_mock = mocker.patch.object(cluster_manager._compute_fleet_status_manager, "_table")
    dynamodb_table_mock.get_item.return_value = {"Item": {"Id": "COMPUTE_FLEET", "Status": "RUNNING"}}
    mocker.patch.object(cluster_manager, "_write_timestamp_to_file", auto_spec=True)
    if mocked_active_nodes is Exception or mocked_active_nodes is Exception:
        mocker.patch.object(
            cluster_manager,
            "_get_node_info_from_partition",
            side_effect=ClusterManager.SchedulerUnavailable,
        )
    else:
        mocker.patch.object(
            cluster_manager,
            "_get_node_info_from_partition",
            return_value=(mocked_active_nodes, mocked_inactive_nodes),
        )
    cluster_manager._instance_manager._store_assigned_hostnames = mocker.MagicMock()
    cluster_manager._instance_manager._update_dns_hostnames = mocker.MagicMock()
    cluster_manager.manage_cluster()

    assert_that(expected_error_messages).is_length(len(caplog.records))
    for actual, expected in zip(caplog.records, expected_error_messages):
        assert_that(actual.message).matches(expected)


@pytest.mark.parametrize(
    "fleet_initial_status, fleet_status_transitions, update_partition_status, terminate_all_nodes, "
    "partitions_updated_successfully, nodes_terminated_successfully, error_messages",
    [
        (ComputeFleetStatus.RUNNING, [], None, False, None, None, []),
        (
            ComputeFleetStatus.START_REQUESTED,
            [ComputeFleetStatus.STARTING, ComputeFleetStatus.RUNNING],
            PartitionStatus.UP,
            False,
            True,
            None,
            [],
        ),
        (ComputeFleetStatus.STARTING, [ComputeFleetStatus.RUNNING], PartitionStatus.UP, False, True, None, []),
        (
            ComputeFleetStatus.START_REQUESTED,
            [ComputeFleetStatus.STARTING, ComputeFleetStatus.RUNNING],
            PartitionStatus.UP,
            False,
            True,
            None,
            [],
        ),
        (
            ComputeFleetStatus.STOP_REQUESTED,
            [ComputeFleetStatus.STOPPING, ComputeFleetStatus.STOPPED],
            PartitionStatus.INACTIVE,
            True,
            True,
            True,
            [],
        ),
        (ComputeFleetStatus.STOPPING, [ComputeFleetStatus.STOPPED], PartitionStatus.INACTIVE, True, True, True, []),
        (ComputeFleetStatus.STOPPED, [], PartitionStatus.INACTIVE, True, True, True, []),
        (
            ComputeFleetStatus.START_REQUESTED,
            [ComputeFleetStatus.STARTING],
            PartitionStatus.UP,
            False,
            False,
            False,
            ["Failed when updating partitions with error"],
        ),
        (
            ComputeFleetStatus.STOP_REQUESTED,
            [ComputeFleetStatus.STOPPING],
            PartitionStatus.INACTIVE,
            True,
            False,
            True,
            ["Failed when updating partitions with error"],
        ),
    ],
)
def test_manage_compute_fleet_status_transitions(
    mocker,
    caplog,
    fleet_initial_status,
    fleet_status_transitions,
    update_partition_status,
    terminate_all_nodes,
    partitions_updated_successfully,
    nodes_terminated_successfully,
    error_messages,
):
    config = SimpleNamespace(
        region="us-east-2",
        cluster_name="hit-test",
        boto3_config=botocore.config.Config(),
        dynamodb_table="table_name",
        head_node_private_ip="head.node.ip",
        head_node_hostname="head-node-hostname",
        terminate_max_batch_size=4,
        hosted_zone="hosted_zone",
        dns_domain="dns.domain",
        use_private_hostname=False,
        instance_name_type_mapping={"c5xlarge": "c5.xlarge"},
    )
    cluster_manager = ClusterManager(config)
    mocker.patch("subprocess.run", side_effect=None if partitions_updated_successfully else Exception)
    update_all_partitions_spy = mocker.patch(
        "slurm_plugin.clustermgtd.update_all_partitions", wraps=update_all_partitions
    )
    compute_fleet_status_manager_mock = mocker.patch.object(
        cluster_manager, "_compute_fleet_status_manager", auto_spec=True
    )
    compute_fleet_status_manager_mock.get_status.return_value = fleet_initial_status
    instance_manager_mock = mocker.patch.object(cluster_manager, "_instance_manager", auto_spec=True)
    instance_manager_mock.terminate_all_compute_nodes.return_value = nodes_terminated_successfully

    cluster_manager._manage_compute_fleet_status_transitions()

    # Check partitions updated correctly
    if update_partition_status:
        if update_partition_status == PartitionStatus.UP:
            update_all_partitions_spy.assert_called_with(update_partition_status, reset_node_addrs_hostname=False)
        else:
            update_all_partitions_spy.assert_called_with(update_partition_status, reset_node_addrs_hostname=True)
    else:
        update_all_partitions_spy.assert_not_called()
    # Check nodes terminated correctly
    if terminate_all_nodes:
        instance_manager_mock.terminate_all_compute_nodes.assert_called_with(config.terminate_max_batch_size)
    else:
        instance_manager_mock.assert_not_called()
    # Check compute fleet status transitions
    update_status_calls = []
    previous = fleet_initial_status
    for transition in fleet_status_transitions:
        update_status_calls.append(call(current_status=previous, next_status=transition))
        previous = transition
    compute_fleet_status_manager_mock.update_status.assert_has_calls(update_status_calls)
    assert_that(compute_fleet_status_manager_mock.update_status.call_count).is_equal_to(len(update_status_calls))
    # Check errors in logs
    for actual, expected in zip(caplog.records, error_messages):
        assert_that(actual.message).matches(expected)
    assert_that(error_messages).is_length(len(caplog.records))


def test_manage_compute_fleet_status_transitions_concurrency(mocker, caplog):
    config = SimpleNamespace(
        region="us-east-2",
        cluster_name="hit-test",
        boto3_config=botocore.config.Config(),
        dynamodb_table="table_name",
        head_node_private_ip="head.node.ip",
        head_node_hostname="head-node-hostname",
        terminate_max_batch_size=4,
        hosted_zone="hosted_zone",
        dns_domain="dns.domain",
        use_private_hostname=False,
        instance_name_type_mapping={},
    )
    cluster_manager = ClusterManager(config)
    mocker.patch("slurm_plugin.clustermgtd.update_all_partitions")
    compute_fleet_status_manager_mock = mocker.patch.object(
        cluster_manager, "_compute_fleet_status_manager", auto_spec=True
    )
    compute_fleet_status_manager_mock.get_status.return_value = ComputeFleetStatus.STOP_REQUESTED
    compute_fleet_status_manager_mock.update_status.side_effect = (
        ComputeFleetStatusManager.ConditionalStatusUpdateFailed
    )

    cluster_manager._manage_compute_fleet_status_transitions()

    assert_that(caplog.text).contains("Cluster status was updated while handling a transition")
    assert_that(compute_fleet_status_manager_mock.update_status.call_count).is_equal_to(1)


class TestComputeFleetStatusManager:
    @pytest.fixture
    def compute_fleet_status_manager(self, mocker):
        status_manager = ComputeFleetStatusManager("table", botocore.config.Config(), "us-east-1")
        mocker.patch.object(status_manager, "_table")

        return status_manager

    @pytest.mark.parametrize(
        "get_item_response, fallback, expected_status",
        [
            ({"Item": {"Id": "COMPUTE_FLEET", "Status": "RUNNING"}}, None, ComputeFleetStatus.RUNNING),
            (
                {},
                ComputeFleetStatus.STOPPED,
                ComputeFleetStatus.STOPPED,
            ),
            (
                Exception,
                ComputeFleetStatus.STOPPED,
                ComputeFleetStatus.STOPPED,
            ),
        ],
        ids=["success", "empty_response", "exception"],
    )
    def test_get_status(self, compute_fleet_status_manager, get_item_response, fallback, expected_status):
        if get_item_response is Exception:
            compute_fleet_status_manager._table.get_item.side_effect = get_item_response
        else:
            compute_fleet_status_manager._table.get_item.return_value = get_item_response
        status = compute_fleet_status_manager.get_status(fallback)
        assert_that(status).is_equal_to(expected_status)
        compute_fleet_status_manager._table.get_item.assert_called_with(
            ConsistentRead=True, Key={"Id": "COMPUTE_FLEET"}
        )

    @pytest.mark.parametrize(
        "put_item_response, expected_exception",
        [
            (
                {},
                None,
            ),
            (
                boto3.client("dynamodb", region_name="us-east-1").exceptions.ConditionalCheckFailedException(
                    {"Error": {}}, {}
                ),
                ComputeFleetStatusManager.ConditionalStatusUpdateFailed,
            ),
            (Exception(), Exception),
        ],
        ids=["success", "conditional_check_failed", "exception"],
    )
    def test_update_status(self, compute_fleet_status_manager, put_item_response, expected_exception):
        if isinstance(put_item_response, Exception):
            compute_fleet_status_manager._table.put_item.side_effect = put_item_response
            with pytest.raises(expected_exception):
                compute_fleet_status_manager.update_status(ComputeFleetStatus.STARTING, ComputeFleetStatus.RUNNING)
        else:
            compute_fleet_status_manager._table.put_item.return_value = put_item_response
            compute_fleet_status_manager.update_status(ComputeFleetStatus.STARTING, ComputeFleetStatus.RUNNING)


@pytest.fixture()
def initialize_instance_manager_mock(mocker):
    return mocker.patch.object(
        ClusterManager, "_initialize_instance_manager", spec=ClusterManager._initialize_instance_manager
    )


@pytest.fixture()
def initialize_compute_fleet_status_manager_mock(mocker):
    compute_fleet_status_manager_mock = mocker.Mock(spec=ComputeFleetStatusManager)
    compute_fleet_status_manager_mock.get_status.return_value = ComputeFleetStatus.RUNNING
    return mocker.patch.object(
        ClusterManager,
        "_initialize_compute_fleet_status_manager",
        spec=ClusterManager._initialize_compute_fleet_status_manager,
        return_value=compute_fleet_status_manager_mock,
    )
