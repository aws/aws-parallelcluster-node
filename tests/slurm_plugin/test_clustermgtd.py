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
from unittest.mock import ANY, call

import botocore
import pytest
import slurm_plugin
from assertpy import assert_that
from slurm_plugin.clustermgtd import ClusterManager, ClustermgtdConfig, ComputeFleetStatus, ComputeFleetStatusManager
from slurm_plugin.console_logger import ConsoleLogger
from slurm_plugin.fleet_manager import EC2Instance
from slurm_plugin.slurm_resources import (
    EC2_HEALTH_STATUS_UNHEALTHY_STATES,
    EC2_INSTANCE_ALIVE_STATES,
    EC2_SCHEDULED_EVENT_CODES,
    ComputeResourceFailureEvent,
    DynamicNode,
    EC2InstanceHealthState,
    PartitionStatus,
    SlurmPartition,
    StaticNode,
)

from tests.common import FLEET_CONFIG, LAUNCH_OVERRIDES, MockedBoto3Request, client_error


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
    return "slurm_plugin.instance_manager.boto3"


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
                    "node_replacement_timeout": 1800,
                    "terminate_drain_nodes": True,
                    "terminate_down_nodes": True,
                    "orphaned_instance_timeout": 120,
                    # health check configs
                    "disable_ec2_health_check": False,
                    "disable_scheduled_event_health_check": False,
                    "disable_all_health_checks": False,
                    "health_check_timeout": 180,
                    "protected_failure_count": 10,
                    "insufficient_capacity_timeout": 600,
                    # Compute console logging configs
                    "compute_console_logging_enabled": True,
                    "compute_console_logging_max_sample_size": 1,
                    "compute_console_wait_time": 300,
                    # Task executor configs
                    "worker_pool_size": 5,
                    "worker_pool_max_backlog": 100,
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
                    "protected_failure_count": 5,
                    "insufficient_capacity_timeout": 50.5,
                    # Compute console logging configs
                    "compute_console_logging_enabled": False,
                    "compute_console_logging_max_sample_size": 50,
                    "compute_console_wait_time": 10,
                    # Task executor configs
                    "worker_pool_size": 2,
                    "worker_pool_max_backlog": 5,
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
        mocker.patch(
            "slurm_plugin.clustermgtd.read_json", side_effect=[FLEET_CONFIG, LAUNCH_OVERRIDES, LAUNCH_OVERRIDES]
        )
        sync_config = ClustermgtdConfig(test_datadir / config_file)
        for key in expected_attributes:
            assert_that(sync_config.__dict__.get(key)).is_equal_to(expected_attributes.get(key))

    @pytest.mark.parametrize(
        ("file_to_compare", "read_json_mock"),
        [
            ("config_modified.conf", 4 * [FLEET_CONFIG, LAUNCH_OVERRIDES, LAUNCH_OVERRIDES]),
            (
                "config.conf",
                3 * [FLEET_CONFIG, LAUNCH_OVERRIDES, LAUNCH_OVERRIDES]
                + [{**FLEET_CONFIG, **{"queue7": "fake"}}, LAUNCH_OVERRIDES, LAUNCH_OVERRIDES],
            ),
        ],
        ids=["different_config_file", "same_file_different_fleet_config"],
    )
    def test_config_comparison(self, test_datadir, mocker, file_to_compare, read_json_mock):
        mocker.patch("slurm_plugin.clustermgtd.read_json", side_effect=read_json_mock)
        config = test_datadir / "config.conf"
        config_modified = test_datadir / file_to_compare

        assert_that(ClustermgtdConfig(config)).is_equal_to(ClustermgtdConfig(config))
        assert_that(ClustermgtdConfig(config)).is_not_equal_to(ClustermgtdConfig(config_modified))


@pytest.mark.usefixtures("initialize_console_logger_mock")
def test_set_config(initialize_instance_manager_mock):
    initial_config = SimpleNamespace(
        some_key_1="some_value_1",
        some_key_2="some_value_2",
        insufficient_capacity_timeout=20,
        worker_pool_size=5,
        worker_pool_max_backlog=10,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    updated_config = SimpleNamespace(
        some_key_1="some_value_1",
        some_key_2="some_value_2_changed",
        insufficient_capacity_timeout=10,
        worker_pool_size=10,
        worker_pool_max_backlog=5,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )

    cluster_manager = ClusterManager(initial_config)
    assert_that(cluster_manager._config).is_equal_to(initial_config)
    cluster_manager.set_config(initial_config)
    assert_that(cluster_manager._config).is_equal_to(initial_config)
    cluster_manager.set_config(updated_config)
    assert_that(cluster_manager._config).is_equal_to(updated_config)

    assert_that(initialize_instance_manager_mock.call_count).is_equal_to(2)


@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_exception_from_report_console_output_from_nodes(mocker):
    config = SimpleNamespace(
        some_key_1="some_value_1",
        some_key_2="some_value_2",
        insufficient_capacity_timeout=20,
        worker_pool_size=5,
        worker_pool_max_backlog=0,
        compute_console_logging_max_sample_size=2,
        compute_console_wait_time=10,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    unhealthy_nodes = [
        StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "some_state", "queue1"),
    ]
    reset_nodes_mock = mocker.patch("slurm_plugin.clustermgtd.reset_nodes", autospec=True)
    cluster_manager = ClusterManager(config)

    mock_console_logger = mocker.patch.object(cluster_manager, "_console_logger", spec=ConsoleLogger)
    report_mock = mock_console_logger.report_console_output_from_nodes
    report_mock.side_effect = Exception

    cluster_manager._handle_unhealthy_static_nodes(unhealthy_nodes)

    # Make sure report_console_output_from_nodes was called.
    report_mock.assert_called_once()

    # Make sure the code after _report_console_from_output_nodes is called.
    reset_nodes_mock.assert_called_once_with(
        ANY, state="down", reason="Static node maintenance: unhealthy node is being replaced"
    )


@pytest.mark.parametrize(
    (
        "inactive_instances",
        "slurm_inactive_nodes",
        "expected_reset",
        "delete_instances_side_effect",
        "reset_nodes_side_effect",
    ),
    [
        (
            [
                EC2Instance("id-1", "ip-1", "hostname", "some_launch_time"),
                None,
                None,
                EC2Instance("id-3", "ip-3", "hostname", "some_launch_time"),
                None,
                None,
                None,
            ],
            [
                StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "some_state", "queue1"),
                StaticNode(
                    "queue1-st-c5xlarge-5", "queue1-st-c5xlarge-5", "queue1-st-c5xlarge-5", "some_state", "queue1"
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-4", "queue1-dy-c5xlarge-4", "queue1-dy-c5xlarge-4", "some_state", "queue1"
                ),
                StaticNode("queue1-st-c5xlarge-6", "ip-6", "ip-6", "IDLE", "queue1"),
                StaticNode(
                    "queue1-st-c5xlarge-7", "queue1-st-c5xlarge-7", "queue1-st-c5xlarge-7", "POWERING_DOWN", "queue1"
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-8",
                    "queue1-dy-c5xlarge-8",
                    "queue1-dy-c5xlarge-8",
                    "IDLE+NOT_RESPONDING",
                    "queue1",
                ),
                StaticNode(
                    "queue1-st-c5xlarge-9",
                    "queue1-st-c5xlarge-9",
                    "queue1-st-c5xlarge-9",
                    "IDLE+NOT_RESPONDING",
                    "queue1",
                ),
            ],
            {"queue1-st-c5xlarge-3", "queue1-dy-c5xlarge-4", "queue1-st-c5xlarge-6", "queue1-dy-c5xlarge-8"},
            None,
            None,
        ),
        (
            [EC2Instance("id-3", "ip-3", "hostname", "some_launch_time")],
            [StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "some_state", "queue1")],
            None,
            Exception,
            None,
        ),
        (
            [EC2Instance("id-3", "ip-3", "hostname", "some_launch_time")],
            [StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "some_state", "queue1")],
            {"queue1-st-c5xlarge-3"},
            None,
            Exception,
        ),
    ],
    ids=["normal", "delete_exception", "reset_exception"],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_clean_up_inactive_partition(
    inactive_instances,
    slurm_inactive_nodes,
    expected_reset,
    delete_instances_side_effect,
    reset_nodes_side_effect,
    mocker,
):
    # Test setup
    mock_sync_config = SimpleNamespace(
        terminate_max_batch_size=4,
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    part = SlurmPartition("partition4", "placeholder_nodes", "INACTIVE")
    for node, instance in zip(slurm_inactive_nodes, inactive_instances):
        node.instance = instance
    part.slurm_nodes = slurm_inactive_nodes
    mock_instance_manager = cluster_manager._instance_manager
    if delete_instances_side_effect:
        mock_instance_manager.delete_instances.side_effect = delete_instances_side_effect
    if reset_nodes_side_effect:
        mock_reset_node = mocker.patch(
            "slurm_plugin.clustermgtd.reset_nodes", side_effect=reset_nodes_side_effect, autospec=True
        )
    else:
        mock_reset_node = mocker.patch("slurm_plugin.clustermgtd.reset_nodes", autospec=True)
    cluster_manager._clean_up_inactive_partition([part])
    inactive_instance_ids = {instance.id for instance in inactive_instances if instance}
    mock_instance_manager.delete_instances.assert_called_with(inactive_instance_ids, terminate_batch_size=4)
    if expected_reset:
        mock_reset_node.assert_called_with(
            expected_reset, raise_on_error=False, reason="inactive partition", state="down"
        )
    else:
        mock_reset_node.assert_not_called()


@pytest.mark.usefixtures("initialize_executor_mock", "initialize_console_logger_mock")
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
        run_instances_overrides={},
        create_fleet_overrides={},
        insufficient_capacity_timeout=600,
        fleet_config=FLEET_CONFIG,
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._instance_manager.get_cluster_instances = mocker.MagicMock()
    mocker.patch("time.sleep")
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
                        "id-1": StaticNode(
                            name="queue1-st-c5xlarge-3",
                            nodeaddr="ip-1",
                            nodehostname="hostname",
                            state="some_state",
                            partitions="queue1",
                            instance=EC2Instance(
                                id="id-1", private_ip="ip-1", hostname="hostname", launch_time="some_launch_time"
                            ),
                        ),
                        "id-2": StaticNode(
                            name="queue1-st-c5xlarge-5",
                            nodeaddr="ip-2",
                            nodehostname="queue1-st-c5xlarge-5",
                            state="some_state",
                            partitions="queue1",
                            instance=EC2Instance(
                                id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                            ),
                        ),
                    },
                    health_check_type=ClusterManager.HealthCheckTypes.ec2_health,
                ),
                call(
                    ["some_instance_health_states"],
                    {
                        "id-1": StaticNode(
                            name="queue1-st-c5xlarge-3",
                            nodeaddr="ip-1",
                            nodehostname="hostname",
                            state="some_state",
                            partitions="queue1",
                            instance=EC2Instance(
                                id="id-1", private_ip="ip-1", hostname="hostname", launch_time="some_launch_time"
                            ),
                        ),
                        "id-2": StaticNode(
                            name="queue1-st-c5xlarge-5",
                            nodeaddr="ip-2",
                            nodehostname="queue1-st-c5xlarge-5",
                            state="some_state",
                            partitions="queue1",
                            instance=EC2Instance(
                                id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                            ),
                        ),
                    },
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
                        "id-1": StaticNode(
                            name="queue1-st-c5xlarge-3",
                            nodeaddr="ip-1",
                            nodehostname="hostname",
                            state="some_state",
                            partitions="queue1",
                            instance=EC2Instance(
                                id="id-1", private_ip="ip-1", hostname="hostname", launch_time="some_launch_time"
                            ),
                        ),
                        "id-2": StaticNode(
                            name="queue1-st-c5xlarge-5",
                            nodeaddr="ip-2",
                            nodehostname="queue1-st-c5xlarge-5",
                            state="some_state",
                            partitions="queue1",
                            instance=EC2Instance(
                                id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                            ),
                        ),
                    },
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
                        "id-1": StaticNode(
                            name="queue1-st-c5xlarge-3",
                            nodeaddr="ip-1",
                            nodehostname="hostname",
                            state="some_state",
                            partitions="queue1",
                            instance=EC2Instance(
                                id="id-1", private_ip="ip-1", hostname="hostname", launch_time="some_launch_time"
                            ),
                        ),
                        "id-2": StaticNode(
                            name="queue1-st-c5xlarge-5",
                            nodeaddr="ip-2",
                            nodehostname="queue1-st-c5xlarge-5",
                            state="some_state",
                            partitions="queue1",
                            instance=EC2Instance(
                                id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                            ),
                        ),
                    },
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
@pytest.mark.usefixtures("initialize_executor_mock", "initialize_console_logger_mock")
def test_perform_health_check_actions(
    mock_instance_health_states,
    disable_ec2_health_check,
    disable_scheduled_event_health_check,
    expected_handle_health_check_calls,
    mocker,
):
    part = SlurmPartition("partition4", "placeholder_nodes", "ACTIVE")
    slurm_nodes = [
        StaticNode("queue1-st-c5xlarge-3", "ip-1", "hostname", "some_state", "queue1"),
        StaticNode("queue1-st-c5xlarge-5", "ip-2", "queue1-st-c5xlarge-5", "some_state", "queue1"),
    ]
    instances = [
        EC2Instance("id-1", "ip-1", "hostname", "some_launch_time"),
        EC2Instance("id-2", "ip-2", "hostname", "some_launch_time"),
    ]
    for node, instance in zip(slurm_nodes, instances):
        node.instance = instance
    part.slurm_nodes = slurm_nodes
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
        run_instances_overrides={},
        create_fleet_overrides={},
        fleet_config=FLEET_CONFIG,
        insufficient_capacity_timeout=600,
        head_node_instance_id="i-instance-id",
    )
    # Mock functions
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._instance_manager.get_unhealthy_cluster_instance_status = mocker.MagicMock(
        return_value=mock_instance_health_states
    )
    cluster_manager._handle_health_check = mocker.MagicMock().patch()

    # Run test
    cluster_manager._perform_health_check_actions([part])
    # Check function calls
    if expected_handle_health_check_calls:
        cluster_manager._handle_health_check.assert_has_calls(expected_handle_health_check_calls)
    else:
        cluster_manager._handle_health_check.assert_not_called()


@pytest.mark.parametrize(
    (
        "health_check_type",
        "mock_fail_ec2_side_effect",
        "mock_fail_scheduled_events_side_effect",
        "expected_failed_nodes",
    ),
    [
        (
            ClusterManager.HealthCheckTypes.scheduled_event,
            [True, False, True, False, True, True, True],
            [False, True, False, True, False, False, False],
            {"queue1-dy-c5xlarge-2", "queue1-st-c5xlarge-4"},
        ),
        (
            ClusterManager.HealthCheckTypes.ec2_health,
            [True, False, True, False, True, True, True],
            [False, True, True, False, False, False, False],
            {"queue1-st-c5xlarge-1", "queue1-st-c5xlarge-3", "queue1-st-c5xlarge-7"},
        ),
        (
            ClusterManager.HealthCheckTypes.ec2_health,
            [False, False, False, False, False, False, False],
            [False, True, False, True, False, True, True],
            {},
        ),
    ],
    ids=["scheduled_event", "ec2_health", "all_healthy"],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_handle_health_check(
    health_check_type,
    mock_fail_ec2_side_effect,
    mock_fail_scheduled_events_side_effect,
    expected_failed_nodes,
    mocker,
):
    # Define variables that will be used for all tests
    mocker_current_time = datetime(2023, 1, 23, 18, 00, 0).astimezone(tz=timezone.utc)
    health_state_1 = EC2InstanceHealthState("id-1", "some_state", "some_status", "some_status", "some_event")
    health_state_2 = EC2InstanceHealthState("id-2", "some_state", "some_status", "some_status", "some_event")
    health_state_3 = EC2InstanceHealthState("id-3", "some_state", "some_status", "some_status", "some_event")
    health_state_4 = EC2InstanceHealthState("id-4", "some_state", "some_status", "some_status", "some_event")
    health_state_5 = EC2InstanceHealthState("id-5", "some_state", "some_status", "some_status", "some_event")
    health_state_6 = EC2InstanceHealthState("id-6", "some_state", "some_status", "some_status", "some_event")
    health_state_7 = EC2InstanceHealthState("id-7", "some_state", "some_status", "some_status", "some_event")
    placeholder_states = [
        health_state_1,
        health_state_2,
        health_state_3,
        health_state_4,
        health_state_5,
        health_state_6,
        health_state_7,
    ]
    instance_id_to_active_node_map = {
        "id-1": StaticNode("queue1-st-c5xlarge-1", "ip-1", "host-1", "DOWN+CLOUD+NOT_RESPONDING", "queue1"),
        "id-2": DynamicNode(
            "queue1-dy-c5xlarge-2", "ip-2", "host-2", "ALLOCATED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
        ),
        "id-3": StaticNode("queue1-st-c5xlarge-3", "ip-3", "host-3", "ALLOCATED+CLOUD", "queue1"),
        "id-4": StaticNode("queue1-st-c5xlarge-4", "ip-4", "host-4", "ALLOCATED+CLOUD", "queue1"),
        # This node is still rebooting, so it's not drained due to EC2 health checks failing
        "id-5": StaticNode(
            "queue1-st-c5xlarge-5",
            "ip-5",
            "host-5",
            "DOWN+CLOUD+REBOOT_ISSUED",
            "queue1",
            slurmdstarttime=datetime(2023, 1, 23, 17, 56, 0).astimezone(tz=timezone.utc),
        ),
        # This node was rebooted very recently, so it's not drained due to EC2 health checks failing
        "id-6": StaticNode(
            "queue1-st-c5xlarge-6",
            "ip-6",
            "host-6",
            "IDLE+CLOUD",
            "queue1",
            slurmdstarttime=datetime(2023, 1, 23, 17, 59, 0).astimezone(tz=timezone.utc),
        ),
        # This node was rebooted a long time ago, so it can be drained due to EC2 health checks failing
        "id-7": StaticNode(
            "queue1-st-c5xlarge-7",
            "ip-7",
            "host-7",
            "IDLE+CLOUD",
            "queue1",
            slurmdstarttime=datetime(2023, 1, 23, 17, 56, 0).astimezone(tz=timezone.utc),
        ),
    }
    mock_ec2_health_check = mocker.patch(
        "slurm_plugin.slurm_resources.EC2InstanceHealthState.fail_ec2_health_check",
        side_effect=mock_fail_ec2_side_effect,
    )
    mock_scheduled_health_check = mocker.patch(
        "slurm_plugin.slurm_resources.EC2InstanceHealthState.fail_scheduled_events_check",
        side_effect=mock_fail_scheduled_events_side_effect,
    )
    # Setup mocking
    mock_sync_config = SimpleNamespace(
        health_check_timeout=10,
        health_check_timeout_after_slurmdstarttime=180,
        protected_failure_count=2,
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )

    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._current_time = mocker_current_time
    drain_node_mock = mocker.patch("slurm_plugin.clustermgtd.set_nodes_drain", autospec=True)

    # Run tests
    cluster_manager._handle_health_check(placeholder_states, instance_id_to_active_node_map, health_check_type)
    # Assert on calls
    if health_check_type == ClusterManager.HealthCheckTypes.scheduled_event:
        mock_scheduled_health_check.assert_has_calls([call(), call()])
    else:
        mock_ec2_health_check.assert_has_calls(
            [
                call(mocker_current_time, 10),
                call(mocker_current_time, 10),
            ]
        ),
    if expected_failed_nodes:
        drain_node_mock.assert_called_with(expected_failed_nodes, reason=f"Node failing {health_check_type}")
    else:
        drain_node_mock.assert_not_called()


@pytest.mark.parametrize(
    "current_replacing_nodes, slurm_nodes, expected_replacing_nodes",
    [
        (
            {"queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-4"},
            [
                DynamicNode("queue1-st-c5xlarge-1", "ip", "hostname", "IDLE+CLOUD", "queue1"),
                DynamicNode("queue1-st-c5xlarge-2", "ip", "hostname", "DOWN+CLOUD", "queue1"),
                DynamicNode("queue1-st-c5xlarge-3", "ip", "hostname", "IDLE+CLOUD", "queue1"),
            ],
            {"queue1-st-c5xlarge-2"},
        )
    ],
    ids=["mixed"],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_update_static_nodes_in_replacement(current_replacing_nodes, slurm_nodes, expected_replacing_nodes, mocker):
    mock_sync_config = SimpleNamespace(
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._static_nodes_in_replacement = current_replacing_nodes
    cluster_manager._update_static_nodes_in_replacement(slurm_nodes)
    assert_that(cluster_manager._static_nodes_in_replacement).is_equal_to(expected_replacing_nodes)


@pytest.mark.parametrize(
    "unhealthy_dynamic_nodes, instances, instances_to_terminate, expected_power_save_node_list, "
    "disable_nodes_on_insufficient_capacity",
    [
        (
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-1",
                    "ip-1",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue1",
                    "(Code:ServiceUnavailable)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-2",
                    "ip-1",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue1",
                ),
            ],
            [EC2Instance("id-1", "ip-1", "hostname", "some_launch_time"), None],
            ["id-1"],
            ["queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-2"],
            True,
        ),
        (
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-1",
                    "ip-1",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue1",
                    "(Code:Exception)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-2",
                    "ip-1",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue1",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-3",
                    "ip-1",
                    "hostname",
                    "DOWN+CLOUD+NOT_RESPONDING+POWERING_UP",
                    "queue1",
                    "(Code:InsufficientInstanceCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-4",
                    "ip-1",
                    "hostname",
                    "DOWN+CLOUD+NOT_RESPONDING+POWERING_UP",
                    "queue1",
                    "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-5",
                    "ip-1",
                    "hostname",
                    "DOWN+CLOUD+POWERED_DOWN+NOT_RESPONDING",
                    "queue1",
                    "(Code:MaxSpotInstanceCountExceeded)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
            ],
            [EC2Instance("id-1", "ip-1", "hostname", "some_launch_time"), None, None, None, None],
            ["id-1"],
            [
                "queue1-dy-c5xlarge-1",
                "queue1-dy-c5xlarge-2",
                "queue1-dy-c5xlarge-3",
                "queue1-dy-c5xlarge-4",
                "queue1-dy-c5xlarge-5",
            ],
            False,
        ),
    ],
    ids=["basic", "disable smart instance capacity"],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_handle_unhealthy_dynamic_nodes(
    unhealthy_dynamic_nodes,
    instances,
    instances_to_terminate,
    expected_power_save_node_list,
    disable_nodes_on_insufficient_capacity,
    mocker,
):
    for node, instance in zip(unhealthy_dynamic_nodes, instances):
        node.instance = instance
    mock_sync_config = SimpleNamespace(
        terminate_max_batch_size=4,
        disable_nodes_on_insufficient_capacity=disable_nodes_on_insufficient_capacity,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    mock_instance_manager = cluster_manager._instance_manager
    power_save_mock = mocker.patch("slurm_plugin.clustermgtd.set_nodes_power_down", autospec=True)
    cluster_manager._handle_unhealthy_dynamic_nodes(unhealthy_dynamic_nodes)
    mock_instance_manager.delete_instances.assert_called_with(instances_to_terminate, terminate_batch_size=4)
    power_save_mock.assert_called_with(expected_power_save_node_list, reason="Scheduler health check failed")


@pytest.mark.parametrize(
    "slurm_nodes, instances, instances_to_terminate, expected_powering_down_nodes",
    [
        (
            [
                DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
                DynamicNode("queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"),
                DynamicNode("queue1-dy-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD+POWERED_DOWN", "queue1"),
                DynamicNode("queue1-dy-c5xlarge-4", "ip-4", "hostname", "IDLE+CLOUD+POWERED_", "queue1"),
                DynamicNode(
                    "queue1-dy-c5xlarge-5", "queue1-dy-c5xlarge-5", "queue1-dy-c5xlarge-5", "POWERING_DOWN", "queue1"
                ),
                DynamicNode("queue1-dy-c5xlarge-7", "ip-7", "hostname", "IDLE+CLOUD+POWER_DOWN", "queue1"),
                StaticNode("queue1-st-c5xlarge-6", "ip-6", "hostname", "POWERING_DOWN", "queue1"),
                StaticNode("queue1-st-c5xlarge-8", "ip-8", "hostname", "IDLE+CLOUD+DRAIN+POWER_DOWN", "queue1"),
                StaticNode("queue1-st-c5xlarge-9", "ip-9", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"),
            ],
            [
                EC2Instance("id-1", "ip-1", "hostname", "some_launch_time"),
                None,
                EC2Instance("id-3", "ip-3", "hostname", "some_launch_time"),
                None,
                None,
                EC2Instance("id-7", "ip-7", "hostname", "some_launch_time"),
                EC2Instance("id-6", "ip-6", "hostname", "some_launch_time"),
                EC2Instance("id-8", "ip-8", "hostname", "some_launch_time"),
                EC2Instance("id-9", "ip-9", "hostname", "some_launch_time"),
            ],
            ["id-3", "id-6", "id-9"],
            ["queue1-dy-c5xlarge-2", "queue1-dy-c5xlarge-3", "queue1-st-c5xlarge-6", "queue1-st-c5xlarge-9"],
        )
    ],
    ids=["basic"],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_handle_powering_down_nodes(
    slurm_nodes, instances, instances_to_terminate, expected_powering_down_nodes, mocker
):
    for node, instance in zip(slurm_nodes, instances):
        node.instance = instance
    mock_sync_config = SimpleNamespace(
        terminate_max_batch_size=4,
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    mock_instance_manager = cluster_manager._instance_manager
    reset_nodes_mock = mocker.patch("slurm_plugin.clustermgtd.reset_nodes", autospec=True)
    cluster_manager._handle_powering_down_nodes(slurm_nodes)
    mock_instance_manager.delete_instances.assert_called_with(instances_to_terminate, terminate_batch_size=4)
    reset_nodes_mock.assert_called_with(nodes=expected_powering_down_nodes)


@pytest.mark.parametrize(
    (
        "current_replacing_nodes",
        "unhealthy_static_nodes",
        "instances",
        "launched_instances",
        "expected_replacing_nodes",
        "delete_instance_list",
        "add_node_list",
        "output_enabled",
        "set_nodes_down_exception",
        "sample_size",
        "expected_warnings",
    ),
    [
        (
            {"current-queue1-st-c5xlarge-6"},
            [
                StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
                StaticNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD", "queue1"),
                StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue1"),
            ],
            [
                EC2Instance("id-1", "ip-1", "hostname", "some_launch_time"),
                EC2Instance("id-2", "ip-2", "hostname", "some_launch_time"),
                None,
            ],
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
            ["id-1", "id-2"],
            ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3"],
            True,
            None,
            100,
            [],
        ),
        (
            {"current-queue1-st-c5xlarge-6"},
            [
                StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
                StaticNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD", "queue1"),
                StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue1"),
            ],
            [None, None, None],
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
            False,
            None,
            1,
            [],
        ),
        (
            {"current-queue1-st-c5xlarge-6"},
            [
                StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
                StaticNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD", "queue1"),
                StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue1"),
            ],
            [None, None],
            [
                EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
            ],
            {"current-queue1-st-c5xlarge-6", "queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2"},
            [],
            ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3"],
            False,
            None,
            1,
            [
                r"Failed to launch instances due to limited EC2 capacity for following nodes: .*",
                r'{"datetime": ".*", "version": 0, "scheduler": "slurm", "cluster-name": "hit-test", '
                + r'"node-role": "HeadNode", "component": "clustermgtd", "level": "WARNING", '
                + r'"instance-id": "i-instance-id", "event-type": "node-launch-failure-count", '
                + r'"message": ".*", "detail": {"failure-type": "ice-failures", "count": 1, '
                + r'"error-details": {"LimitedInstanceCapacity": {"count": 1, '
                + r'"nodes": \[{"name": "queue1-st-c5xlarge-3"}\]}}}}',
            ],
        ),
        (
            {"current-queue1-st-c5xlarge-6"},
            [
                StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
                StaticNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD", "queue1"),
                StaticNode("queue1-st-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue1"),
            ],
            [None, None],
            [
                EC2Instance("id-1", "ip-1", "hostname-1", "some_launch_time"),
                EC2Instance("id-2", "ip-2", "hostname-2", "some_launch_time"),
            ],
            {"current-queue1-st-c5xlarge-6", "queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2"},
            [],
            ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3"],
            False,
            Exception,
            1,
            [],
        ),
    ],
    ids=["basic", "no_associated_instances", "partial_launch", "set_nodes_down_exception"],
)
def test_handle_unhealthy_static_nodes(
    current_replacing_nodes,
    unhealthy_static_nodes,
    instances,
    launched_instances,
    expected_replacing_nodes,
    delete_instance_list,
    add_node_list,
    mocker,
    caplog,
    output_enabled,
    set_nodes_down_exception,
    sample_size,
    expected_warnings,
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
        fleet_config=FLEET_CONFIG,
        protected_failure_count=10,
        insufficient_capacity_timeout=600,
        run_instances_overrides={},
        create_fleet_overrides={},
        compute_console_logging_enabled=output_enabled,
        compute_console_logging_max_sample_size=sample_size,
        compute_console_wait_time=1,
        worker_pool_size=5,
        worker_pool_max_backlog=10,
        head_node_instance_id="i-instance-id",
    )
    for node, instance in zip(unhealthy_static_nodes, instances):
        node.instance = instance
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._static_nodes_in_replacement = current_replacing_nodes

    # Mock associated function
    cluster_manager._instance_manager.delete_instances = mocker.MagicMock()
    cluster_manager._instance_manager._parse_nodes_resume_list = mocker.MagicMock(
        return_value={
            "queue1": {
                "c5xlarge": [
                    "queue1-st-c5xlarge-1",
                    "queue1-st-c5xlarge-2",
                    "queue1-st-c5xlarge-3",
                ]
            }
        }
    )

    report_mock = mocker.patch.object(
        cluster_manager._console_logger, "report_console_output_from_nodes", autospec=True
    )

    mocker.patch("slurm_plugin.instance_manager.update_nodes")
    cluster_manager._instance_manager._store_assigned_hostnames = mocker.MagicMock()
    cluster_manager._instance_manager._update_dns_hostnames = mocker.MagicMock()
    # Mock add_instances_for_nodes but still try to execute original code
    original_add_instances = cluster_manager._instance_manager.add_instances
    cluster_manager._instance_manager.add_instances = mocker.MagicMock(side_effect=original_add_instances)
    reset_mock = mocker.patch("slurm_plugin.clustermgtd.reset_nodes", autospec=True)
    if set_nodes_down_exception is Exception:
        reset_mock.side_effect = set_nodes_down_exception
    else:
        reset_mock.return_value = None

    # patch fleet manager
    mocker.patch.object(
        slurm_plugin.fleet_manager.FleetManager, "launch_ec2_instances", return_value=launched_instances
    )

    # Run test
    cluster_manager._handle_unhealthy_static_nodes(unhealthy_static_nodes)

    if set_nodes_down_exception is Exception:
        assert_that(caplog.text).contains("Encountered exception when setting unhealthy static nodes into down state:")
        return
    # Assert calls
    reset_mock.assert_called_with(
        add_node_list, reason="Static node maintenance: unhealthy node is being replaced", state="down"
    )
    if delete_instance_list:
        cluster_manager._instance_manager.delete_instances.assert_called_with(
            delete_instance_list, terminate_batch_size=1
        )
    else:
        cluster_manager._instance_manager.delete_instances.assert_not_called()
    cluster_manager._instance_manager.add_instances.assert_called_with(
        node_list=add_node_list, launch_batch_size=5, update_node_address=True
    )
    assert_that(caplog.records).is_length(len(expected_warnings))
    for actual, expected in zip(caplog.records, expected_warnings):
        assert_that(actual.message).matches(expected)
    assert_that(cluster_manager._static_nodes_in_replacement).is_equal_to(expected_replacing_nodes)
    report_mock.assert_called_once()


@pytest.mark.parametrize(
    "active_nodes, instances, _is_protected_mode_enabled, disable_nodes_on_insufficient_capacity, "
    "expected_ice_compute_resources_nodes_map",
    [
        (
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"
                ),  # powering_down
                StaticNode(
                    "queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue1"
                ),  # unhealthy static
                DynamicNode(
                    "queue-dy-c5xlarge-1",
                    "ip-3",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue",
                    "Failure when resuming nodes",
                ),  # unhealthy dynamic
                StaticNode(
                    "queue2-st-c5xlarge-1", "ip-4", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"
                ),  # bootstrap failure static
                DynamicNode(
                    "queue1-dy-c5xlarge-1",
                    "ip-1",
                    "hostname",
                    "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP",
                    "queue1",
                ),  # bootstrap failure dynamic
                StaticNode("queue3-st-c5xlarge-1", "ip-5", "hostname", "IDLE", "queue2"),  # healthy static
                DynamicNode("queue3-dy-c5xlarge-1", "ip-6", "hostname", "IDLE+CLOUD", "queue1"),  # healthy dynamic
                DynamicNode(
                    "queue-dy-c5-xlarge-5",
                    "ip-3",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue",
                    "(Code:MaxSpotInstanceCountExceeded)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),  # ice dynamic node does not belong to unhealthy node when enable fast capacity failover
            ],
            [
                EC2Instance("id-2", "ip-2", "hostname", "some_launch_time"),
                # Setting launch time here for instance for static node to trigger replacement timeout
                EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                None,
                EC2Instance("id-2", "ip-4", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                None,
                EC2Instance("id-5", "ip-5", "hostname", "some_launch_time"),
                EC2Instance("id-6", "ip-6", "hostname", "some_launch_time"),
                None,
            ],
            True,
            True,
            {
                "queue": {
                    "c5-xlarge": [
                        DynamicNode(
                            "queue-dy-c5-xlarge-5",
                            "ip-3",
                            "hostname",
                            "IDLE+CLOUD",
                            "queue",
                            "(Code:MaxSpotInstanceCountExceeded)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                        )
                    ]
                }
            },
        ),
        (
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1", ""
                ),  # powering_down
                StaticNode(
                    "queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue1"
                ),  # unhealthy static
                DynamicNode(
                    "queue-dy-c5xlarge-1",
                    "ip-3",
                    "hostname",
                    "DOWN+CLOUD+NOT_RESPONDING+POWERING_UP",
                    "queue",
                    "(Code:MaxSpotInstanceCountExceeded)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),  # unhealthy dynamic
                StaticNode(
                    "queue2-st-c5xlarge-1", "ip-4", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"
                ),  # bootstrap failure static
                DynamicNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),  # bootstrap failure dynamic
                StaticNode("queue3-st-c5xlarge-1", "ip-5", "hostname", "IDLE", "queue2"),  # healthy static
                DynamicNode("queue3-dy-c5xlarge-1", "ip-6", "hostname", "IDLE+CLOUD", "queue1"),  # healthy dynamic
            ],
            [
                EC2Instance("id-2", "ip-2", "hostname", "some_launch_time"),
                # Setting launch time here for instance for static node to trigger replacement timeout
                EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                None,
                EC2Instance("id-2", "ip-4", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                None,
                EC2Instance("id-5", "ip-5", "hostname", "some_launch_time"),
                EC2Instance("id-6", "ip-6", "hostname", "some_launch_time"),
            ],
            True,
            False,
            {},
        ),
    ],
    ids=["basic", "disable smart instance capacity fail over mechanism"],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_maintain_nodes(
    active_nodes,
    instances,
    _is_protected_mode_enabled,
    disable_nodes_on_insufficient_capacity,
    expected_ice_compute_resources_nodes_map,
    mocker,
):
    static_nodes_in_replacement = {"queue1-st-c5xlarge-1", "queue2-st-c5xlarge-1"}
    # Mock functions
    for node, instance in zip(active_nodes, instances):
        node.instance = instance

    expected_unhealthy_static_nodes = [active_nodes[1], active_nodes[3]]
    expected_unhealthy_dynamic_nodes = [active_nodes[2], active_nodes[4]]
    mock_sync_config = SimpleNamespace(
        terminate_drain_nodes=True,
        terminate_down_nodes=True,
        insufficient_capacity_timeout=20,
        disable_nodes_on_insufficient_capacity=disable_nodes_on_insufficient_capacity,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._static_nodes_in_replacement = static_nodes_in_replacement
    cluster_manager._current_time = datetime(2020, 1, 2, 0, 0, 0)
    cluster_manager._config.node_replacement_timeout = 30
    mock_update_replacement = mocker.patch.object(cluster_manager, "_update_static_nodes_in_replacement", autospec=True)
    mock_handle_dynamic = mocker.patch.object(cluster_manager, "_handle_unhealthy_dynamic_nodes", autospec=True)
    mock_handle_static = mocker.patch.object(cluster_manager, "_handle_unhealthy_static_nodes", autospec=True)
    mock_handle_powering_down_nodes = mocker.patch.object(cluster_manager, "_handle_powering_down_nodes", autospec=True)
    mock_handle_protected_mode_process = mocker.patch(
        "slurm_plugin.clustermgtd.ClusterManager._handle_protected_mode_process"
    )
    mock_handle_failed_health_check_nodes_in_replacement = mocker.patch.object(
        cluster_manager, "_handle_failed_health_check_nodes_in_replacement", autospec=True
    )
    mock_handle_ice_nodes = mocker.patch.object(cluster_manager, "_handle_ice_nodes", autospec=True)
    mocker.patch.object(cluster_manager, "_is_protected_mode_enabled", return_value=_is_protected_mode_enabled)
    part1 = SlurmPartition("queue1", "placeholder_nodes", "ACTIVE")
    part2 = SlurmPartition("queue2", "placeholder_nodes", "INACTIVE")
    part1.slurm_nodes = active_nodes
    partitions = {part1.name: part1, part2.name: part2}
    # Run test
    cluster_manager._maintain_nodes(partitions, {})
    # Check function calls
    mock_update_replacement.assert_called_with(active_nodes)
    mock_handle_dynamic.assert_called_with(expected_unhealthy_dynamic_nodes)
    mock_handle_static.assert_called_with(expected_unhealthy_static_nodes)
    mock_handle_powering_down_nodes.assert_called_with(active_nodes)
    mock_handle_failed_health_check_nodes_in_replacement.assert_called_with(active_nodes)
    if _is_protected_mode_enabled:
        mock_handle_protected_mode_process.assert_called_with(active_nodes, partitions)
    else:
        mock_handle_protected_mode_process.assert_not_called()

    if disable_nodes_on_insufficient_capacity:
        mock_handle_ice_nodes.assert_called_with(expected_ice_compute_resources_nodes_map, {})
    else:
        mock_handle_ice_nodes.assert_not_called()


@pytest.mark.parametrize(
    "cluster_instances, slurm_nodes, current_time, expected_instance_to_terminate",
    [
        (
            [
                EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                EC2Instance("id-2", "ip-2", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            ],
            [
                DynamicNode("queue1-st-c5xlarge-1", "ip", "hostname", "some_state", "queue1"),
                DynamicNode("queue1-st-c5xlarge-2", "ip", "hostname", "some_state", "queue1"),
            ],
            datetime(2020, 1, 1, 0, 0, 30),
            [],
        ),
        (
            [
                EC2Instance("id-3", "ip-3", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                EC2Instance("id-2", "ip-2", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            ],
            [None, None],
            datetime(2020, 1, 1, 0, 0, 30),
            ["id-3", "id-2"],
        ),
        (
            [
                EC2Instance("id-3", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                EC2Instance("id-2", "ip-2", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            ],
            [None, None],
            datetime(2020, 1, 1, 0, 0, 29),
            [],
        ),
    ],
    ids=["all_good", "orphaned", "orphaned_timeout"],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_terminate_orphaned_instances(
    cluster_instances, slurm_nodes, current_time, expected_instance_to_terminate, mocker
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
        protected_failure_count=10,
        insufficient_capacity_timeout=600,
        node_replacement_timeout=1800,
        run_instances_overrides={},
        create_fleet_overrides={},
        fleet_config=FLEET_CONFIG,
        head_node_instance_id="i-instance-id",
    )
    for instance, node in zip(cluster_instances, slurm_nodes):
        instance.slurm_node = node
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._current_time = current_time
    cluster_manager._instance_manager.delete_instances = mocker.MagicMock()
    # Run test
    cluster_manager._terminate_orphaned_instances(cluster_instances)
    # Check function calls
    if expected_instance_to_terminate:
        cluster_manager._instance_manager.delete_instances.assert_called_with(
            expected_instance_to_terminate, terminate_batch_size=4
        )


@pytest.mark.parametrize(
    "disable_cluster_management, disable_health_check, mock_cluster_instances, nodes, partitions, status, "
    "queue_compute_resource_nodes_map",
    [
        (
            False,
            False,
            [
                EC2Instance("id-1", "ip", "hostname", "launch_time"),
                EC2Instance("id-2", "ip", "hostname", "launch_time"),
            ],
            [
                StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "some_state", "queue1"),
                DynamicNode("queue1-dy-c5xlarge-2", "ip-2", "hostname", "some_state", "queue1"),
            ],
            {
                "queue1": SlurmPartition("queue1", "placeholder_nodes", "UP"),
            },
            ComputeFleetStatus.RUNNING,
            {
                "queue1": {
                    "c5xlarge": [
                        StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "some_state", "queue1"),
                        DynamicNode("queue1-dy-c5xlarge-2", "ip-2", "hostname", "some_state", "queue1"),
                    ]
                },
            },
        ),
        (
            True,
            False,
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [
                StaticNode("queue1-st-c5xlarge-1", "ip", "hostname", "some_state", "queue1"),
                DynamicNode("queue1-dy-c5xlarge-2", "ip", "hostname", "some_state", "queue1"),
            ],
            {
                "queue1": SlurmPartition("queue1", "placeholder_nodes", "UP"),
            },
            ComputeFleetStatus.RUNNING,
            {},
        ),
        (
            False,
            True,
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [
                StaticNode("queue1-st-c5xlarge-1", "ip", "hostname", "some_state", "queue1"),
                DynamicNode("queue1-dy-c5xlarge-2", "ip", "hostname", "some_state", "queue1"),
            ],
            {
                "queue1": SlurmPartition("queue1", "placeholder_nodes", "UP"),
            },
            ComputeFleetStatus.RUNNING,
            {
                "queue1": {
                    "c5xlarge": [
                        StaticNode("queue1-st-c5xlarge-1", "ip", "hostname", "some_state", "queue1"),
                        DynamicNode("queue1-dy-c5xlarge-2", "ip", "hostname", "some_state", "queue1"),
                    ]
                },
            },
        ),
        (
            False,
            True,
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [],
            {"queue1": SlurmPartition("queue1", "placeholder_nodes", "UP")},
            ComputeFleetStatus.RUNNING,
            {},
        ),
        (
            False,
            None,
            [],
            [],
            {},
            ComputeFleetStatus.STOPPED,
            {},
        ),
        (
            True,
            None,
            [],
            [],
            {},
            ComputeFleetStatus.STOPPED,
            {},
        ),
    ],
    ids=[
        "all_enabled",
        "disable_all",
        "disable_health_check",
        "no_node",
        "stopped_enabled",
        "stopped_disabled",
    ],
)
@pytest.mark.usefixtures("initialize_executor_mock", "initialize_console_logger_mock")
def test_manage_cluster(
    disable_cluster_management,
    disable_health_check,
    mock_cluster_instances,
    nodes,
    mocker,
    initialize_instance_manager_mock,
    caplog,
    partitions,
    status,
    queue_compute_resource_nodes_map,
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
        protected_failure_count=10,
        insufficient_capacity_timeout=600,
        node_replacement_timeout=1800,
        terminate_max_batch_size=1,
        head_node_instance_id="i-instance-id",
    )
    mocker.patch("time.sleep")
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._current_time = "current_time"
    cluster_manager._static_nodes_in_replacement = {}
    # Set up function mocks
    mocker.patch("slurm_plugin.clustermgtd.datetime").now.return_value = datetime(2020, 1, 1, 0, 0, 0)
    compute_fleet_status_manager_mock = mocker.patch.object(
        cluster_manager, "_compute_fleet_status_manager", spec=ComputeFleetStatusManager
    )
    compute_fleet_status_manager_mock.get_status.return_value = status
    get_partitions_info_with_retry_mock = mocker.patch(
        "slurm_plugin.clustermgtd.ClusterManager._get_partitions_info_with_retry", return_value=partitions
    )

    write_timestamp_to_file_mock = mocker.patch.object(ClusterManager, "_write_timestamp_to_file", autospec=True)
    perform_health_check_actions_mock = mocker.patch.object(
        ClusterManager, "_perform_health_check_actions", autospec=True
    )
    clean_up_inactive_partition_mock = mocker.patch.object(
        ClusterManager, "_clean_up_inactive_partition", return_value=mock_cluster_instances, autospec=True
    )
    terminate_orphaned_instances_mock = mocker.patch.object(
        ClusterManager, "_terminate_orphaned_instances", autospec=True
    )
    maintain_nodes_mock = mocker.patch.object(ClusterManager, "_maintain_nodes", autospec=True)
    get_ec2_instances_mock = mocker.patch.object(
        ClusterManager, "_get_ec2_instances", autospec=True, return_value=mock_cluster_instances
    )
    get_node_info_with_retry_mock = mocker.patch.object(
        ClusterManager,
        "_get_node_info_with_retry",
        autospec=True,
        return_value=nodes,
    )
    update_all_partitions_mock = mocker.patch("slurm_plugin.clustermgtd.update_all_partitions", autospec=True)

    # Run test
    cluster_manager.manage_cluster()
    # Assert function calls
    initialize_instance_manager_mock.assert_called_once()
    write_timestamp_to_file_mock.assert_called_once()
    compute_fleet_status_manager_mock.get_status.assert_called_once()
    if disable_cluster_management:
        perform_health_check_actions_mock.assert_not_called()
        clean_up_inactive_partition_mock.assert_not_called()
        terminate_orphaned_instances_mock.assert_not_called()
        maintain_nodes_mock.assert_not_called()
        get_node_info_with_retry_mock.assert_not_called()
        get_ec2_instances_mock.assert_not_called()
        update_all_partitions_mock.assert_not_called()
        initialize_instance_manager_mock().terminate_all_compute_nodes.assert_not_called()
        return
    if status == ComputeFleetStatus.RUNNING:
        clean_up_inactive_partition_mock.assert_called_with(cluster_manager, list(partitions.values()))
        get_ec2_instances_mock.assert_called_once()

        if disable_health_check:
            perform_health_check_actions_mock.assert_not_called()
            maintain_nodes_mock.assert_called_with(cluster_manager, partitions, queue_compute_resource_nodes_map)
        else:
            perform_health_check_actions_mock.assert_called_with(cluster_manager, list(partitions.values()))
            maintain_nodes_mock.assert_called_with(cluster_manager, partitions, queue_compute_resource_nodes_map)
        terminate_orphaned_instances_mock.assert_called_with(cluster_manager, mock_cluster_instances)

        assert_that(caplog.text).is_empty()
        get_partitions_info_with_retry_mock.assert_called_once()
        update_all_partitions_mock.assert_not_called()
        initialize_instance_manager_mock().terminate_all_compute_nodes.assert_not_called()
    elif status == ComputeFleetStatus.STOPPED:
        update_all_partitions_mock.assert_called_with(PartitionStatus.INACTIVE, reset_node_addrs_hostname=True)
        initialize_instance_manager_mock().terminate_all_compute_nodes.assert_called_with(
            mock_sync_config.terminate_max_batch_size
        )
        clean_up_inactive_partition_mock.assert_not_called()
        get_ec2_instances_mock.assert_not_called()
        terminate_orphaned_instances_mock.assert_not_called()
        get_partitions_info_with_retry_mock.assert_not_called()


@pytest.mark.parametrize(
    (
        "config_file, mocked_active_nodes, mocked_inactive_nodes, mocked_boto3_request, "
        "launched_instances, expected_error_messages"
    ),
    [
        (
            # basic: This is the most comprehensive case in manage_cluster with max number of boto3 calls
            "default.conf",
            [
                # This node fail scheduler state check and corresponding instance will be terminated and replaced
                StaticNode("queue-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+DRAIN", "queue1"),
                # This node fail scheduler state check and node will be power_down
                DynamicNode("queue-dy-c5xlarge-2", "ip-2", "hostname", "DOWN+CLOUD", "queue1"),
                # This node is good and should not be touched by clustermgtd
                DynamicNode("queue-dy-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue1"),
                # This node is in power_saving state but still has running backing instance, it should be terminated
                DynamicNode("queue-dy-c5xlarge-6", "ip-6", "hostname", "IDLE+CLOUD+POWER", "queue1"),
                # This node is in powering_down but still has no valid backing instance, no boto3 call
                DynamicNode("queue-dy-c5xlarge-8", "ip-8", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"),
            ],
            [
                StaticNode("queue-st-c5xlarge-4", "ip-4", "hostname", "IDLE+CLOUD", "queue2"),
                DynamicNode("queue-dy-c5xlarge-5", "ip-5", "hostname", "DOWN+CLOUD", "queue2"),
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
                                    {
                                        "InstanceId": "i-3",
                                        "PrivateIpAddress": "ip-3",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                        "NetworkInterfaces": [
                                            {
                                                "Attachment": {
                                                    "DeviceIndex": 0,
                                                    "NetworkCardIndex": 0,
                                                },
                                                "PrivateIpAddress": "ip-3",
                                            },
                                        ],
                                    },
                                    {
                                        "InstanceId": "i-4",
                                        "PrivateIpAddress": "ip-4",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                        "NetworkInterfaces": [
                                            {
                                                "Attachment": {
                                                    "DeviceIndex": 0,
                                                    "NetworkCardIndex": 0,
                                                },
                                                "PrivateIpAddress": "ip-4",
                                            },
                                        ],
                                    },
                                    # Return an orphaned instance
                                    {
                                        "InstanceId": "i-999",
                                        "PrivateIpAddress": "ip-999",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                        "NetworkInterfaces": [
                                            {
                                                "Attachment": {
                                                    "DeviceIndex": 0,
                                                    "NetworkCardIndex": 0,
                                                },
                                                "PrivateIpAddress": "ip-999",
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
                # _maintain_nodes/delete_instances: terminate dynamic down nodes
                # dynamic down nodes are handled with suspend script, and its boto3 call should not be reflected here
                MockedBoto3Request(
                    method="terminate_instances",
                    response={},
                    expected_params={"InstanceIds": ["i-2"]},
                    generate_error=False,
                ),
                # _maintain_nodes/delete_instances: terminate static down nodes
                # dynamic down nodes are handled with suspend script, and its boto3 call should not be reflected here
                MockedBoto3Request(
                    method="terminate_instances",
                    response={},
                    expected_params={"InstanceIds": ["i-1"]},
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
            # _maintain_nodes/add_instances_for_nodes: launch new instance for static node
            [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-1234",
                            "PrivateIpAddress": "ip-1234",
                            "PrivateDnsName": "hostname-1234",
                            "LaunchTime": datetime(2020, 1, 1, 0, 0, 0),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip-1234",
                                },
                            ],
                        }
                    ]
                }
            ],
            [],
        ),
        (
            # failures: All failure tolerant module will have an exception, but the program should not crash
            "default.conf",
            [
                StaticNode(
                    "queue-st-c5xlarge-1",
                    "ip-1",
                    "hostname",
                    "DOWN+CLOUD",
                    "queue1",
                    slurmdstarttime=datetime(2020, 1, 1, tzinfo=timezone.utc),
                ),
                DynamicNode(
                    "queue-dy-c5xlarge-2",
                    "ip-2",
                    "hostname",
                    "DOWN+CLOUD",
                    "queue1",
                    slurmdstarttime=datetime(2020, 1, 1, tzinfo=timezone.utc),
                ),
                DynamicNode(
                    "queue-dy-c5xlarge-3",
                    "ip-3",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue1",
                    slurmdstarttime=datetime(2020, 1, 1, tzinfo=timezone.utc),
                ),
            ],
            [
                StaticNode(
                    "queue-st-c5xlarge-4",
                    "ip-4",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue2",
                    slurmdstarttime=datetime(2020, 1, 1, tzinfo=timezone.utc),
                ),
                DynamicNode(
                    "queue-dy-c5xlarge-5",
                    "ip-5",
                    "hostname",
                    "DOWN+CLOUD",
                    "queue2",
                    slurmdstarttime=datetime(2020, 1, 1, tzinfo=timezone.utc),
                ),
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
                                    {
                                        "InstanceId": "i-3",
                                        "PrivateIpAddress": "ip-3",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                        "NetworkInterfaces": [
                                            {
                                                "Attachment": {
                                                    "DeviceIndex": 0,
                                                    "NetworkCardIndex": 0,
                                                },
                                                "PrivateIpAddress": "ip-3",
                                            },
                                        ],
                                    },
                                    {
                                        "InstanceId": "i-4",
                                        "PrivateIpAddress": "ip-4",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                        "NetworkInterfaces": [
                                            {
                                                "Attachment": {
                                                    "DeviceIndex": 0,
                                                    "NetworkCardIndex": 0,
                                                },
                                                "PrivateIpAddress": "ip-4",
                                            },
                                        ],
                                    },
                                    # Return an orphaned instance
                                    {
                                        "InstanceId": "i-999",
                                        "PrivateIpAddress": "ip-999",
                                        "PrivateDnsName": "hostname",
                                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                        "NetworkInterfaces": [
                                            {
                                                "Attachment": {
                                                    "DeviceIndex": 0,
                                                    "NetworkCardIndex": 0,
                                                },
                                                "PrivateIpAddress": "ip-999",
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
                # _terminate_orphaned_instances: terminate orphaned instances
                # Produce an error, cluster should be able to handle exception and move on
                MockedBoto3Request(
                    method="terminate_instances",
                    response={"error for i-999"},
                    expected_params={"InstanceIds": ["i-999"]},
                    generate_error=True,
                ),
            ],
            [client_error("some run_instances error")],
            [
                r"Failed TerminateInstances request:",
                r"Failed when terminating instances \(x1\) \['i-4'\].*{'error for i-4'}",
                r"Failed when getting health status for unhealthy EC2 instances",
                r"Failed when performing health check action with exception",
                r"Failed TerminateInstances request:",
                r"Failed when terminating instances \(x1\) \['i-2'\].*{'error for i-2'}",
                r"Failed TerminateInstances request:",
                r"Failed when terminating instances \(x1\) \['i-1'\].*{'error for i-1'}",
                (
                    r"Encountered exception when launching instances for nodes \(x1\) \['queue-st-c5xlarge-1'\].*"
                    r"some run_instances error"
                ),
                r"Failed TerminateInstances request:",
                r"Failed when terminating instances \(x1\) \['i-999'\].*{'error for i-999'}",
            ],
        ),
        (
            # critical_failure_1: _get_ec2_instances will have an exception, but the program should not crash
            "default.conf",
            [
                StaticNode("queue-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD", "queue1"),
                DynamicNode("queue-dy-c5xlarge-2", "ip-2", "hostname", "DOWN+CLOUD", "queue1"),
                DynamicNode("queue-dy-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue1"),
            ],
            [
                StaticNode("queue-st-c5xlarge-4", "ip-4", "hostname", "IDLE+CLOUD", "queue2"),
                DynamicNode("queue-dy-c5xlarge-5", "ip-5", "hostname", "DOWN+CLOUD", "queue2"),
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
            [{}],
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
            [{}],
            [
                "Unable to get partition/node info from slurm, no other action can be performed",
            ],
        ),
    ],
    ids=["basic", "failures", "critical_failure_1", "critical_failure_2"],
)
@pytest.mark.usefixtures("initialize_executor_mock", "initialize_console_logger_mock")
def test_manage_cluster_boto3(
    boto3_stubber,
    config_file,
    mocked_active_nodes,
    mocked_inactive_nodes,
    mocked_boto3_request,
    launched_instances,
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
    mocker.patch("slurm_plugin.clustermgtd.read_json", side_effect=[FLEET_CONFIG, LAUNCH_OVERRIDES, LAUNCH_OVERRIDES])
    sync_config = ClustermgtdConfig(test_datadir / config_file)
    sync_config.launch_overrides = {"dynamic": {"c5.xlarge": {"InstanceType": "t2.micro"}}}
    cluster_manager = ClusterManager(sync_config)
    check_command_output_mocked = mocker.patch("slurm_plugin.clustermgtd.check_command_output")
    check_command_output_mocked.return_value = '{"status": "RUNNING"}'
    mocker.patch.object(cluster_manager, "_write_timestamp_to_file", autospec=True)
    part_active = SlurmPartition("queue1", "placeholder_nodes", "UP")
    part_active.slurm_nodes = mocked_active_nodes
    part_inactive = SlurmPartition("queue2", "placeholder_nodes", "INACTIVE")
    part_inactive.slurm_nodes = mocked_inactive_nodes
    partitions = {"queue1": part_active, "queue2": part_inactive}

    # patch fleet manager calls
    mocker.patch.object(
        slurm_plugin.fleet_manager.Ec2RunInstancesManager, "_launch_instances", side_effect=launched_instances
    )

    if mocked_active_nodes is Exception or mocked_inactive_nodes is Exception:
        mocker.patch.object(cluster_manager, "_get_node_info_with_retry", side_effect=Exception)

    else:
        mocker.patch.object(
            cluster_manager, "_get_node_info_with_retry", return_value=mocked_active_nodes + mocked_inactive_nodes
        )
        mocker.patch(
            "slurm_plugin.clustermgtd.ClusterManager._parse_scheduler_nodes_data", return_value=[partitions, {}]
        )
    cluster_manager._instance_manager._store_assigned_hostnames = mocker.MagicMock()
    cluster_manager._instance_manager._update_dns_hostnames = mocker.MagicMock()
    cluster_manager.manage_cluster()

    assert_that(caplog.records).is_length(len(expected_error_messages))
    for actual, expected in zip(caplog.records, expected_error_messages):
        assert_that(actual.message).matches(expected)


class TestComputeFleetStatusManager:
    @pytest.mark.parametrize(
        "get_item_response, fallback, expected_status",
        [
            ('{"status": "RUNNING"}', None, ComputeFleetStatus.RUNNING),
            ("", ComputeFleetStatus.STOPPED, ComputeFleetStatus.STOPPED),
            (Exception, ComputeFleetStatus.STOPPED, ComputeFleetStatus.STOPPED),
        ],
        ids=["success", "empty_response", "exception"],
    )
    def test_get_status(self, mocker, get_item_response, fallback, expected_status):
        check_command_output_mocked = mocker.patch("slurm_plugin.clustermgtd.check_command_output", autospec=True)
        compute_fleet_status_manager = ComputeFleetStatusManager()

        if get_item_response is Exception:
            check_command_output_mocked.side_effect = get_item_response
        else:
            check_command_output_mocked.return_value = get_item_response
        status = compute_fleet_status_manager.get_status(fallback)
        assert_that(status).is_equal_to(expected_status)
        check_command_output_mocked.assert_called_once_with("get-compute-fleet-status.sh")

    @pytest.mark.parametrize(
        "desired_status, update_item_response",
        [
            (ComputeFleetStatus.PROTECTED, None),
            (ComputeFleetStatus.STOPPED, AttributeError),
        ],
        ids=["success", "exception"],
    )
    def test_update_status(self, mocker, desired_status, update_item_response):
        check_command_output_mocked = mocker.patch("slurm_plugin.clustermgtd.check_command_output", autospec=True)
        compute_fleet_status_manager = ComputeFleetStatusManager()

        if update_item_response is AttributeError:
            check_command_output_mocked.side_effect = update_item_response
            with pytest.raises(AttributeError):
                compute_fleet_status_manager.update_status(desired_status)
        else:
            check_command_output_mocked.return_value = update_item_response
            compute_fleet_status_manager.update_status(desired_status)

        check_command_output_mocked.assert_called_once_with(f"update-compute-fleet-status.sh --status {desired_status}")


@pytest.mark.parametrize(
    "healthy_nodes, expected_partitions_protected_failure_count_map",
    [
        (
            [
                DynamicNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
                DynamicNode(
                    "queue2-st-c5xlarge-2", "ip-2", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue2"
                ),
            ],
            {"queue2": {"c5xlarge": 8}},
        ),
        (
            [
                DynamicNode(
                    "queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),
                DynamicNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "MIXED+CLOUD+NOT_RESPONDING", "queue1"),
            ],
            {"queue1": {"c5xlarge": 5, "c5large": 3}, "queue2": {"c5xlarge": 8}},
        ),
    ],
)
@pytest.mark.usefixtures("initialize_executor_mock", "initialize_console_logger_mock")
def test_handle_successfully_launched_nodes(
    healthy_nodes,
    expected_partitions_protected_failure_count_map,
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
        head_node_private_ip="master.ip",
        head_node_hostname="master-hostname",
        hosted_zone="hosted_zone",
        dns_domain="dns.domain",
        use_private_hostname=False,
        protected_failure_count=10,
        insufficient_capacity_timeout=600,
        terminate_drain_nodes=True,
        terminate_down_nodes=True,
        run_instances_overrides={},
        create_fleet_overrides={},
        fleet_config=FLEET_CONFIG,
        head_node_instance_id="i-instance-id",
    )
    # Mock associated function
    cluster_manager = ClusterManager(mock_sync_config)
    partition1 = SlurmPartition("queue1", "queue1-st-c5xlarge-1", "ACTIVE")
    for node in healthy_nodes:
        node.instance = "instance"
    partition1.slurm_nodes = healthy_nodes
    partition2 = SlurmPartition("queue2", "queue2-st-c5xlarge-1", "ACTIVE")
    partitions_name_map = {"queue1": partition1, "queue2": partition2}
    cluster_manager._partitions_protected_failure_count_map = {
        "queue1": {"c5xlarge": 5, "c5large": 3},
        "queue2": {"c5xlarge": 8},
    }

    # Run test
    cluster_manager._handle_successfully_launched_nodes(partitions_name_map)
    # Assert calls
    assert_that(cluster_manager._partitions_protected_failure_count_map).is_equal_to(
        expected_partitions_protected_failure_count_map
    )


@pytest.mark.parametrize(
    "nodes, initial_map, expected_map",
    [
        (
            [
                DynamicNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "some_state", "queue1"),
                DynamicNode("queue2-st-c5xlarge-1", "ip-1", "hostname", "some_state", "queue2"),
            ],
            {"queue1": {"c5xlarge": 1}, "queue2": {"c5xlarge": 1}},
            {"queue1": {"c5xlarge": 2}, "queue2": {"c5xlarge": 2}},
        ),
        (
            [DynamicNode("queue2-st-c5xlarge-1", "ip-1", "hostname", "some_state", "queue2")],
            {"queue1": {"c5xlarge": 1}},
            {"queue1": {"c5xlarge": 1}, "queue2": {"c5xlarge": 1}},
        ),
        (
            [
                DynamicNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "some_state", "queue1"),
                DynamicNode("queue1-st-c5large-1", "ip-1", "hostname", "some_state", "queue1"),
            ],
            {},
            {"queue1": {"c5large": 1, "c5xlarge": 1}},
        ),
        (  # Custom queue is not managed by protected mode
            [DynamicNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "some_state", "queue1,custom_queue1")],
            {},
            {"queue1": {"c5xlarge": 1}},
        ),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_increase_partitions_protected_failure_count(nodes, initial_map, expected_map, mocker):
    # Mock associated function
    mock_sync_config = SimpleNamespace(
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._partitions_protected_failure_count_map = initial_map
    # Run test
    cluster_manager._increase_partitions_protected_failure_count(nodes)
    # Assert calls
    assert_that(cluster_manager._partitions_protected_failure_count_map).is_equal_to(expected_map)


@pytest.mark.parametrize(
    "partition, expected_map",
    [
        ("queue1", {"queue2": 1}),
        ("queue2", {"queue1": 2}),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_reset_partition_failure_count(mocker, partition, expected_map):
    # Mock associated function
    mock_sync_config = SimpleNamespace(
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._partitions_protected_failure_count_map = {"queue1": 2, "queue2": 1}
    cluster_manager._reset_partition_failure_count(partition)
    assert_that(cluster_manager._partitions_protected_failure_count_map).is_equal_to(expected_map)


@pytest.mark.parametrize(
    "partitions, slurm_nodes_list, active_nodes, expected_partitions_to_disable, "
    "partitions_protected_failure_count_map",
    [
        (
            [
                SlurmPartition("queue1", "queue1-st-c5xlarge-1", "ACTIVE"),
                SlurmPartition("queue2", "queue2-st-c5xlarge-1", "ACTIVE"),
                SlurmPartition("queue2", "queue2-st-c5xlarge-1", "ACTIVE"),
            ],
            [[StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN", "queue1")], [], []],
            [
                StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN", "queue1"),
            ],
            ["queue1", "queue2"],
            {
                "queue1": {"c5xlarge": 6, "c5large": 5},
                "queue2": {"c5xlarge": 6, "c5large": 6},
                "queue3": {"c5xlarge": 6},
            },
        ),
        (
            [
                SlurmPartition("queue1", "queue1-st-c5xlarge-1", "ACTIVE"),
                SlurmPartition("queue2", "queue2-st-c5xlarge-1", "ACTIVE"),
                SlurmPartition("queue2", "queue2-st-c5xlarge-1", "INACTIVE"),
            ],
            [
                [
                    StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN", "queue1"),
                    StaticNode("queue1-dy-c5large-2", "ip-2", "hostname", "MIXED", "queue1"),
                ],
                [DynamicNode("queue2-dy-c5large-2", "ip-2", "hostname", "MIXED", "queue2")],
                [],
            ],
            [
                DynamicNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN", "queue1"),
            ],
            [],
            {
                "queue1": {"c5xlarge": 6, "c5large": 5},
                "queue2": {"c5xlarge": 6, "c5large": 6},
                "queue3": {"c5xlarge": 6},
            },
        ),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_handle_protected_mode_process(
    partitions,
    slurm_nodes_list,
    active_nodes,
    expected_partitions_to_disable,
    partitions_protected_failure_count_map,
    mocker,
    caplog,
):
    mock_sync_config = SimpleNamespace(
        protected_failure_count=10,
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    caplog.set_level(logging.INFO)
    cluster_manager = ClusterManager(mock_sync_config)
    mock_handle_successfully_launched_nodes = mocker.patch.object(
        cluster_manager, "_handle_successfully_launched_nodes"
    )
    mock_handle_bootstrap_failure_nodes = mocker.patch.object(cluster_manager, "_handle_bootstrap_failure_nodes")

    mock_enter_protected_mode = mocker.patch.object(cluster_manager, "_enter_protected_mode")
    cluster_manager._partitions_protected_failure_count_map = partitions_protected_failure_count_map
    for partition, slurm_nodes in zip(partitions, slurm_nodes_list):
        partition.slurm_nodes = slurm_nodes
    partitions_name_map = {partition.name: partition for partition in partitions}
    # Run test
    cluster_manager._handle_protected_mode_process(active_nodes, partitions_name_map)
    # Assert calls
    mock_handle_bootstrap_failure_nodes.assert_called_with(active_nodes)

    mock_handle_successfully_launched_nodes.assert_called_with(partitions_name_map)
    if expected_partitions_to_disable:
        mock_enter_protected_mode.assert_called_with(expected_partitions_to_disable)


@pytest.mark.parametrize(
    "partitions_to_disable, compute_fleet_status",
    [
        (
            {"queue1", "queue2"},
            ComputeFleetStatus.PROTECTED,
        ),
        (
            {"queue1"},
            ComputeFleetStatus.RUNNING,
        ),
        (
            {"queue1", "queue2"},
            ComputeFleetStatus.STOPPED,
        ),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_enter_protected_mode(
    partitions_to_disable,
    mocker,
    compute_fleet_status,
    caplog,
):
    caplog.set_level(logging.INFO)
    mock_sync_config = SimpleNamespace(
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    mock_update_compute_fleet_status = mocker.patch.object(cluster_manager, "_update_compute_fleet_status")
    mocker.patch("common.schedulers.slurm_commands.run_command", autospec=True)
    cluster_manager._compute_fleet_status = compute_fleet_status
    cluster_manager._enter_protected_mode(partitions_to_disable)
    if compute_fleet_status != ComputeFleetStatus.PROTECTED:
        assert_that(caplog.text).contains(
            "Setting cluster into protected mode due to failures detected in node provisioning"
        )
        mock_update_compute_fleet_status.assert_called_with(ComputeFleetStatus.PROTECTED)
    assert_that(caplog.text).contains("Placing bootstrap failure partitions to INACTIVE")


@pytest.fixture()
def initialize_instance_manager_mock(mocker):
    return mocker.patch.object(
        ClusterManager, "_initialize_instance_manager", spec=ClusterManager._initialize_instance_manager
    )


@pytest.fixture()
def initialize_executor_mock(mocker):
    return mocker.patch.object(ClusterManager, "_initialize_executor", spec=ClusterManager._initialize_executor)


@pytest.fixture()
def initialize_console_logger_mock(mocker):
    return mocker.patch.object(
        ClusterManager,
        "_initialize_console_logger",
        spec=ClusterManager._initialize_console_logger,
    )


@pytest.mark.parametrize(
    "current_replacing_nodes, node, instance, current_time, expected_result",
    [
        (
            set(),
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            datetime(2020, 1, 1, 0, 0, 29),
            False,
        ),
        (
            {"queue1-st-c5xlarge-1"},
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
            None,
            datetime(2020, 1, 1, 0, 0, 29),
            False,
        ),
        (
            {"queue1-st-c5xlarge-1"},
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            datetime(2020, 1, 1, 0, 0, 29),
            True,
        ),
        (
            {"queue1-st-c5xlarge-1"},
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            datetime(2020, 1, 1, 0, 0, 30),
            False,
        ),
    ],
    ids=["not_in_replacement", "no-backing-instance", "in_replacement", "timeout"],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_is_node_being_replaced(current_replacing_nodes, node, instance, current_time, expected_result):
    mock_sync_config = SimpleNamespace(
        node_replacement_timeout=30,
        insufficient_capacity_timeout=3,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._current_time = current_time
    cluster_manager._static_nodes_in_replacement = current_replacing_nodes
    node.instance = instance
    assert_that(cluster_manager._is_node_being_replaced(node)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, instance, current_node_in_replacement, is_replacement_timeout",
    [
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"),
            None,
            {"queue1-st-c5xlarge-1"},
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            {"queue1-st-c5xlarge-1"},
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"),
            None,
            {"some_node_in_replacement"},
            False,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "queue1-dy-c5xlarge-1",
                "hostname",
                "DOWN+CLOUD+POWERED_DOWN+NOT_RESPONDING",
                "queue1",
            ),
            EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            {"some_node_in_replacement"},
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            {"some_node_in_replacement"},
            False,
        ),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_is_node_replacement_timeout(node, current_node_in_replacement, is_replacement_timeout, instance):
    node.instance = instance
    mock_sync_config = SimpleNamespace(
        node_replacement_timeout=30,
        insufficient_capacity_timeout=-2.2,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._current_time = datetime(2020, 1, 2, 0, 0, 0)
    cluster_manager._static_nodes_in_replacement = current_node_in_replacement
    assert_that(cluster_manager._is_node_replacement_timeout(node)).is_equal_to(is_replacement_timeout)


@pytest.mark.parametrize(
    "active_nodes, is_static_nodes_in_replacement, is_failing_health_check, current_nodes_in_replacement, "
    "expected_nodes_in_replacement",
    [
        (
            [
                StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"),
                DynamicNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),
            ],
            [True, False],
            [True, True],
            {"queue1-st-c5xlarge-1", "queue1-st-c5large-1"},
            {"queue1-st-c5large-1"},
        ),
        (
            [
                StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"),
                DynamicNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),
            ],
            [False, False],
            [True, True],
            {"some_node_in_replacement"},
            {"some_node_in_replacement"},
        ),
        (
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                )
            ],
            [True],
            [False],
            {"some_node_in_replacement"},
            {"some_node_in_replacement"},
        ),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_handle_failed_health_check_nodes_in_replacement(
    active_nodes,
    is_static_nodes_in_replacement,
    is_failing_health_check,
    current_nodes_in_replacement,
    expected_nodes_in_replacement,
    mocker,
):
    for node, is_node_in_replacement, is_node_failing_health_check in zip(
        active_nodes, is_static_nodes_in_replacement, is_failing_health_check
    ):
        node.is_static_nodes_in_replacement = is_node_in_replacement
        node.is_failing_health_check = is_node_failing_health_check

    mock_sync_config = SimpleNamespace(
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._static_nodes_in_replacement = current_nodes_in_replacement
    # Run tests
    cluster_manager._handle_failed_health_check_nodes_in_replacement(active_nodes)
    # Assertions
    assert_that(cluster_manager._static_nodes_in_replacement).is_equal_to(expected_nodes_in_replacement)


@pytest.mark.parametrize(
    "active_nodes, instances",
    [
        (
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"
                ),  # powering_down
                StaticNode(
                    "queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue1"
                ),  # unhealthy static
                DynamicNode("queue-dy-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),  # unhealthy dynamic
                StaticNode(
                    "queue2-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"
                ),  # bootstrap failure static
                DynamicNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),  # bootstrap failure dynamic
                StaticNode("queue3-st-c5xlarge-1", "ip-5", "hostname", "IDLE", "queue2"),  # healthy static
                DynamicNode("queue3-dy-c5xlarge-1", "ip-6", "hostname", "IDLE+CLOUD", "queue1"),  # healthy dynamic
                StaticNode("queue3-st-c5xlarge-1", "ip-5", "hostname", "IDLE", "queue2"),  # fail health check
                DynamicNode(
                    "queue3-dy-c5xlarge-1", "ip-6", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),  # fail health check
            ],
            [
                EC2Instance("id-3", "ip-3", "hostname", "some_launch_time"),
                EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                None,
                EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                None,
                EC2Instance("id-5", "ip-5", "hostname", "some_launch_time"),
                EC2Instance("id-6", "ip-6", "hostname", "some_launch_time"),
            ],
        ),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_handle_bootstrap_failure_nodes(
    active_nodes,
    instances,
    mocker,
):
    # Mock functions
    mock_sync_config = SimpleNamespace(
        terminate_drain_nodes=True,
        terminate_down_nodes=True,
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    for node, instance in zip(active_nodes, instances):
        node.instance = instance
    active_nodes[1].is_static_nodes_in_replacement = True
    active_nodes[3].is_static_nodes_in_replacement = True
    active_nodes[3]._is_replacement_timeout = True
    active_nodes[7].is_static_nodes_in_replacement = True
    active_nodes[7].is_failing_health_check = True
    active_nodes[8].is_failing_health_check = True

    expected_bootstrap_failure_nodes = [active_nodes[3], active_nodes[4], active_nodes[7], active_nodes[8]]
    mock_increase_partitions_protected_failure_count = mocker.patch.object(
        cluster_manager, "_increase_partitions_protected_failure_count"
    )
    cluster_manager._handle_bootstrap_failure_nodes(active_nodes)
    mock_increase_partitions_protected_failure_count.assert_called_with(expected_bootstrap_failure_nodes)


@pytest.mark.parametrize(
    "active_nodes, instances",
    [
        (
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"
                ),  # powering_down
                StaticNode(
                    "queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue1"
                ),  # unhealthy static
                DynamicNode("queue-dy-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),  # unhealthy dynamic
                StaticNode(
                    "queue2-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"
                ),  # bootstrap failure static
                DynamicNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),  # bootstrap failure dynamic
                StaticNode("queue3-st-c5xlarge-1", "ip-5", "hostname", "IDLE", "queue2"),  # healthy static
                DynamicNode("queue3-dy-c5xlarge-1", "ip-6", "hostname", "IDLE+CLOUD", "queue1"),  # healthy dynamic
            ],
            [
                EC2Instance("id-3", "ip-3", "hostname", "some_launch_time"),
                EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                None,
                EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                None,
                EC2Instance("id-5", "ip-5", "hostname", "some_launch_time"),
                EC2Instance("id-6", "ip-6", "hostname", "some_launch_time"),
            ],
        ),
    ],
    ids=["basic"],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_find_bootstrap_failure_nodes(active_nodes, instances):
    # Mock functions
    mock_sync_config = SimpleNamespace(
        terminate_drain_nodes=True,
        terminate_down_nodes=True,
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    for node, instance in zip(active_nodes, instances):
        node.instance = instance
    active_nodes[1].is_static_nodes_in_replacement = True
    active_nodes[3].is_static_nodes_in_replacement = True
    active_nodes[3]._is_replacement_timeout = True

    expected_bootstrap_failure_nodes = [active_nodes[3], active_nodes[4]]
    assert_that(cluster_manager._find_bootstrap_failure_nodes(active_nodes)).is_equal_to(
        expected_bootstrap_failure_nodes
    )


@pytest.mark.parametrize(
    "queue_compute_resource_nodes_map, ice_compute_resources_and_nodes_map, "
    "initial_insufficient_capacity_compute_resources, expected_insufficient_capacity_compute_resources, "
    "expected_power_save_node_list, expected_nodes_to_down",
    [
        (
            {
                "queue1": {
                    "c5xlarge": [
                        DynamicNode(
                            "queue1-dy-c5xlarge-1",
                            "queue1-dy-c5xlarge-1",
                            "queue1-dy-c5xlarge-1",
                            "IDLE+CLOUD+POWERED_DOWN",
                            "queue1",
                            "some reason",
                        ),  # Dynamic powered down node belongs to ICE compute resources need to be down
                        StaticNode(
                            "queue1-st-c5xlarge-1",
                            "nodeip",
                            "queue1-st-c5xlarge-1",
                            "IDLE+CLOUD",
                            "queue1",
                        ),  # Static powered down node belongs to ICE compute resources not set to down
                        DynamicNode(
                            "queue1-dy-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue"
                        ),  # Dynamic not powered down node,
                        DynamicNode(
                            "queue1-dy-c5xlarge-2",
                            "nodeip",
                            "nodehostname",
                            "COMPLETING+DRAIN",
                            "queue1",
                            "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes "
                            "[root@2023-01-31T21:24:55]",
                        ),  # unhealthy ice node
                        DynamicNode(
                            "queue1-dy-c5xlarge-3",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                        ),  # unhealthy ice node
                    ],
                    "c4xlarge": [
                        DynamicNode(
                            "queue1-dy-c4xlarge-1", "ip-1", "hostname", "DOWN", "queue1"
                        ),  # unhealthy not ice node
                    ],
                },
                "queue2": {
                    "c5xlarge": [
                        DynamicNode(
                            "queue2-dy-c5large-1",
                            "queue2-dy-c5large-1",
                            "queue2-dy-c5large-1",
                            "IDLE+CLOUD+POWERED_DOWN",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                        ),  # powered down ice node
                        DynamicNode(
                            "queue2-dy-c5large-2",
                            "queue2-dy-c5large-2",
                            "queue2-dy-c5large-2",
                            "IDLE+CLOUD+POWERED_DOWN",
                            "queue2",  # Dynamic node belongs to ICE compute resources need to be down
                        ),
                    ],
                    "c5large": [
                        DynamicNode(
                            "queue2-dy-c5large-3",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity "
                            "[root@2023-01-31T21:24:55]",
                        ),  # unhealthy ice node
                    ],
                },
            },
            {
                "queue1": {
                    "c5xlarge": [
                        DynamicNode(
                            "queue1-dy-c5xlarge-2",
                            "nodeip",
                            "nodehostname",
                            "COMPLETING+DRAIN",
                            "queue1",
                            "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes "
                            "[root@2023-01-31T21:24:55]",
                        ),  # unhealthy ice node
                        DynamicNode(
                            "queue1-dy-c5xlarge-3",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                        ),  # unhealthy ice node
                    ],
                },
                "queue2": {
                    "c5large": [
                        DynamicNode(
                            "queue2-dy-c5large-3",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity "
                            "[root@2023-01-31T21:24:55]",
                        ),  # unhealthy ice node
                    ],
                },
            },
            {
                "queue2": {
                    "c5large": ComputeResourceFailureEvent(
                        timestamp=datetime(2021, 1, 1, 0, 0), error_code="InsufficientHostCapacity"
                    ),
                },
            },
            {
                "queue1": {
                    "c5xlarge": ComputeResourceFailureEvent(
                        timestamp=datetime(2021, 1, 2, 0, 0), error_code="InsufficientReservedInstanceCapacity"
                    ),
                },
            },
            ["queue2-dy-c5large-3"],
            [
                call(
                    ["queue1-dy-c5xlarge-1"],
                    reason="(Code:InsufficientReservedInstanceCapacity)Temporarily disabling node due to insufficient "
                    "capacity",
                ),
            ],
        ),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_handle_ice_nodes(
    queue_compute_resource_nodes_map,
    ice_compute_resources_and_nodes_map,
    initial_insufficient_capacity_compute_resources,
    expected_insufficient_capacity_compute_resources,
    expected_power_save_node_list,
    expected_nodes_to_down,
    mocker,
    caplog,
):
    mock_sync_config = SimpleNamespace(
        protected_failure_count=10,
        insufficient_capacity_timeout=600,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    caplog.set_level(logging.INFO)
    cluster_manager = ClusterManager(mock_sync_config)
    cluster_manager._current_time = datetime(2021, 1, 2, 0, 0, 0)
    cluster_manager._insufficient_capacity_compute_resources = initial_insufficient_capacity_compute_resources
    power_save_mock = mocker.patch("slurm_plugin.clustermgtd.set_nodes_power_down", autospec=True)
    power_down_mock = mocker.patch("slurm_plugin.clustermgtd.set_nodes_down", autospec=True)
    # Run test
    cluster_manager._handle_ice_nodes(ice_compute_resources_and_nodes_map, queue_compute_resource_nodes_map)
    # Assert calls
    assert_that(cluster_manager._insufficient_capacity_compute_resources).is_equal_to(
        expected_insufficient_capacity_compute_resources
    )
    if expected_power_save_node_list:
        power_save_mock.assert_called_with(
            expected_power_save_node_list, reason="Enabling node since insufficient capacity timeout expired"
        )
    else:
        power_save_mock.assert_not_called()
    if expected_nodes_to_down:
        power_down_mock.assert_has_calls(expected_nodes_to_down)
    else:
        power_down_mock.assert_not_called()


@pytest.mark.parametrize(
    "ice_compute_resources_and_nodes_map, initial_insufficient_capacity_compute_resources, "
    "expected_insufficient_capacity_compute_resources",
    [
        (
            {
                "queue1": {
                    "c5xlarge": [
                        DynamicNode(
                            "queue1-dy-c5xlarge-1",
                            "nodeip",
                            "nodehostname",
                            "COMPLETING+DRAIN",
                            "queue1",
                            "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes "
                            "[root@2023-01-31T21:24:55]",
                        ),
                    ],
                },
                "queue2": {
                    "c5large": [
                        DynamicNode(
                            "queue2-dy-c5large-1",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                        ),
                        DynamicNode(
                            "queue2-dy-c5large-2",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity "
                            "[root@2023-01-31T21:24:55]",
                        ),
                    ],
                },
            },
            {},
            {
                "queue1": {
                    "c5xlarge": ComputeResourceFailureEvent(
                        timestamp=datetime(2020, 1, 2, 0, 0), error_code="InsufficientReservedInstanceCapacity"
                    ),
                },
                "queue2": {
                    "c5large": ComputeResourceFailureEvent(
                        timestamp=datetime(2020, 1, 2, 0, 0), error_code="InsufficientHostCapacity"
                    ),
                },
            },
        ),
        (
            {
                "queue1": {
                    "c5xlarge": [
                        DynamicNode(
                            "queue1-dy-c5xlarge-1",
                            "nodeip",
                            "nodehostname",
                            "COMPLETING+DRAIN",
                            "queue1",
                            "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes "
                            "[root@2023-01-31T21:24:55]",
                        ),
                    ],
                },
                "queue2": {
                    "c5large": [
                        DynamicNode(
                            "queue2-dy-c5large-1",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                        ),
                        DynamicNode(
                            "queue2-dy-c5large-2",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity "
                            "[root@2023-01-31T21:24:55]",
                        ),
                    ],
                },
            },
            {
                "queue1": {
                    "c5xlarge": ComputeResourceFailureEvent(
                        timestamp=datetime(2020, 1, 2, 0, 0), error_code="InsufficientReservedInstanceCapacity"
                    ),
                },
            },
            {
                "queue1": {
                    "c5xlarge": ComputeResourceFailureEvent(
                        timestamp=datetime(2020, 1, 2, 0, 0), error_code="InsufficientReservedInstanceCapacity"
                    ),
                },
                "queue2": {
                    "c5large": ComputeResourceFailureEvent(
                        timestamp=datetime(2020, 1, 2, 0, 0), error_code="InsufficientHostCapacity"
                    ),
                },
            },
        ),
        (
            {},
            {
                "queue1": {
                    "c5xlarge": ComputeResourceFailureEvent(
                        timestamp=datetime(2020, 1, 2, 0, 0), error_code="InsufficientReservedInstanceCapacity"
                    ),
                },
            },
            {
                "queue1": {
                    "c5xlarge": ComputeResourceFailureEvent(
                        timestamp=datetime(2020, 1, 2, 0, 0), error_code="InsufficientReservedInstanceCapacity"
                    ),
                },
            },
        ),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_update_insufficient_capacity_compute_resources(
    ice_compute_resources_and_nodes_map,
    initial_insufficient_capacity_compute_resources,
    expected_insufficient_capacity_compute_resources,
    mocker,
    caplog,
):
    caplog.set_level(logging.INFO)
    cluster_manager = ClusterManager(mocker.MagicMock())
    cluster_manager._current_time = datetime(2020, 1, 2, 0, 0, 0)
    cluster_manager._insufficient_capacity_compute_resources = initial_insufficient_capacity_compute_resources

    # Run test
    cluster_manager._update_insufficient_capacity_compute_resources(ice_compute_resources_and_nodes_map)
    # Assert calls
    assert_that(cluster_manager._insufficient_capacity_compute_resources).is_equal_to(
        expected_insufficient_capacity_compute_resources
    )


@pytest.mark.parametrize(
    "ice_compute_resources_and_nodes_map, initial_insufficient_capacity_compute_resources, "
    "expected_insufficient_capacity_compute_resources, expected_power_save_node_list",
    [
        (
            {
                "queue1": {
                    "c5xlarge": [
                        DynamicNode(
                            "queue1-dy-c5xlarge-1",
                            "nodeip",
                            "nodehostname",
                            "COMPLETING+DRAIN",
                            "queue1",
                            "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes "
                            "[root@2023-01-31T21:24:55]",
                        ),
                    ],
                },
                "queue2": {
                    "c5large": [
                        DynamicNode(
                            "queue2-dy-c5large-1",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                        ),
                        DynamicNode(
                            "queue2-dy-c5large-2",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity "
                            "[root@2023-01-31T21:24:55]",
                        ),
                    ],
                },
            },
            {
                "queue1": {
                    "c5xlarge": ComputeResourceFailureEvent(
                        timestamp=datetime(2021, 1, 2, 0, 0, 0), error_code="InsufficientReservedInstanceCapacity"
                    ),
                },
                "queue2": {
                    "c5large": ComputeResourceFailureEvent(
                        timestamp=datetime(2021, 1, 2, 0, 0, 0), error_code="InsufficientHostCapacity"
                    ),
                },
            },
            {
                "queue1": {
                    "c5xlarge": ComputeResourceFailureEvent(
                        timestamp=datetime(2021, 1, 2, 0, 0, 0), error_code="InsufficientReservedInstanceCapacity"
                    ),
                },
                "queue2": {
                    "c5large": ComputeResourceFailureEvent(
                        timestamp=datetime(2021, 1, 2, 0, 0, 0), error_code="InsufficientHostCapacity"
                    ),
                },
            },
            [],
        ),
        (
            {
                "queue1": {
                    "c5xlarge": [
                        DynamicNode(
                            "queue1-dy-c5xlarge-1",
                            "nodeip",
                            "nodehostname",
                            "COMPLETING+DRAIN",
                            "queue1",
                            "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes "
                            "[root@2023-01-31T21:24:55]",
                        ),
                    ],
                },
                "queue2": {
                    "c5large": [
                        DynamicNode(
                            "queue2-dy-c5large-1",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                        ),
                        DynamicNode(
                            "queue2-dy-c5large-2",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity "
                            "[root@2023-01-31T21:24:55]",
                        ),
                    ],
                },
                "queue3": {
                    "c5large": [
                        DynamicNode(
                            "queue3-dy-c5large-1",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                        ),
                        DynamicNode(
                            "queue3-dy-c5large-2",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity "
                            "[root@2023-01-31T21:24:55]",
                        ),
                    ],
                },
            },
            {
                "queue1": {
                    "c5xlarge": ComputeResourceFailureEvent(
                        timestamp=datetime(2021, 1, 2, 0, 0, 0), error_code="InsufficientReservedInstanceCapacity"
                    )
                },
                "queue2": {
                    "c5large": ComputeResourceFailureEvent(
                        timestamp=datetime(2021, 1, 1, 0, 0, 0), error_code="InsufficientHostCapacity"
                    ),
                },
                "queue3": {
                    "c5large": ComputeResourceFailureEvent(
                        timestamp=datetime(2021, 1, 1, 0, 0, 0), error_code="InsufficientHostCapacity"
                    ),
                },
            },
            {
                "queue1": {
                    "c5xlarge": ComputeResourceFailureEvent(
                        timestamp=datetime(2021, 1, 2, 0, 0, 0), error_code="InsufficientReservedInstanceCapacity"
                    ),
                },
            },
            ["queue2-dy-c5large-1", "queue2-dy-c5large-2", "queue3-dy-c5large-1", "queue3-dy-c5large-2"],
        ),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_reset_timeout_expired_compute_resources(
    ice_compute_resources_and_nodes_map,
    initial_insufficient_capacity_compute_resources,
    expected_insufficient_capacity_compute_resources,
    expected_power_save_node_list,
    mocker,
    caplog,
):
    caplog.set_level(logging.INFO)
    config = SimpleNamespace(
        insufficient_capacity_timeout=20,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(config)
    cluster_manager._current_time = datetime(2021, 1, 2, 0, 0, 0)
    cluster_manager._insufficient_capacity_compute_resources = initial_insufficient_capacity_compute_resources
    power_save_mock = mocker.patch("slurm_plugin.clustermgtd.set_nodes_power_down", autospec=True)

    # Run test
    cluster_manager._reset_timeout_expired_compute_resources(ice_compute_resources_and_nodes_map)
    # Assert calls
    assert_that(cluster_manager._insufficient_capacity_compute_resources).is_equal_to(
        expected_insufficient_capacity_compute_resources
    )
    if expected_power_save_node_list:
        power_save_mock.assert_called_with(
            expected_power_save_node_list, reason="Enabling node since insufficient capacity timeout expired"
        )
    else:
        power_save_mock.assert_not_called()


@pytest.mark.parametrize(
    "queue_compute_resource_nodes_map, insufficient_capacity_compute_resources, expected_nodes_to_down",
    [
        (
            {
                "queue1": {
                    "c5xlarge": [
                        DynamicNode(
                            "queue1-dy-c5xlarge-1",
                            "queue1-dy-c5xlarge-1",
                            "queue1-dy-c5xlarge-1",
                            "IDLE+CLOUD+POWERED_DOWN",
                            "queue1",
                            "some reason",
                        ),  # Dynamic node belongs to ICE compute resources need to be down
                        StaticNode(
                            "queue1-st-c5xlarge-1",
                            "nodeip",
                            "IDLE+CLOUD",
                            "somestate",
                            "queue1",
                        ),  # Static nodes
                        DynamicNode(
                            "queue1-dy-c5xlarge-3", "ip-3", "hostname", "IDLE+CLOUD", "queue"
                        ),  # Dynamic not powered down node],
                    ],
                },
                "queue2": {
                    "c5large": [
                        DynamicNode(
                            "queue2-dy-c5large-1",
                            "queue2-dy-c5large-1",
                            "queue2-dy-c5large-1",
                            "IDLE+CLOUD+POWERED_DOWN",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                        ),  # powered down ice node
                        DynamicNode(
                            "queue2-dy-c5large-2",
                            "queue2-dy-c5large-2",
                            "queue2-dy-c5large-2",
                            "IDLE+CLOUD+POWERED_DOWN",
                            "queue2",  # Dynamic node belongs to ICE compute resources need to be down
                        ),
                    ],
                },
            },
            {
                "queue1": {
                    "c5xlarge": ComputeResourceFailureEvent(
                        timestamp=datetime(2020, 1, 2, 0, 0), error_code="InsufficientReservedInstanceCapacity"
                    )
                },
                "queue2": {
                    "c5large": ComputeResourceFailureEvent(
                        timestamp=datetime(2020, 1, 2, 0, 0), error_code="InsufficientHostCapacity"
                    ),
                },
            },
            [
                call(
                    ["queue1-dy-c5xlarge-1"],
                    reason="(Code:InsufficientReservedInstanceCapacity)Temporarily disabling node due to insufficient "
                    "capacity",
                ),
                call(
                    ["queue2-dy-c5large-1", "queue2-dy-c5large-2"],
                    reason="(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity",
                ),
            ],
        ),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_set_ice_compute_resources_to_down(
    queue_compute_resource_nodes_map,
    insufficient_capacity_compute_resources,
    expected_nodes_to_down,
    mocker,
    caplog,
):
    caplog.set_level(logging.INFO)
    cluster_manager = ClusterManager(mocker.MagicMock())
    power_down_mock = mocker.patch("slurm_plugin.clustermgtd.set_nodes_down", autospec=True)
    cluster_manager._insufficient_capacity_compute_resources = insufficient_capacity_compute_resources

    # Run test
    cluster_manager._set_ice_compute_resources_to_down(queue_compute_resource_nodes_map)
    # Assert calls
    if expected_nodes_to_down:
        power_down_mock.assert_has_calls(expected_nodes_to_down)
    else:
        power_down_mock.assert_not_called()


@pytest.mark.parametrize(
    "active_nodes, expected_unhealthy_dynamic_nodes, expected_unhealthy_static_nodes, "
    "expected_ice_compute_resources_and_nodes_map, disable_nodes_on_insufficient_capacity",
    [
        (
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"
                ),  # powering_down
                StaticNode(
                    "queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue1"
                ),  # unhealthy static
                DynamicNode("queue-dy-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),  # unhealthy dynamic
                StaticNode(
                    "queue2-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"
                ),  # bootstrap failure static
                DynamicNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),  # bootstrap failure dynamic
                StaticNode(
                    "queue3-st-c5xlarge-1",
                    "ip-5",
                    "hostname",
                    "IDLE",
                    "queue2",
                    instance=EC2Instance("id-5", "ip-5", "hostname", "some_launch_time"),
                ),  # healthy static
                DynamicNode(
                    "queue3-dy-c5xlarge-1",
                    "ip-6",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue1",
                    instance=EC2Instance("id-6", "ip-6", "hostname", "some_launch_time"),
                ),  # healthy dynamic
                DynamicNode("queue1-dy-c4xlarge-1", "ip-1", "hostname", "DOWN", "queue1"),
                DynamicNode(
                    "queue1-dy-c5xlarge-3",
                    "nodeip",
                    "nodehostname",
                    "COMPLETING+DRAIN",
                    "queue1",
                    "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
                DynamicNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
                DynamicNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity "
                    "[root@2023-01-31T21:24:55]",
                ),
            ],
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"
                ),  # powering_down
                DynamicNode("queue-dy-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),
                DynamicNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),  # bootstrap failure dynamic
                DynamicNode("queue1-dy-c4xlarge-1", "ip-1", "hostname", "DOWN", "queue1"),
            ],
            [
                StaticNode("queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue1"),
                StaticNode(
                    "queue2-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"
                ),  # bootstrap failure static
            ],
            {
                "queue1": {
                    "c5xlarge": [
                        DynamicNode(
                            "queue1-dy-c5xlarge-3",
                            "nodeip",
                            "nodehostname",
                            "COMPLETING+DRAIN",
                            "queue1",
                            "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes "
                            "[root@2023-01-31T21:24:55]",
                        ),
                    ]
                },
                "queue2": {
                    "c5large": [
                        DynamicNode(
                            "queue2-dy-c5large-1",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                        ),
                        DynamicNode(
                            "queue2-dy-c5large-2",
                            "nodeip",
                            "nodehostname",
                            "DOWN+CLOUD",
                            "queue2",
                            "(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity "
                            "[root@2023-01-31T21:24:55]",
                        ),
                    ]
                },
            },
            True,
        ),
        (
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"
                ),  # powering_down
                StaticNode(
                    "queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue1"
                ),  # unhealthy static
                DynamicNode("queue-dy-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),  # unhealthy dynamic
                StaticNode(
                    "queue2-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"
                ),  # bootstrap failure static
                DynamicNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),  # bootstrap failure dynamic
                StaticNode(
                    "queue3-st-c5xlarge-1",
                    "ip-5",
                    "hostname",
                    "IDLE",
                    "queue2",
                    instance=EC2Instance("id-5", "ip-5", "hostname", "some_launch_time"),
                ),  # healthy static
                DynamicNode(
                    "queue3-dy-c5xlarge-1",
                    "ip-6",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue1",
                    instance=EC2Instance("id-6", "ip-6", "hostname", "some_launch_time"),
                ),  # healthy dynamic
                DynamicNode("queue1-dy-c4xlarge-1", "ip-1", "hostname", "DOWN", "queue1"),
                DynamicNode(
                    "queue1-dy-c5xlarge-3",
                    "nodeip",
                    "nodehostname",
                    "COMPLETING+DRAIN",
                    "queue1",
                    "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
                DynamicNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
                DynamicNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity "
                    "[root@2023-01-31T21:24:55]",
                ),
            ],
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"
                ),  # powering_down
                DynamicNode("queue-dy-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),
                DynamicNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),  # bootstrap failure dynamic
                DynamicNode("queue1-dy-c4xlarge-1", "ip-1", "hostname", "DOWN", "queue1"),
                DynamicNode(
                    "queue1-dy-c5xlarge-3",
                    "nodeip",
                    "nodehostname",
                    "COMPLETING+DRAIN",
                    "queue1",
                    "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
                DynamicNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                ),
                DynamicNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Temporarily disabling node due to insufficient capacity "
                    "[root@2023-01-31T21:24:55]",
                ),
            ],
            [
                StaticNode("queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue1"),
                StaticNode(
                    "queue2-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"
                ),  # bootstrap failure static
            ],
            {},
            False,
        ),
    ],
)
@pytest.mark.usefixtures(
    "initialize_instance_manager_mock", "initialize_executor_mock", "initialize_console_logger_mock"
)
def test_find_unhealthy_slurm_nodes(
    active_nodes,
    expected_unhealthy_dynamic_nodes,
    expected_unhealthy_static_nodes,
    expected_ice_compute_resources_and_nodes_map,
    disable_nodes_on_insufficient_capacity,
):
    mock_sync_config = SimpleNamespace(
        terminate_drain_nodes=True,
        terminate_down_nodes=True,
        disable_nodes_on_insufficient_capacity=disable_nodes_on_insufficient_capacity,
        cluster_name="cluster",
        head_node_instance_id="i-instance-id",
    )
    cluster_manager = ClusterManager(mock_sync_config)
    # Run test
    (
        unhealthy_dynamic_nodes,
        unhealthy_static_nodes,
        ice_compute_resources_and_nodes_map,
    ) = cluster_manager._find_unhealthy_slurm_nodes(active_nodes)
    # Assert calls
    assert_that(unhealthy_dynamic_nodes).is_equal_to(expected_unhealthy_dynamic_nodes)
    assert_that(unhealthy_static_nodes).is_equal_to(expected_unhealthy_static_nodes)
    assert_that(ice_compute_resources_and_nodes_map).is_equal_to(expected_ice_compute_resources_and_nodes_map)


@pytest.mark.parametrize(
    "partitions_name_map, expected_nodelist",
    [
        pytest.param(
            {
                "queue1": SimpleNamespace(
                    name="queue1",
                    nodenames="queue1-st-cr1-1,queue1-st-cr1-2",
                    state="UP",
                    slurm_nodes=[
                        StaticNode(name="queue1-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                        StaticNode(name="queue1-st-cr1-2", nodeaddr="", nodehostname="", state=""),
                    ],
                ),
                "queue2": SimpleNamespace(
                    name="queue2",
                    nodenames="queue2-st-cr1-1,queue2-st-cr1-2",
                    state="UP",
                    slurm_nodes=[
                        StaticNode(name="queue2-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                        StaticNode(name="queue2-st-cr1-2", nodeaddr="", nodehostname="", state=""),
                    ],
                ),
            },
            [
                StaticNode(name="queue1-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                StaticNode(name="queue1-st-cr1-2", nodeaddr="", nodehostname="", state=""),
                StaticNode(name="queue2-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                StaticNode(name="queue2-st-cr1-2", nodeaddr="", nodehostname="", state=""),
            ],
            id="Two non-overlapping partitions",
        ),
        pytest.param(
            {
                "queue1": SimpleNamespace(
                    name="queue1",
                    nodenames="queue1-st-cr1-1,queue1-st-cr1-2",
                    state="UP",
                    slurm_nodes=[
                        StaticNode(name="queue1-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                        StaticNode(name="queue1-st-cr1-2", nodeaddr="", nodehostname="", state=""),
                    ],
                ),
                "custom_partition": SimpleNamespace(
                    name="custom_partition",
                    nodenames="queue1-st-cr1-1,queue1-st-cr1-2",
                    state="UP",
                    slurm_nodes=[
                        StaticNode(name="queue1-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                        StaticNode(name="queue1-st-cr1-2", nodeaddr="", nodehostname="", state=""),
                    ],
                ),
            },
            [
                StaticNode(name="queue1-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                StaticNode(name="queue1-st-cr1-2", nodeaddr="", nodehostname="", state=""),
            ],
            id="Two overlapping partitions obtained by creating a custom partition that includes nodes from a "
            "PC-managed queue",
        ),
        pytest.param(
            {},
            [],
            id="Empty cluster with no partitions",
        ),
        pytest.param(
            {
                "queue1": SimpleNamespace(
                    name="queue1",
                    nodenames="queue1-st-cr1-1,queue1-st-cr1-2",
                    state="UP",
                    slurm_nodes=[
                        StaticNode(name="queue1-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                        StaticNode(name="queue1-st-cr1-2", nodeaddr="", nodehostname="", state=""),
                    ],
                ),
                "queue2": SimpleNamespace(
                    name="queue2",
                    nodenames="queue2-st-cr1-1,queue2-st-cr1-2",
                    state="INACTIVE",
                    slurm_nodes=[
                        StaticNode(name="queue2-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                        StaticNode(name="queue2-st-cr1-2", nodeaddr="", nodehostname="", state=""),
                    ],
                ),
            },
            [
                StaticNode(name="queue1-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                StaticNode(name="queue1-st-cr1-2", nodeaddr="", nodehostname="", state=""),
            ],
            id="Two non-overlapping partitions, one active and one inactive",
        ),
        pytest.param(
            {
                "queue1": SimpleNamespace(
                    name="queue1",
                    nodenames="queue1-st-cr1-1,queue1-st-cr1-2",
                    state="INACTIVE",
                    slurm_nodes=[
                        StaticNode(name="queue1-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                        StaticNode(name="queue1-st-cr1-2", nodeaddr="", nodehostname="", state=""),
                    ],
                ),
                "queue2": SimpleNamespace(
                    name="queue2",
                    nodenames="queue2-st-cr1-1,queue2-st-cr1-2",
                    state="INACTIVE",
                    slurm_nodes=[
                        StaticNode(name="queue2-st-cr1-1", nodeaddr="", nodehostname="", state=""),
                        StaticNode(name="queue2-st-cr1-2", nodeaddr="", nodehostname="", state=""),
                    ],
                ),
            },
            [],
            id="Two inactive non-overlapping partitions",
        ),
    ],
)
def test_find_active_nodes(partitions_name_map, expected_nodelist):
    """Unit test for the `ClusterManager._find_active_nodes()` method."""
    result_nodelist = ClusterManager._find_active_nodes(partitions_name_map)
    assert_that(result_nodelist).is_equal_to(expected_nodelist)
