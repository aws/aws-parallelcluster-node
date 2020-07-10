import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest
from assertpy import assert_that

import slurm_plugin
from common.schedulers.slurm_commands import SlurmNode, SlurmPartition
from slurm_plugin.clustermgtd import ClusterManager, ClustermgtdConfig
from slurm_plugin.common import EC2Instance, EC2InstanceHealthState, InstanceManager


@pytest.mark.parametrize(
    ("config_file", "expected_attributes"),
    [
        (
            "default.conf",
            {
                # basic configs
                "cluster_name": "hit",
                "region": "us-east-2",
                "_boto3_config": {"retries": {"max_attempts": 5, "mode": "standard"}},
                "loop_time": 30,
                "disable_all_cluster_management": False,
                "heartbeat_file_path": "/home/ec2-user/clustermgtd_heartbeat",
                "logging_config": os.path.join(
                    os.path.dirname(slurm_plugin.__file__), "logging", "parallelcluster_clustermgtd_logging.conf"
                ),
                # launch configs
                "update_node_address": True,
                "launch_max_batch_size": 100,
                # terminate configs
                "terminate_max_batch_size": 1000,
                "node_replacement_timeout": 600,
                "terminate_drain_nodes": True,
                "terminate_down_nodes": True,
                "orphaned_instance_timeout": 180,
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
                    "retries": {"max_attempts": 5, "mode": "standard"},
                    "proxies": {"https": "https://fake.proxy"},
                },
                "loop_time": 60,
                "disable_all_cluster_management": True,
                "heartbeat_file_path": "/home/ubuntu/clustermgtd_heartbeat",
                "logging_config": "/my/logging/config",
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
                    "retries": {"max_attempts": 5, "mode": "standard"},
                    "proxies": {"https": "https://fake.proxy"},
                },
                "loop_time": 60,
                "disable_all_cluster_management": True,
                "heartbeat_file_path": "/home/ubuntu/clustermgtd_heartbeat",
                "logging_config": "/my/logging/config",
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
def test_clustermgtd_config(config_file, expected_attributes, test_datadir):
    sync_config = ClustermgtdConfig(test_datadir / config_file)
    for key in expected_attributes:
        assert_that(sync_config.__dict__.get(key)).is_equal_to(expected_attributes.get(key))


@pytest.mark.parametrize(
    "partitions, get_nodes_side_effect, expected_inactive_nodes, expected_active_nodes",
    [
        (
            [
                SlurmPartition("partition1", "placeholder_nodes", "UP"),
                SlurmPartition("partition2", "placeholder_nodes", "INACTIVE"),
                SlurmPartition("partition3", "placeholder_nodes", "DRAIN"),
            ],
            [
                [
                    SlurmNode("node1", "nodeaddr", "nodeaddr", "DOWN"),
                    SlurmNode("node2", "nodeaddr", "nodeaddr", "IDLE"),
                ],
                [
                    SlurmNode("node3", "nodeaddr", "nodeaddr", "IDLE"),
                    SlurmNode("node4", "nodeaddr", "nodeaddr", "IDLE"),
                ],
                [SlurmNode("node5", "nodeaddr", "nodeaddr", "DRAIN")],
            ],
            [SlurmNode("node3", "nodeaddr", "nodeaddr", "IDLE"), SlurmNode("node4", "nodeaddr", "nodeaddr", "IDLE")],
            [
                SlurmNode("node1", "nodeaddr", "nodeaddr", "DOWN"),
                SlurmNode("node2", "nodeaddr", "nodeaddr", "IDLE"),
                SlurmNode("node5", "nodeaddr", "nodeaddr", "DRAIN"),
            ],
        ),
    ],
    ids=["mixed"],
)
def test_get_node_info_from_partition(
    partitions, get_nodes_side_effect, expected_inactive_nodes, expected_active_nodes, mocker
):
    mocker.patch("slurm_plugin.clustermgtd.get_partition_info", return_value=partitions, autospec=True)
    mocker.patch("slurm_plugin.clustermgtd.get_nodes_info", side_effect=get_nodes_side_effect, autospec=True)
    cluster_manager = ClusterManager()
    active_nodes, inactive_nodes = cluster_manager._get_node_info_from_partition()
    assert_that(active_nodes).is_equal_to(expected_active_nodes)
    assert_that(inactive_nodes).is_equal_to(expected_inactive_nodes)


def test_clean_up_inactive_parititon():
    # Test setup
    inactive_nodes = ["some inactive nodes"]
    mock_sync_config = SimpleNamespace(
        terminate_max_batch_size=4, instance_manager=InstanceManager("region", "cluster_name", "boto3_config"),
    )
    cluster_manager = ClusterManager()
    cluster_manager.set_sync_config(mock_sync_config)
    cluster_manager.sync_config.instance_manager.terminate_associated_instances = MagicMock()
    cluster_manager._clean_up_inactive_partition(inactive_nodes)
    cluster_manager.sync_config.instance_manager.terminate_associated_instances.assert_called_with(
        ["some inactive nodes"], terminate_batch_size=4
    )


def test_get_ec2_states():
    # Test setup
    cluster_manager = ClusterManager()
    mock_sync_config = SimpleNamespace(instance_manager=InstanceManager("region", "cluster_name", "boto3_config"),)
    cluster_manager.set_sync_config(mock_sync_config)
    cluster_manager.sync_config.instance_manager.get_cluster_instances = MagicMock()
    # Run test
    cluster_manager._get_ec2_states()
    # Assert calls
    cluster_manager.sync_config.instance_manager.get_cluster_instances.assert_called_with(
        include_master=False, alive_states_only=True
    )


@pytest.mark.parametrize(
    "disable_ec2_health_check, disable_scheduled_event_health_check, expected_handle_health_check_calls",
    [
        (
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
        (True, True, []),
        (
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
    ],
)
def test_perform_health_check_actions(
    disable_ec2_health_check, disable_scheduled_event_health_check, expected_handle_health_check_calls
):
    mock_instance_health_states = ["some_instance_health_states"]
    mock_cluster_instances = [
        EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
        EC2Instance("id-2", "ip-2", "hostname", "launch_time"),
    ]
    slurm_nodes_ip_phonebook = {"ip-1": "some_slurm_node1", "ip-2": "some_slurm_node2"}
    mock_sync_config = SimpleNamespace(
        disable_ec2_health_check=disable_ec2_health_check,
        disable_scheduled_event_health_check=disable_scheduled_event_health_check,
        instance_manager=InstanceManager("region", "cluster_name", "boto3_config"),
    )
    # Mock functions
    cluster_manager = ClusterManager()
    cluster_manager.set_sync_config(mock_sync_config)
    cluster_manager.sync_config.instance_manager.get_instance_health_states = MagicMock(
        return_value=mock_instance_health_states
    )
    cluster_manager._handle_health_check = MagicMock()
    # Run test
    cluster_manager._perform_health_check_actions(mock_cluster_instances, slurm_nodes_ip_phonebook)
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
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 0)}], "Status": "not-applicable"},
                None,
            ),
            datetime(2020, 1, 1, 0, 0, 30),
            True,
        ),
        (
            EC2InstanceHealthState(
                "id-12345",
                "stopped",
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 0)}], "Status": "initializing"},
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
                {"Details": [{"ImpairedSince": datetime(2020, 1, 1, 0, 0, 0)}], "Status": "initializing"},
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
    mock_sync_config = SimpleNamespace(health_check_timeout=30)
    cluster_manager = ClusterManager()
    cluster_manager.set_current_time(current_time)
    cluster_manager.set_sync_config(mock_sync_config)
    assert_that(cluster_manager._fail_ec2_health_check(instance_health_state)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "instance_health_state, current_time, expected_result",
    [
        (
            EC2InstanceHealthState(
                "id-12345", "running", {"Details": [{}], "Status": "ok"}, {"Details": [{}], "Status": "ok"}, [],
            ),
            datetime(2020, 1, 1, 0, 0, 30),
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
            datetime(2020, 1, 1, 0, 0, 29),
            True,
        ),
    ],
    ids=["no_event", "has_event"],
)
def test_fail_scheduled_events_health_check(instance_health_state, current_time, expected_result):
    mock_sync_config = SimpleNamespace(health_check_timeout=30)
    cluster_manager = ClusterManager()
    cluster_manager.set_current_time(current_time)
    cluster_manager.set_sync_config(mock_sync_config)
    assert_that(cluster_manager._fail_scheduled_events_check(instance_health_state)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "health_check_type, mock_fail_ec2_side_effect, mock_fail_scheduled_events_side_effect, expected_failed_nodes",
    [
        (ClusterManager.HealthCheckTypes.scheduled_event, [True, False], [False, True], ["nodename-2"]),
        (ClusterManager.HealthCheckTypes.ec2_health, [True, False], [False, True], ["nodename-1"]),
        (ClusterManager.HealthCheckTypes.ec2_health, [False, False], [False, True], []),
    ],
    ids=["scheduled_event", "ec2_health", "all_healthy"],
)
def test_handle_health_check(
    health_check_type, mock_fail_ec2_side_effect, mock_fail_scheduled_events_side_effect, expected_failed_nodes, mocker
):
    placeholder_states = [
        EC2InstanceHealthState("id-1", "some_state", "some_status", "some_status", "some_event"),
        EC2InstanceHealthState("id-2", "some_state", "some_status", "some_status", "some_event"),
    ]
    instances_id_phonebook = {
        "id-1": EC2Instance("id-1", "ip-1", "host-1", "some_launch_time"),
        "id-2": EC2Instance("id-2", "ip-2", "host-2", "some_launch_time"),
    }
    slurm_nodes_ip_phonebook = {
        "ip-1": SlurmNode("nodename-1", "ip-1", "host-1", "some_states"),
        "ip-2": SlurmNode("nodename-2", "ip-2", "host-2", "some_states"),
    }
    cluster_manager = ClusterManager()
    cluster_manager._fail_ec2_health_check = MagicMock(side_effect=mock_fail_ec2_side_effect)
    cluster_manager._fail_scheduled_events_check = MagicMock(side_effect=mock_fail_scheduled_events_side_effect)
    drain_node_mock = mocker.patch("slurm_plugin.clustermgtd.set_nodes_drain", autospec=True)
    cluster_manager._handle_health_check(
        placeholder_states, instances_id_phonebook, slurm_nodes_ip_phonebook, health_check_type
    )
    if expected_failed_nodes:
        drain_node_mock.assert_called_with(expected_failed_nodes, reason=f"Node failing {health_check_type}")
    else:
        drain_node_mock.assert_not_called()


@pytest.mark.parametrize(
    "current_replacing_nodes, slurm_nodes, expected_replacing_nodes",
    [
        (
            {"node-1", "node-2"},
            [
                SlurmNode("node-1", "ip", "hostname", "IDLE+CLOUD"),
                SlurmNode("node-2", "ip", "hostname", "DOWN+CLOUD"),
                SlurmNode("node-3", "ip", "hostname", "IDLE+CLOUD"),
            ],
            {"node-2"},
        )
    ],
    ids=["mixed"],
)
def test_update_replacing_nodes(current_replacing_nodes, slurm_nodes, expected_replacing_nodes):
    cluster_manager = ClusterManager()
    cluster_manager._set_replacing_nodes(current_replacing_nodes)
    cluster_manager._update_replacing_nodes(slurm_nodes)
    assert_that(cluster_manager.replacing_nodes).is_equal_to(expected_replacing_nodes)


@pytest.mark.parametrize(
    "current_replacing_nodes, node, instances_ip_phonebook, current_time, expected_result",
    [
        (
            set(),
            SlurmNode("node-1", "ip-1", "hostname", "IDLE+CLOUD"),
            {"ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0))},
            datetime(2020, 1, 1, 0, 0, 29),
            False,
        ),
        ({"node-1"}, SlurmNode("node-1", "ip-1", "hostname", "IDLE+CLOUD"), {}, datetime(2020, 1, 1, 0, 0, 29), False,),
        (
            {"node-1"},
            SlurmNode("node-1", "ip-1", "hostname", "DOWN+CLOUD"),
            {"ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0))},
            datetime(2020, 1, 1, 0, 0, 29),
            True,
        ),
        (
            {"node-1"},
            SlurmNode("node-1", "ip-1", "hostname", "IDLE+CLOUD"),
            {"ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0))},
            datetime(2020, 1, 1, 0, 0, 30),
            False,
        ),
    ],
    ids=["not_in_replacement", "no-backing-instance", "in_replacement", "timeout"],
)
def test_is_node_being_replaced(current_replacing_nodes, node, instances_ip_phonebook, current_time, expected_result):
    mock_sync_config = SimpleNamespace(node_replacement_timeout=30)
    cluster_manager = ClusterManager()
    cluster_manager.set_sync_config(mock_sync_config)
    cluster_manager.set_current_time(current_time)
    cluster_manager._set_replacing_nodes(current_replacing_nodes)
    assert_that(cluster_manager._is_node_being_replaced(node, instances_ip_phonebook)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, instances_ips_in_cluster, expected_result",
    [
        (SlurmNode("node-static-c5.xlarge-1", "node-static-c5.xlarge-1", "hostname", "IDLE+CLOUD"), ["ip-1"], False,),
        (SlurmNode("node-static-c5.xlarge-1", "ip-1", "hostname", "IDLE+CLOUD"), ["ip-2"], False,),
        (
            SlurmNode("node-dynamic-c5.xlarge-1", "node-dynamic-c5.xlarge-1", "hostname", "IDLE+CLOUD+POWER"),
            ["ip-2"],
            True,
        ),
        (SlurmNode("node-dynamic-c5.xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+POWER"), ["ip-2"], False,),
        (SlurmNode("node-static-c5.xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+POWER"), ["ip-1"], True,),
    ],
    ids=["static_addr_not_set", "static_no_backing", "dynamic_power_save", "dynamic_no_backing", "static_valid"],
)
def test_is_node_addr_valid(node, instances_ips_in_cluster, expected_result):
    assert_that(ClusterManager._is_node_addr_valid(node, instances_ips_in_cluster)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, mock_sync_config, mock_is_node_being_replaced, expected_result",
    [
        (
            SlurmNode("node-1", "some_ip", "hostname", "MIXED+CLOUD"),
            SimpleNamespace(terminate_drain_nodes=True, terminate_down_nodes=True),
            None,
            True,
        ),
        (
            SlurmNode("node-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN"),
            SimpleNamespace(terminate_drain_nodes=True, terminate_down_nodes=True),
            False,
            False,
        ),
        (
            SlurmNode("node-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN"),
            SimpleNamespace(terminate_drain_nodes=True, terminate_down_nodes=True),
            True,
            True,
        ),
        (
            SlurmNode("node-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN"),
            SimpleNamespace(terminate_drain_nodes=False, terminate_down_nodes=True),
            False,
            True,
        ),
        (
            SlurmNode("node-1", "some_ip", "hostname", "DOWN+CLOUD"),
            SimpleNamespace(terminate_drain_nodes=True, terminate_down_nodes=True),
            False,
            False,
        ),
        (
            SlurmNode("node-1", "some_ip", "hostname", "DOWN+CLOUD"),
            SimpleNamespace(terminate_drain_nodes=True, terminate_down_nodes=True),
            True,
            True,
        ),
        (
            SlurmNode("node-1", "some_ip", "hostname", "DOWN+CLOUD"),
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
def test_is_node_state_healthy(node, mock_sync_config, mock_is_node_being_replaced, expected_result):
    cluster_manager = ClusterManager()
    cluster_manager.set_sync_config(mock_sync_config)
    cluster_manager._is_node_being_replaced = MagicMock(return_value=mock_is_node_being_replaced)
    assert_that(
        cluster_manager._is_node_state_healthy(node, instances_ip_phonebook={"placeholder phonebook"})
    ).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, instances_ip_phonebook, instance_ips_in_cluster",
    [
        (
            SlurmNode("node-1", "ip-1", "hostname", "IDLE+CLOUD"),
            {
                "ip-1": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
                "ip-2": EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            },
            ["ip-1", "ip-2"],
        )
    ],
    ids=["basic"],
)
def test_is_node_healthy(node, instances_ip_phonebook, instance_ips_in_cluster):
    cluster_manager = ClusterManager()
    cluster_manager._is_node_state_healthy = MagicMock()
    ClusterManager._is_node_addr_valid = MagicMock()
    cluster_manager._is_node_healthy(node, instances_ip_phonebook)
    cluster_manager._is_node_state_healthy.assert_called_with(node, instances_ip_phonebook)
    ClusterManager._is_node_addr_valid.assert_called_with(node, instance_ips_in_cluster=instance_ips_in_cluster)


@pytest.mark.parametrize(
    "unhealthy_dynamic_nodes, expected_power_save_node_list",
    [
        (
            [
                SlurmNode("node-1", "ip-1", "hostname", "IDLE+CLOUD"),
                SlurmNode("node-2", "ip-1", "hostname", "IDLE+CLOUD"),
            ],
            ["node-1", "node-2"],
        )
    ],
    ids=["basic"],
)
def test_handle_unhealthy_dynamic_nodes(unhealthy_dynamic_nodes, expected_power_save_node_list, mocker):
    power_save_mock = mocker.patch(
        "slurm_plugin.clustermgtd.set_nodes_down_and_power_save", return_value=None, autospec=True
    )
    ClusterManager._handle_unhealthy_dynamic_nodes(unhealthy_dynamic_nodes)
    power_save_mock.assert_called_with(expected_power_save_node_list, reason="Schduler health check failed")


@pytest.mark.parametrize(
    "current_replacing_nodes, unhealthy_static_nodes, expected_replacing_nodes, applicable_node_list",
    [
        (
            {"some_current_node"},
            [
                SlurmNode("node-1", "ip-1", "hostname", "IDLE+CLOUD"),
                SlurmNode("node-2", "ip-1", "hostname", "IDLE+CLOUD"),
            ],
            {"some_current_node", "node-1", "node-2"},
            ["node-1", "node-2"],
        )
    ],
    ids=["basic"],
)
def test_handle_unhealthy_static_nodes(
    current_replacing_nodes, unhealthy_static_nodes, expected_replacing_nodes, applicable_node_list, mocker
):
    # Test setup
    mock_sync_config = SimpleNamespace(
        terminate_max_batch_size=1,
        launch_max_batch_size=5,
        update_node_address=False,
        instance_manager=InstanceManager("region", "cluster_name", "boto3_config"),
    )
    cluster_manager = ClusterManager()
    cluster_manager.set_sync_config(mock_sync_config)
    cluster_manager._set_replacing_nodes(current_replacing_nodes)
    # Mock associated function
    cluster_manager.sync_config.instance_manager.terminate_associated_instances = MagicMock()
    cluster_manager.sync_config.instance_manager.add_instances_for_nodes = MagicMock()
    update_mock = mocker.patch("slurm_plugin.clustermgtd.set_nodes_down", return_value=None, autospec=True)
    # Run test
    cluster_manager._handle_unhealthy_static_nodes(unhealthy_static_nodes)
    # Assert calls
    update_mock.assert_called_with(
        applicable_node_list, reason="Static node maintenance: unhealthy node is being replaced"
    )
    cluster_manager.sync_config.instance_manager.terminate_associated_instances.assert_called_with(
        unhealthy_static_nodes, terminate_batch_size=1
    )
    cluster_manager.sync_config.instance_manager.add_instances_for_nodes.assert_called_with(
        applicable_node_list, 5, False
    )
    assert_that(cluster_manager.replacing_nodes).is_equal_to(expected_replacing_nodes)


@pytest.mark.parametrize(
    "cluster_instances, active_nodes, mock_unhealthy_nodes",
    [
        (
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [
                SlurmNode("node-1", "ip-1", "hostname", "some_state"),
                SlurmNode("node-2", "ip-2", "hostname", "some_state"),
            ],
            (["node-1"], ["node-2"]),
        ),
        (
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [
                SlurmNode("node-1", "ip-1", "hostname", "some_state"),
                SlurmNode("node-1-repetitive-ip", "ip-1", "hostname", "some_state"),
                SlurmNode("node-2", "ip-2", "hostname", "some_state"),
            ],
            (["node-1", "node-1-repetitive-ip"], ["node-2"]),
        ),
    ],
    ids=["basic", "repetitive_ip"],
)
def test_maintain_nodes(cluster_instances, active_nodes, mock_unhealthy_nodes):
    # Mock functions
    mock_instances_ip_phonebook = {instance.private_ip: instance for instance in cluster_instances}
    cluster_manager = ClusterManager()
    cluster_manager._update_replacing_nodes = MagicMock()
    cluster_manager._find_unhealthy_slurm_nodes = MagicMock(return_value=mock_unhealthy_nodes)
    ClusterManager._handle_unhealthy_dynamic_nodes = MagicMock()
    cluster_manager._handle_unhealthy_static_nodes = MagicMock()
    # Run test
    cluster_manager._maintain_nodes(cluster_instances, active_nodes)
    # Check function calls
    cluster_manager._update_replacing_nodes.assert_called_with(active_nodes)
    cluster_manager._find_unhealthy_slurm_nodes.assert_called_with(active_nodes, mock_instances_ip_phonebook)
    ClusterManager._handle_unhealthy_dynamic_nodes(mock_unhealthy_nodes[0])
    cluster_manager._handle_unhealthy_static_nodes(mock_unhealthy_nodes[1])


@pytest.mark.parametrize(
    "cluster_instances, instances_ip_phonebook, current_time, expected_instance_to_terminate",
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
def test_terminate_orphaned_instances(
    cluster_instances, instances_ip_phonebook, current_time, expected_instance_to_terminate
):
    # Mock functions
    cluster_manager = ClusterManager()
    mock_sync_config = SimpleNamespace(
        orphaned_instance_timeout=30,
        terminate_max_batch_size=4,
        instance_manager=InstanceManager("region", "cluster_name", "boto3_config"),
    )
    cluster_manager.set_sync_config(mock_sync_config)
    cluster_manager.set_current_time(current_time)
    cluster_manager.sync_config.instance_manager.delete_instances = MagicMock()
    # Run test
    cluster_manager._terminate_orphaned_instances(cluster_instances, instances_ip_phonebook)
    # Check function calls
    if expected_instance_to_terminate:
        cluster_manager.sync_config.instance_manager.delete_instances.assert_called_with(
            expected_instance_to_terminate, terminate_batch_size=4
        )


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
    assert_that(ClusterManager._time_is_up(initial_time, current_time, grace_time)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "disable_cluster_management, disable_health_check, mock_cluster_instances, mock_active_nodes, mock_inactive_nodes",
    [
        (
            False,
            False,
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [
                SlurmNode("some_active_node1", "ip", "hostname", "some_state"),
                SlurmNode("some_active_node2", "ip", "hostname", "some_state"),
            ],
            [],
        ),
        (
            True,
            False,
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [
                SlurmNode("some_active_node1", "ip", "hostname", "some_state"),
                SlurmNode("some_active_node2", "ip", "hostname", "some_state"),
            ],
            [],
        ),
        (
            False,
            True,
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [
                SlurmNode("some_active_node1", "ip", "hostname", "some_state"),
                SlurmNode("some_active_node2", "ip", "hostname", "some_state"),
            ],
            [],
        ),
        (
            False,
            True,
            [EC2Instance("id-1", "ip-1", "hostname", "launch_time")],
            [],
            [
                SlurmNode("some_inactive_node1", "ip", "hostname", "some_state"),
                SlurmNode("some_inactive_node2", "ip", "hostname", "some_state"),
            ],
        ),
        (False, True, [EC2Instance("id-1", "ip-1", "hostname", "launch_time")], [], [],),
    ],
    ids=["all_enabled", "disable_all", "disable_health_check", "no_active", "no_node"],
)
def test_manage_cluster(
    disable_cluster_management, disable_health_check, mock_cluster_instances, mock_active_nodes, mock_inactive_nodes
):
    mock_sync_config = SimpleNamespace(
        disable_all_cluster_management=disable_cluster_management, disable_all_health_checks=disable_health_check
    )
    slurm_nodes_ip_phonebook = {node.nodeaddr: node for node in mock_active_nodes}
    current_time = datetime(2020, 1, 1, 0, 0, 0)
    cluster_manager = ClusterManager()
    # Set up function mocks
    cluster_manager.set_sync_config = MagicMock()
    cluster_manager.set_current_time = MagicMock()
    cluster_manager._write_timestamp_to_file = MagicMock()
    cluster_manager._perform_health_check_actions = MagicMock()
    cluster_manager._clean_up_inactive_partition = MagicMock()
    cluster_manager._terminate_orphaned_instances = MagicMock()
    cluster_manager._maintain_nodes = MagicMock()
    ClusterManager._get_node_info_from_partition = MagicMock()
    cluster_manager._get_ec2_states = MagicMock()
    ClusterManager._get_node_info_from_partition = MagicMock(return_value=(mock_active_nodes, mock_inactive_nodes))
    cluster_manager._get_ec2_states = MagicMock(return_value=mock_cluster_instances)
    # Run test
    cluster_manager.manage_cluster(mock_sync_config, current_time)
    # Assert function calls
    cluster_manager.set_sync_config.assert_called_with(mock_sync_config)
    cluster_manager.set_current_time.assert_called_with(current_time)
    cluster_manager._write_timestamp_to_file.assert_called()
    if disable_cluster_management:
        cluster_manager._perform_health_check_actions.assert_not_called()
        cluster_manager._clean_up_inactive_partition.assert_not_called()
        cluster_manager._terminate_orphaned_instances.assert_not_called()
        cluster_manager._maintain_nodes.assert_not_called()
        ClusterManager._get_node_info_from_partition.assert_not_called()
        cluster_manager._get_ec2_states.assert_not_called()
        return
    if mock_inactive_nodes:
        cluster_manager._clean_up_inactive_partition.assert_called_with(mock_inactive_nodes)
    cluster_manager._get_ec2_states.assert_called()
    if not mock_active_nodes:
        cluster_manager._terminate_orphaned_instances.assert_called_with(mock_cluster_instances, ips_used_by_slurm=[])
        cluster_manager._perform_health_check_actions.assert_not_called()
        cluster_manager._maintain_nodes.assert_not_called()
        return
    if disable_health_check:
        cluster_manager._perform_health_check_actions.assert_not_called()
    else:
        cluster_manager._perform_health_check_actions.assert_called_with(
            mock_cluster_instances, slurm_nodes_ip_phonebook
        )
    cluster_manager._maintain_nodes.assert_called_with(mock_cluster_instances, mock_active_nodes)
    cluster_manager._terminate_orphaned_instances.assert_called_with(
        mock_cluster_instances, ips_used_by_slurm=list(slurm_nodes_ip_phonebook.keys())
    )
