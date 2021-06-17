# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
import logging
from datetime import datetime

import pytest
from assertpy import assert_that
from slurm_plugin.slurm_resources import DynamicNode, EC2Instance, EC2InstanceHealthState, SlurmPartition, StaticNode


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (DynamicNode("queue-name-st-t2micro-1", "nodeip", "nodehostname", "somestate", "queue-name"), True),
        (
            DynamicNode("queuename-dy-t2micro-1", "queuename-dy-t2micro-1", "nodehostname", "somestate", "queuename"),
            False,
        ),
    ],
)
def test_slurm_node_is_nodeaddr_set(node, expected_output):
    assert_that(node.is_nodeaddr_set()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "somestate", "queue1"), False),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DRAIN", "queue1"), True),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DRAIN", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "COMPLETING+DRAIN", "queue1"), True),
    ],
)
def test_slurm_node_has_job(node, expected_output):
    assert_that(node.has_job()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "somestate", "queue1"), False),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DRAIN", "queue1"), False),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DRAIN", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE*+CLOUD+DRAIN", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+DRAIN", "queue1"), True),
    ],
)
def test_slurm_node_is_drained(node, expected_output):
    assert_that(node.is_drained()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "somestate", "queue1"), False),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DOWN", "queue1"), True),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DRAIN", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN*+CLOUD", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+POWER", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE~+CLOUD+POWERING_DOWN", "queue1"), False),
    ],
)
def test_slurm_node_is_down(node, expected_output):
    assert_that(node.is_down()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWER", "queue1"), True),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DRAIN", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DOWN", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE#+CLOUD", "queue1"), True),
    ],
)
def test_slurm_node_is_up(node, expected_output):
    assert_that(node.is_up()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWER", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD", "queue1"), True),
    ],
)
def test_slurm_node_is_powering_up(node, expected_output):
    assert_that(node.is_powering_up()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD", "queue1"), True),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD", "queue1"), True),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED+CLOUD", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "COMPLETING+CLOUD", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE*+CLOUD+DRAIN", "queue1"), False),
    ],
)
def test_slurm_node_is_online(node, expected_output):
    assert_that(node.is_online()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD", "queue1"), True),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE#+CLOUD", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED+CLOUD", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "COMPLETING+CLOUD", "queue1"), False),
    ],
)
def test_slurm_node_is_configuring_job(node, expected_output):
    assert_that(node.is_configuring_job()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD", "queue1"), False),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DOWN", "queue1"), False),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDEL#+CLOUD", "queue1"), False),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED+CLOUD", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "COMPLETING+CLOUD", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+POWER", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED*+CLOUD", "queue1"), True),
    ],
)
def test_slurm_node_is_running_job(node, expected_output):
    assert_that(node.is_running_job()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+POWER", "queue1"), True),
        (DynamicNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD", "queue1"), False),
    ],
)
def test_slurm_node_is_power_with_job(node, expected_output):
    assert_that(node.is_power_with_job()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "nodes, expected_output",
    [
        (
            [
                StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD", "queue1"),
                StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD", "queue1"),
            ],
            True,
        ),
        (
            [
                DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+POWER", "queue1"),
                StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DOWN", "queue1"),
            ],
            False,
        ),
        ([DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED*+CLOUD", "queue1")], True),
    ],
)
def test_partition_is_inactive(nodes, expected_output):
    partition = SlurmPartition("name", "nodenames", "state")
    partition.slurm_nodes = nodes
    assert_that(partition.has_running_job()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, terminate_drain_nodes, terminate_down_nodes, mock_is_node_being_replaced, expected_result",
    [
        (
            DynamicNode("queue-dy-c5xlarge-1", "some_ip", "hostname", "MIXED+CLOUD", "queue"),
            True,
            True,
            False,
            True,
        ),
        (
            StaticNode("queue-st-c5xlarge-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN", "queue"),
            True,
            True,
            False,
            False,
        ),
        (
            StaticNode("queue-st-c5xlarge-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN", "queue"),
            True,
            True,
            True,
            True,
        ),
        (
            DynamicNode("queue-dy-c5xlarge-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN", "queue"),
            False,
            True,
            False,
            True,
        ),
        (
            StaticNode("queue-st-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD", "queue"),
            True,
            True,
            False,
            False,
        ),
        (
            StaticNode("queue-st-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD", "queue"),
            True,
            True,
            True,
            True,
        ),
        (
            DynamicNode("queue-dy-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD", "queue"),
            True,
            False,
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
def test_slurm_node_is_state_healthy(
    node, mock_is_node_being_replaced, terminate_drain_nodes, terminate_down_nodes, expected_result, mocker
):
    node._is_being_replaced = mock_is_node_being_replaced
    assert_that(node.is_state_healthy(terminate_drain_nodes, terminate_down_nodes)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, instance, is_static_nodes_in_replacement, is_replacement_timeout, bootstrap_failure_messages, "
    "is_failing_health_check, is_node_bootstrap_failure",
    [
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN*+CLOUD", "queue1"),
            None,
            True,
            False,
            "Node bootstrap error: Node queue1-st-c5xlarge-1(ip-1) is currently in replacement and no backing instance",
            False,
            True,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN*+CLOUD", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            True,
            True,
            "Node bootstrap error: Replacement timeout expires for node queue1-st-c5xlarge-1(ip-1) in replacement",
            False,
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED#+CLOUD", "queue1"),
            None,
            False,
            False,
            "Node bootstrap error: Node queue1-dy-c5xlarge-1(ip-1) is in power up state without valid backing instance",
            False,
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "DOWN*+CLOUD+POWER", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
            False,
            False,
            "Node bootstrap error: Resume timeout expires",
            False,
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "hostname", "DOWN#+CLOUD", "queue1"),
            None,
            False,
            False,
            None,
            False,
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "queue1-dy-c5xlarge-1", "hostname", "DOWN*+CLOUD", "queue1"),
            None,
            False,
            False,
            None,
            False,
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN*+CLOUD", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            False,
            False,
            None,
            False,
            False,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "hostname", "DOWN+CLOUD+POWER", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
            False,
            False,
            None,
            False,
            False,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "DRAIN+CLOUD", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
            False,
            False,
            None,
            False,
            False,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+POWERING_DOWN", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
            False,
            False,
            None,
            False,
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+POWERING_DOWN", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
            True,
            False,
            "failed during bootstrap when performing health check",
            True,
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED#+CLOUD", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
            False,
            False,
            "failed during bootstrap when performing health check",
            True,
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "hostname", "DOWN*+CLOUD+POWER", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", "launch_time"),
            False,
            False,
            None,
            False,
            False,
        ),
    ],
    ids=[
        "static_self_terminate",
        "static_timeout",
        "dynamic_self_terminate",
        "dynamic_timeout",
        "dynamic_runinstance",
        "static_runinstance",
        "static_joined_cluster",
        "dynamic_reset_incorrect",
        "normal_down_1",
        "normal_down_2",
        "static_fail_health_check",
        "dynamic_fail_health_check",
        "dynamic_pcluster_stop",
    ],
)
def test_slurm_node_is_bootstrap_failure(
    node,
    is_static_nodes_in_replacement,
    is_replacement_timeout,
    bootstrap_failure_messages,
    is_node_bootstrap_failure,
    instance,
    is_failing_health_check,
    caplog,
):
    node.instance = instance
    node.is_static_nodes_in_replacement = is_static_nodes_in_replacement
    node._is_replacement_timeout = is_replacement_timeout
    node.is_failing_health_check = is_failing_health_check
    caplog.set_level(logging.WARNING)
    # Run tests and assert calls
    assert_that(node.is_bootstrap_failure()).is_equal_to(is_node_bootstrap_failure)
    if bootstrap_failure_messages:
        assert_that(caplog.text).contains(bootstrap_failure_messages)


@pytest.mark.parametrize(
    "node, instance, expected_result",
    [
        (
            DynamicNode("queue-dy-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue"),
            EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            True,
        ),
        (
            StaticNode("queue-st-c5xlarge-1", "queue-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue"),
            EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            False,
        ),
        (
            DynamicNode("queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue"),
            None,
            True,
        ),
        (
            DynamicNode("queue-dy-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),
            None,
            False,
        ),
        (
            DynamicNode("queue-st-c5xlarge-1", "ip-2", "hostname", "DOWN+CLOUD", "queue"),
            None,
            False,
        ),
        # Powering_down nodes are handled separately, always considered healthy by this workflow
        (
            DynamicNode("queue-dy-c5xlarge-1", "ip-2", "hostname", "DOWN+CLOUD+POWERING_DOWN", "queue"),
            EC2Instance("id-2", "ip-2", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            True,
        ),
        # Node in POWER_SAVE, but still has ip associated should be considered unhealthy
        (
            DynamicNode("queue-dy-c5xlarge-1", "ip-2", "hostname", "IDLE+CLOUD+POWER", "queue"),
            None,
            False,
        ),
        # Node in POWER_SAVE, but also in DOWN should be considered unhealthy
        (
            DynamicNode("queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "hostname", "DOWN+CLOUD+POWER", "queue"),
            None,
            False,
        ),
        (
            DynamicNode(
                "queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "IDLE+CLOUD+POWER", "queue"
            ),
            None,
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
def test_slurm_node_is_healthy(node, instance, expected_result):
    node.instance = instance
    assert_that(node.is_healthy(terminate_drain_nodes=True, terminate_down_nodes=True)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, expected_result",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+POWER", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWER", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "POWERING_DOWN", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "nodehostname", "MIXED*+CLOUD", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED*+CLOUD", "queue1"), False),
    ],
)
def test_slurm_node_is_powering_down_with_nodeaddr(node, expected_result):
    assert_that(node.is_powering_down_with_nodeaddr()).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, instance, expected_result",
    [
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
            None,
            False,
        ),
        (
            DynamicNode("node-dy-c5xlarge-1", "node-dy-c5xlarge-1", "hostname", "IDLE+CLOUD+POWER", "node"),
            None,
            True,
        ),
        (
            DynamicNode("node-dy-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+POWER", "node"),
            None,
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+POWER", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", datetime(2020, 1, 1, 0, 0, 0)),
            True,
        ),
    ],
    ids=["static_no_backing", "dynamic_power_save", "dynamic_no_backing", "static_valid"],
)
def test_slurm_node_is_backing_instance_valid(node, instance, expected_result):
    node.instance = instance
    assert_that(node.is_backing_instance_valid()).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, expected_result",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD", "queue1"), True),
        (
            StaticNode("queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "nodehostname", "MIXED+CLOUD+POWER", "queue1"),
            False,
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWER", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "nodehostname", "POWERING_DOWN", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "nodehostname", "DOWN+CLOUD", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "nodehostname", "MIXED*+CLOUD", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED*+CLOUD", "queue1"), True),
    ],
)
def test_slurm_node_needs_reset_when_inactive(node, expected_result):
    assert_that(node.needs_reset_when_inactive()).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, expected_result",
    [
        (StaticNode("queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue1"), False),
        (StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"), True),
    ],
    ids=["static_addr_not_set", "static_valid"],
)
def test_is_static_node_configuration_valid(node, expected_result):
    assert_that(node._is_static_node_configuration_valid()).is_equal_to(expected_result)


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
    assert_that(instance_health_state.fail_ec2_health_check(current_time, health_check_timeout=30)).is_equal_to(
        expected_result
    )


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
    assert_that(instance_health_state.fail_scheduled_events_check()).is_equal_to(expected_result)
