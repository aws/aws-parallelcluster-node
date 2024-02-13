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
from slurm_plugin.fleet_manager import EC2Instance
from slurm_plugin.slurm_resources import (
    DynamicNode,
    EC2InstanceHealthState,
    InvalidNodenameError,
    SlurmPartition,
    SlurmResumeJob,
    StaticNode,
    get_node_list,
)


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
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+DRAIN+POWERING_UP", "queue1"), True),
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED+CLOUD+DRAIN+NOT_RESPONDING", "queue1"
            ),
            True,
        ),
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
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+DRAIN+POWERING_UP", "queue1"),
            False,
        ),
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED+CLOUD+DRAIN+NOT_RESPONDING", "queue1"
            ),
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+DRAIN+POWER_DOWN", "queue1"),
            True,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+DRAIN+POWERING_DOWN", "queue1"),
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+DRAIN+NOT_RESPONDING", "queue1"),
            True,
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+DRAIN", "queue1"), True),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+DRAIN+POWER_DOWN", "queue1"),
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+DRAIN+POWERING_DOWN", "queue1"),
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+REBOOT_ISSUED", "queue1"),
            False,
        ),
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+DRAIN+REBOOT_REQUESTED", "queue1"
            ),
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+DRAIN+REBOOT_ISSUED", "queue1"),
            True,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+DRAIN+COMPLETING", "queue1"),
            False,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+DRAIN+COMPLETING", "queue1"),
            False,
        ),
    ],
)
def test_slurm_node_is_drained(node, expected_output):
    assert_that(node.is_drained()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+REBOOT_REQUESTED", "queue1"),
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+REBOOT_ISSUED", "queue1"),
            True,
        ),
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+DRAIN+REBOOT_REQUESTED", "queue1"
            ),
            True,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+DRAIN+REBOOT_ISSUED", "queue1"),
            True,
        ),
    ],
)
def test_slurm_node_is_rebooting(node, expected_output):
    assert_that(node.is_rebooting()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD", "queue1"),
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+INVALID_REG", "queue1"),
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD", "queue1"),
            False,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+DRAIN+INVALID_REG+POWERING_DOWN", "queue1"
            ),
            True,
        ),
    ],
)
def test_slurm_node_is_invalid_slurm_registration(node, expected_output):
    assert_that(node.is_invalid_slurm_registration()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "somestate", "queue1"), False),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+DOWN+POWERING_UP", "queue1"), True),
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED+CLOUD+DRAIN+NOT_RESPONDING", "queue1"
            ),
            False,
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+POWER", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"), False),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "DOWN+CLOUD+POWER_DOWN+POWERED_DOWN",
                "queue1",
            ),
            True,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "DOWN+CLOUD+POWER_DOWN",
                "queue1",
            ),
            False,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "DOWN+CLOUD+POWERING_DOWN",
                "queue1",
            ),
            False,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "DOWN+CLOUD+POWERING_DOWN+POWERED_DOWN",
                "queue1",
            ),
            False,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "DOWN+CLOUD+POWER_DOWN+POWERING_DOWN+POWERED_DOWN",
                "queue1",
            ),
            False,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+REBOOT_ISSUED", "queue1"),
            True,
        ),
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+DRAIN+REBOOT_REQUESTED", "queue1"
            ),
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+DRAIN+REBOOT_ISSUED", "queue1"),
            True,
        ),
    ],
)
def test_slurm_node_is_down(node, expected_output):
    assert_that(node.is_down()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWER", "queue1"), True),
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+DRAIN+POWERING_UP", "queue1"),
            False,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED+CLOUD+DOWN+NOT_RESPONDING", "queue1"
            ),
            False,
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"), False),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
            ),
            True,
        ),
    ],
)
def test_slurm_node_is_up(node, expected_output):
    assert_that(node.is_up()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWER", "queue1"), False),
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+MAINTENANCE+RESERVED", "queue1"),
            True,
        ),
        (
            StaticNode(
                "queue1-st-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "DOWN+CLOUD+MAINTENANCE+POWERED_DOWN+RESERVED",
                "queue1",
            ),
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+RESERVED", "queue1"),
            True,
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "RESERVED", "queue1"), True),
    ],
)
def test_slurm_node_is_reserved(node, expected_output):
    assert_that(node._is_reserved()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWER", "queue1"), False),
        (
            StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+MAINTENANCE+RESERVED", "queue1"),
            True,
        ),
        (
            StaticNode(
                "queue1-st-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "DOWN+CLOUD+MAINTENANCE+POWERED_DOWN+RESERVED",
                "queue1",
            ),
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+MAINTENANCE", "queue1"),
            False,  # RESERVED is required as well
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MAINTENANCE", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MAINTENANCE+RESERVED", "queue1"), True),
    ],
)
def test_slurm_node_is_in_maintenance(node, expected_output):
    assert_that(node.is_in_maintenance()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWER", "queue1"), False),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
            ),
            True,
        ),
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
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
            ),
            False,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+DRAIN+NOT_RESPONDING", "queue1"),
            False,
        ),
    ],
)
def test_slurm_node_is_online(node, expected_output):
    assert_that(node.is_online()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
            ),
            True,
        ),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWERING_UP", "queue1"), False),
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
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
            ),
            False,
        ),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+DOWN+POWERING_UP", "queue1"), False),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDEL+CLOUD+POWERING_UP", "queue1"), False),
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED+CLOUD", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "COMPLETING+CLOUD", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+POWERED_DOWN", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING", "queue1"), True),
    ],
)
def test_slurm_node_is_running_job(node, expected_output):
    assert_that(node.is_running_job()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+POWERED_DOWN", "queue1"), True),
        (DynamicNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD", "queue1"), False),
    ],
)
def test_slurm_node_is_power_with_job(node, expected_output):
    assert_that(node.is_power_with_job()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "somestate", "queue1", "Failed to resume"),
            False,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "MIXED+CLOUD+DRAIN+POWERING_UP",
                "queue1",
                "Some reason",
            ),
            False,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "ALLOCATED+CLOUD+DRAIN+NOT_RESPONDING",
                "queue1",
                "(Code:RequestLimitExceeded)Failure when resuming nodes [root@2023-01-31T21:24:55]",
            ),
            False,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "IDLE+CLOUD",
                "queue1",
                "(Code:InsufficientInstanceCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
            ),
            True,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "DOWN+CLOUD",
                "queue1",
                "(Code:InsufficientHostCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
            ),
            True,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "COMPLETING+DRAIN",
                "queue1",
                "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
            ),
            True,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "DOWN+CLOUD",
                "queue1",
                "(Code:Unsupported)Failure when resuming nodes [root@2023-01-31T21:24:55]",
            ),
            True,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "nodeip",
                "nodehostname",
                "DOWN+CLOUD",
                "queue1",
                "(Code:SpotMaxPriceTooLow)Failure when resuming nodes [root@2023-01-31T21:24:55]",
            ),
            True,
        ),
    ],
)
def test_slurm_node_is_ice(node, expected_output):
    assert_that(node.is_ice()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "nodes, expected_output",
    [
        (
            [
                StaticNode(
                    "queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),
                StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD", "queue1"),
            ],
            True,
        ),
        (
            [
                DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+POWERED_DOWN", "queue1"),
                StaticNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+DOWN+POWERING_UP", "queue1"),
            ],
            False,
        ),
        ([DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING", "queue1")], True),
    ],
)
def test_partition_is_inactive(nodes, expected_output):
    partition = SlurmPartition("name", "nodenames", "state")
    partition.slurm_nodes = nodes
    assert_that(partition.has_running_job()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, consider_drain_as_unhealthy, consider_down_as_unhealthy, mock_is_node_being_replaced, expected_result",
    [
        pytest.param(
            DynamicNode("queue-dy-c5xlarge-1", "some_ip", "hostname", "MIXED+CLOUD", "queue"),
            True,
            True,
            False,
            True,
            id="healthy_node",
        ),
        pytest.param(
            StaticNode("queue-st-c5xlarge-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN", "queue"),
            True,
            True,
            False,
            False,
            id="drained_not_in_replacement",
        ),
        pytest.param(
            StaticNode("queue-st-c5xlarge-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN", "queue"),
            True,
            True,
            True,
            True,
            id="drained_in_replacement",
        ),
        pytest.param(
            DynamicNode("queue-dy-c5xlarge-1", "some_ip", "hostname", "IDLE+CLOUD+DRAIN", "queue"),
            False,
            True,
            False,
            True,
            id="drain_not_term",
        ),
        pytest.param(
            StaticNode("queue-st-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD", "queue"),
            True,
            True,
            False,
            False,
            id="down_not_in_replacement",
        ),
        pytest.param(
            StaticNode("queue-st-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD", "queue"),
            True,
            True,
            True,
            True,
            id="down_in_replacement",
        ),
        pytest.param(
            DynamicNode("queue-dy-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD", "queue"),
            True,
            False,
            False,
            True,
            id="down_not_term",
        ),
        pytest.param(
            StaticNode(
                "queue-dy-c5xlarge-1",
                "some_ip",
                "hostname",
                "DOWN+CLOUD+POWER_DOWN+POWERED_DOWN+NOT_RESPONDING",
                "queue",
            ),
            True,
            True,
            False,
            False,
            id="unhealthy_static_node",
        ),
        pytest.param(
            DynamicNode(
                "queue-dy-c5xlarge-1",
                "some_ip",
                "hostname",
                "DOWN+CLOUD+POWER_DOWN+POWERED_DOWN+NOT_RESPONDING",
                "queue",
            ),
            True,
            True,
            False,
            False,
            id="unhealthy_dynamic_node",
        ),
        pytest.param(
            StaticNode("queue-dy-c5xlarge-1", "some_ip", "hostname", "IDLE+CLOUD+POWER_DOWN+POWERED_DOWN", "queue"),
            True,
            True,
            False,
            True,
            id="power_static_node",
        ),
        pytest.param(
            DynamicNode("queue-dy-c5xlarge-1", "some_ip", "hostname", "IDLE+CLOUD+POWER_DOWN+POWERED_DOWN", "queue"),
            True,
            True,
            False,
            True,
            id="power_dynamic_node",
        ),
        pytest.param(
            StaticNode("queue-st-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD+REBOOT_ISSUED", "queue"),
            True,
            True,
            False,
            True,
            id="scontrol_reboot_issued_static",
        ),
        pytest.param(
            DynamicNode("queue-dy-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD+REBOOT_ISSUED", "queue"),
            True,
            True,
            False,
            True,
            id="scontrol_reboot_issued_dynamic",
        ),
        pytest.param(
            StaticNode("queue-st-c5xlarge-1", "some_ip", "hostname", "DRAIN+CLOUD+REBOOT_REQUESTED", "queue"),
            True,
            True,
            False,
            True,
            id="scontrol_reboot_asap_requested_static",
        ),
        pytest.param(
            DynamicNode("queue-dy-c5xlarge-1", "some_ip", "hostname", "DRAIN+CLOUD+REBOOT_REQUESTED", "queue"),
            True,
            True,
            False,
            True,
            id="scontrol_reboot_asap_requested_dynamic",
        ),
        pytest.param(
            StaticNode("queue-st-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD+DRAIN+REBOOT_ISSUED", "queue"),
            True,
            True,
            False,
            True,
            id="scontrol_reboot_asap_issued_static",
        ),
        pytest.param(
            DynamicNode("queue-dy-c5xlarge-1", "some_ip", "hostname", "DOWN+CLOUD+DRAIN+REBOOT_ISSUED", "queue"),
            True,
            True,
            False,
            True,
            id="scontrol_reboot_asap_issued_dynamic",
        ),
    ],
)
def test_slurm_node_is_state_healthy(
    node, mock_is_node_being_replaced, consider_drain_as_unhealthy, consider_down_as_unhealthy, expected_result
):
    node.is_being_replaced = mock_is_node_being_replaced
    assert_that(node.is_state_healthy(consider_drain_as_unhealthy, consider_down_as_unhealthy)).is_equal_to(
        expected_result
    )


@pytest.mark.parametrize(
    "node, instance, max_count, count_map, is_static_nodes_in_replacement, is_replacement_timeout, "
    "bootstrap_failure_messages, is_failing_health_check, is_node_bootstrap_failure",
    [
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"),
            None,
            0,
            {},
            True,
            False,
            "Node bootstrap error: Node queue1-st-c5xlarge-1(ip-1) is currently in replacement and no backing instance",
            False,
            True,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, datetime(2020, 1, 1, 0, 0, 0)),
            0,
            {},
            True,
            True,
            "Node bootstrap error: Replacement timeout expires for node queue1-st-c5xlarge-1(ip-1) in replacement",
            False,
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"),
            None,
            0,
            {},
            False,
            False,
            "Node bootstrap error: Node queue1-dy-c5xlarge-1(ip-1) is in power up state without valid backing instance",
            False,
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+POWERED_DOWN+NOT_RESPONDING", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, "launch_time"),
            0,
            {},
            False,
            False,
            "Node bootstrap error: Resume timeout expires",
            False,
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "hostname", "DOWN+CLOUD+POWERING_UP", "queue1"),
            None,
            0,
            {},
            False,
            False,
            None,
            False,
            False,
        ),
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "queue1-dy-c5xlarge-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"
            ),
            None,
            0,
            {},
            False,
            False,
            None,
            False,
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+NOT_RESPONDING", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, datetime(2020, 1, 1, 0, 0, 0)),
            0,
            {},
            False,
            False,
            None,
            False,
            False,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "hostname", "DOWN+CLOUD+POWERED_DOWN", "queue1"
            ),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, "launch_time"),
            0,
            {},
            False,
            False,
            None,
            False,
            False,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "DRAIN+CLOUD", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, "launch_time"),
            0,
            {},
            False,
            False,
            None,
            False,
            False,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+POWERING_DOWN", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, "launch_time"),
            0,
            {},
            False,
            False,
            None,
            False,
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "DOWN+CLOUD+POWERING_DOWN", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, "launch_time"),
            0,
            {},
            True,
            False,
            "failed during bootstrap when performing health check",
            True,
            True,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, "launch_time"),
            0,
            {},
            False,
            False,
            "failed during bootstrap when performing health check",
            True,
            True,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "queue1-dy-c5xlarge-1",
                "hostname",
                "DOWN+CLOUD+POWERED_DOWN+NOT_RESPONDING",
                "queue1",
            ),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, "launch_time"),
            0,
            {},
            False,
            False,
            None,
            False,
            False,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"),
            None,
            0,
            {},
            False,
            False,
            "Node bootstrap error: Node queue1-dy-c5xlarge-1(ip-1) is in power up state without valid backing instance",
            False,
            True,
        ),
        # Dynamic node that has just failed the Slurm registration and has not yet been processed by clustermgtd
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+DRAIN+INVALID_REG", "queue1"),
            None,
            0,
            {},
            False,
            False,
            "Node bootstrap error: Node queue1-dy-c5xlarge-1(ip-1) failed to register to the Slurm management daemon, "
            "node state: IDLE+CLOUD+DRAIN+INVALID_REG",
            False,
            True,
        ),
        # Dynamic node that failed the Slurm registration and was powered down by clustermgtd
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+DRAIN+INVALID_REG+POWER_DOWN", "queue1"
            ),
            None,
            0,
            {},
            False,
            False,
            None,
            False,
            False,
        ),
        # Dynamic node that failed the Slurm registration, was powered down by clustermgtd and is still powering down
        (
            DynamicNode("queue1-dy-c5xlarge-1", "ip-1", "hostname", "IDLE+DRAIN+INVALID_REG+POWERING_DOWN", "queue1"),
            None,
            0,
            {},
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
        "idle_powering_up",
        "dynamic_node_failed_registration",
        "dynamic_node_failed_registration_power_down",
        "dynamic_node_failed_registration_powering_down",
    ],
)
def test_slurm_node_is_bootstrap_failure(
    node,
    max_count,
    count_map,
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
    assert_that(node.is_bootstrap_failure(max_count, count_map)).is_equal_to(is_node_bootstrap_failure)
    if bootstrap_failure_messages:
        assert_that(caplog.text).contains(bootstrap_failure_messages)


@pytest.mark.parametrize(
    "node, instance, max_count, count_map, expected_result",
    [
        (
            DynamicNode("queue-dy-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue"),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, datetime(2020, 1, 1, 0, 0, 0)),
            0,
            {},
            True,
        ),
        (
            StaticNode("queue-st-c5xlarge-1", "queue-st-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue"),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, datetime(2020, 1, 1, 0, 0, 0)),
            0,
            {},
            False,
        ),
        (
            DynamicNode("queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "hostname", "IDLE+CLOUD", "queue"),
            None,
            0,
            {},
            True,
        ),
        (
            DynamicNode("queue-dy-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),
            None,
            0,
            {},
            False,
        ),
        (
            DynamicNode("queue-st-c5xlarge-1", "ip-2", "hostname", "DOWN+CLOUD", "queue"),
            None,
            0,
            {},
            False,
        ),
        # Powering_down nodes with backing instance is considered as healthy
        (
            DynamicNode("queue-dy-c5xlarge-1", "ip-2", "hostname", "DOWN+CLOUD+POWERING_DOWN", "queue"),
            EC2Instance("id-2", "ip-2", "hostname", {"ip-2"}, datetime(2020, 1, 1, 0, 0, 0)),
            0,
            {},
            True,
        ),
        # Powering_down nodes without backing instance is considered as unhealthy
        (
            DynamicNode("queue-dy-c5xlarge-1", "ip-2", "hostname", "DOWN+CLOUD+POWERING_DOWN", "queue"),
            None,
            0,
            {},
            False,
        ),
        # Node in POWER_SAVE, but still has ip associated should be considered unhealthy
        (
            DynamicNode("queue-dy-c5xlarge-1", "ip-2", "hostname", "IDLE+CLOUD+POWER", "queue"),
            None,
            0,
            {},
            False,
        ),
        # Node in POWER_SAVE, but also in DOWN should be considered unhealthy
        (
            DynamicNode("queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "hostname", "DOWN+CLOUD+POWER", "queue"),
            None,
            0,
            {},
            False,
        ),
        (
            DynamicNode(
                "queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "queue-dy-c5xlarge-1", "IDLE+CLOUD+POWER", "queue"
            ),
            None,
            0,
            {},
            True,
        ),
    ],
    ids=[
        "basic",
        "static_nodeaddr_not_set",
        "dynamic_nodeaddr_not_set",
        "dynamic_unhealthy",
        "static_unhealthy",
        "powering_down_healthy",
        "powering_down_unhealthy",
        "power_unhealthy1",
        "power_unhealthy2",
        "power_healthy",
    ],
)
def test_slurm_node_is_healthy(node, instance, max_count, count_map, expected_result):
    node.instance = instance
    assert_that(
        node.is_healthy(
            consider_drain_as_unhealthy=True,
            consider_down_as_unhealthy=True,
            ec2_instance_missing_max_count=max_count,
            nodes_without_backing_instance_count_map=count_map,
        )
    ).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, expected_result",
    [
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
            ),
            False,
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+POWERED_DOWN", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWERED_DOWN", "queue1"), True),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "nodehostname", "IDLE+CLOUD+POWERED_DOWN", "queue1"
            ),
            False,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWERED_DOWN+POWER_DOWN", "queue1"
            ),
            True,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "queue1-dy-c5xlarge-1",
                "nodehostname",
                "IDLE+CLOUD+POWERED_DOWN+POWER_DOWN",
                "queue1",
            ),
            False,
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "POWERING_DOWN", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "nodehostname", "POWERING_DOWN", "queue1"), False),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING", "queue1"
            ),
            False,
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING", "queue1"), False),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
            ),
            False,
        ),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+DRAIN+POWER_DOWN", "queue1"),
            False,
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+DRAIN+POWER_DOWN", "queue1"), False),
        (
            DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+DRAIN+POWERING_DOWN", "queue1"),
            True,
        ),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1",
                "queue1-dy-c5xlarge-1",
                "nodehostname",
                "IDLE+CLOUD+DRAIN+POWERING_DOWN",
                "queue1",
            ),
            False,
        ),
    ],
)
def test_slurm_node_is_powering_down_with_nodeaddr(node, expected_result):
    assert_that(node.is_powering_down_with_nodeaddr()).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "node, instance, max_count, count_map, final_map, expected_result",
    [
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD", "queue1"),
            None,
            0,
            {},
            {},
            False,
        ),
        (
            DynamicNode("node-dy-c5xlarge-1", "node-dy-c5xlarge-1", "hostname", "IDLE+CLOUD+POWER", "node"),
            None,
            0,
            {},
            {},
            True,
        ),
        (
            DynamicNode("node-dy-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+POWER", "node"),
            None,
            0,
            {},
            {},
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+POWER", "queue1"),
            EC2Instance("id-1", "ip-1", "hostname", {"ip-1"}, datetime(2020, 1, 1, 0, 0, 0)),
            0,
            {},
            {},
            True,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+POWER", "queue1"),
            None,
            2,
            {"queue1-st-c5xlarge-1": 1},
            {"queue1-st-c5xlarge-1": 2},
            True,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+POWER", "queue1"),
            None,
            2,
            {"queue1-st-c5xlarge-1": 2},
            {"queue1-st-c5xlarge-1": 2},
            False,
        ),
        (
            StaticNode("queue1-st-c5xlarge-1", "ip-1", "hostname", "IDLE+CLOUD+POWER", "queue1"),
            "Instance",
            2,
            {"queue1-st-c5xlarge-1": 3},
            {},
            True,
        ),
    ],
    ids=[
        "static_no_backing_zero_max_count",
        "dynamic_power_save_zero_max_count",
        "dynamic_no_backing_zero_max_count",
        "static_valid_zero_max_count",
        "static_no_backing_count_not_exceeded",
        "static_no_backing_with_count_exceeded",
        "static_backed_with_count_exceeded",
    ],
)
def test_slurm_node_is_backing_instance_valid(node, instance, max_count, count_map, final_map, expected_result):
    node.instance = instance
    assert_that(
        node.is_backing_instance_valid(
            ec2_instance_missing_max_count=max_count, nodes_without_backing_instance_count_map=count_map
        )
    ).is_equal_to(expected_result)
    assert_that(node.ec2_backing_instance_valid).is_equal_to(expected_result)
    if count_map:
        assert_that(count_map[node.name]).is_equal_to(final_map.get(node.name, None))


@pytest.mark.parametrize(
    "node, expected_result",
    [
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
            ),
            True,
        ),
        (
            StaticNode(
                "queue1-st-c5xlarge-1", "queue1-st-c5xlarge-1", "nodehostname", "MIXED+CLOUD+POWERED_DOWN", "queue1"
            ),
            False,
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWER", "queue1"), True),
        (DynamicNode("queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "nodehostname", "POWERING_DOWN", "queue1"), False),
        (DynamicNode("queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "nodehostname", "DOWN+CLOUD", "queue1"), False),
        (
            DynamicNode(
                "queue1-dy-c5xlarge-1", "queue1-dy-c5xlarge-1", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING", "queue1"
            ),
            True,
        ),
        (DynamicNode("queue1-dy-c5xlarge-1", "nodeip", "nodehostname", "MIXED+CLOUD+NOT_RESPONDING", "queue1"), True),
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
def test_is_static_node_ip_configuration_valid(node, expected_result):
    assert_that(node._is_static_node_ip_configuration_valid()).is_equal_to(expected_result)


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


@pytest.mark.parametrize(
    "nodenames, expected_node_list, expected_exception",
    [
        (
            "queue1-st-c5xlarge-[1,3,4-6],queue1-dy-c5large-20",
            [
                "queue1-st-c5xlarge-1",
                "queue1-st-c5xlarge-3",
                "queue1-st-c5xlarge-4",
                "queue1-st-c5xlarge-5",
                "queue1-st-c5xlarge-6",
                "queue1-dy-c5large-20",
            ],
            False,
        ),
        (
            "queue1-st-c5large-20,queue1-dy-c5xlarge-[1,3,4-6]",
            [
                "queue1-st-c5large-20",
                "queue1-dy-c5xlarge-1",
                "queue1-dy-c5xlarge-3",
                "queue1-dy-c5xlarge-4",
                "queue1-dy-c5xlarge-5",
                "queue1-dy-c5xlarge-6",
            ],
            False,
        ),
        (
            "q4-dy-c4-1-1,q4-dy-c4-2-1",
            ["q4-dy-c4-1-1", "q4-dy-c4-2-1"],
            False,
        ),
        (
            "queue-1-2-dy-c5x-large-1-2-10",
            ["queue-1-2-dy-c5x-large-1-2-10"],
            False,
        ),
        (
            "queue-1-broken-c5x-large-1-1",
            [],
            True,
        ),
        (
            "queue-1-st-c5x-large-1-broken",
            [],
            True,
        ),
        (
            "",
            [],
            True,
        ),
        (
            "  ",
            [],
            True,
        ),
        (
            "broken",
            [],
            True,
        ),
        (
            "-st-c5large-20",
            [],
            True,
        ),
        (
            "queue-st-c5large-[20",
            [],
            True,
        ),
        (
            "queue-st-c5large-20]",
            [],
            True,
        ),
        (
            "--dy-a-[-]",
            [],
            True,
        ),
        (
            "--dy-a-[,]",
            [],
            True,
        ),
    ],
)
def test_get_node_list(nodenames, expected_node_list, expected_exception):
    if expected_exception:
        with pytest.raises(InvalidNodenameError):
            get_node_list(nodenames)
    else:
        node_list = get_node_list(nodenames)
        assert_that(node_list).is_equal_to(expected_node_list)


class TestSlurmJob:
    @pytest.mark.parametrize(
        "job_json, expected_nodes_alloc, expected_nodes_resume, expected_is_exclusive, expected_exception",
        [
            (
                {
                    "extra": None,
                    "features": None,
                    "job_id": 140816,
                    "nodes_alloc": "queue1-st-c5xlarge-[1,3,4-5]",
                    "nodes_resume": "queue1-st-c5xlarge-[7-8]",
                    "oversubscribe": "NO",
                    "partition": "cloud_exclusive",
                    "reservation": None,
                },
                ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-3", "queue1-st-c5xlarge-4", "queue1-st-c5xlarge-5"],
                ["queue1-st-c5xlarge-7", "queue1-st-c5xlarge-8"],
                True,
                False,
            ),
            (
                {
                    "extra": None,
                    "features": None,
                    "job_id": 140816,
                    "nodes_alloc": "queue1-st-c5xlarge-[1,3,4-5]",
                    "nodes_resume": "queue1-st-c5xlarge-[7-8]",
                    "oversubscribe": "YES",
                    "partition": "cloud_exclusive",
                    "reservation": None,
                },
                ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-3", "queue1-st-c5xlarge-4", "queue1-st-c5xlarge-5"],
                ["queue1-st-c5xlarge-7", "queue1-st-c5xlarge-8"],
                False,
                False,
            ),
            (
                {
                    "extra": None,
                    "features": None,
                    "job_id": 140816,
                    "nodes_alloc": "queue1-st-c5xlarge-[1,3,4-5]",
                    "nodes_resume": "broken",
                    "oversubscribe": "NO",
                    "partition": "cloud_exclusive",
                    "reservation": None,
                },
                [],
                [],
                None,
                True,
            ),
            (
                {
                    "extra": None,
                    "features": None,
                    "job_id": 140816,
                    "nodes_alloc": "broken",
                    "nodes_resume": "queue1-st-c5xlarge-[1,3,4-5]",
                    "oversubscribe": "NO",
                    "partition": "cloud_exclusive",
                    "reservation": None,
                },
                [],
                [],
                None,
                True,
            ),
            (
                {
                    "extra": None,
                    "features": None,
                    "job_id": 140816,
                    "nodes_alloc": "queue1-st-c5xlarge-1",
                    "nodes_resume": "queue1-st-c5xlarge-1",
                    "oversubscribe": "NOT_FOUND",
                    "partition": "cloud_exclusive",
                    "reservation": None,
                },
                ["queue1-st-c5xlarge-1"],
                ["queue1-st-c5xlarge-1"],
                False,
                False,
            ),
        ],
    )
    def test_slurm_job(
        self, job_json, expected_nodes_alloc, expected_nodes_resume, expected_is_exclusive, expected_exception
    ):
        if expected_exception:
            with pytest.raises(InvalidNodenameError):
                SlurmResumeJob(**job_json)
        else:
            job = SlurmResumeJob(**job_json)
            assert_that(job.nodes_alloc).is_equal_to(expected_nodes_alloc)
            assert_that(job.nodes_resume).is_equal_to(expected_nodes_resume)
            assert_that(job.is_exclusive()).is_equal_to(expected_is_exclusive)

    @pytest.mark.parametrize(
        "jobs_json, expected_equal",
        [
            (
                [
                    {
                        "extra": None,
                        "features": None,
                        "job_id": 140816,
                        "nodes_alloc": "queue1-st-c5xlarge-[1-3,4,5]",
                        "nodes_resume": "queue1-st-c5xlarge-[7-8]",
                        "oversubscribe": "NO",
                        "partition": "cloud_exclusive",
                        "reservation": None,
                    },
                    {
                        "extra": None,
                        "features": None,
                        "job_id": 140816,
                        "nodes_alloc": "queue1-st-c5xlarge-[1-3,4,5]",
                        "nodes_resume": "queue1-st-c5xlarge-[7-8]",
                        "oversubscribe": "NO",
                        "partition": "cloud_exclusive",
                        "reservation": None,
                    },
                ],
                True,
            ),
            (
                [
                    {
                        "extra": None,
                        "features": None,
                        "job_id": 140816,
                        "nodes_alloc": "queue1-st-c5xlarge-[1,3,4-5]",
                        "nodes_resume": "queue1-st-c5xlarge-[7-8]",
                        "oversubscribe": "NO",
                        "partition": "cloud_exclusive",
                        "reservation": None,
                    },
                    {
                        "job_id": 140816,
                        "nodes_alloc": "queue1-st-c5xlarge-[1,3,4-5]",
                        "nodes_resume": "queue1-st-c5xlarge-[7-8]",
                        "oversubscribe": "NO",
                    },
                ],
                False,
            ),
            (
                [
                    {
                        "extra": None,
                        "features": None,
                        "job_id": 140816,
                        "nodes_alloc": "queue1-st-c5xlarge-[1,3,4-5]",
                        "nodes_resume": "queue1-st-c5xlarge-[7-8]",
                        "oversubscribe": "NO",
                        "partition": "cloud_exclusive",
                        "reservation": None,
                    },
                    {
                        "extra": None,
                        "features": None,
                        "job_id": 140816,
                        "nodes_alloc": "queue1-st-c5xlarge-[1,3,4-5]",
                        "nodes_resume": "queue1-st-c5xlarge-[7-8]",
                        "oversubscribe": "OK",
                        "partition": "cloud_exclusive",
                        "reservation": None,
                    },
                ],
                False,
            ),
            (
                [
                    {
                        "extra": None,
                        "features": None,
                        "job_id": 140816,
                        "nodes_alloc": "queue1-st-c5xlarge-[1,3,4-5]",
                        "nodes_resume": "queue1-st-c5xlarge-[7-8]",
                        "oversubscribe": "NO",
                        "partition": "cloud_exclusive",
                        "reservation": None,
                    },
                    {
                        "extra": None,
                        "features": None,
                        "job_id": 140817,
                        "nodes_alloc": "queue1-st-c5xlarge-[1,3,4-5]",
                        "nodes_resume": "queue1-st-c5xlarge-[7-8]",
                        "oversubscribe": "NO",
                        "partition": "cloud_exclusive",
                        "reservation": None,
                    },
                ],
                False,
            ),
        ],
    )
    def test_slurm_job_eq(
        self,
        jobs_json,
        expected_equal,
    ):
        job_1 = SlurmResumeJob(**jobs_json[0])
        job_2 = SlurmResumeJob(**jobs_json[1])

        if expected_equal:
            assert_that(job_1).is_equal_to(job_2)
        else:
            assert_that(job_1).is_not_equal_to(job_2)

        assert_that(job_1).is_not_equal_to("140816")
        assert_that(job_2).is_not_equal_to("140817")

    @pytest.mark.parametrize(
        "job_json, expected_hash",
        [
            (
                {
                    "extra": None,
                    "features": None,
                    "job_id": 140816,
                    "nodes_alloc": "queue1-st-c5xlarge-[1,3,4-5]",
                    "nodes_resume": "queue1-st-c5xlarge-[7-8]",
                    "oversubscribe": "NO",
                    "partition": "cloud_exclusive",
                    "reservation": None,
                },
                140816,
            ),
        ],
    )
    def test_slurm_job_hash(
        self,
        job_json,
        expected_hash,
    ):
        job = SlurmResumeJob(**job_json)
        assert_that(hash(job)).is_equal_to(expected_hash)

    @pytest.mark.parametrize(
        "job_json, expected_repr",
        [
            (
                {
                    "extra": None,
                    "features": None,
                    "job_id": 140816,
                    "nodes_alloc": "queue1-st-c5xlarge-1",
                    "nodes_resume": "queue1-st-c5xlarge-1",
                    "oversubscribe": "NO",
                    "partition": "cloud_exclusive",
                    "reservation": None,
                },
                "SlurmResumeJob(job_id=140816, "
                "nodes_alloc=['queue1-st-c5xlarge-1'], "
                "nodes_resume=['queue1-st-c5xlarge-1'], "
                "oversubscribe=<JobOversubscribe.NO: 'NO'>, "
                "partition='cloud_exclusive', "
                "reservation=None, "
                "features=None, "
                "extra=None)",
            ),
        ],
    )
    def test_slurm_job_repr(
        self,
        job_json,
        expected_repr,
    ):
        job = SlurmResumeJob(**job_json)
        assert_that(repr(job)).is_equal_to(expected_repr)
