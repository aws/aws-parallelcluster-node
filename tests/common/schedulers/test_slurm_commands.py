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
from unittest.mock import call

import pytest
from assertpy import assert_that
from common.schedulers.slurm_commands import (
    PartitionStatus,
    SlurmNode,
    SlurmPartition,
    _batch_node_info,
    _parse_nodes_info,
    is_static_node,
    parse_nodename,
    set_nodes_down,
    set_nodes_drain,
    set_nodes_idle,
    set_nodes_power_down,
    update_all_partitions,
    update_nodes,
    update_partitions,
)


@pytest.mark.parametrize(
    (
        "nodename",
        "expected_queue",
        "expected_node_type",
        "expected_instance_name",
        "expected_failure",
    ),
    [
        ("queue1-st-c5xlarge-1", "queue1", "st", "c5xlarge", False),
        ("queue-1-st-c5xlarge-1", "queue-1", "st", "c5xlarge", False),
        ("queue1-st-dy-c5xlarge-1", "queue1-st", "dy", "c5xlarge", False),
        ("queue1-dy-st-c5xlarge-1", "queue1-dy", "st", "c5xlarge", False),
        ("queue1-dy-dy-dy-dy-c5xlarge-1", "queue1-dy-dy-dy", "dy", "c5xlarge", False),
        ("queue1-st-i3enmetal2tb-1", "queue1", "st", "i3enmetal2tb", False),
        ("queue1-st-u6tb1metal-1", "queue1", "st", "u6tb1metal", False),
        ("queue1-st-c5.xlarge-1", None, None, None, True),
        ("queue_1-st-c5-xlarge-1", None, None, None, True),
    ],
)
def test_parse_nodename(nodename, expected_queue, expected_node_type, expected_instance_name, expected_failure):
    if expected_failure:
        with pytest.raises(Exception):
            parse_nodename(nodename)
    else:
        queue_name, node_type, instance_name = parse_nodename(nodename)
        assert_that(expected_queue).is_equal_to(queue_name)
        assert_that(expected_node_type).is_equal_to(node_type)
        assert_that(expected_instance_name).is_equal_to(instance_name)


@pytest.mark.parametrize(
    ("nodename", "expected_is_static"),
    [
        ("queue1-st-c5xlarge-1", True),
        ("queue-1-st-c5xlarge-1", True),
        ("queue1-st-dy-c5xlarge-1", False),
        ("queue1-dy-st-c5xlarge-1", True),
        ("queue1-dy-dy-dy-dy-c5xlarge-1", False),
        ("queue1-st-i3enmetal2tb-1", True),
        ("queue1-st-u6tb1metal-1", True),
    ],
)
def test_is_static_node(nodename, expected_is_static):
    assert_that(expected_is_static).is_equal_to(is_static_node(nodename))


@pytest.mark.parametrize(
    "node_info, expected_parsed_nodes_output",
    [
        (
            (
                "multiple-dy-c5xlarge-1\n"
                "172.31.10.155\n"
                "172-31-10-155\n"
                "MIXED+CLOUD\n"
                "multiple\n"
                "---\n"
                "multiple-dy-c5xlarge-2\n"
                "172.31.7.218\n"
                "172-31-7-218\n"
                "IDLE+CLOUD+POWER\n"
                "multiple\n"
                "---\n"
                "multiple-dy-c5xlarge-3\n"
                "multiple-dy-c5xlarge-3\n"
                "multiple-dy-c5xlarge-3\n"
                "IDLE+CLOUD+POWER\n"
                "multiple\n"
                "---\n"
                "multiple-dy-c5xlarge-4\n"
                "multiple-dy-c5xlarge-4\n"
                "multiple-dy-c5xlarge-4\n"
                "IDLE+CLOUD+POWER\n"
                "multiple,multiple2\n"
                "---\n"
                "multiple-dy-c5xlarge-5\n"
                "multiple-dy-c5xlarge-5\n"
                "multiple-dy-c5xlarge-5\n"
                "IDLE+CLOUD+POWER\n"
                # missing partitions
                "---"
            ),
            [
                SlurmNode("multiple-dy-c5xlarge-1", "172.31.10.155", "172-31-10-155", "MIXED+CLOUD", "multiple"),
                SlurmNode("multiple-dy-c5xlarge-2", "172.31.7.218", "172-31-7-218", "IDLE+CLOUD+POWER", "multiple"),
                SlurmNode(
                    "multiple-dy-c5xlarge-3",
                    "multiple-dy-c5xlarge-3",
                    "multiple-dy-c5xlarge-3",
                    "IDLE+CLOUD+POWER",
                    "multiple",
                ),
                SlurmNode(
                    "multiple-dy-c5xlarge-4",
                    "multiple-dy-c5xlarge-4",
                    "multiple-dy-c5xlarge-4",
                    "IDLE+CLOUD+POWER",
                    "multiple,multiple2",
                ),
                SlurmNode(
                    "multiple-dy-c5xlarge-5",
                    "multiple-dy-c5xlarge-5",
                    "multiple-dy-c5xlarge-5",
                    "IDLE+CLOUD+POWER",
                    None,
                ),
            ],
        )
    ],
)
def test_parse_nodes_info(node_info, expected_parsed_nodes_output):
    assert_that(_parse_nodes_info(node_info)).is_equal_to(expected_parsed_nodes_output)


@pytest.mark.parametrize(
    "nodenames, nodeaddrs, hostnames, batch_size, expected_result",
    [
        (
            "queue1-st-c5xlarge-1,queue1-st-c5xlarge-2,queue1-st-c5xlarge-3",
            None,
            None,
            2,
            [("queue1-st-c5xlarge-1,queue1-st-c5xlarge-2,queue1-st-c5xlarge-3", None, None)],
        ),
        (
            # Only split on commas after bucket
            # So nodename like queue1-st-c5xlarge-[1,3] can be processed safely
            "queue1-st-c5xlarge-[1-2],queue1-st-c5xlarge-2,queue1-st-c5xlarge-3,queue1-st-c5xlarge-[4,6]",
            "nodeaddr-[1-2],nodeaddr-2,nodeaddr-3,nodeaddr-[4,6]",
            None,
            2,
            [
                (
                    "queue1-st-c5xlarge-[1-2],queue1-st-c5xlarge-2,queue1-st-c5xlarge-3,queue1-st-c5xlarge-[4,6]",
                    "nodeaddr-[1-2],nodeaddr-2,nodeaddr-3,nodeaddr-[4,6]",
                    None,
                )
            ],
        ),
        (
            "queue1-st-c5xlarge-[1-2],queue1-st-c5xlarge-2,queue1-st-c5xlarge-[3],queue1-st-c5xlarge-[4,6]",
            "nodeaddr-[1-2],nodeaddr-2,nodeaddr-[3],nodeaddr-[4,6]",
            "nodehostname-[1-2],nodehostname-2,nodehostname-[3],nodehostname-[4,6]",
            2,
            [
                (
                    "queue1-st-c5xlarge-[1-2],queue1-st-c5xlarge-2,queue1-st-c5xlarge-[3]",
                    "nodeaddr-[1-2],nodeaddr-2,nodeaddr-[3]",
                    "nodehostname-[1-2],nodehostname-2,nodehostname-[3]",
                ),
                ("queue1-st-c5xlarge-[4,6]", "nodeaddr-[4,6]", "nodehostname-[4,6]"),
            ],
        ),
        ("queue1-st-c5xlarge-1,queue1-st-c5xlarge-[2],queue1-st-c5xlarge-3", ["nodeaddr-1"], None, 2, ValueError),
        (
            "queue1-st-c5xlarge-1,queue1-st-c5xlarge-[2],queue1-st-c5xlarge-3",
            None,
            ["nodehostname-1"],
            2,
            ValueError,
        ),
        (
            "queue1-st-c5xlarge-1,queue1-st-c5xlarge-2,queue1-st-c5xlarge-3",
            ["nodeaddr-1", "nodeaddr-2"],
            "nodehostname-1,nodehostname-2,nodehostname-3",
            2,
            ValueError,
        ),
        (
            ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3"],
            "nodeaddr-[1],nodeaddr-[2],nodeaddr-3",
            ["nodehostname-1", "nodehostname-2", "nodehostname-3"],
            2,
            [
                (
                    "queue1-st-c5xlarge-1,queue1-st-c5xlarge-2",
                    "nodeaddr-[1],nodeaddr-[2]",
                    "nodehostname-1,nodehostname-2",
                ),
                ("queue1-st-c5xlarge-3", "nodeaddr-3", "nodehostname-3"),
            ],
        ),
        (
            # Test with strings of same length but different number of node entries
            "queue1-st-c5xlarge-[1-fillerr],queue1-st-c5xlarge-[2-fillerr],queue1-st-c5xlarge-[3-filler]",
            "nodeaddr-1,nodeaddr-2,nodeaddr-3",
            ["nodehostname-1", "nodehostname-2", "nodehostname-3"],
            2,
            ValueError,
        ),
    ],
    ids=[
        "nodename_only",
        "name+addr",
        "name+addr+hostname",
        "incorrect_addr1",
        "incorrect_hostname1",
        "incorrect_addr2",
        "mixed_format",
        "same_length_string",
    ],
)
def test_batch_node_info(nodenames, nodeaddrs, hostnames, batch_size, expected_result):
    if expected_result is not ValueError:
        assert_that(list(_batch_node_info(nodenames, nodeaddrs, hostnames, batch_size))).is_equal_to(expected_result)
    else:
        try:
            _batch_node_info(nodenames, nodeaddrs, hostnames, batch_size)
        except Exception as e:
            assert_that(e).is_instance_of(ValueError)
            pass
        else:
            pytest.fail("Expected _batch_node_info to raise ValueError.")


@pytest.mark.parametrize(
    "nodes, reason, reset_addrs, update_call_kwargs",
    [
        (
            "nodes-1,nodes[2-6]",
            None,
            False,
            {"nodes": "nodes-1,nodes[2-6]", "state": "resume", "reason": None, "raise_on_error": False},
        ),
        (
            "nodes-1,nodes[2-6]",
            "debugging",
            True,
            {
                "nodes": "nodes-1,nodes[2-6]",
                "nodeaddrs": "nodes-1,nodes[2-6]",
                "nodehostnames": "nodes-1,nodes[2-6]",
                "state": "resume",
                "reason": "debugging",
                "raise_on_error": False,
            },
        ),
        (
            ["nodes-1", "nodes[2-4]", "nodes-5"],
            "debugging",
            True,
            {
                "nodes": ["nodes-1", "nodes[2-4]", "nodes-5"],
                "nodeaddrs": ["nodes-1", "nodes[2-4]", "nodes-5"],
                "nodehostnames": ["nodes-1", "nodes[2-4]", "nodes-5"],
                "state": "resume",
                "reason": "debugging",
                "raise_on_error": False,
            },
        ),
    ],
)
def test_set_nodes_idle(nodes, reason, reset_addrs, update_call_kwargs, mocker):
    update_mock = mocker.patch("common.schedulers.slurm_commands.update_nodes", autospec=True)
    set_nodes_idle(nodes, reason, reset_addrs)
    update_mock.assert_called_with(**update_call_kwargs)


@pytest.mark.parametrize(
    "nodes, reason, reset_addrs, update_call_kwargs",
    [
        (
            "nodes-1,nodes[2-6]",
            "debugging",
            True,
            {"nodes": "nodes-1,nodes[2-6]", "state": "down", "reason": "debugging"},
        ),
        (
            ["nodes-1", "nodes[2-4]", "nodes-5"],
            "debugging",
            True,
            {"nodes": ["nodes-1", "nodes[2-4]", "nodes-5"], "state": "down", "reason": "debugging"},
        ),
    ],
)
def test_set_nodes_down(nodes, reason, reset_addrs, update_call_kwargs, mocker):
    update_mock = mocker.patch("common.schedulers.slurm_commands.update_nodes", autospec=True)
    set_nodes_down(nodes, reason)
    update_mock.assert_called_with(**update_call_kwargs)


@pytest.mark.parametrize(
    "nodes, reason, reset_addrs, update_call_kwargs",
    [
        (
            "nodes-1,nodes[2-6]",
            None,
            False,
            {"nodes": "nodes-1,nodes[2-6]", "state": "power_down_force", "reason": None, "raise_on_error": True},
        ),
        (
            "nodes-1,nodes[2-6]",
            "debugging",
            True,
            {"nodes": "nodes-1,nodes[2-6]", "state": "power_down_force", "reason": "debugging", "raise_on_error": True},
        ),
        (
            ["nodes-1", "nodes[2-4]", "nodes-5"],
            "debugging",
            True,
            {
                "nodes": ["nodes-1", "nodes[2-4]", "nodes-5"],
                "state": "power_down_force",
                "reason": "debugging",
                "raise_on_error": True,
            },
        ),
    ],
)
def test_set_nodes_power_down(nodes, reason, reset_addrs, update_call_kwargs, mocker):
    update_mock = mocker.patch("common.schedulers.slurm_commands.reset_nodes", autospec=True)
    set_nodes_power_down(nodes, reason)
    update_mock.assert_called_with(**update_call_kwargs)


@pytest.mark.parametrize(
    "nodes, reason, reset_addrs, update_call_kwargs",
    [
        (
            "nodes-1,nodes[2-6]",
            "debugging",
            True,
            {"nodes": "nodes-1,nodes[2-6]", "state": "drain", "reason": "debugging"},
        ),
        (
            ["nodes-1", "nodes[2-4]", "nodes-5"],
            "debugging",
            True,
            {"nodes": ["nodes-1", "nodes[2-4]", "nodes-5"], "state": "drain", "reason": "debugging"},
        ),
    ],
)
def test_set_nodes_drain(nodes, reason, reset_addrs, update_call_kwargs, mocker):
    update_mock = mocker.patch("common.schedulers.slurm_commands.update_nodes", autospec=True)
    set_nodes_drain(nodes, reason)
    update_mock.assert_called_with(**update_call_kwargs)


@pytest.mark.parametrize(
    "batch_node_info, state, reason, raise_on_error, run_command_calls",
    [
        (
            [("queue1-st-c5xlarge-1", None, None), ("queue1-st-c5xlarge-2,queue1-st-c5xlarge-3", None, None)],
            None,
            None,
            False,
            [
                call(
                    "/opt/slurm/bin/scontrol update nodename=queue1-st-c5xlarge-1",
                    raise_on_error=False,
                    timeout=60,
                    shell=True,
                ),
                call(
                    "/opt/slurm/bin/scontrol update nodename=queue1-st-c5xlarge-2,queue1-st-c5xlarge-3",
                    raise_on_error=False,
                    timeout=60,
                    shell=True,
                ),
            ],
        ),
        (
            [
                ("queue1-st-c5xlarge-1", None, "hostname-1"),
                ("queue1-st-c5xlarge-2,queue1-st-c5xlarge-3", "addr-2,addr-3", None),
            ],
            "power_down",
            None,
            True,
            [
                call(
                    "/opt/slurm/bin/scontrol update state=power_down "
                    "nodename=queue1-st-c5xlarge-1 nodehostname=hostname-1",
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
                call(
                    "/opt/slurm/bin/scontrol update state=power_down "
                    "nodename=queue1-st-c5xlarge-2,queue1-st-c5xlarge-3 nodeaddr=addr-2,addr-3",
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
            ],
        ),
        (
            [
                ("queue1-st-c5xlarge-1", None, "hostname-1"),
                ("queue1-st-c5xlarge-[3-6]", "addr-[3-6]", "hostname-[3-6]"),
            ],
            "down",
            "debugging",
            True,
            [
                call(
                    (
                        '/opt/slurm/bin/scontrol update state=down reason="debugging"'
                        + " nodename=queue1-st-c5xlarge-1 nodehostname=hostname-1"
                    ),
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
                call(
                    (
                        '/opt/slurm/bin/scontrol update state=down reason="debugging"'
                        + " nodename=queue1-st-c5xlarge-[3-6] nodeaddr=addr-[3-6] nodehostname=hostname-[3-6]"
                    ),
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
            ],
        ),
    ],
)
def test_update_nodes(batch_node_info, state, reason, raise_on_error, run_command_calls, mocker):
    mocker.patch("common.schedulers.slurm_commands._batch_node_info", return_value=batch_node_info, autospec=True)
    cmd_mock = mocker.patch("common.schedulers.slurm_commands.run_command", autospec=True)
    update_nodes(batch_node_info, "some_nodeaddrs", "some_hostnames", state, reason, raise_on_error)
    cmd_mock.assert_has_calls(run_command_calls)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (SlurmNode("queue-name-st-t2micro-1", "nodeip", "nodehostname", "somestate", "queue-name"), True),
        (SlurmNode("queue-name-st-dy-t2micro-1", "nodeip", "nodehostname", "somestate", "queue-name-st"), False),
        (SlurmNode("queuename-dy-t2micro-1", "nodeip", "nodehostname", "somestate", "queuename"), False),
        (
            SlurmNode("queuename-dy-dy-dy-st-t2micro-1", "nodeip", "nodehostname", "somestate", "queuename-dy-dy-dy"),
            True,
        ),
    ],
)
def test_slurm_node_is_static(node, expected_output):
    assert_that(node.is_static).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (SlurmNode("queue-name-st-t2micro-1", "nodeip", "nodehostname", "somestate", "queue-name"), True),
        (
            SlurmNode("queuename-dy-t2micro-1", "queuename-dy-t2micro-1", "nodehostname", "somestate", "queuename"),
            False,
        ),
    ],
)
def test_slurm_node_is_nodeaddr_set(node, expected_output):
    assert_that(node.is_nodeaddr_set()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "somestate", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DRAIN", "queue1"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DRAIN", "queue1"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "COMPLETING+DRAIN", "queue1"), True),
    ],
)
def test_slurm_node_has_job(node, expected_output):
    assert_that(node.has_job()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "somestate", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DRAIN", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DRAIN", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE*+CLOUD+DRAIN", "queue1"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+DRAIN", "queue1"), True),
    ],
)
def test_slurm_node_is_drained(node, expected_output):
    assert_that(node.is_drained()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "somestate", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DOWN", "queue1"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DRAIN", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN*+CLOUD", "queue1"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+POWER", "queue1"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE~+CLOUD+POWERING_DOWN", "queue1"), False),
    ],
)
def test_slurm_node_is_down(node, expected_output):
    assert_that(node.is_down()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWER", "queue1"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DRAIN", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DOWN", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE#+CLOUD", "queue1"), True),
    ],
)
def test_slurm_node_is_up(node, expected_output):
    assert_that(node.is_up()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "partitions, state, run_command_calls, run_command_side_effects, expected_succeeded_partitions",
    [
        (
            ["part-1", "part-2"],
            PartitionStatus.INACTIVE,
            [
                call(
                    "/opt/slurm/bin/scontrol update partitionname=part-1 state=INACTIVE",
                    raise_on_error=True,
                    shell=True,
                ),
                call(
                    "/opt/slurm/bin/scontrol update partitionname=part-2 state=INACTIVE",
                    raise_on_error=True,
                    shell=True,
                ),
            ],
            [Exception, None],
            ["part-2"],
        ),
        (
            ["part-1", "part-2"],
            "UP",
            [
                call("/opt/slurm/bin/scontrol update partitionname=part-1 state=UP", raise_on_error=True, shell=True),
                call("/opt/slurm/bin/scontrol update partitionname=part-2 state=UP", raise_on_error=True, shell=True),
            ],
            [Exception, None],
            ["part-2"],
        ),
        (
            [],
            "UP",
            [],
            [],
            [],
        ),
    ],
)
def test_update_partitions(
    partitions, state, run_command_calls, run_command_side_effects, expected_succeeded_partitions, mocker
):
    run_command_spy = mocker.patch(
        "common.schedulers.slurm_commands.run_command", side_effect=run_command_side_effects, auto_spec=True
    )
    assert_that(update_partitions(partitions, state)).is_equal_to(expected_succeeded_partitions)
    if run_command_calls:
        run_command_spy.assert_has_calls(run_command_calls)
    else:
        run_command_spy.assert_not_called()


@pytest.mark.parametrize(
    (
        "mock_partitions",
        "state",
        "reset_node_info",
        "expected_reset_nodes_calls",
        "partitions_to_update",
        "mock_succeeded_partitions",
        "expected_results",
    ),
    [
        (
            [
                SlurmPartition("part-1", "node-1,node-2", "INACTIVE"),
                SlurmPartition("part-2", "node-3,node-4", "UP"),
            ],
            PartitionStatus.INACTIVE,
            True,
            [call("node-3,node-4", reason="stopping cluster")],
            ["part-2"],
            ["part-2"],
            True,
        ),
        (
            [
                SlurmPartition("part-1", "node-1,node-2", "DRAIN"),
                SlurmPartition("part-2", "node-3,node-4", "UP"),
            ],
            PartitionStatus.INACTIVE,
            True,
            [
                call("node-1,node-2", reason="stopping cluster"),
                call("node-3,node-4", reason="stopping cluster"),
            ],
            ["part-1", "part-2"],
            ["part-1", "part-2"],
            True,
        ),
        (
            [
                SlurmPartition("part-1", "node-1,node-2", "DRAIN"),
                SlurmPartition("part-2", "node-3,node-4", "UP"),
            ],
            PartitionStatus.INACTIVE,
            False,
            [],
            ["part-1", "part-2"],
            ["part-1", "part-2"],
            True,
        ),
        (
            [
                SlurmPartition("part-1", "node-1,node-2", "DRAIN"),
                SlurmPartition("part-2", "node-3,node-4", "UP"),
            ],
            PartitionStatus.UP,
            False,
            [],
            ["part-1"],
            [],
            False,
        ),
        (
            [
                SlurmPartition("part-1", "node-1,node-2", "DRAIN"),
                SlurmPartition("part-2", "node-3,node-4", "UP"),
            ],
            "UP",
            False,
            [],
            ["part-1"],
            ["part-1"],
            True,
        ),
    ],
)
def test_update_all_partitions(
    mock_partitions,
    state,
    reset_node_info,
    expected_reset_nodes_calls,
    partitions_to_update,
    mock_succeeded_partitions,
    expected_results,
    mocker,
):
    set_nodes_power_down_spy = mocker.patch("common.schedulers.slurm_commands.set_nodes_power_down", auto_spec=True)
    update_partitions_spy = mocker.patch(
        "common.schedulers.slurm_commands.update_partitions", return_value=mock_succeeded_partitions, auto_spec=True
    )
    get_part_spy = mocker.patch(
        "common.schedulers.slurm_commands.get_partition_info", return_value=mock_partitions, auto_spec=True
    )
    assert_that(update_all_partitions(state, reset_node_addrs_hostname=reset_node_info)).is_equal_to(expected_results)
    get_part_spy.assert_called_with(get_all_nodes=True)
    if expected_reset_nodes_calls:
        set_nodes_power_down_spy.assert_has_calls(expected_reset_nodes_calls)
    else:
        set_nodes_power_down_spy.assert_not_called()
    update_partitions_spy.assert_called_with(partitions_to_update, state)
