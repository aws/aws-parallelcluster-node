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
import os.path
import platform
from datetime import datetime, timezone
from typing import Dict, List
from unittest.mock import call, patch

import pytest
from assertpy import assert_that
from common.schedulers.slurm_commands import (
    SCONTROL,
    SCONTROL_OUTPUT_AWK_PARSER,
    SINFO,
    PartitionNodelistMapping,
    _batch_node_info,
    _get_all_partition_nodes,
    _get_partition_grep_filter,
    _get_slurm_nodes,
    _parse_nodes_info,
    get_nodes_info,
    is_static_node,
    parse_nodename,
    resume_powering_down_nodes,
    set_nodes_down,
    set_nodes_drain,
    set_nodes_idle,
    set_nodes_power_down,
    update_all_partitions,
    update_nodes,
    update_partitions,
)
from common.utils import check_command_output
from slurm_plugin.slurm_resources import DynamicNode, InvalidNodenameError, PartitionStatus, SlurmPartition, StaticNode


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
        ("queue-1-st-c5-xl-ar-g-e---1", "queue-1", "st", "c5-xl-ar-g-e--", False),
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
        with pytest.raises(InvalidNodenameError):
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
    "node_info, expected_parsed_nodes_output, invalid_name",
    [
        (
            "NodeName=multiple-st-c5xlarge-1\n"
            "NodeAddr=172.31.10.155\n"
            "NodeHostName=172-31-10-155\n"
            "State=MIXED+CLOUD\n"
            "Partitions=multiple\n"
            "SlurmdStartTime=2023-01-23T17:57:07\n"
            "######\n"
            "NodeName=multiple-dy-c5xlarge-2\n"
            "NodeAddr=172.31.7.218\n"
            "NodeHostName=172-31-7-218\n"
            "State=IDLE+CLOUD+POWER\n"
            "Partitions=multiple\n"
            "SlurmdStartTime=2023-01-23T17:57:07\n"
            "######\n",
            [
                StaticNode(
                    "multiple-st-c5xlarge-1",
                    "172.31.10.155",
                    "172-31-10-155",
                    "MIXED+CLOUD",
                    "multiple",
                    slurmdstarttime=datetime(2023, 1, 23, 17, 57, 7).astimezone(tz=timezone.utc),
                ),
                DynamicNode(
                    "multiple-dy-c5xlarge-2",
                    "172.31.7.218",
                    "172-31-7-218",
                    "IDLE+CLOUD+POWER",
                    "multiple",
                    slurmdstarttime=datetime(2023, 1, 23, 17, 57, 7).astimezone(tz=timezone.utc),
                ),
            ],
            False,
        ),
        (
            "NodeName=queue1-st-crt2micro-1\n"
            "NodeAddr=10.0.236.182\n"
            "NodeHostName=queue1-st-crt2micro-1\n"
            "State=IDLE+CLOUD+MAINTENANCE+RESERVED\n"
            "Partitions=queue1\n"
            "SlurmdStartTime=2023-01-23T17:57:07\n"
            "LastBusyTime=2023-10-13T10:13:20\n"
            "ReservationName=root_1\n"
            "######\n"
            "NodeName=queuep4d-dy-crp4d-1\n"
            "NodeAddr=queuep4d-dy-crp4d-1\n"
            "NodeHostName=queuep4d-dy-crp4d-1\n"
            "State=DOWN+CLOUD+MAINTENANCE+POWERED_DOWN+RESERVED\n"
            "Partitions=queuep4d\n"
            "SlurmdStartTime=None\n"
            "LastBusyTime=Unknown\n"
            "Reason=test [slurm@2023-10-20T07:18:35]\n"
            "ReservationName=root_6\n"
            "######\n",
            [
                StaticNode(
                    "queue1-st-crt2micro-1",
                    "10.0.236.182",
                    "queue1-st-crt2micro-1",
                    "IDLE+CLOUD+MAINTENANCE+RESERVED",
                    "queue1",
                    slurmdstarttime=datetime(2023, 1, 23, 17, 57, 7).astimezone(tz=timezone.utc),
                    lastbusytime=datetime(2023, 10, 13, 10, 13, 20).astimezone(tz=timezone.utc),
                    reservation_name="root_1",
                ),
                DynamicNode(
                    "queuep4d-dy-crp4d-1",
                    "queuep4d-dy-crp4d-1",
                    "queuep4d-dy-crp4d-1",
                    "DOWN+CLOUD+MAINTENANCE+POWERED_DOWN+RESERVED",
                    "queuep4d",
                    reservation_name="root_6",
                    reason="test [slurm@2023-10-20T07:18:35]",
                ),
            ],
            False,
        ),
        (
            "NodeName=multiple-dy-c5xlarge-3\n"
            "NodeAddr=multiple-dy-c5xlarge-3\n"
            "NodeHostName=multiple-dy-c5xlarge-3\n"
            "State=IDLE+CLOUD+POWER\n"
            "Partitions=multiple\n"
            "Reason=some reason  \n"
            "SlurmdStartTime=None\n"
            "######\n",
            [
                DynamicNode(
                    "multiple-dy-c5xlarge-3",
                    "multiple-dy-c5xlarge-3",
                    "multiple-dy-c5xlarge-3",
                    "IDLE+CLOUD+POWER",
                    "multiple",
                    "some reason  ",
                    slurmdstarttime=None,
                ),
            ],
            False,
        ),
        (
            "NodeName=multiple-dy-c5xlarge-4\n"
            "NodeAddr=multiple-dy-c5xlarge-4\n"
            "NodeHostName=multiple-dy-c5xlarge-4\n"
            "State=IDLE+CLOUD+POWER\n"
            "Partitions=multiple,multiple2\n"
            "Reason=(Code:InsufficientInstanceCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]\n"
            "SlurmdStartTime=2023-01-23T17:57:07\n"
            "######\n",
            [
                DynamicNode(
                    "multiple-dy-c5xlarge-4",
                    "multiple-dy-c5xlarge-4",
                    "multiple-dy-c5xlarge-4",
                    "IDLE+CLOUD+POWER",
                    "multiple,multiple2",
                    "(Code:InsufficientInstanceCapacity)Failure when resuming nodes [root@2023-01-31T21:24:55]",
                    slurmdstarttime=datetime(2023, 1, 23, 17, 57, 7).astimezone(tz=timezone.utc),
                ),
            ],
            False,
        ),
        (
            "NodeName=multiple-dy-c5xlarge-5\n"
            "NodeAddr=multiple-dy-c5xlarge-5\n"
            "NodeHostName=multiple-dy-c5xlarge-5\n"
            "State=IDLE+CLOUD+POWER\n"
            "SlurmdStartTime=2023-01-23T17:57:07\n"
            "LastBusyTime=2023-01-23T17:57:07\n"
            # missing partitions
            "######\n"
            # Invalid node name
            "NodeName=test-no-partition\n"
            "NodeAddr=test-no-partition\n"
            "NodeHostName=test-no-partition\n"
            "State=IDLE+CLOUD+POWER\n"
            "SlurmdStartTime=2023-01-23T17:57:07\n"
            # missing partitions
            "######\n",
            [
                DynamicNode(
                    "multiple-dy-c5xlarge-5",
                    "multiple-dy-c5xlarge-5",
                    "multiple-dy-c5xlarge-5",
                    "IDLE+CLOUD+POWER",
                    None,
                    slurmdstarttime=datetime(2023, 1, 23, 17, 57, 7).astimezone(tz=timezone.utc),
                    lastbusytime=datetime(2023, 1, 23, 17, 57, 7).astimezone(tz=timezone.utc),
                ),
            ],
            True,
        ),
    ],
)
def test_parse_nodes_info(node_info, expected_parsed_nodes_output, invalid_name, caplog):
    parsed_node_info = _parse_nodes_info(node_info)
    assert_that(parsed_node_info).is_equal_to(expected_parsed_nodes_output)
    if invalid_name:
        assert_that(caplog.text).contains("Ignoring node test-no-partition because it has an invalid name")


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
    "batch_node_info, state, reason, raise_on_error, run_command_calls, expected_exception",
    [
        (
            [("queue1-st-c5xlarge-1", None, None), ("queue1-st-c5xlarge-2,queue1-st-c5xlarge-3", None, None)],
            None,
            None,
            False,
            [
                call(
                    "sudo /opt/slurm/bin/scontrol update nodename=queue1-st-c5xlarge-1",
                    raise_on_error=False,
                    timeout=60,
                    shell=True,
                ),
                call(
                    "sudo /opt/slurm/bin/scontrol update nodename=queue1-st-c5xlarge-2,queue1-st-c5xlarge-3",
                    raise_on_error=False,
                    timeout=60,
                    shell=True,
                ),
            ],
            None,
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
                    "sudo /opt/slurm/bin/scontrol update state=power_down "
                    "nodename=queue1-st-c5xlarge-1 nodehostname=hostname-1",
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
                call(
                    "sudo /opt/slurm/bin/scontrol update state=power_down "
                    "nodename=queue1-st-c5xlarge-2,queue1-st-c5xlarge-3 nodeaddr=addr-2,addr-3",
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
            ],
            None,
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
                        'sudo /opt/slurm/bin/scontrol update state=down reason="debugging"'
                        + " nodename=queue1-st-c5xlarge-1 nodehostname=hostname-1"
                    ),
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
                call(
                    (
                        'sudo /opt/slurm/bin/scontrol update state=down reason="debugging"'
                        + " nodename=queue1-st-c5xlarge-[3-6] nodeaddr=addr-[3-6] nodehostname=hostname-[3-6]"
                    ),
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
            ],
            None,
        ),
        (
            [
                ("queue1-st-c5xlarge-1 & rm -rf /", None, "hostname-1"),
            ],
            "down",
            "debugging",
            None,
            None,
            ValueError,
        ),
        (
            [
                ("queue1-st-c5xlarge-1", " & rm -rf /", "hostname-1"),
            ],
            "down",
            "debugging",
            None,
            None,
            ValueError,
        ),
        (
            [
                ("queue1-st-c5xlarge-1", None, " & rm -rf /"),
            ],
            "down",
            "debugging",
            None,
            None,
            ValueError,
        ),
        (
            [
                ("queue1-st-c5xlarge-1", None, "hostname-1"),
            ],
            " & rm -rf /",
            "debugging",
            None,
            None,
            ValueError,
        ),
        (
            [
                ("queue1-st-c5xlarge-1", None, "hostname-1"),
            ],
            "down",
            " & rm -rf /",
            None,
            None,
            ValueError,
        ),
    ],
)
def test_update_nodes(batch_node_info, state, reason, raise_on_error, run_command_calls, expected_exception, mocker):
    mocker.patch("common.schedulers.slurm_commands._batch_node_info", return_value=batch_node_info, autospec=True)
    if expected_exception is ValueError:
        with pytest.raises(ValueError):
            update_nodes(batch_node_info, "some_nodeaddrs", "some_hostnames", state, reason, raise_on_error)
    else:
        cmd_mock = mocker.patch("common.schedulers.slurm_commands.run_command", autospec=True)
        update_nodes(batch_node_info, "some_nodeaddrs", "some_hostnames", state, reason, raise_on_error)
        cmd_mock.assert_has_calls(run_command_calls)


@pytest.mark.parametrize(
    "partitions, state, run_command_calls, run_command_side_effects, expected_succeeded_partitions",
    [
        (
            ["part-1", "part-2"],
            PartitionStatus.INACTIVE,
            [
                call(
                    "sudo /opt/slurm/bin/scontrol update partitionname=part-1 state=INACTIVE",
                    raise_on_error=True,
                    shell=True,
                ),
                call(
                    "sudo /opt/slurm/bin/scontrol update partitionname=part-2 state=INACTIVE",
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
                call(
                    "sudo /opt/slurm/bin/scontrol update partitionname=part-1 state=UP", raise_on_error=True, shell=True
                ),
                call(
                    "sudo /opt/slurm/bin/scontrol update partitionname=part-2 state=UP", raise_on_error=True, shell=True
                ),
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
        (
            ["part-1", "part-2"],
            "UP & rm -rf /",
            [],
            [],
            ValueError,
        ),
        (
            ["part-1 & rm -rf /", "part-2"],
            "UP",
            [
                call(
                    "sudo /opt/slurm/bin/scontrol update partitionname=part-2 state=UP", raise_on_error=True, shell=True
                ),
            ],
            [None, None],
            ["part-2"],
        ),
    ],
)
def test_update_partitions(
    partitions, state, run_command_calls, run_command_side_effects, expected_succeeded_partitions, mocker
):
    run_command_spy = mocker.patch(
        "common.schedulers.slurm_commands.run_command", side_effect=run_command_side_effects, autospec=True
    )
    if expected_succeeded_partitions is ValueError:
        with pytest.raises(ValueError):
            update_partitions(partitions, state)
    else:
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
    set_nodes_power_down_spy = mocker.patch("common.schedulers.slurm_commands.set_nodes_power_down", autospec=True)
    update_partitions_spy = mocker.patch(
        "common.schedulers.slurm_commands.update_partitions", return_value=mock_succeeded_partitions, autospec=True
    )
    get_partitions_info_mocked = mocker.patch(
        "common.schedulers.slurm_commands.get_partitions_info", return_value=mock_partitions, autospec=True
    )
    assert_that(update_all_partitions(state, reset_node_addrs_hostname=reset_node_info)).is_equal_to(expected_results)
    get_partitions_info_mocked.assert_called()
    if expected_reset_nodes_calls:
        set_nodes_power_down_spy.assert_has_calls(expected_reset_nodes_calls)
    else:
        set_nodes_power_down_spy.assert_not_called()
    update_partitions_spy.assert_called_with(partitions_to_update, state)


def test_resume_powering_down_nodes(mocker):
    get_slurm_nodes_mocked = mocker.patch("common.schedulers.slurm_commands._get_slurm_nodes", autospec=True)
    update_nodes_mocked = mocker.patch("common.schedulers.slurm_commands.update_nodes", autospec=True)

    resume_powering_down_nodes()
    get_slurm_nodes_mocked.assert_called_with(states="powering_down")
    update_nodes_mocked.assert_called_with(nodes=get_slurm_nodes_mocked(), state="resume", raise_on_error=False)


@pytest.mark.parametrize(
    "states, partition_name, partition_nodelist_mapping, expected_command, expected_exception",
    [
        pytest.param(
            None,
            None,
            {"test": "test-st-cr1-[1-10],test-dy-cr2-[1-2]"},
            f"{SINFO} -h -N -o %N -p test",
            None,
            id="No partition nor state provided",
        ),
        pytest.param(
            "power_down,powering_down",
            "test",
            {"test": "test-st-cr1-[1-10]"},
            f"{SINFO} -h -N -o %N -p test -t power_down,powering_down",
            None,
            id="Partition provided",
        ),
        pytest.param(
            "power_down,& rm -rf",
            "test",
            {"test": "test-st-cr1-[1-10]"},
            None,
            ValueError,
            id="Bad state provided",
        ),
        pytest.param(
            "power_down,powering_down",
            "test & rm -rf",
            {"test": "test-st-cr1-[1-10]"},
            None,
            ValueError,
            id="Bad partition provided",
        ),
    ],
)
def test_get_slurm_nodes_argument_validation(
    mocker,
    states,
    partition_name,
    partition_nodelist_mapping,
    expected_command,
    expected_exception,
):
    mapping_instance = PartitionNodelistMapping.instance()
    mapping_instance.get_partition_nodelist_mapping = mocker.MagicMock(return_value=partition_nodelist_mapping)
    if expected_exception is ValueError:
        with pytest.raises(ValueError):
            _get_slurm_nodes(states=states, partition_name=partition_name, command_timeout=10)
    else:
        check_command_output_mocked = mocker.patch(
            "common.schedulers.slurm_commands.check_command_output", autospec=True
        )

        _get_slurm_nodes(states=states, partition_name=partition_name, command_timeout=10)
        check_command_output_mocked.assert_called_with(expected_command, timeout=10, shell=True)


@pytest.mark.parametrize(
    "states, partition_name, partition_nodelist_mapping, expected_command",
    [
        pytest.param(
            None,
            None,
            {"test": "test-st-cr1-[1-10],test-dy-cr2-[1-2]"},
            f"{SINFO} -h -N -o %N -p test",
            id="No partition nor state provided, one PC-managed partition in cluster",
        ),
        pytest.param(
            None,
            None,
            {
                "test": "test-st-cr1-[1-10],test-dy-cr2-[1-2]",
                "test2": "test2-st-cr1-[1-10],test2-dy-cr2-[1-2]",
            },
            f"{SINFO} -h -N -o %N -p test,test2",
            id="No partition nor state provided, two PC-managed partitions in cluster",
        ),
        pytest.param(
            "power_down,powering_down",
            "test",
            {
                "test": "test-st-cr1-[1-10],test-dy-cr2-[1-2]",
                "test2": "test2-st-cr1-[1-10],test2-dy-cr2-[1-2]",
            },
            f"{SINFO} -h -N -o %N -p test -t power_down,powering_down",
            id="First partition provided, two PC-managed partition in cluster",
        ),
        pytest.param(
            "power_down,powering_down",
            "test2",
            {
                "test": "test-st-cr1-[1-10],test-dy-cr2-[1-2]",
                "test2": "test2-st-cr1-[1-10],test2-dy-cr2-[1-2]",
            },
            f"{SINFO} -h -N -o %N -p test2 -t power_down,powering_down",
            id="Second partition provided, two PC-managed partition in cluster",
        ),
    ],
)
def test_get_slurm_nodes(
    mocker,
    states,
    partition_name,
    partition_nodelist_mapping,
    expected_command,
):
    """Test for the main functionality of the _get_slurm_nodes() function."""
    mapping_instance = PartitionNodelistMapping.instance()
    mapping_instance.get_partition_nodelist_mapping = mocker.MagicMock(return_value=partition_nodelist_mapping)
    check_command_output_mocked = mocker.patch("common.schedulers.slurm_commands.check_command_output", autospec=True)
    _get_slurm_nodes(states=states, partition_name=partition_name, command_timeout=10)
    check_command_output_mocked.assert_called_with(expected_command, timeout=10, shell=True)


@pytest.mark.parametrize(
    "partition_name, cmd_timeout, run_command_call, run_command_side_effect, expected_exception",
    [
        (
            "partition",
            30,
            f"{SINFO} -h -p partition -o %N",
            None,
            None,
        ),
        (
            "partition & rm -rf /",
            None,
            None,
            None,
            ValueError,
        ),
    ],
)
def test_get_all_partition_nodes(
    partition_name, cmd_timeout, run_command_call, run_command_side_effect, expected_exception, mocker
):
    if expected_exception is ValueError:
        with pytest.raises(ValueError):
            _get_all_partition_nodes(partition_name, cmd_timeout)
    else:
        check_command_output_mocked = mocker.patch(
            "common.schedulers.slurm_commands.check_command_output",
            side_effect=run_command_side_effect,
            autospec=True,
        )
        _get_all_partition_nodes(partition_name, cmd_timeout)
        check_command_output_mocked.assert_called_with(run_command_call, timeout=30, shell=True)


@pytest.mark.parametrize(
    "nodes, cmd_timeout, partition_nodelist_mapping, expected_command",
    [
        pytest.param(
            "node1 node2",
            30,
            {
                "test": "test-st-cr1-[1-10],test-dy-cr2-[1-2]",
                "test2": "test2-st-cr1-[1-10],test2-dy-cr2-[1-2]",
            },
            f"{SCONTROL} show nodes node1 node2 | {SCONTROL_OUTPUT_AWK_PARSER}",
            id="Test with nodes provided by caller",
        ),
        pytest.param(
            "",
            30,
            {
                "test": "test-st-cr1-[1-10],test-dy-cr2-[1-2]",
                "test2": "test2-st-cr1-[1-10],test2-dy-cr2-[1-2]",
            },
            f"{SCONTROL} show nodes test-st-cr1-[1-10],test-dy-cr2-[1-2],test2-st-cr1-[1-10],test2-dy-cr2-[1-2] | "
            f"{SCONTROL_OUTPUT_AWK_PARSER}",
            id="Test with nodes not provided by caller. Nodes are retrieved from PC-managed partitions ",
        ),
    ],
)
def test_get_nodes_info(nodes, cmd_timeout, partition_nodelist_mapping: Dict, expected_command, mocker):
    # Mock get_partitions() method of the PartitionNodelistMapping singleton used in get_nodes_info()
    mocker.patch(
        "common.schedulers.slurm_commands.PartitionNodelistMapping.get_partitions",
        return_value=list(partition_nodelist_mapping.keys()),
    )
    # Mock _get_all_partition_nodes function used in get_nodes_info()
    mocker.patch(
        "common.schedulers.slurm_commands._get_all_partition_nodes",
        return_value=",".join([nodelist for partition, nodelist in partition_nodelist_mapping.items()]),
    )
    # Mock check_command_output call performed in get_nodes_info()
    check_command_output_mocked = mocker.patch(
        "common.schedulers.slurm_commands.check_command_output",
        autospec=True,
    )
    get_nodes_info(nodes, cmd_timeout)
    check_command_output_mocked.assert_called_with(expected_command, timeout=cmd_timeout, shell=True)


@pytest.mark.parametrize(
    "nodes, cmd_timeout, run_command_call, run_command_side_effect, expected_exception",
    [
        (
            "node1 node2",
            30,
            f"{SCONTROL} show nodes node1 node2 | {SCONTROL_OUTPUT_AWK_PARSER}",
            None,
            None,
        ),
        (
            "node1 & rm -rf / node2",
            None,
            None,
            None,
            ValueError,
        ),
    ],
)
def test_get_nodes_info_argument_validation(
    nodes, cmd_timeout, run_command_call, run_command_side_effect, expected_exception, mocker
):
    if expected_exception is ValueError:
        with pytest.raises(ValueError):
            get_nodes_info(nodes, cmd_timeout)
    else:
        check_command_output_mocked = mocker.patch(
            "common.schedulers.slurm_commands.check_command_output",
            side_effect=run_command_side_effect,
            autospec=True,
        )
        get_nodes_info(nodes, cmd_timeout)
        check_command_output_mocked.assert_called_with(run_command_call, timeout=30, shell=True)


@pytest.mark.parametrize(
    "scontrol_output, expected_parsed_output",
    [
        (
            (
                "NodeName=queue1-st-compute-resource-1-1 Arch=x86_64 CoresPerSocket=1\n"
                "   CPUAlloc=0 CPUEfctv=2 CPUTot=2 CPULoad=0.03\n"
                "   AvailableFeatures=static,t2.medium,compute-resource-1\n"
                "   ActiveFeatures=static,t2.medium,compute-resource-1\n"
                "   Gres=(null)\n"
                "   NodeAddr=192.168.123.191 NodeHostName=queue1-st-compute-resource-1-1 Version=22.05.7\n"
                "   OS=Linux 5.15.0-1028-aws #32~20.04.1-Ubuntu SMP Mon Jan 9 18:02:08 UTC 2023\n"
                "   RealMemory=3891 AllocMem=0 FreeMem=3018 Sockets=2 Boards=1\n"
                "   State=DOWN+CLOUD+REBOOT_ISSUED ThreadsPerCore=1 TmpDisk=0 Weight=1 Owner=N/A MCS_label=N/A\n"
                "   NextState=RESUME\n"
                "   Partitions=queue1\n"
                "   BootTime=2023-01-26T09:56:30 SlurmdStartTime=2023-01-26T09:57:15\n"
                "   LastBusyTime=2023-01-26T09:57:15\n"
                "   CfgTRES=cpu=2,mem=3891M,billing=2\n"
                "   AllocTRES=\n"
                "   CapWatts=n/a\n"
                "   CurrentWatts=0 AveWatts=0\n"
                "   ExtSensorsJoules=n/s ExtSensorsWatts=0 ExtSensorsTemp=n/s\n"
                "   Reason=Reboot ASAP : reboot issued [slurm@2023-01-26T10:11:39]\n"
                "   Comment=some comment \n\n"
                "NodeName=queue1-st-compute-resource-1-2 Arch=x86_64 CoresPerSocket=1\n"
                "   CPUAlloc=0 CPUEfctv=2 CPUTot=2 CPULoad=0.03\n"
                "   AvailableFeatures=static,t2.medium,compute-resource-1\n"
                "   ActiveFeatures=static,t2.medium,compute-resource-1\n"
                "   Gres=(null)\n"
                "   NodeAddr=192.168.123.192 NodeHostName=queue1-st-compute-resource-1-2 Version=22.05.7\n"
                "   OS=Linux 5.15.0-1028-aws #32~20.04.1-Ubuntu SMP Mon Jan 9 18:02:08 UTC 2023\n"
                "   RealMemory=3891 AllocMem=0 FreeMem=3018 Sockets=2 Boards=1\n"
                "   State=DOWN+CLOUD+REBOOT_ISSUED ThreadsPerCore=1 TmpDisk=0 Weight=1 Owner=N/A MCS_label=N/A\n"
                "   NextState=RESUME\n"
                "   Partitions=queue1\n"
                "   BootTime=2023-01-26T09:56:30 SlurmdStartTime=2023-01-26T09:57:16\n"
                "   LastBusyTime=Unknown\n"
                "   CfgTRES=cpu=2,mem=3891M,billing=2\n"
                "   AllocTRES=\n"
                "   CapWatts=n/a\n"
                "   CurrentWatts=0 AveWatts=0\n"
                "   ExtSensorsJoules=n/s ExtSensorsWatts=0 ExtSensorsTemp=n/s\n"
                "   Reason=Reboot ASAP : reboot issued [slurm@2023-01-26T10:11:40]\n"
                "   Comment=some comment \n\n"
                "NodeName=queue1-st-crt2micro-1 Arch=x86_64 CoresPerSocket=1\n"
                "   CPUAlloc=0 CPUEfctv=1 CPUTot=1 CPULoad=0.00\n"
                "   AvailableFeatures=static,t2.micro,crt2micro\n"
                "   ActiveFeatures=static,t2.micro,crt2micro\n"
                "   Gres=(null)\n"
                "   NodeAddr=10.0.236.182 NodeHostName=queue1-st-crt2micro-1 Version=23.02.4\n"
                "   OS=Linux 5.10.186-179.751.amzn2.x86_64 #1 SMP Tue Aug 1 20:51:38 UTC 2023\n"
                "   RealMemory=972 AllocMem=0 FreeMem=184 Sockets=1 Boards=1\n"
                "   State=IDLE+CLOUD+MAINTENANCE+RESERVED ThreadsPerCore=1 TmpDisk=0 Weight=1 Owner=N/A MCS_label=N/A\n"
                "   Partitions=queue1\n"
                "   BootTime=2023-10-13T10:09:58 SlurmdStartTime=2023-10-13T10:13:17\n"
                "   LastBusyTime=2023-10-13T10:13:20 ResumeAfterTime=None\n"
                "   CfgTRES=cpu=1,mem=972M,billing=1\n"
                "   AllocTRES=\n"
                "   CapWatts=n/a\n"
                "   CurrentWatts=0 AveWatts=0\n"
                "   ExtSensorsJoules=n/s ExtSensorsWatts=0 ExtSensorsTemp=n/s\n"
                "   ReservationName=root_5\n"
            ),
            (
                "NodeName=queue1-st-compute-resource-1-1\nNodeAddr=192.168.123.191\n"
                "NodeHostName=queue1-st-compute-resource-1-1\nState=DOWN+CLOUD+REBOOT_ISSUED\nPartitions=queue1\n"
                "SlurmdStartTime=2023-01-26T09:57:15\n"
                "LastBusyTime=2023-01-26T09:57:15\n"
                "Reason=Reboot ASAP : reboot issued [slurm@2023-01-26T10:11:39]\n######\n"
                "NodeName=queue1-st-compute-resource-1-2\nNodeAddr=192.168.123.192\n"
                "NodeHostName=queue1-st-compute-resource-1-2\nState=DOWN+CLOUD+REBOOT_ISSUED\nPartitions=queue1\n"
                "SlurmdStartTime=2023-01-26T09:57:16\n"
                "LastBusyTime=Unknown\n"
                "Reason=Reboot ASAP : reboot issued [slurm@2023-01-26T10:11:40]\n######\n"
                "NodeName=queue1-st-crt2micro-1\nNodeAddr=10.0.236.182\n"
                "NodeHostName=queue1-st-crt2micro-1\nState=IDLE+CLOUD+MAINTENANCE+RESERVED\nPartitions=queue1\n"
                "SlurmdStartTime=2023-10-13T10:13:17\n"
                "LastBusyTime=2023-10-13T10:13:20\nReservationName=root_5######\n"
            ),
        )
    ],
)
def test_scontrol_output_awk_parser(scontrol_output, expected_parsed_output):
    # This test makes use of grep option -P that is only supported by GNU grep.
    # To have the test passing on MacOS you have to install GNU Grep: brew install grep.
    scontrol_awk_parser = (
        SCONTROL_OUTPUT_AWK_PARSER
        if platform.system() != "Darwin"
        else SCONTROL_OUTPUT_AWK_PARSER.replace("grep", "ggrep")
    )
    parsed_output = check_command_output(f'echo "{scontrol_output}" | {scontrol_awk_parser}', shell=True)
    assert_that(parsed_output).is_equal_to(expected_parsed_output)


@pytest.mark.parametrize(
    "partitions, expected_grep_filter",
    [
        pytest.param(
            ["queue1", "queue2", "queue3"],
            ' | grep -e "PartitionName=queue1" -e "PartitionName=queue2" -e "PartitionName=queue3"',
            id="Regular list of partitions",
        ),
        pytest.param([], "", id="Empty list of partitions (it should not be possible as of 3.7.0)"),
    ],
)
def test_grep_partition_filter(partitions: List[str], expected_grep_filter: str):
    assert_that(_get_partition_grep_filter(partitions)).is_equal_to(expected_grep_filter)


class TestPartitionNodelistMapping:
    @pytest.mark.parametrize(
        "expected_partition_nodelist_mapping",
        [
            pytest.param(
                {
                    "test": "test-st-cr1-[1-10],test-dy-cr2-[1-2]",
                    "test2": "test2-st-cr1-[1-10],test2-dy-cr2-[1-2]",
                },
            ),
        ],
    )
    def test_get_partition_nodelist_mapping(self, test_datadir, expected_partition_nodelist_mapping):
        mapping_instance = PartitionNodelistMapping.instance()
        with patch("common.schedulers.slurm_commands.SLURM_CONF_DIR", os.path.join(test_datadir, "slurm_dir/etc")):
            partition_nodelist_mapping = mapping_instance.get_partition_nodelist_mapping()
        assert_that(partition_nodelist_mapping).is_equal_to(expected_partition_nodelist_mapping)

    @pytest.mark.parametrize(
        "expected_partitions",
        [
            pytest.param(["test", "test2"]),
        ],
    )
    def test_get_partitions(self, test_datadir, expected_partitions):
        mapping_instance = PartitionNodelistMapping.instance()
        with patch("common.schedulers.slurm_commands.SLURM_CONF_DIR", os.path.join(test_datadir, "slurm_dir/etc")):
            partitions = list(mapping_instance.get_partitions())
        assert_that(partitions).is_equal_to(expected_partitions)

    def test_get_singleton_instance(self):
        PartitionNodelistMapping.instance()
        assert_that(PartitionNodelistMapping._instance).is_not_none()

    def test_reset_singleton_instance(self):
        mapping_instance = PartitionNodelistMapping.instance()
        mapping_instance.reset()
        assert_that(PartitionNodelistMapping._instance).is_none()
