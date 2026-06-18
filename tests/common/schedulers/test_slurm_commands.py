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
import subprocess
from datetime import datetime, timezone
from typing import Dict
from unittest.mock import call, patch

import pytest
from assertpy import assert_that
from common.schedulers.slurm_commands import (
    SCONTROL,
    SCONTROL_NODE_INFO_FIELD_REGEX,
    SCONTROL_PARTITION_INFO_FIELD_REGEX,
    SINFO,
    PartitionNodelistMapping,
    _batch_node_info,
    _extract_scontrol_records,
    _get_all_partition_nodes,
    _get_slurm_nodes,
    _parse_nodes_info,
    _parse_partitions_info,
    _run_scontrol_command,
    get_nodes_info,
    is_static_node,
    parse_nodename,
    reset_nodes_in_inactive_partitions,
    resume_powering_down_nodes,
    set_nodes_down,
    set_nodes_drain,
    set_nodes_idle,
    set_nodes_power_down,
    update_all_partitions,
    update_nodes,
    update_partitions,
)
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
    "node_records, expected_parsed_nodes_output, invalid_name",
    [
        (
            [
                {
                    "NodeName": "multiple-st-c5xlarge-1",
                    "NodeAddr": "172.31.10.155",
                    "NodeHostName": "172-31-10-155",
                    "State": "MIXED+CLOUD",
                    "Partitions": "multiple",
                    "SlurmdStartTime": "2023-01-23T17:57:07",
                },
                {
                    "NodeName": "multiple-dy-c5xlarge-2",
                    "NodeAddr": "172.31.7.218",
                    "NodeHostName": "172-31-7-218",
                    "State": "IDLE+CLOUD+POWER",
                    "Partitions": "multiple",
                    "SlurmdStartTime": "2023-01-23T17:57:07",
                },
            ],
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
            [
                {
                    "NodeName": "queue1-st-crt2micro-1",
                    "NodeAddr": "10.0.236.182",
                    "NodeHostName": "queue1-st-crt2micro-1",
                    "State": "IDLE+CLOUD+MAINTENANCE+RESERVED",
                    "Partitions": "queue1",
                    "SlurmdStartTime": "2023-01-23T17:57:07",
                    "LastBusyTime": "2023-10-13T10:13:20",
                    "ReservationName": "root_1",
                },
                {
                    "NodeName": "queuep4d-dy-crp4d-1",
                    "NodeAddr": "queuep4d-dy-crp4d-1",
                    "NodeHostName": "queuep4d-dy-crp4d-1",
                    "State": "DOWN+CLOUD+MAINTENANCE+POWERED_DOWN+RESERVED",
                    "Partitions": "queuep4d",
                    "SlurmdStartTime": "None",
                    "LastBusyTime": "Unknown",
                    "Reason": "test [slurm@2023-10-20T07:18:35]",
                    "ReservationName": "root_6",
                },
            ],
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
            [
                {
                    "NodeName": "multiple-dy-c5xlarge-3",
                    "NodeAddr": "multiple-dy-c5xlarge-3",
                    "NodeHostName": "multiple-dy-c5xlarge-3",
                    "State": "IDLE+CLOUD+POWER",
                    "Partitions": "multiple",
                    "Reason": "some reason  ",
                    "SlurmdStartTime": "None",
                },
            ],
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
            [
                {
                    "NodeName": "multiple-dy-c5xlarge-3",
                    "NodeAddr": "multiple-dy-c5xlarge-3",
                    "NodeHostName": "multiple-dy-c5xlarge-3",
                    "State": "IDLE+CLOUD+POWER",
                    "Partitions": "multiple",
                    "Reason": "some reason containing key=value entries ",
                    "SlurmdStartTime": "None",
                },
            ],
            [
                DynamicNode(
                    "multiple-dy-c5xlarge-3",
                    "multiple-dy-c5xlarge-3",
                    "multiple-dy-c5xlarge-3",
                    "IDLE+CLOUD+POWER",
                    "multiple",
                    "some reason containing key=value entries ",
                    slurmdstarttime=None,
                ),
            ],
            False,
        ),
        (
            [
                {
                    "NodeName": "multiple-dy-c5xlarge-5",
                    "NodeAddr": "multiple-dy-c5xlarge-5",
                    "NodeHostName": "multiple-dy-c5xlarge-5",
                    "State": "IDLE+CLOUD+POWER",
                    "SlurmdStartTime": "2023-01-23T17:57:07",
                    "LastBusyTime": "2023-01-23T17:57:07",
                    # missing partitions
                },
                # Invalid node name
                {
                    "NodeName": "test-no-partition",
                    "NodeAddr": "test-no-partition",
                    "NodeHostName": "test-no-partition",
                    "State": "IDLE+CLOUD+POWER",
                    "SlurmdStartTime": "2023-01-23T17:57:07",
                    # missing partitions
                },
            ],
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
        # Test case: InstanceId is parsed from scontrol show nodes output; "(null)" is normalized to None
        (
            [
                {
                    "NodeName": "queue1-st-c5xlarge-1",
                    "NodeAddr": "10.0.1.1",
                    "NodeHostName": "queue1-st-c5xlarge-1",
                    "State": "IDLE+CLOUD",
                    "Partitions": "queue1",
                    "SlurmdStartTime": "2023-01-23T17:57:07",
                    "InstanceId": "i-0abc123def456",
                },
                {
                    "NodeName": "queue1-dy-c5xlarge-2",
                    "NodeAddr": "queue1-dy-c5xlarge-2",
                    "NodeHostName": "queue1-dy-c5xlarge-2",
                    "State": "IDLE+CLOUD+POWER",
                    "Partitions": "queue1",
                    "SlurmdStartTime": "None",
                    "InstanceId": "(null)",
                },
            ],
            [
                StaticNode(
                    "queue1-st-c5xlarge-1",
                    "10.0.1.1",
                    "queue1-st-c5xlarge-1",
                    "IDLE+CLOUD",
                    "queue1",
                    slurmdstarttime=datetime(2023, 1, 23, 17, 57, 7).astimezone(tz=timezone.utc),
                    instance_id="i-0abc123def456",
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-2",
                    "queue1-dy-c5xlarge-2",
                    "queue1-dy-c5xlarge-2",
                    "IDLE+CLOUD+POWER",
                    "queue1",
                    slurmdstarttime=None,
                    instance_id=None,
                ),
            ],
            False,
        ),
    ],
)
def test_parse_nodes_info(node_records, expected_parsed_nodes_output, invalid_name, caplog):
    parsed_node_info = _parse_nodes_info(node_records)
    assert_that(parsed_node_info).is_equal_to(expected_parsed_nodes_output)
    if invalid_name:
        assert_that(caplog.text).contains("Ignoring node test-no-partition because it has an invalid name")


@pytest.mark.parametrize(
    "nodenames, nodeaddrs, hostnames, instance_ids, batch_size, expected_result",
    [
        (
            "queue1-st-c5xlarge-1,queue1-st-c5xlarge-2,queue1-st-c5xlarge-3",
            None,
            None,
            None,
            2,
            [("queue1-st-c5xlarge-1,queue1-st-c5xlarge-2,queue1-st-c5xlarge-3", None, None, None)],
        ),
        (
            # Only split on commas after bucket
            # So nodename like queue1-st-c5xlarge-[1,3] can be processed safely
            "queue1-st-c5xlarge-[1-2],queue1-st-c5xlarge-2,queue1-st-c5xlarge-3,queue1-st-c5xlarge-[4,6]",
            "nodeaddr-[1-2],nodeaddr-2,nodeaddr-3,nodeaddr-[4,6]",
            None,
            None,
            2,
            [
                (
                    "queue1-st-c5xlarge-[1-2],queue1-st-c5xlarge-2,queue1-st-c5xlarge-3,queue1-st-c5xlarge-[4,6]",
                    "nodeaddr-[1-2],nodeaddr-2,nodeaddr-3,nodeaddr-[4,6]",
                    None,
                    None,
                )
            ],
        ),
        (
            "queue1-st-c5xlarge-[1-2],queue1-st-c5xlarge-2,queue1-st-c5xlarge-[3],queue1-st-c5xlarge-[4,6]",
            "nodeaddr-[1-2],nodeaddr-2,nodeaddr-[3],nodeaddr-[4,6]",
            "nodehostname-[1-2],nodehostname-2,nodehostname-[3],nodehostname-[4,6]",
            None,
            2,
            [
                (
                    "queue1-st-c5xlarge-[1-2],queue1-st-c5xlarge-2,queue1-st-c5xlarge-[3]",
                    "nodeaddr-[1-2],nodeaddr-2,nodeaddr-[3]",
                    "nodehostname-[1-2],nodehostname-2,nodehostname-[3]",
                    None,
                ),
                ("queue1-st-c5xlarge-[4,6]", "nodeaddr-[4,6]", "nodehostname-[4,6]", None),
            ],
        ),
        (
            # nodeaddr and instanceid are batched together, distributed across the nodes in each batch
            ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3"],
            ["nodeaddr-1", "nodeaddr-2", "nodeaddr-3"],
            None,
            ["i-1", "i-2", "i-3"],
            2,
            [
                ("queue1-st-c5xlarge-1,queue1-st-c5xlarge-2", "nodeaddr-1,nodeaddr-2", None, "i-1,i-2"),
                ("queue1-st-c5xlarge-3", "nodeaddr-3", None, "i-3"),
            ],
        ),
        ("queue1-st-c5xlarge-1,queue1-st-c5xlarge-[2],queue1-st-c5xlarge-3", ["nodeaddr-1"], None, None, 2, ValueError),
        (
            "queue1-st-c5xlarge-1,queue1-st-c5xlarge-[2],queue1-st-c5xlarge-3",
            None,
            ["nodehostname-1"],
            None,
            2,
            ValueError,
        ),
        (
            # instance_ids count does not match nodenames count
            "queue1-st-c5xlarge-1,queue1-st-c5xlarge-[2],queue1-st-c5xlarge-3",
            None,
            None,
            ["i-1"],
            2,
            ValueError,
        ),
        (
            "queue1-st-c5xlarge-1,queue1-st-c5xlarge-2,queue1-st-c5xlarge-3",
            ["nodeaddr-1", "nodeaddr-2"],
            "nodehostname-1,nodehostname-2,nodehostname-3",
            None,
            2,
            ValueError,
        ),
        (
            ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2", "queue1-st-c5xlarge-3"],
            "nodeaddr-[1],nodeaddr-[2],nodeaddr-3",
            ["nodehostname-1", "nodehostname-2", "nodehostname-3"],
            None,
            2,
            [
                (
                    "queue1-st-c5xlarge-1,queue1-st-c5xlarge-2",
                    "nodeaddr-[1],nodeaddr-[2]",
                    "nodehostname-1,nodehostname-2",
                    None,
                ),
                ("queue1-st-c5xlarge-3", "nodeaddr-3", "nodehostname-3", None),
            ],
        ),
        (
            # Test with strings of same length but different number of node entries
            "queue1-st-c5xlarge-[1-fillerr],queue1-st-c5xlarge-[2-fillerr],queue1-st-c5xlarge-[3-filler]",
            "nodeaddr-1,nodeaddr-2,nodeaddr-3",
            ["nodehostname-1", "nodehostname-2", "nodehostname-3"],
            None,
            2,
            ValueError,
        ),
    ],
    ids=[
        "nodename_only",
        "name+addr",
        "name+addr+hostname",
        "name+addr+instanceid",
        "incorrect_addr1",
        "incorrect_hostname1",
        "incorrect_instanceid",
        "incorrect_addr2",
        "mixed_format",
        "same_length_string",
    ],
)
def test_batch_node_info(nodenames, nodeaddrs, hostnames, instance_ids, batch_size, expected_result):
    if expected_result is not ValueError:
        assert_that(list(_batch_node_info(nodenames, nodeaddrs, hostnames, instance_ids, batch_size))).is_equal_to(
            expected_result
        )
    else:
        try:
            _batch_node_info(nodenames, nodeaddrs, hostnames, instance_ids, batch_size)
        except Exception as e:
            assert_that(e).is_instance_of(ValueError)
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
            [
                ("queue1-st-c5xlarge-1", None, None, None),
                ("queue1-st-c5xlarge-2,queue1-st-c5xlarge-3", None, None, None),
            ],
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
                ("queue1-st-c5xlarge-1", None, "hostname-1", None),
                ("queue1-st-c5xlarge-2,queue1-st-c5xlarge-3", "addr-2,addr-3", None, None),
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
                ("queue1-st-c5xlarge-1", None, "hostname-1", None),
                ("queue1-st-c5xlarge-[3-6]", "addr-[3-6]", "hostname-[3-6]", None),
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
            # InstanceId is set in the same batched command as NodeAddr (Slurm >= 25.11.6)
            [
                ("queue1-st-c5xlarge-[1-2]", "addr-1,addr-2", None, "i-111,i-222"),
            ],
            None,
            None,
            True,
            [
                call(
                    "sudo /opt/slurm/bin/scontrol update "
                    "nodename=queue1-st-c5xlarge-[1-2] nodeaddr=addr-1,addr-2 instanceid=i-111,i-222",
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
            ],
            None,
        ),
        (
            [
                ("queue1-st-c5xlarge-1 & rm -rf /", None, "hostname-1", None),
            ],
            "down",
            "debugging",
            None,
            None,
            ValueError,
        ),
        (
            [
                ("queue1-st-c5xlarge-1", " & rm -rf /", "hostname-1", None),
            ],
            "down",
            "debugging",
            None,
            None,
            ValueError,
        ),
        (
            [
                ("queue1-st-c5xlarge-1", None, " & rm -rf /", None),
            ],
            "down",
            "debugging",
            None,
            None,
            ValueError,
        ),
        (
            [
                ("queue1-st-c5xlarge-1", None, None, " & rm -rf /"),
            ],
            None,
            None,
            None,
            None,
            ValueError,
        ),
        (
            [
                ("queue1-st-c5xlarge-1", None, "hostname-1", None),
            ],
            " & rm -rf /",
            "debugging",
            None,
            None,
            ValueError,
        ),
        (
            [
                ("queue1-st-c5xlarge-1", None, "hostname-1", None),
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
            update_nodes(
                batch_node_info,
                "some_nodeaddrs",
                "some_hostnames",
                state=state,
                reason=reason,
                raise_on_error=raise_on_error,
            )
    else:
        cmd_mock = mocker.patch("common.schedulers.slurm_commands.run_command", autospec=True)
        update_nodes(
            batch_node_info,
            "some_nodeaddrs",
            "some_hostnames",
            state=state,
            reason=reason,
            raise_on_error=raise_on_error,
        )
        cmd_mock.assert_has_calls(run_command_calls)


@pytest.mark.parametrize(
    "nodes, nodeaddrs, instance_ids, expected_run_command_calls",
    [
        (
            # InstanceId and NodeAddr are set together in a single batched scontrol update command,
            # distributed across the nodes in the range (requires Slurm >= 25.11.6).
            ["queue1-st-c5xlarge-1", "queue1-st-c5xlarge-2"],
            ["ip-1", "ip-2"],
            ["i-111", "i-222"],
            [
                call(
                    "sudo /opt/slurm/bin/scontrol update "
                    "nodename=queue1-st-c5xlarge-1,queue1-st-c5xlarge-2 nodeaddr=ip-1,ip-2 instanceid=i-111,i-222",
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
            ],
        ),
        (
            # Batches larger than 100 nodes are split; each batch keeps its own nodeaddr/instanceid slice.
            [f"queue1-st-c5xlarge-{i}" for i in range(1, 102)],
            [f"ip-{i}" for i in range(1, 102)],
            [f"i-{i}" for i in range(1, 102)],
            [
                call(
                    "sudo /opt/slurm/bin/scontrol update "
                    f"nodename={','.join(f'queue1-st-c5xlarge-{i}' for i in range(1, 101))} "
                    f"nodeaddr={','.join(f'ip-{i}' for i in range(1, 101))} "
                    f"instanceid={','.join(f'i-{i}' for i in range(1, 101))}",
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
                call(
                    "sudo /opt/slurm/bin/scontrol update "
                    "nodename=queue1-st-c5xlarge-101 nodeaddr=ip-101 instanceid=i-101",
                    raise_on_error=True,
                    timeout=60,
                    shell=True,
                ),
            ],
        ),
    ],
    ids=["single_batch", "split_batches"],
)
def test_update_nodes_with_instance_ids(nodes, nodeaddrs, instance_ids, expected_run_command_calls, mocker):
    """Verify InstanceId is set in the same batched scontrol update command as NodeAddr."""
    cmd_mock = mocker.patch("common.schedulers.slurm_commands.run_command", autospec=True)
    update_nodes(nodes, nodeaddrs=nodeaddrs, instance_ids=instance_ids)
    cmd_mock.assert_has_calls(expected_run_command_calls)
    assert_that(cmd_mock.call_count).is_equal_to(len(expected_run_command_calls))


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


@pytest.mark.parametrize(
    ("mock_partitions", "expected_get_nodes_arg", "inactive_nodes_info", "expected_reset_nodes"),
    [
        # Only INACTIVE partitions are considered; the UP partition's nodes are not queried/reset. Among the INACTIVE
        # nodes, only those needing reset (nodeaddr still set) are reset.
        (
            [
                SlurmPartition("queue1", "queue1-dy-c5xlarge-1,queue1-dy-c5xlarge-2", "INACTIVE"),
                SlurmPartition("queue2", "queue2-dy-c5xlarge-1", "UP"),
                SlurmPartition("queue3", "queue3-dy-c5xlarge-1", "INACTIVE"),
            ],
            "queue1-dy-c5xlarge-1,queue1-dy-c5xlarge-2,queue3-dy-c5xlarge-1",
            [
                # nodeaddr set (dirty) -> needs reset
                DynamicNode("queue1-dy-c5xlarge-1", "1.2.3.4", "1.2.3.4", "IDLE+CLOUD+POWERING_UP", "queue1"),
                # nodeaddr already equal to name and powered down (clean) -> does not need reset
                DynamicNode(
                    "queue1-dy-c5xlarge-2", "queue1-dy-c5xlarge-2", "queue1-dy-c5xlarge-2", "DOWN+CLOUD", "queue1"
                ),
                DynamicNode("queue3-dy-c5xlarge-1", "1.2.3.5", "1.2.3.5", "IDLE+CLOUD+POWERING_UP", "queue3"),
            ],
            {"queue1-dy-c5xlarge-1", "queue3-dy-c5xlarge-1"},
        ),
        # No INACTIVE partition: get_nodes_info / reset_nodes are not called at all.
        (
            [
                SlurmPartition("queue1", "queue1-dy-c5xlarge-1", "UP"),
                SlurmPartition("queue2", "queue2-dy-c5xlarge-1", "UP"),
            ],
            None,
            [],
            None,
        ),
        # INACTIVE partition with no nodes: skipped so the node list is never malformed, nothing is queried/reset.
        (
            [
                SlurmPartition("queue1", "", "INACTIVE"),
            ],
            None,
            [],
            None,
        ),
    ],
)
def test_reset_nodes_in_inactive_partitions(
    mock_partitions, expected_get_nodes_arg, inactive_nodes_info, expected_reset_nodes, mocker
):
    reset_nodes_spy = mocker.patch("common.schedulers.slurm_commands.reset_nodes", autospec=True)
    get_nodes_info_spy = mocker.patch(
        "common.schedulers.slurm_commands.get_nodes_info", return_value=inactive_nodes_info, autospec=True
    )
    mocker.patch("common.schedulers.slurm_commands.get_partitions_info", return_value=mock_partitions, autospec=True)

    reset_nodes_in_inactive_partitions()

    if expected_get_nodes_arg is None:
        get_nodes_info_spy.assert_not_called()
        reset_nodes_spy.assert_not_called()
    else:
        get_nodes_info_spy.assert_called_once_with(expected_get_nodes_arg)
        if expected_reset_nodes:
            reset_nodes_spy.assert_called_once_with(
                expected_reset_nodes, state="down", reason="inactive partition", raise_on_error=False
            )
        else:
            reset_nodes_spy.assert_not_called()


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
    "nodes, cmd_timeout, partition_nodelist_mapping, expected_scontrol_args",
    [
        pytest.param(
            "node1 node2",
            30,
            {
                "test": "test-st-cr1-[1-10],test-dy-cr2-[1-2]",
                "test2": "test2-st-cr1-[1-10],test2-dy-cr2-[1-2]",
            },
            "show nodes node1 node2",
            id="Test with nodes provided by caller",
        ),
        pytest.param(
            "",
            30,
            {
                "test": "test-st-cr1-[1-10],test-dy-cr2-[1-2]",
                "test2": "test2-st-cr1-[1-10],test2-dy-cr2-[1-2]",
            },
            "show nodes test-st-cr1-[1-10],test-dy-cr2-[1-2],test2-st-cr1-[1-10],test2-dy-cr2-[1-2]",
            id="Test with nodes not provided by caller. Nodes are retrieved from PC-managed partitions ",
        ),
    ],
)
def test_get_nodes_info(nodes, cmd_timeout, partition_nodelist_mapping: Dict, expected_scontrol_args, mocker):
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
    # Mock the standalone scontrol invocation performed in get_nodes_info()
    run_scontrol_command_mocked = mocker.patch(
        "common.schedulers.slurm_commands._run_scontrol_command",
        return_value="",
        autospec=True,
    )
    get_nodes_info(nodes, cmd_timeout)
    run_scontrol_command_mocked.assert_called_with(expected_scontrol_args, command_timeout=cmd_timeout)


@pytest.mark.parametrize(
    "nodes, cmd_timeout, expected_scontrol_args, expected_exception",
    [
        (
            "node1 node2",
            30,
            "show nodes node1 node2",
            None,
        ),
        (
            "node1 & rm -rf / node2",
            None,
            None,
            ValueError,
        ),
    ],
)
def test_get_nodes_info_argument_validation(nodes, cmd_timeout, expected_scontrol_args, expected_exception, mocker):
    if expected_exception is ValueError:
        with pytest.raises(ValueError):
            get_nodes_info(nodes, cmd_timeout)
    else:
        run_scontrol_command_mocked = mocker.patch(
            "common.schedulers.slurm_commands._run_scontrol_command",
            return_value="",
            autospec=True,
        )
        get_nodes_info(nodes, cmd_timeout)
        run_scontrol_command_mocked.assert_called_with(expected_scontrol_args, command_timeout=30)


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
                [
                    {
                        "NodeName": "queue1-st-compute-resource-1-1",
                        "NodeAddr": "192.168.123.191",
                        "NodeHostName": "queue1-st-compute-resource-1-1",
                        "State": "DOWN+CLOUD+REBOOT_ISSUED",
                        "Partitions": "queue1",
                        "SlurmdStartTime": "2023-01-26T09:57:15",
                        "LastBusyTime": "2023-01-26T09:57:15",
                        "Reason": "Reboot ASAP : reboot issued [slurm@2023-01-26T10:11:39]",
                    },
                    {
                        "NodeName": "queue1-st-compute-resource-1-2",
                        "NodeAddr": "192.168.123.192",
                        "NodeHostName": "queue1-st-compute-resource-1-2",
                        "State": "DOWN+CLOUD+REBOOT_ISSUED",
                        "Partitions": "queue1",
                        "SlurmdStartTime": "2023-01-26T09:57:16",
                        "LastBusyTime": "Unknown",
                        "Reason": "Reboot ASAP : reboot issued [slurm@2023-01-26T10:11:40]",
                    },
                    {
                        "NodeName": "queue1-st-crt2micro-1",
                        "NodeAddr": "10.0.236.182",
                        "NodeHostName": "queue1-st-crt2micro-1",
                        "State": "IDLE+CLOUD+MAINTENANCE+RESERVED",
                        "Partitions": "queue1",
                        "SlurmdStartTime": "2023-10-13T10:13:17",
                        "LastBusyTime": "2023-10-13T10:13:20",
                        "ReservationName": "root_5",
                    },
                ]
            ),
        )
    ],
)
def test_extract_scontrol_records(scontrol_output, expected_parsed_output):
    # _extract_scontrol_records reproduces in pure Python the record splitting and field extraction that was
    # previously done via an awk/grep shell pipeline, so no external awk/grep is required.
    parsed_output = _extract_scontrol_records(scontrol_output, SCONTROL_NODE_INFO_FIELD_REGEX)
    assert_that(parsed_output).is_equal_to(expected_parsed_output)


@pytest.mark.parametrize(
    "raw_partition_info, expected_records",
    [
        pytest.param(
            "PartitionName=queue1\n"
            "   AllocNodes=ALL Default=YES QoS=N/A\n"
            "   State=UP TotalCPUs=10 TotalNodes=5\n"
            "\n"
            "PartitionName=queue2\n"
            "   AllocNodes=ALL Default=NO QoS=N/A\n"
            "   State=INACTIVE TotalCPUs=20 TotalNodes=10\n",
            [
                {"PartitionName": "queue1", "State": "UP"},
                {"PartitionName": "queue2", "State": "INACTIVE"},
            ],
            id="Multi-line partitions split on blank lines",
        ),
        pytest.param("", [], id="Empty scontrol output"),
    ],
)
def test_extract_scontrol_records_partitions(raw_partition_info, expected_records):
    records = _extract_scontrol_records(raw_partition_info, SCONTROL_PARTITION_INFO_FIELD_REGEX)
    assert_that(records).is_equal_to(expected_records)


@pytest.mark.parametrize(
    "partition_records, managed_partitions, expected_partitions_info",
    [
        pytest.param(
            [
                {"PartitionName": "queue1", "State": "UP"},
                {"PartitionName": "queue2", "State": "INACTIVE"},
                {"PartitionName": "queue3", "State": "UP"},
            ],
            {"queue1", "queue3"},
            [("queue1", "UP"), ("queue3", "UP")],
            id="Filter to managed partitions only",
        ),
        pytest.param(
            [
                {"PartitionName": "queue1", "State": "UP"},
                {"PartitionName": "queue2", "State": "INACTIVE"},
            ],
            None,
            [("queue1", "UP"), ("queue2", "INACTIVE")],
            id="No filter returns all partitions",
        ),
        pytest.param(
            [{"State": "UP"}, {"PartitionName": "queue1"}],
            None,
            [],
            id="Records missing name or state are skipped",
        ),
        pytest.param([], {"queue1"}, [], id="No records"),
    ],
)
def test_parse_partitions_info(partition_records, managed_partitions, expected_partitions_info):
    assert_that(_parse_partitions_info(partition_records, managed_partitions)).is_equal_to(expected_partitions_info)


def _completed_process(returncode, stdout, stderr):
    return subprocess.CompletedProcess(args=["scontrol"], returncode=returncode, stdout=stdout, stderr=stderr)


def test_run_scontrol_command_success_with_output(mocker):
    subprocess_run_mocked = mocker.patch(
        "common.schedulers.slurm_commands.subprocess.run",
        return_value=_completed_process(0, "NodeName=q1-st-c5-1\n", ""),
    )
    output = _run_scontrol_command("show nodes q1-st-c5-1", command_timeout=15)
    assert_that(output).is_equal_to("NodeName=q1-st-c5-1\n")
    # scontrol is run as a standalone command (no shell), split into args
    args, kwargs = subprocess_run_mocked.call_args
    assert_that(args[0]).is_equal_to(SCONTROL.split() + ["show", "nodes", "q1-st-c5-1"])
    assert_that(kwargs["shell"] if "shell" in kwargs else False).is_false()
    assert_that(kwargs["timeout"]).is_equal_to(15)


def test_run_scontrol_command_success_empty_output_returns_empty(mocker):
    # scontrol exiting 0 with no output is returned as-is (no error). In practice scontrol prints a
    # "No <resource> in the system" line for empty results, so this is a defensive case.
    mocker.patch(
        "common.schedulers.slurm_commands.subprocess.run",
        return_value=_completed_process(0, "", ""),
    )
    assert_that(_run_scontrol_command("show nodes q1-st-c5-1")).is_equal_to("")


def test_run_scontrol_command_nonzero_no_output_raises_and_logs_stderr(mocker, caplog):
    mocker.patch(
        "common.schedulers.slurm_commands.subprocess.run",
        return_value=_completed_process(1, "", "sudo: a password is required"),
    )
    with pytest.raises(subprocess.CalledProcessError):
        _run_scontrol_command("show nodes q1-st-c5-1")
    # The failure must be logged with scontrol's exit code and stderr so it is diagnosable.
    assert_that(caplog.text).contains("failed with exit code 1")
    assert_that(caplog.text).contains("sudo: a password is required")


def test_run_scontrol_command_nonzero_no_output_no_raise(mocker, caplog):
    mocker.patch(
        "common.schedulers.slurm_commands.subprocess.run",
        return_value=_completed_process(1, "", "some error"),
    )
    output = _run_scontrol_command("show reservations", raise_on_error=False)
    assert_that(output).is_equal_to("")
    assert_that(caplog.text).contains("failed with exit code 1")


def test_run_scontrol_command_nonzero_with_output_warns_and_returns(mocker, caplog):
    # Slurm prints valid data for existing nodes but exits 1 when a requested node is not found. This must be
    # tolerated (output returned) and only warned about, distinct from a hard failure.
    stdout = "NodeName=q1-st-c5-1 State=IDLE+CLOUD\nNode q1-st-c5-2 not found\n"
    mocker.patch(
        "common.schedulers.slurm_commands.subprocess.run",
        return_value=_completed_process(1, stdout, ""),
    )
    output = _run_scontrol_command("show nodes q1-st-c5-1,q1-st-c5-2")
    assert_that(output).is_equal_to(stdout)
    assert_that(caplog.text).contains("returned non-zero exit code 1 but produced output")


def test_run_scontrol_command_timeout_raises_and_logs(mocker, caplog):
    mocker.patch(
        "common.schedulers.slurm_commands.subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="scontrol", timeout=30),
    )
    with pytest.raises(subprocess.TimeoutExpired):
        _run_scontrol_command("show nodes q1-st-c5-1", command_timeout=30)
    assert_that(caplog.text).contains("timed out after 30 seconds")


def test_run_scontrol_command_oserror_raises_and_logs(mocker, caplog):
    # scontrol/sudo binary missing or not executable: subprocess.run raises an OSError before any exit code.
    mocker.patch(
        "common.schedulers.slurm_commands.subprocess.run",
        side_effect=FileNotFoundError("No such file or directory: 'sudo'"),
    )
    with pytest.raises(OSError):
        _run_scontrol_command("show nodes q1-st-c5-1")
    assert_that(caplog.text).contains("Unable to execute scontrol command")


def test_get_nodes_info_parsing_failure_propagates(mocker):
    # If scontrol returns a field the parser cannot interpret (e.g. an unparseable date), the error surfaces
    # instead of being silently masked by the (former) shell pipeline.
    mocker.patch(
        "common.schedulers.slurm_commands._run_scontrol_command",
        return_value="NodeName=q1-st-c5-1\nSlurmdStartTime=not-a-valid-date\n",
        autospec=True,
    )
    with pytest.raises(ValueError):
        get_nodes_info("q1-st-c5-1")


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
