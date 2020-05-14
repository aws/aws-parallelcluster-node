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
import pytest

from assertpy import assert_that
from common.utils import EventType, Host, UpdateEvent
from sqswatcher.plugins.slurm import (
    _is_node_locked,
    _update_gres_node_lists,
    _update_node_lists,
    perform_health_actions,
)


# Input: existing gres_node_list, events to be processed.
# Expected results: updated gres_node_list
@pytest.mark.parametrize(
    "gres_node_list, events, expected_result",
    [
        (
            ["NodeName=ip-10-0-000-111 Name=gpu Type=tesla File=/dev/nvidia[0-15]\n"],
            [
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 16)),
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
            ],
            [
                "NodeName=ip-10-0-000-111 Name=gpu Type=tesla File=/dev/nvidia[0-15]\n",
                "NodeName=ip-10-0-000-222 Name=gpu Type=tesla File=/dev/nvidia[0-15]\n",
            ],
        ),
        (
            ["NodeName=ip-10-0-000-111 Name=gpu Type=tesla File=/dev/nvidia[0-15]\n"],
            [
                UpdateEvent(EventType.REMOVE, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent(EventType.REMOVE, "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 16)),
            ],
            [],
        ),
        (
            # GPU files should be updated after remove/add sequence
            ["NodeName=ip-10-0-000-111 Name=gpu Type=tesla File=/dev/nvidia[0-1]\n"],
            [
                UpdateEvent(EventType.REMOVE, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
            ],
            ["NodeName=ip-10-0-000-111 Name=gpu Type=tesla File=/dev/nvidia[0-15]\n"],
        ),
    ],
    ids=["repetitive_add", "remove_nonexisting_node", "reusing_nodename"],
)
def test_update_gres_node_lists(gres_node_list, events, expected_result, mocker):
    mocker.patch("sqswatcher.plugins.slurm._read_node_list", return_value=gres_node_list, autospec=True)

    assert_that(_update_gres_node_lists(events)).is_equal_to(expected_result)


# Input: existing gpu_node_list, events to be processed.
# Expected results: (updated node_list, nodes_to_restart)
@pytest.mark.parametrize(
    "gpu_node_list, events, expected_result",
    [
        (
            ["NodeName=ip-10-0-000-111 CPUs=32 Gres=gpu:tesla:16 State=UNKNOWN\n"],
            [
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 16)),
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
            ],
            (
                [
                    "NodeName=ip-10-0-000-111 CPUs=32 Gres=gpu:tesla:16 State=UNKNOWN\n",
                    "NodeName=ip-10-0-000-222 CPUs=32 Gres=gpu:tesla:16 State=UNKNOWN\n",
                ],
                # Note nodes_to_restart list is expected to be repetitive because we want to restart with every ADD
                ["ip-10-0-000-111", "ip-10-0-000-222", "ip-10-0-000-111"],
            ),
        ),
        (
            ["NodeName=ip-10-0-000-111 CPUs=32 Gres=gpu:tesla:16 State=UNKNOWN\n"],
            [
                UpdateEvent(EventType.REMOVE, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent(EventType.REMOVE, "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 16)),
            ],
            ([], []),
        ),
        (
            # CPU/GPU information should be updated after remove/add sequence
            ["NodeName=ip-10-0-000-111 CPUs=8 Gres=gpu:tesla:1 State=UNKNOWN\n"],
            [
                UpdateEvent(EventType.REMOVE, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
            ],
            (["NodeName=ip-10-0-000-111 CPUs=32 Gres=gpu:tesla:16 State=UNKNOWN\n"], ["ip-10-0-000-111"]),
        ),
    ],
    ids=["repetitive_add", "remove_nonexisting_node", "reusing_nodename"],
)
def test_gpu_update_node_lists(gpu_node_list, events, expected_result, mocker):
    mocker.patch("sqswatcher.plugins.slurm._read_node_list", return_value=gpu_node_list, autospec=True)

    assert_that(_update_node_lists(events)).is_equal_to(expected_result)


# Input: existing gpu_node_list, events to be processed.
# Expected results: (updated node_list, nodes_to_restart)
@pytest.mark.parametrize(
    "node_list, events, expected_result",
    [
        (
            ["NodeName=ip-10-0-000-111 CPUs=32 State=UNKNOWN\n"],
            [
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 0)),
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 0)),
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 0)),
            ],
            (
                [
                    "NodeName=ip-10-0-000-111 CPUs=32 State=UNKNOWN\n",
                    "NodeName=ip-10-0-000-222 CPUs=32 State=UNKNOWN\n",
                ],
                # Note nodes_to_restart list is expected to be repetitive because we want to restart with every ADD
                ["ip-10-0-000-111", "ip-10-0-000-222", "ip-10-0-000-111"],
            ),
        ),
        (
            ["NodeName=ip-10-0-000-111 CPUs=32 State=UNKNOWN\n"],
            [
                UpdateEvent(EventType.REMOVE, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 0)),
                UpdateEvent(EventType.REMOVE, "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 0)),
            ],
            ([], []),
        ),
        (
            # CPU information should be updated after remove/add sequence
            ["NodeName=ip-10-0-000-111 CPUs=8 State=UNKNOWN\n"],
            [
                UpdateEvent(EventType.REMOVE, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 0)),
                UpdateEvent(EventType.ADD, "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 0)),
            ],
            (["NodeName=ip-10-0-000-111 CPUs=32 State=UNKNOWN\n"], ["ip-10-0-000-111"]),
        ),
    ],
    ids=["repetitive_add", "remove_nonexisting_node", "reusing_nodename"],
)
def test_update_node_lists(node_list, events, expected_result, mocker):
    mocker.patch("sqswatcher.plugins.slurm._read_node_list", return_value=node_list, autospec=True)

    assert_that(_update_node_lists(events)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "hostname, get_node_state_output, expected_result",
    [
        ("ip-10-0-000-111", "draining*", True),
        ("ip-10-0-000-111", "drained#", True),
        ("ip-10-0-000-111", "idle", False),
        ("ip-10-0-000-111", "down", False),
        ("ip-10-0-000-111", "unknown", False),
        ("ip-10-0-000-111", Exception, False),
    ],
)
def test_is_node_locked(hostname, get_node_state_output, expected_result, mocker):
    if get_node_state_output is Exception:
        mocker.patch("sqswatcher.plugins.slurm.get_node_state", side_effect=Exception(), autospec=True)
    else:
        mocker.patch("sqswatcher.plugins.slurm.get_node_state", return_value=get_node_state_output, autospec=True)

    assert_that(_is_node_locked(hostname)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "events, lock_node_side_effect, is_node_locked_output, failed_events, succeeded_events",
    [
        (
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            None,
            [False, True],
            [],
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
        ),
        (
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            None,
            [True, True],
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            [],
        ),
        (
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            Exception,
            [False, False],
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            [],
        ),
        (
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            Exception,
            [False, True],
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            [],
        ),
        (
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            None,
            [False, False],
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            [],
        ),
    ],
    ids=[
        "all_good",
        "node_already_in_lock",
        "exception_when_locking_node_unlocked",
        "exception_when_locking_node_locked",
        "no_exception_failed_to_lock",
    ],
)
def test_perform_health_actions(
    events, lock_node_side_effect, is_node_locked_output, failed_events, succeeded_events, mocker
):
    if lock_node_side_effect is Exception:
        mocker.patch("sqswatcher.plugins.slurm.lock_node", side_effect=Exception, autospec=True)
    else:
        mocker.patch("sqswatcher.plugins.slurm.lock_node", autospec=True)
    mocker.patch(
        "sqswatcher.plugins.slurm._is_node_locked", side_effect=is_node_locked_output, autospec=True,
    )
    failed, succeeded = perform_health_actions(events)
    assert_that(failed_events).is_equal_to(failed)
    assert_that(succeeded_events).is_equal_to(succeeded)
