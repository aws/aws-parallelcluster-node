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
from sqswatcher.plugins.slurm import _update_gres_node_lists, _update_node_lists
from sqswatcher.sqswatcher import Host, UpdateEvent


# Input: existing gres_node_list, events to be processed.
# Expected results: updated gres_node_list
@pytest.mark.parametrize(
    "gres_node_list, events, expected_result",
    [
        (
            ["NodeName=ip-10-0-000-111 Name=gpu Type=tesla File=/dev/nvidia[0-15]\n"],
            [
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 16)),
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
            ],
            [
                "NodeName=ip-10-0-000-111 Name=gpu Type=tesla File=/dev/nvidia[0-15]\n",
                "NodeName=ip-10-0-000-222 Name=gpu Type=tesla File=/dev/nvidia[0-15]\n",
            ],
        ),
        (
            ["NodeName=ip-10-0-000-111 Name=gpu Type=tesla File=/dev/nvidia[0-15]\n"],
            [
                UpdateEvent("REMOVE", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent("REMOVE", "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 16)),
            ],
            [],
        ),
        (
            # GPU files should be updated after remove/add sequence
            ["NodeName=ip-10-0-000-111 Name=gpu Type=tesla File=/dev/nvidia[0-1]\n"],
            [
                UpdateEvent("REMOVE", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
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
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 16)),
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
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
                UpdateEvent("REMOVE", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent("REMOVE", "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 16)),
            ],
            ([], []),
        ),
        (
            # CPU/GPU information should be updated after remove/add sequence
            ["NodeName=ip-10-0-000-111 CPUs=8 Gres=gpu:tesla:1 State=UNKNOWN\n"],
            [
                UpdateEvent("REMOVE", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 16)),
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
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 0)),
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 0)),
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 0)),
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
                UpdateEvent("REMOVE", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 0)),
                UpdateEvent("REMOVE", "some message", Host("i-0c1234567", "ip-10-0-000-222", "32", 0)),
            ],
            ([], []),
        ),
        (
            # CPU information should be updated after remove/add sequence
            ["NodeName=ip-10-0-000-111 CPUs=8 State=UNKNOWN\n"],
            [
                UpdateEvent("REMOVE", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 0)),
                UpdateEvent("ADD", "some message", Host("i-0c1234567", "ip-10-0-000-111", "32", 0)),
            ],
            (["NodeName=ip-10-0-000-111 CPUs=32 State=UNKNOWN\n"], ["ip-10-0-000-111"]),
        ),
    ],
    ids=["repetitive_add", "remove_nonexisting_node", "reusing_nodename"],
)
def test_update_node_lists(node_list, events, expected_result, mocker):
    mocker.patch("sqswatcher.plugins.slurm._read_node_list", return_value=node_list, autospec=True)

    assert_that(_update_node_lists(events)).is_equal_to(expected_result)
