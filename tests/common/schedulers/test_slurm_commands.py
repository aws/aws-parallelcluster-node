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
    SlurmJob,
    SlurmNode,
    SlurmPartition,
    _batch_node_info,
    _parse_nodes_info,
    get_jobs_info,
    get_pending_jobs_info,
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
from tests.common import read_text


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
    "squeue_mocked_response, expected_output",
    [
        (
            "squeue_output_mix.txt",
            [
                SlurmJob(
                    cpus_total=5,
                    cpus_min_per_node=1,
                    cpus_per_task=1,
                    state="PD",
                    nodes=2,
                    tasks=5,
                    id="72",
                    pending_reason="Resources",
                    tres_per_job={},
                    tres_per_task={},
                    cpus_per_tres={},
                ),
                SlurmJob(
                    cpus_total=10,
                    cpus_min_per_node=1,
                    cpus_per_task=1,
                    state="R",
                    nodes=3,
                    tasks=10,
                    id="84",
                    pending_reason="Resources",
                    tres_per_job={"gpu": 12},
                    tres_per_task={},
                    cpus_per_tres={},
                ),
                SlurmJob(
                    cpus_total=40,
                    cpus_min_per_node=4,
                    cpus_per_task=4,
                    state="PD",
                    nodes=10,
                    tasks=10,
                    id="86",
                    pending_reason="ReqNodeNotAvail, May be reserved for other job",
                    tres_per_job={},
                    tres_per_task={"gpu": 4},
                    cpus_per_tres={},
                ),
                SlurmJob(
                    cpus_total=10,
                    cpus_min_per_node=1,
                    cpus_per_task=1,
                    state="PD",
                    nodes=10,
                    tasks=10,
                    id="87",
                    pending_reason="ReqNodeNotAvail, May be reserved for other job",
                    tres_per_job={"gpu": 12},
                    tres_per_task={"gpu": 4},
                    tres_per_node={"gpu": 6},
                    cpus_per_tres={},
                ),
                SlurmJob(
                    cpus_total=15,
                    cpus_min_per_node=3,
                    cpus_per_task=3,
                    state="PD",
                    nodes=4,
                    tasks=5,
                    id="90_1",
                    pending_reason="PartitionConfig",
                    tres_per_job={"gpu": 12},
                    tres_per_task={"gpu": 4},
                    tres_per_node={"gpu": 6},
                    cpus_per_tres={},
                ),
                SlurmJob(
                    cpus_total=15,
                    cpus_min_per_node=3,
                    cpus_per_task=3,
                    state="PD",
                    nodes=4,
                    tasks=5,
                    id="90_2",
                    pending_reason="PartitionNodeLimit",
                    tres_per_job={"gpu": 12},
                    tres_per_task={"gpu": 4},
                    tres_per_node={"gpu": 6},
                    cpus_per_tres={"gpu": 5},
                ),
                SlurmJob(
                    cpus_total=15,
                    cpus_min_per_node=3,
                    cpus_per_task=3,
                    state="PD",
                    nodes=4,
                    tasks=5,
                    id="90_3",
                    pending_reason="Resources",
                    tres_per_job={"gpu": 12},
                    tres_per_task={"gpu": 4},
                    tres_per_node={"gpu": 6},
                    cpus_per_tres={"gpu": 5},
                ),
            ],
        ),
        (
            "squeue_output_extra_column.txt",
            [
                SlurmJob(
                    id="72",
                    state="PD",
                    nodes=2,
                    tasks=5,
                    cpus_total=5,
                    cpus_min_per_node=1,
                    cpus_per_task=1,
                    pending_reason="Resources",
                )
            ],
        ),
        (
            "squeue_output_missing_column.txt",
            [
                SlurmJob(
                    cpus_total=5,
                    tres_per_job=None,
                    cpus_min_per_node=0,
                    cpus_per_task=1,
                    tres_per_task=None,
                    state="",
                    nodes=2,
                    tasks=5,
                    id="72",
                    pending_reason="Resources",
                )
            ],
        ),
        ("squeue_output_empty.txt", []),
    ],
    ids=["mixed_output", "extra_column", "missing_column", "empty"],
)
def test_get_jobs_info(squeue_mocked_response, expected_output, test_datadir, mocker):
    qstat_output = read_text(test_datadir / squeue_mocked_response)
    mock = mocker.patch(
        "common.schedulers.slurm_commands.check_command_output", return_value=qstat_output, autospec=True
    )

    jobs = get_jobs_info(job_state_filter="PD,R")

    mock.assert_called_with(
        "/opt/slurm/bin/squeue -r -O 'jobid:200,statecompact:200,numnodes:200,numcpus:200,numtasks:200,"
        "cpus-per-task:200,mincpus:200,reason:200,tres-per-job:200,tres-per-task:200,tres-per-node:200,"
        "cpus-per-tres:200' --states PD,R"
    )
    assert_that(jobs).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "pending_jobs, instance_properties, max_nodes_filter, filter_by_pending_reasons, expected_output",
    [
        (
            [SlurmJob(id="72", state="PD", nodes=2, cpus_total=5, cpus_min_per_node=1, pending_reason="Priority")],
            {"slots": 4, "gpus": 0},
            2,
            ["Priority"],
            [SlurmJob(id="72", state="PD", nodes=2, cpus_total=5, cpus_min_per_node=1, pending_reason="Priority")],
        ),
        (
            [
                SlurmJob(id="72", state="PD", nodes=2, cpus_total=4, cpus_min_per_node=2, pending_reason="Priority"),
                SlurmJob(
                    id="73", state="PD", nodes=2, cpus_total=5, cpus_min_per_node=2, pending_reason="Priority"
                ),  # nodes gets incremented by 1
                SlurmJob(id="74", state="PD", nodes=1, cpus_total=2, cpus_min_per_node=1, pending_reason="Priority"),
            ],
            {"slots": 2, "gpus": 0},
            2,
            ["Priority"],
            [
                SlurmJob(id="72", state="PD", nodes=2, cpus_total=4, cpus_min_per_node=2, pending_reason="Priority"),
                SlurmJob(id="74", state="PD", nodes=1, cpus_total=2, cpus_min_per_node=1, pending_reason="Priority"),
            ],
        ),
        (
            [SlurmJob(id="72", state="PD", nodes=2, cpus_total=5, cpus_min_per_node=1, pending_reason="Priority")],
            {"slots": 1, "gpus": 0},
            1,
            ["Priority"],
            [],
        ),
        (
            [SlurmJob(id="72", state="PD", nodes=2, cpus_total=5, cpus_min_per_node=5, pending_reason="Priority")],
            {"slots": 4, "gpus": 0},
            2,
            ["Priority"],
            [],
        ),
        (
            [
                SlurmJob(
                    id="72", state="PD", nodes=2, cpus_total=2, cpus_min_per_node=1, pending_reason="PartitionNodeLimit"
                )
            ],
            {"slots": 2, "gpus": 0},
            2,
            ["Priority"],
            [],
        ),
        (
            [SlurmJob(id="72", state="PD", nodes=4, cpus_total=15, cpus_min_per_node=3, pending_reason="Priority")],
            {"slots": 4, "gpus": 0},
            5,
            [],
            [
                SlurmJob(
                    id="72",
                    state="PD",
                    nodes=5,  # nodes got incremented by 1
                    cpus_total=15,
                    cpus_min_per_node=3,
                    pending_reason="Priority",
                )
            ],
        ),
        (
            [SlurmJob(id="72", state="PD", nodes=4, cpus_total=15, cpus_min_per_node=3, pending_reason="Priority")],
            {"slots": 4, "gpus": 0},
            4,
            [],
            [],
        ),
        (
            [SlurmJob(id="72", state="PD", nodes=4, cpus_total=15, cpus_min_per_node=3, pending_reason="Priority")],
            None,
            None,
            None,
            [SlurmJob(id="72", state="PD", nodes=4, cpus_total=15, cpus_min_per_node=3, pending_reason="Priority")],
        ),
        (
            [
                # sbatch --gpus=3 - no changes required
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=1,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 3},
                    tres_per_task={},
                ),
                # sbatch --gpus=12 - recompute number of nodes
                SlurmJob(
                    id="2",
                    state="PD",
                    nodes=3,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 12},
                    tres_per_task={},
                ),
                # sbatch --gpus=13 - recompute number of nodes and discard
                SlurmJob(
                    id="3",
                    state="PD",
                    nodes=1,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 13},
                    tres_per_task={},
                ),
                # sbatch --gpus=4 -N 2 - no changes required
                SlurmJob(
                    id="4",
                    state="PD",
                    nodes=2,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 4},
                    tres_per_task={},
                ),
            ],
            {"slots": 32, "gpus": 4},
            3,
            ["Priority"],
            [
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=1,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 3},
                    tres_per_task={},
                ),
                SlurmJob(
                    id="2",
                    state="PD",
                    nodes=3,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 12},
                    tres_per_task={},
                ),
                SlurmJob(
                    id="4",
                    state="PD",
                    nodes=2,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 4},
                    tres_per_task={},
                ),
            ],
        ),
        (
            [
                # sbatch --gpus-per-task=2 -n 2 - no changes required
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=1,
                    tasks=2,
                    cpus_per_task=1,
                    cpus_total=2,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={"gpu": 2},
                ),
                # sbatch --gpus-per-task=2 -n 3 - recompute number of nodes
                SlurmJob(
                    id="2",
                    state="PD",
                    nodes=1,
                    tasks=3,
                    cpus_per_task=1,
                    cpus_total=3,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={"gpu": 2},
                ),
                # sbatch --wrap "sleep 100" --gpus-per-task=2 -n 3 -N 3 - no changes required
                SlurmJob(
                    id="3",
                    state="PD",
                    nodes=3,
                    tasks=3,
                    cpus_per_task=1,
                    cpus_total=3,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={"gpu": 2},
                ),
                # sbatch --wrap "sleep 100" --gpus-per-task=2 -n 3 -c 22 - no changes required
                SlurmJob(
                    id="4",
                    state="PD",
                    nodes=3,
                    tasks=3,
                    cpus_per_task=22,
                    cpus_total=66,
                    cpus_min_per_node=22,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={"gpu": 2},
                ),
                # sbatch --gpus-per-task=2 -n 3 - recompute number of nodes and discard
                SlurmJob(
                    id="5",
                    state="PD",
                    nodes=1,
                    tasks=7,
                    cpus_per_task=1,
                    cpus_total=7,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={"gpu": 2},
                ),
            ],
            {"slots": 32, "gpus": 4},
            3,
            ["Priority"],
            [
                # sbatch --gpus-per-task=2 -n 2 - no changes required
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=1,
                    tasks=2,
                    cpus_per_task=1,
                    cpus_total=2,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={"gpu": 2},
                ),
                # sbatch --gpus-per-task=2 -n 3 - recompute number of nodes
                SlurmJob(
                    id="2",
                    state="PD",
                    nodes=2,
                    tasks=3,
                    cpus_per_task=1,
                    cpus_total=3,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={"gpu": 2},
                ),
                # sbatch --wrap "sleep 100" --gpus-per-task=2 -n 3 -N 3 - no changes required
                SlurmJob(
                    id="3",
                    state="PD",
                    nodes=3,
                    tasks=3,
                    cpus_per_task=1,
                    cpus_total=3,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={"gpu": 2},
                ),
                # sbatch --wrap "sleep 100" --gpus-per-task=2 -n 3 -c 22 - no changes required
                SlurmJob(
                    id="4",
                    state="PD",
                    nodes=3,
                    tasks=3,
                    cpus_per_task=22,
                    cpus_total=66,
                    cpus_min_per_node=22,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={"gpu": 2},
                ),
            ],
        ),
        (
            [
                # sbatch --gpus-per-task=5 -n 3 - nodes recomputed
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=1,
                    tasks=3,
                    cpus_per_task=1,
                    cpus_total=3,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={"gpu": 5},
                )
            ],
            {"slots": 32, "gpus": 8},
            3,
            ["Priority"],
            [
                # sbatch --gpus-per-task=5 -n 3 - nodes recomputed
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=3,
                    tasks=3,
                    cpus_per_task=1,
                    cpus_total=3,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={"gpu": 5},
                )
            ],
        ),
        (
            [
                # sbatch --wrap "sleep 100" -n 40 --gpus-per-node=1 - no changes required
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=2,
                    tasks=40,
                    cpus_per_task=1,
                    cpus_total=40,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={},
                ),
                # sbatch --wrap "sleep 100" --gres=gpu:4 -n 2 -c 20 - no changes required
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=2,
                    tasks=2,
                    cpus_per_task=20,
                    cpus_total=40,
                    cpus_min_per_node=20,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={},
                ),
            ],
            {"slots": 32, "gpus": 4},
            3,
            ["Priority"],
            [
                # sbatch --wrap "sleep 100" -n 40 --gpus-per-node=1 - no changes required
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=2,
                    tasks=40,
                    cpus_per_task=1,
                    cpus_total=40,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={},
                ),
                # sbatch --wrap "sleep 100" --gres=gpu:4 -n 2 -c 20 - no changes required
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=2,
                    tasks=2,
                    cpus_per_task=20,
                    cpus_total=40,
                    cpus_min_per_node=20,
                    pending_reason="Priority",
                    tres_per_job={},
                    tres_per_task={},
                ),
            ],
        ),
        (
            [
                # sbatch --wrap "sleep 100" --gpus=4 --gpus-per-node=1 - discarded
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=4,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=4,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 4},
                    tres_per_task={},
                ),
                # sbatch --wrap "sleep 100" --gpus=5 --cpus-per-gpu=15 - recompute number of nodes, recompute cpus_total
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=1,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 5},
                    tres_per_task={},
                    cpus_per_tres={"gpu": 15},
                ),
                # sbatch --wrap "sleep 100" --gpus=10 --cpus-per-gpu=10 - discarded
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=1,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 10},
                    tres_per_task={},
                    cpus_per_tres={"gpu": 10},
                ),
                # sbatch --wrap "sleep 100" -n 1 -c 33 --gpus=10 --cpus-per-gpu=1 - discarded
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=1,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=1,
                    cpus_min_per_node=33,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 10},
                    tres_per_task={},
                    cpus_per_tres={"gpu": 1},
                ),
                # sbatch --wrap "sleep 100" --gpus=5 --gpus-per-task=1 - recomputed number of nodes
                SlurmJob(
                    id="2",
                    state="PD",
                    nodes=1,
                    tasks=5,
                    cpus_per_task=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 5},
                    tres_per_task={"gpu": 1},
                ),
            ],
            {"slots": 32, "gpus": 4},
            3,
            ["Priority"],
            [
                # sbatch --wrap "sleep 100" --gpus=4 --cpus-per-gpu=9 - recompute number of nodes, recompute cpus_total
                SlurmJob(
                    id="1",
                    state="PD",
                    nodes=3,
                    tasks=1,
                    cpus_per_task=1,
                    cpus_total=75,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 5},
                    tres_per_task={},
                    cpus_per_tres={"gpu": 15},
                ),
                # sbatch --wrap "sleep 100" --gpus=5 --gpus-per-task=1 - recomputed number of nodes
                SlurmJob(
                    id="2",
                    state="PD",
                    nodes=2,
                    tasks=5,
                    cpus_per_task=1,
                    cpus_total=2,
                    cpus_min_per_node=1,
                    pending_reason="Priority",
                    tres_per_job={"gpu": 5},
                    tres_per_task={"gpu": 1},
                ),
            ],
        ),
    ],
    ids=[
        "single",
        "multiple",
        "max_nodes",
        "max_cpus",
        "filter_state",
        "additional_node_required",
        "discarded_after_node_adjustment",
        "no_filters",
        "gpus_per_job",
        "gpus_per_task",
        "gpus_per_task_2",
        "gpus_per_node",
        "gpus_mix",
    ],
)
def test_get_pending_jobs_info(
    pending_jobs, instance_properties, max_nodes_filter, filter_by_pending_reasons, expected_output, mocker
):
    mock = mocker.patch("common.schedulers.slurm_commands.get_jobs_info", return_value=pending_jobs, autospec=True)

    pending_jobs = get_pending_jobs_info(instance_properties, max_nodes_filter, filter_by_pending_reasons)

    mock.assert_called_with(job_state_filter="PD")
    assert_that(pending_jobs).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node_info, expected_parsed_nodes_output",
    [
        (
            (
                "multiple-dy-c5xlarge-1\n"
                "172.31.10.155\n"
                "172-31-10-155\n"
                "MIXED+CLOUD\n"
                "multiple-dy-c5xlarge-2\n"
                "172.31.7.218\n"
                "172-31-7-218\n"
                "IDLE+CLOUD+POWER\n"
                "multiple-dy-c5xlarge-3\n"
                "multiple-dy-c5xlarge-3\n"
                "multiple-dy-c5xlarge-3\n"
                "IDLE+CLOUD+POWER"
            ),
            [
                SlurmNode("multiple-dy-c5xlarge-1", "172.31.10.155", "172-31-10-155", "MIXED+CLOUD"),
                SlurmNode("multiple-dy-c5xlarge-2", "172.31.7.218", "172-31-7-218", "IDLE+CLOUD+POWER"),
                SlurmNode(
                    "multiple-dy-c5xlarge-3",
                    "multiple-dy-c5xlarge-3",
                    "multiple-dy-c5xlarge-3",
                    "IDLE+CLOUD+POWER",
                ),
            ],
        )
    ],
)
def test_parse_nodes_info(node_info, expected_parsed_nodes_output, mocker):
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
            {"nodes": "nodes-1,nodes[2-6]", "state": "power_down", "reason": None, "raise_on_error": True},
        ),
        (
            "nodes-1,nodes[2-6]",
            "debugging",
            True,
            {"nodes": "nodes-1,nodes[2-6]", "state": "power_down", "reason": "debugging", "raise_on_error": True},
        ),
        (
            ["nodes-1", "nodes[2-4]", "nodes-5"],
            "debugging",
            True,
            {
                "nodes": ["nodes-1", "nodes[2-4]", "nodes-5"],
                "state": "power_down",
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
        (SlurmNode("queue-name-st-t2micro-1", "nodeip", "nodehostname", "somestate"), True),
        (SlurmNode("queue-name-st-dy-t2micro-1", "nodeip", "nodehostname", "somestate"), False),
        (SlurmNode("queuename-dy-t2micro-1", "nodeip", "nodehostname", "somestate"), False),
        (SlurmNode("queuename-dy-dy-dy-st-t2micro-1", "nodeip", "nodehostname", "somestate"), True),
    ],
)
def test_slurm_node_is_static(node, expected_output):
    assert_that(node.is_static).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (SlurmNode("queue-name-st-t2micro-1", "nodeip", "nodehostname", "somestate"), True),
        (SlurmNode("queuename-dy-t2micro-1", "queuename-dy-t2micro-1", "nodehostname", "somestate"), False),
    ],
)
def test_slurm_node_is_nodeaddr_set(node, expected_output):
    assert_that(node.is_nodeaddr_set()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "somestate"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DRAIN"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DRAIN"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "COMPLETING+DRAIN"), True),
    ],
)
def test_slurm_node_has_job(node, expected_output):
    assert_that(node.has_job()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "somestate"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DRAIN"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DRAIN"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE*+CLOUD+DRAIN"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+DRAIN"), True),
    ],
)
def test_slurm_node_is_drained(node, expected_output):
    assert_that(node.is_drained()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "somestate"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DOWN"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DRAIN"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN*+CLOUD"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "DOWN+CLOUD+POWER"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE~+CLOUD+POWERING_DOWN"), False),
    ],
)
def test_slurm_node_is_down(node, expected_output):
    assert_that(node.is_down()).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "node, expected_output",
    [
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWER"), True),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "MIXED#+CLOUD+DRAIN"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "ALLOCATED*+CLOUD+DOWN"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE+CLOUD+POWERING_DOWN"), False),
        (SlurmNode("queue1-st-c5xlarge-1", "nodeip", "nodehostname", "IDLE#+CLOUD"), True),
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
            [call("node-3,node-4", reason="stopping cluster", state="power_down")],
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
                call("node-1,node-2", reason="stopping cluster", state="power_down"),
                call("node-3,node-4", reason="stopping cluster", state="power_down"),
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
    reset_node_spy = mocker.patch("common.schedulers.slurm_commands.reset_nodes", auto_spec=True)
    update_partitions_spy = mocker.patch(
        "common.schedulers.slurm_commands.update_partitions", return_value=mock_succeeded_partitions, auto_spec=True
    )
    get_part_spy = mocker.patch(
        "common.schedulers.slurm_commands.get_partition_info", return_value=mock_partitions, auto_spec=True
    )
    assert_that(update_all_partitions(state, reset_node_addrs_hostname=reset_node_info)).is_equal_to(expected_results)
    get_part_spy.assert_called_with(get_all_nodes=True)
    if expected_reset_nodes_calls:
        reset_node_spy.assert_has_calls(expected_reset_nodes_calls)
    else:
        reset_node_spy.assert_not_called()
    update_partitions_spy.assert_called_with(partitions_to_update, state)
