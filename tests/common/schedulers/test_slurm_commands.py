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
from common.schedulers.slurm_commands import SlurmJob, get_jobs_info, get_pending_jobs_info
from tests.common import read_text


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
        "cpus-per-task:200,mincpus:200,reason:200,tres-per-job:200,tres-per-task:200' --states PD,R"
    )
    assert_that(jobs).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "pending_jobs, max_slots_filter, max_nodes_filter, filter_by_pending_reasons, expected_output",
    [
        (
            [SlurmJob(id="72", state="PD", nodes=2, cpus_total=5, cpus_min_per_node=1, pending_reason="Priority")],
            4,
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
            2,
            2,
            ["Priority"],
            [
                SlurmJob(id="72", state="PD", nodes=2, cpus_total=4, cpus_min_per_node=2, pending_reason="Priority"),
                SlurmJob(id="74", state="PD", nodes=1, cpus_total=2, cpus_min_per_node=1, pending_reason="Priority"),
            ],
        ),
        (
            [SlurmJob(id="72", state="PD", nodes=2, cpus_total=5, cpus_min_per_node=1, pending_reason="Priority")],
            1,
            1,
            ["Priority"],
            [],
        ),
        (
            [SlurmJob(id="72", state="PD", nodes=2, cpus_total=5, cpus_min_per_node=5, pending_reason="Priority")],
            4,
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
            2,
            2,
            ["Priority"],
            [],
        ),
        (
            [SlurmJob(id="72", state="PD", nodes=4, cpus_total=15, cpus_min_per_node=3, pending_reason="Priority")],
            4,
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
            4,
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
    ],
)
def test_get_pending_jobs_info(
    pending_jobs, max_slots_filter, max_nodes_filter, filter_by_pending_reasons, expected_output, mocker
):
    mock = mocker.patch("common.schedulers.slurm_commands.get_jobs_info", return_value=pending_jobs, autospec=True)

    pending_jobs = get_pending_jobs_info(max_slots_filter, max_nodes_filter, filter_by_pending_reasons)

    mock.assert_called_with(job_state_filter="PD")
    assert_that(pending_jobs).is_equal_to(expected_output)
