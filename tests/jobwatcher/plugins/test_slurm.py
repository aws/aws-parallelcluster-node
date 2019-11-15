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
from common.schedulers.slurm_commands import PENDING_RESOURCES_REASONS, SlurmJob, get_pending_jobs_info
from jobwatcher.plugins.slurm import get_required_nodes


@pytest.mark.parametrize(
    "pending_jobs, expected_required_nodes",
    [
        (
            [
                SlurmJob(
                    id="72", state="PD", nodes=4, cpus_total=15, cpus_min_per_node=3, pending_reason="Priority"
                )  # nodes gets incremented by 1
            ],
            5,
        ),
        (
            [
                SlurmJob(
                    id="72", state="PD", nodes=5, cpus_total=15, cpus_min_per_node=3, pending_reason="Resources"
                ),  # 5 3-slot tasks
                SlurmJob(
                    id="73", state="PD", nodes=1, cpus_total=1, cpus_min_per_node=1, pending_reason="Resources"
                ),  # 1 1-slot task
                SlurmJob(
                    id="74", state="PD", nodes=2, cpus_total=2, cpus_min_per_node=1, pending_reason="Resources"
                ),  # 2 1-slot tasks forced on 2 nodes
                SlurmJob(
                    id="75", state="PD", nodes=3, cpus_total=12, cpus_min_per_node=4, pending_reason="Resources"
                ),  # 3 4-slot tasks
                SlurmJob(
                    id="76", state="PD", nodes=1, cpus_total=3, cpus_min_per_node=1, pending_reason="Resources"
                ),  # 3 1-slot tasks
            ],
            9,
        ),
        ([], 0),
    ],
    ids=["single_job", "multiple_jobs", "no_jobs"],
)
def test_get_required_nodes(pending_jobs, expected_required_nodes, mocker):
    mocker.patch("common.schedulers.slurm_commands.get_jobs_info", return_value=pending_jobs, autospec=True)
    spy = mocker.patch("jobwatcher.plugins.slurm.get_pending_jobs_info", wraps=get_pending_jobs_info)

    instance_properties = {"slots": 4, "gpus": 0}
    max_cluster_size = 10

    assert_that(get_required_nodes(instance_properties, max_cluster_size)).is_equal_to(expected_required_nodes)
    spy.assert_called_with(
        filter_by_pending_reasons=PENDING_RESOURCES_REASONS,
        max_nodes_filter=max_cluster_size,
        instance_properties=instance_properties,
    )


@pytest.mark.parametrize(
    "pending_jobs, expected_required_nodes",
    [
        (
            [
                SlurmJob(
                    id="72",
                    state="PD",
                    nodes=2,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    tres_per_job={"gpu": 5},
                    pending_reason="Priority",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:3, gpu:1}, {cpu:3, gpu:1}]
                SlurmJob(
                    id="73",
                    state="PD",
                    nodes=2,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    tres_per_job={"gpu": 5},
                    pending_reason="Resources",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:3, gpu:1}, {cpu:3, gpu:1}, {cpu:3, gpu:1}, {cpu:3, gpu:1}]
                SlurmJob(
                    id="74",
                    state="PD",
                    nodes=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    tres_per_job={"gpu": 3},
                    pending_reason="Priority",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:3, gpu:1}, {cpu:3, gpu:1}, {cpu:3, gpu:1}, {cpu:3, gpu:1}, {cpu:3, gpu:1}]
            ],
            5,
        ),
        (
            [
                SlurmJob(
                    id="72",
                    state="PD",
                    nodes=2,
                    tasks=3,
                    cpus_total=3,
                    cpus_min_per_node=3,
                    cpus_per_task=1,
                    tres_per_task={"gpu": 2},
                    pending_reason="Priority",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:2, gpu:1}, {cpu:2, gpu:1}]
                SlurmJob(
                    id="73",
                    state="PD",
                    nodes=1,
                    tasks=2,
                    cpus_total=2,
                    cpus_min_per_node=2,
                    cpus_per_task=1,
                    tres_per_task={"gpu": 2},
                    pending_reason="Resources",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:2, gpu:1}, {cpu:2, gpu:1}, {cpu:2, gpu:0}]
                SlurmJob(
                    id="74",
                    state="PD",
                    nodes=1,
                    tasks=1,
                    cpus_total=2,
                    cpus_min_per_node=2,
                    cpus_per_task=2,
                    tres_per_task={"gpu": 1},
                    pending_reason="Resources",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:0, gpu:0}, {cpu:2, gpu:1}, {cpu:2, gpu:0}]
            ],
            3,
        ),
        (
            [
                SlurmJob(
                    id="72",
                    state="PD",
                    nodes=2,
                    cpus_total=2,
                    cpus_min_per_node=1,
                    tres_per_node={"gpu": 2},
                    pending_reason="Priority",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:3, gpu:2}, {cpu:3, gpu:2}]
                SlurmJob(
                    id="73",
                    state="PD",
                    nodes=3,
                    cpus_total=3,
                    cpus_min_per_node=1,
                    tres_per_node={"gpu": 1},
                    pending_reason="Resources",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:2, gpu:1}, {cpu:2, gpu:1}, {cpu:3, gpu:3}]
                SlurmJob(
                    id="74",
                    state="PD",
                    nodes=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    tres_per_node={"gpu": 3},
                    pending_reason="Resources",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:2, gpu:1}, {cpu:2, gpu:1}, {cpu:2, gpu:0}]
            ],
            3,
        ),
        (
            [
                SlurmJob(
                    id="72",
                    state="PD",
                    nodes=2,
                    cpus_total=2,
                    cpus_min_per_node=1,
                    tres_per_job={"gpu": 5},
                    pending_reason="Priority",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:3, gpu:1}, {cpu:3, gpu:1}]
                SlurmJob(
                    id="73",
                    state="PD",
                    nodes=2,
                    tasks=3,
                    cpus_total=3,
                    cpus_min_per_node=1,
                    cpus_per_task=1,
                    tres_per_task={"gpu": 2},
                    pending_reason="Resources",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:3, gpu:1}, {cpu:3, gpu:1}, {cpu:2, gpu:1}, {cpu:2, gpu:1}]
                SlurmJob(
                    id="74",
                    state="PD",
                    nodes=4,
                    cpus_total=4,
                    cpus_min_per_node=1,
                    tres_per_node={"gpu": 1},
                    pending_reason="Resources",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:2, gpu:0}, {cpu:2, gpu:0}, {cpu:1, gpu:0}, {cpu:1, gpu:0}]
            ],
            4,
        ),
        (
            [
                SlurmJob(
                    id="75",
                    state="PD",
                    nodes=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    tres_per_job={"gpu": 2},
                    cpus_per_tres={"gpu": 3},
                    pending_reason="Priority",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:1, gpu:1}, {cpu:1, gpu:1}]
                SlurmJob(
                    id="76",
                    state="PD",
                    nodes=1,
                    cpus_total=1,
                    cpus_min_per_node=1,
                    cpus_per_tres={"gpu": 4},  # Invalid if we don't specify GPU
                    pending_reason="Resources",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:0, gpu:1}, {cpu:1, gpu:1}]
            ],
            2,
        ),
    ],
    ids=["job_only", "task_only", "node_only", "mix", "cpus_per_tres"],
)
def test_get_required_nodes_gpu(pending_jobs, expected_required_nodes, mocker):
    mocker.patch("common.schedulers.slurm_commands.get_jobs_info", return_value=pending_jobs, autospec=True)
    spy = mocker.patch("jobwatcher.plugins.slurm.get_pending_jobs_info", wraps=get_pending_jobs_info)

    instance_properties = {"slots": 4, "gpus": 4}
    max_cluster_size = 10

    assert_that(get_required_nodes(instance_properties, max_cluster_size)).is_equal_to(expected_required_nodes)
    spy.assert_called_with(
        filter_by_pending_reasons=PENDING_RESOURCES_REASONS,
        max_nodes_filter=max_cluster_size,
        instance_properties=instance_properties,
    )


@pytest.mark.parametrize(
    "pending_jobs, expected_required_nodes",
    [
        (
            [
                SlurmJob(id="72", state="PD", nodes=1, cpus_total=4, cpus_min_per_node=4, pending_reason="Priority"),
                # Nodes/resources available after processing this job:
                # {cpu:0, gpu:4}]
                SlurmJob(
                    id="73",
                    state="PD",
                    nodes=2,
                    cpus_total=2,
                    cpus_min_per_node=1,
                    tres_per_job={"gpu": 5},
                    pending_reason="Resources",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:0, gpu:4}, {cpu:3, gpu:1}, {cpu:3, gpu:1}]
                SlurmJob(
                    id="74",
                    state="PD",
                    nodes=2,
                    cpus_total=6,
                    cpus_min_per_node=3,
                    tres_per_node={"gpu": 1},
                    pending_reason="Priority",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:0, gpu:4}, {cpu:0, gpu:0}, {cpu:0, gpu:0}]
            ],
            3,
        ),
        (
            [
                SlurmJob(
                    id="72",
                    state="PD",
                    nodes=1,
                    tasks=3,
                    cpus_total=3,
                    cpus_min_per_node=3,
                    cpus_per_task=1,
                    tres_per_task={"gpu": 1},
                    pending_reason="Priority",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:1, gpu:1}]
                SlurmJob(
                    id="73",
                    state="PD",
                    nodes=1,
                    tasks=2,
                    cpus_total=2,
                    cpus_min_per_node=2,
                    cpus_per_task=1,
                    tres_per_task={"gpu": 2},
                    pending_reason="Resources",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:1, gpu:1}, {cpu:2, gpu:0}]
                SlurmJob(
                    id="74",
                    state="PD",
                    nodes=2,
                    tasks=2,
                    cpus_total=6,
                    cpus_min_per_node=3,
                    cpus_per_task=3,
                    tres_per_task={"gpu": 4},
                    pending_reason="Resources",
                ),
                # Nodes/resources available after processing this job:
                # [{cpu:1, gpu:1}, {cpu:2, gpu:0}, {cpu:1, gpu:0}, {cpu:1, gpu:0}]
            ],
            4,
        ),
    ],
    ids=["actual_less_than_sum_of_nodes", "actual_equal_sum_of_nodes"],
)
def test_get_required_nodes_mix(pending_jobs, expected_required_nodes, mocker):
    mocker.patch("common.schedulers.slurm_commands.get_jobs_info", return_value=pending_jobs, autospec=True)
    spy = mocker.patch("jobwatcher.plugins.slurm.get_pending_jobs_info", wraps=get_pending_jobs_info)

    instance_properties = {"slots": 4, "gpus": 4}
    max_cluster_size = 10

    assert_that(get_required_nodes(instance_properties, max_cluster_size)).is_equal_to(expected_required_nodes)
    spy.assert_called_with(
        filter_by_pending_reasons=PENDING_RESOURCES_REASONS,
        max_nodes_filter=max_cluster_size,
        instance_properties=instance_properties,
    )
