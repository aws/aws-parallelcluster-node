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
from common.schedulers.slurm_commands import PENDING_RESOURCES_REASONS, SlurmJob
from nodewatcher.plugins.slurm import hasPendingJobs


@pytest.mark.parametrize(
    "pending_jobs, expected_result",
    [
        (
            [SlurmJob(id="72", state="PD", nodes=5, cpus_total=15, cpus_min_per_node=3, pending_reason="Priority")],
            (True, False),
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
            (True, False),
        ),
        ([], (False, False)),
        (Exception, (False, True)),
    ],
    ids=["single_job", "multiple_jobs", "no_jobs", "failure"],
)
def test_has_pending_jobs(pending_jobs, expected_result, mocker):
    if pending_jobs is Exception:
        mock = mocker.patch("nodewatcher.plugins.slurm.get_pending_jobs_info", side_effect=Exception(), autospec=True)
    else:
        mock = mocker.patch("nodewatcher.plugins.slurm.get_pending_jobs_info", return_value=pending_jobs, autospec=True)

    instance_properties = {"slots": 4}
    max_cluster_size = 10

    assert_that(hasPendingJobs(instance_properties, max_cluster_size)).is_equal_to(expected_result)
    mock.assert_called_with(
        filter_by_pending_reasons=PENDING_RESOURCES_REASONS,
        max_nodes_filter=max_cluster_size,
        max_slots_filter=instance_properties["slots"],
    )
