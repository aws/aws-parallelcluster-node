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
from common.schedulers.sge_commands import SGE_HOLD_STATE, SgeHost, SgeJob
from jobwatcher.plugins.sge import get_busy_nodes, get_required_nodes


@pytest.mark.parametrize(
    "cluster_nodes, expected_busy_nodes",
    [
        (
            {
                "ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=3,
                    slots_reserved=0,
                    state="",
                    jobs=[SgeJob(number="89", slots=1, state="r", node_type="MASTER", array_index=None, hostname=None)],
                )
            },
            1,
        ),
        (
            {
                "ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="u",
                    jobs=[],
                )
            },
            1,
        ),
        (
            {
                "ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="a",
                    jobs=[],
                ),
                "ip-10-0-0-167.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-167.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="",
                    jobs=[],
                ),
            },
            0,
        ),
        (
            {
                "ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="a",
                    jobs=[],
                ),
                "ip-10-0-0-167.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-167.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="",
                    jobs=[],
                ),
                "ip-10-0-0-168.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-168.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=1,
                    state="",
                    jobs=[],
                ),
                "ip-10-0-0-169.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-169.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=3,
                    slots_reserved=0,
                    state="",
                    jobs=[SgeJob(number="89", slots=1, state="r", node_type="MASTER", array_index=None, hostname=None)],
                ),
                "ip-10-0-0-170.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-170.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="d",
                    jobs=[],
                ),
                "ip-10-0-0-171.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-171.eu-west-1.compute.internal",
                    slots_total=0,
                    slots_used=0,
                    slots_reserved=0,
                    state="ao",
                    jobs=[],
                ),
            },
            3,
        ),
        (
            {
                "ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="auo",
                    jobs=[],
                )
            },
            0,
        ),
    ],
    ids=["single_running_job", "unavailable_node", "available_nodes", "mixed_nodes", "orphaned_node"],
)
def test_get_busy_nodes(cluster_nodes, expected_busy_nodes, mocker):
    mocker.patch("jobwatcher.plugins.sge.get_compute_nodes_info", return_value=cluster_nodes, autospec=True)

    assert_that(get_busy_nodes()).is_equal_to(expected_busy_nodes)


@pytest.mark.parametrize(
    "pending_jobs, expected_required_nodes",
    [
        ([SgeJob(number="89", slots=3, state="qw")], 1),
        (
            [
                SgeJob(number="89", slots=10, state="qw"),
                SgeJob(number="90", slots=5, state="qw"),
                SgeJob(number="91", slots=1, state="qw"),
                SgeJob(number="92", slots=40, state="qw"),
            ],
            14,
        ),
        ([], 0),
    ],
    ids=["single_job", "multiple_jobs", "no_jobs"],
)
def test_get_required_nodes(pending_jobs, expected_required_nodes, mocker):
    mock = mocker.patch("jobwatcher.plugins.sge.get_pending_jobs_info", return_value=pending_jobs, autospec=True)

    instance_properties = {"slots": 4}
    max_cluster_size = 10

    assert_that(get_required_nodes(instance_properties, max_cluster_size)).is_equal_to(expected_required_nodes)
    mock.assert_called_with(
        max_slots_filter=max_cluster_size * instance_properties["slots"], skip_if_state=SGE_HOLD_STATE
    )
