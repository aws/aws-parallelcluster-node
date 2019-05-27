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
from nodewatcher.plugins.sge import hasJobs, hasPendingJobs, is_node_down


@pytest.mark.parametrize(
    "hostname, compute_nodes_output, expected_result",
    [
        (
            "ip-10-0-0-166",
            {
                "ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="",
                    jobs=[],
                )
            },
            False,
        ),
        (
            "ip-10-0-0-166",
            {
                "ip-10-0-0-166": SgeHost(
                    name="ip-10-0-0-166", slots_total=4, slots_used=0, slots_reserved=0, state="", jobs=[]
                )
            },
            False,
        ),
        ("ip-10-0-0-166", {}, True),
        (
            "ip-10-0-0-166",
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
            True,
        ),
        ("ip-10-0-0-166", Exception, True),
    ],
    ids=["healthy", "healthy_short", "not_attached", "error_state", "exception"],
)
def test_terminate_if_down(hostname, compute_nodes_output, expected_result, mocker):
    mocker.patch("nodewatcher.plugins.sge.check_command_output", return_value=hostname, autospec=True)
    mocker.patch(
        "nodewatcher.plugins.sge.socket.getfqdn", return_value=hostname + ".eu-west-1.compute.internal", autospec=True
    )
    if compute_nodes_output is Exception:
        mock = mocker.patch("nodewatcher.plugins.sge.get_compute_nodes_info", side_effect=Exception(), autospec=True)
    else:
        mock = mocker.patch(
            "nodewatcher.plugins.sge.get_compute_nodes_info", return_value=compute_nodes_output, autospec=True
        )

    assert_that(is_node_down()).is_equal_to(expected_result)
    mock.assert_called_with(hostname)


@pytest.mark.parametrize(
    "pending_jobs, expected_result",
    [
        ([SgeJob(number="89", slots=1, state="qw")], (True, False)),
        (
            [
                SgeJob(number="89", slots=10, state="qw"),
                SgeJob(number="90", slots=5, state="qw"),
                SgeJob(number="91", slots=1, state="qw"),
                SgeJob(number="92", slots=40, state="qw"),
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
        mock = mocker.patch("nodewatcher.plugins.sge.get_pending_jobs_info", side_effect=Exception(), autospec=True)
    else:
        mock = mocker.patch("nodewatcher.plugins.sge.get_pending_jobs_info", return_value=pending_jobs, autospec=True)

    instance_properties = {"slots": 4}
    max_cluster_size = 10

    assert_that(hasPendingJobs(instance_properties, max_cluster_size)).is_equal_to(expected_result)
    mock.assert_called_with(
        max_slots_filter=max_cluster_size * instance_properties["slots"], skip_if_state=SGE_HOLD_STATE
    )


@pytest.mark.parametrize(
    "jobs, expected_result",
    [([SgeJob(number="89", slots=1, state="qw")], True), ([], False), (Exception, False)],
    ids=["single_job", "no_jobs", "failure"],
)
def test_has_jobs(jobs, expected_result, mocker):
    if jobs is Exception:
        mock = mocker.patch("nodewatcher.plugins.sge.get_jobs_info", side_effect=Exception(), autospec=True)
    else:
        mock = mocker.patch("nodewatcher.plugins.sge.get_jobs_info", return_value=jobs, autospec=True)

    hostname = "ip-1-0-0-1"

    assert_that(hasJobs(hostname)).is_equal_to(expected_result)
    mock.assert_called_with(hostname_filter=hostname, job_state_filter="rs")
