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
from common.schedulers.torque_commands import TorqueHost, TorqueJob, TorqueResourceList
from nodewatcher.plugins.torque import has_jobs, has_pending_jobs, is_node_down, lock_host


@pytest.mark.parametrize(
    "hostname, compute_nodes_output, has_job_output, expected_result",
    [
        (
            "ip-10-0-0-196",
            {
                "ip-10-0-0-196": TorqueHost(
                    name="ip-10-0-0-196",
                    slots=1000,
                    state="job-exclusive",
                    jobs="0/137.ip-10-0-0-196.eu-west-1.compute.internal",
                )
            },
            True,
            False,
        ),
        ("ip-10-0-0-166", {}, [], True),
        (
            "ip-10-0-0-196",
            {
                "ip-10-0-0-196": TorqueHost(
                    name="ip-10-0-0-196",
                    slots=1000,
                    state="down",
                    jobs="0/137.ip-10-0-0-196.eu-west-1.compute.internal",
                )
            },
            True,
            True,
        ),
        ("ip-10-0-0-166", Exception, [], True),
        (
            "ip-10-0-0-196",
            {
                "ip-10-0-0-196": TorqueHost(
                    name="ip-10-0-0-196",
                    slots=1000,
                    state="offline",
                    jobs="0/137.ip-10-0-0-196.eu-west-1.compute.internal",
                )
            },
            True,
            False,
        ),
        (
            "ip-10-0-0-196",
            {"ip-10-0-0-196": TorqueHost(name="ip-10-0-0-196", slots=1000, state="offline", jobs="",)},
            False,
            True,
        ),
        (
            "ip-10-0-0-196",
            {
                "ip-10-0-0-196": TorqueHost(
                    name="ip-10-0-0-196",
                    slots=1000,
                    state="offline,down",
                    jobs="0/137.ip-10-0-0-196.eu-west-1.compute.internal",
                )
            },
            True,
            True,
        ),
    ],
    ids=["healthy", "not_attached", "error_state", "exception", "locked_has_job", "locked_no_job", "locked_and_error"],
)
def test_is_node_down(hostname, compute_nodes_output, has_job_output, expected_result, mocker):
    mocker.patch("nodewatcher.plugins.torque.check_command_output", return_value=hostname, autospec=True)
    if compute_nodes_output is Exception:
        mock = mocker.patch("nodewatcher.plugins.torque.get_compute_nodes_info", side_effect=Exception(), autospec=True)
    else:
        mock = mocker.patch(
            "nodewatcher.plugins.torque.get_compute_nodes_info", return_value=compute_nodes_output, autospec=True
        )
    mocker.patch("nodewatcher.plugins.torque.has_jobs", return_value=has_job_output, autospec=True)

    assert_that(is_node_down()).is_equal_to(expected_result)
    mock.assert_called_with(hostname_filter=[hostname])


@pytest.mark.parametrize(
    "pending_jobs, expected_result",
    [
        (
            [
                TorqueJob(
                    id="149.ip-10-0-0-196.eu-west-1.compute.internal",
                    state="Q",
                    resources_list=TorqueResourceList(nodes_resources=[(1, 2)], nodes_count=1, ncpus=None),
                )
            ],
            (True, False),
        ),
        (
            [
                TorqueJob(
                    id="149.ip-10-0-0-196.eu-west-1.compute.internal",
                    state="Q",
                    resources_list=TorqueResourceList(nodes_resources=[(1, 2)], nodes_count=1, ncpus=None),
                ),
                TorqueJob(
                    id="150.ip-10-0-0-196.eu-west-1.compute.internal",
                    state="Q",
                    resources_list=TorqueResourceList(nodes_resources=[(1, 2)], nodes_count=1, ncpus=None),
                ),
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
        mock = mocker.patch("nodewatcher.plugins.torque.get_pending_jobs_info", side_effect=Exception(), autospec=True)
    else:
        mock = mocker.patch(
            "nodewatcher.plugins.torque.get_pending_jobs_info", return_value=pending_jobs, autospec=True
        )

    instance_properties = {"slots": 4}
    max_cluster_size = 10

    assert_that(has_pending_jobs(instance_properties, max_cluster_size)).is_equal_to(expected_result)
    mock.assert_called_with(max_slots_filter=instance_properties["slots"], log_pending_jobs=False)


@pytest.mark.parametrize(
    "jobs, expected_result",
    [
        (
            [
                TorqueJob(
                    id="149.ip-10-0-0-196.eu-west-1.compute.internal",
                    state="Q",
                    resources_list=TorqueResourceList(nodes_resources=[(1, 2)], nodes_count=1, ncpus=None),
                )
            ],
            True,
        ),
        ([], False),
        (Exception, False),
    ],
    ids=["single_job", "no_jobs", "failure"],
)
def test_has_jobs(jobs, expected_result, mocker):
    if jobs is Exception:
        mock = mocker.patch("nodewatcher.plugins.torque.get_jobs_info", side_effect=Exception(), autospec=True)
    else:
        mock = mocker.patch("nodewatcher.plugins.torque.get_jobs_info", return_value=jobs, autospec=True)

    hostname = "ip-1-0-0-1.eu-west-1.compute.internal"

    assert_that(has_jobs(hostname)).is_equal_to(expected_result)
    mock.assert_called_with(filter_by_exec_hosts={hostname.split(".")[0]}, filter_by_states=["R", "S"])


@pytest.mark.parametrize(
    "hostname, unlock", [("ip-10-0-0-166", False), ("ip-10-0-0-166", True)],
)
def test_lock_host(hostname, unlock, mocker):
    mock = mocker.patch("nodewatcher.plugins.torque.lock_node", autospec=True)
    lock_host(hostname, unlock)
    mock.assert_called_with(hostname=hostname, unlock=unlock)
