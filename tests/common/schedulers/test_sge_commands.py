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
from common.schedulers.sge_commands import (
    QCONF_COMMANDS,
    SgeHost,
    SgeJob,
    exec_qconf_command,
    get_compute_nodes_info,
    get_jobs_info,
    get_pending_jobs_info,
)
from sqswatcher.sqswatcher import Host
from tests.common import read_text


@pytest.mark.parametrize(
    "qstat_mocked_response, expected_output",
    [
        (
            "qstat_output_mix.xml",
            {
                "ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=3,
                    slots_reserved=0,
                    state="",
                    jobs=[
                        SgeJob(number="89", slots=1, state="r", node_type="MASTER", array_index=None, hostname=None),
                        SgeJob(number="89", slots=1, state="r", node_type="SLAVE", array_index=None, hostname=None),
                        SgeJob(number="91", slots=1, state="r", node_type="MASTER", array_index=3, hostname=None),
                    ],
                ),
                "ip-10-0-0-52.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-52.eu-west-1.compute.internal",
                    slots_total=8,
                    slots_used=0,
                    slots_reserved=0,
                    state="d",
                    jobs=[],
                ),
                "ip-10-0-0-116.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-0-116.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="au",
                    jobs=[],
                ),
            },
        ),
        ("qstat_output_empty.xml", {}),
        ("qstat_output_empty_xml.xml", {}),
    ],
    ids=["mixed_output", "empty_output", "empty_xml_output"],
)
def test_get_compute_nodes_info(qstat_mocked_response, expected_output, test_datadir, mocker):
    qstat_output = read_text(test_datadir / qstat_mocked_response)
    mock = mocker.patch(
        "common.schedulers.sge_commands.check_sge_command_output", return_value=qstat_output, autospec=True
    )

    nodes = get_compute_nodes_info()

    mock.assert_called_with("qstat -xml -g dt -u '*' -f")
    assert_that(nodes).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "qstat_mocked_response, expected_output",
    [
        (
            "qstat_output_mix.xml",
            [
                SgeJob(
                    number="95",
                    slots=1,
                    state="hr",
                    node_type="MASTER",
                    array_index=3,
                    hostname="ip-10-0-0-166.eu-west-1.compute.internal",
                ),
                SgeJob(
                    number="96",
                    slots=1,
                    state="r",
                    node_type="MASTER",
                    array_index=None,
                    hostname="ip-10-0-0-166.eu-west-1.compute.internal",
                ),
                SgeJob(
                    number="96",
                    slots=1,
                    state="r",
                    node_type="SLAVE",
                    array_index=None,
                    hostname="ip-10-0-0-166.eu-west-1.compute.internal",
                ),
                SgeJob(
                    number="96",
                    slots=1,
                    state="r",
                    node_type="SLAVE",
                    array_index=None,
                    hostname="ip-10-0-0-166.eu-west-1.compute.internal",
                ),
                SgeJob(
                    number="73",
                    slots=1,
                    state="s",
                    node_type="MASTER",
                    array_index=None,
                    hostname="ip-10-0-0-52.eu-west-1.compute.internal",
                ),
                SgeJob(
                    number="94",
                    slots=1,
                    state="r",
                    node_type="MASTER",
                    array_index=None,
                    hostname="ip-10-0-0-52.eu-west-1.compute.internal",
                ),
                SgeJob(
                    number="95",
                    slots=1,
                    state="hr",
                    node_type="MASTER",
                    array_index=1,
                    hostname="ip-10-0-0-52.eu-west-1.compute.internal",
                ),
                SgeJob(
                    number="95",
                    slots=1,
                    state="hr",
                    node_type="MASTER",
                    array_index=2,
                    hostname="ip-10-0-0-52.eu-west-1.compute.internal",
                ),
                SgeJob(number="97", slots=3, state="qw", node_type=None, array_index=None, hostname=None),
                SgeJob(number="72", slots=8, state="hqw", node_type=None, array_index=None, hostname=None),
            ],
        ),
        ("qstat_output_empty.xml", []),
        ("qstat_output_empty_xml.xml", []),
    ],
    ids=["mixed_output", "empty_output", "empty_xml_output"],
)
def test_get_jobs_info(qstat_mocked_response, expected_output, test_datadir, mocker):
    qstat_output = read_text(test_datadir / qstat_mocked_response)
    mock = mocker.patch(
        "common.schedulers.sge_commands.check_sge_command_output", return_value=qstat_output, autospec=True
    )

    nodes = get_jobs_info()

    mock.assert_called_with("qstat -xml -g dt -u '*'")
    assert_that(nodes).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "sge_job_xml, expected_output",
    [
        ("running_job.xml", SgeJob("93", 12, "r", None, None)),
        ("array_job.xml", SgeJob("92", 1, "qw", None, 3)),
        ("parallel_job.xml", SgeJob("89", 1, "sr", "SLAVE", None)),
    ],
    ids=["running_job", "array_job", "parallel_job"],
)
def test_sge_job_parsing(sge_job_xml, expected_output, test_datadir):
    job_xml = read_text(test_datadir / sge_job_xml)
    sge_job = SgeJob.from_xml(job_xml)
    assert_that(sge_job).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "sge_host_xml, expected_output",
    [
        (
            "host.xml",
            SgeHost(
                name="ip-10-0-0-166.eu-west-1.compute.internal",
                slots_total=4,
                slots_used=4,
                slots_reserved=0,
                state="",
                jobs=[
                    SgeJob(number="89", slots=1, state="r", node_type="MASTER", array_index=None, hostname=None),
                    SgeJob(number="95", slots=1, state="s", node_type=None, array_index=None, hostname=None),
                    SgeJob(number="89", slots=1, state="r", node_type="SLAVE", array_index=None, hostname=None),
                    SgeJob(number="91", slots=1, state="r", node_type="MASTER", array_index=3, hostname=None),
                ],
            ),
        ),
        (
            "host_no_jobs.xml",
            SgeHost(
                name="ip-10-0-0-166.eu-west-1.compute.internal",
                slots_total=4,
                slots_used=0,
                slots_reserved=0,
                state="au",
                jobs=[],
            ),
        ),
    ],
    ids=["host", "host_no_jobs"],
)
def test_sge_host_parsing(sge_host_xml, expected_output, test_datadir):
    host_xml = read_text(test_datadir / sge_host_xml)
    sge_host = SgeHost.from_xml(host_xml)
    assert_that(sge_host).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "qconf_output, command, expected_succeeded_hosts",
    [
        (
            (
                'adminhost "ip-10-0-0-157.eu-west-1.compute.internal" already exists\n'
                "ip-10-0-0-155.eu-west-1.compute.internal added to administrative host list\n"
                'can\'t resolve hostname "ip-10-0-0"'
            ),
            "ADD_ADMINISTRATIVE_HOST",
            ["ip-10-0-0-157", "ip-10-0-0-155"],
        ),
        (
            (
                'submithost "ip-10-0-0-157.eu-west-1.compute.internal" already exists\n'
                "ip-10-0-0-155.eu-west-1.compute.internal added to submit host list\n"
                'can\'t resolve hostname "ip-10-0-0"'
            ),
            "ADD_SUBMIT_HOST",
            ["ip-10-0-0-157", "ip-10-0-0-155"],
        ),
        (
            (
                'root@ip-10-0-0-208.eu-west-1.compute.internal removed "ip-10-0-0-157.eu-west-1.compute.internal" from administrative host list\n'  # noqa E501
                'denied: administrative host "ip-10-0-0-155" does not exist\n'
                'can\'t resolve hostname "ip-10-0-0"'
            ),
            "REMOVE_ADMINISTRATIVE_HOST",
            ["ip-10-0-0-157", "ip-10-0-0-155"],
        ),
        (
            (
                'root@ip-10-0-0-208.eu-west-1.compute.internal removed "ip-10-0-0-157.eu-west-1.compute.internal" from submit host list\n'  # noqa E501
                'denied: submit host "ip-10-0-0-155" does not exist\n'
                'can\'t resolve hostname "ip-10-0-0"'
            ),
            "REMOVE_SUBMIT_HOST",
            ["ip-10-0-0-157", "ip-10-0-0-155"],
        ),
        (
            (
                'root@ip-10-0-0-208.eu-west-1.compute.internal removed "ip-10-0-0-157.eu-west-1.compute.internal" from execution host list\n'  # noqa E501
                'denied: execution host "ip-10-0-0-155" does not exist\n'
                'can\'t resolve hostname "ip-10-0-0"'
            ),
            "REMOVE_EXECUTION_HOST",
            ["ip-10-0-0-157", "ip-10-0-0-155"],
        ),
    ],
    ids=["add_administrative", "add_submit", "remove_administrative", "remove_submit", "remove_execution"],
)
def test_qconf_commands(qconf_output, command, expected_succeeded_hosts, mocker):
    mock = mocker.patch(
        "common.schedulers.sge_commands.check_sge_command_output", return_value=qconf_output, autospec=True
    )
    hosts = [Host("id", "ip-10-0-0-157", 1, 0), Host("id", "ip-10-0-0-155", 1, 0), Host("id", "ip-10-0-0", 1, 0)]
    succeeded_hosts = exec_qconf_command(hosts, QCONF_COMMANDS[command])

    mock.assert_called_with(
        "qconf {0} ip-10-0-0-157,ip-10-0-0-155,ip-10-0-0".format(QCONF_COMMANDS[command].command_flags),
        raise_on_error=False,
    )
    assert_that([host.hostname for host in succeeded_hosts]).contains_only(*expected_succeeded_hosts)


@pytest.mark.parametrize(
    "pending_jobs, skip_if_state, max_slots, expected_filtered_jobs",
    [
        ([SgeJob(number="89", slots=1, state="qw")], None, None, [SgeJob(number="89", slots=1, state="qw")]),
        ([SgeJob(number="89", slots=1, state="qw")], "a", None, [SgeJob(number="89", slots=1, state="qw")]),
        ([SgeJob(number="89", slots=1, state="qwh")], "h", None, []),
        ([SgeJob(number="89", slots=41, state="qw")], None, 40, []),
        ([SgeJob(number="89", slots=41, state="qw")], None, 41, [SgeJob(number="89", slots=41, state="qw")]),
        (
            [
                SgeJob(number="89", slots=10, state="qw"),
                SgeJob(number="90", slots=5, state="qwh"),
                SgeJob(number="91", slots=1, state="qw"),
                SgeJob(number="92", slots=41, state="qwh"),
            ],
            "h",
            None,
            [SgeJob(number="89", slots=10, state="qw"), SgeJob(number="91", slots=1, state="qw")],
        ),
        (
            [
                SgeJob(number="89", slots=10, state="qw"),
                SgeJob(number="90", slots=5, state="qwh"),
                SgeJob(number="91", slots=1, state="qw"),
                SgeJob(number="92", slots=41, state="qwh"),
            ],
            "h",
            6,
            [SgeJob(number="91", slots=1, state="qw")],
        ),
    ],
    ids=["no_filter", "skip_not_present", "skip_present", "max_slots", "max_slots_no_filter", "mix_skip_state", "mix"],
)
def test_get_pending_jobs_info(pending_jobs, skip_if_state, max_slots, expected_filtered_jobs, mocker):
    mock = mocker.patch("common.schedulers.sge_commands.get_jobs_info", return_value=pending_jobs, autospec=True)

    assert_that(get_pending_jobs_info(max_slots, skip_if_state)).is_equal_to(expected_filtered_jobs)
    mock.assert_called_with(job_state_filter="p")
