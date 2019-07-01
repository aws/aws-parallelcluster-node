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
from common.schedulers.torque_commands import (
    TorqueHost,
    add_nodes,
    delete_nodes,
    get_compute_nodes_info,
    wait_nodes_initialization,
)
from tests.common import read_text


@pytest.mark.parametrize(
    "qmgr_output, hosts, expected_succeeded_hosts",
    [
        ("", ["ip-10-0-0-157", "ip-10-0-0-155"], ["ip-10-0-0-157", "ip-10-0-0-155"]),
        (
            "qmgr obj=ip-10-0-0-157 svr=default: Node name already exists",
            ["ip-10-0-0-157", "ip-10-0-0-155"],
            ["ip-10-0-0-157", "ip-10-0-0-155"],
        ),
        ("qmgr obj=ip-10-0-0-157 svr=default: Error", ["ip-10-0-0-157", "ip-10-0-0-155"], ["ip-10-0-0-155"]),
        ("unexpected error message", ["ip-10-0-0-157", "ip-10-0-0-155"], []),
        ("", [], []),
    ],
    ids=["all_successful", "already_existing", "failed_one_node", "unexpected_err_message", "no_nodes"],
)
def test_add_nodes(qmgr_output, hosts, expected_succeeded_hosts, mocker):
    mock = mocker.patch(
        "common.schedulers.torque_commands.check_command_output", return_value=qmgr_output, autospec=True
    )
    succeeded_hosts = add_nodes(hosts, slots=4)

    if hosts:
        mock.assert_called_with(
            '/opt/torque/bin/qmgr -c "create node {0} np=4"'.format(",".join(hosts)), log_error=False
        )
    if expected_succeeded_hosts:
        assert_that(succeeded_hosts).contains_only(*expected_succeeded_hosts)
    else:
        assert_that(succeeded_hosts).is_empty()


@pytest.mark.parametrize(
    "qmgr_output, hosts, expected_succeeded_hosts",
    [
        ("", ["ip-10-0-0-157", "ip-10-0-0-155"], ["ip-10-0-0-157", "ip-10-0-0-155"]),
        (
            "qmgr obj=ip-10-0-0-157 svr=default: Unknown node",
            ["ip-10-0-0-157", "ip-10-0-0-155"],
            ["ip-10-0-0-157", "ip-10-0-0-155"],
        ),
        ("qmgr obj=ip-10-0-0-157 svr=default: Error", ["ip-10-0-0-157", "ip-10-0-0-155"], ["ip-10-0-0-155"]),
        ("unexpected error message", ["ip-10-0-0-157", "ip-10-0-0-155"], []),
        ("", [], []),
    ],
    ids=["all_successful", "already_existing", "failed_one_node", "unexpected_err_message", "no_nodes"],
)
def test_delete_nodes(qmgr_output, hosts, expected_succeeded_hosts, mocker):
    mock = mocker.patch(
        "common.schedulers.torque_commands.check_command_output", return_value=qmgr_output, autospec=True
    )
    succeeded_hosts = delete_nodes(hosts)

    if hosts:
        mock.assert_called_with('/opt/torque/bin/qmgr -c "delete node {0} "'.format(",".join(hosts)), log_error=False)
    if expected_succeeded_hosts:
        assert_that(succeeded_hosts).contains_only(*expected_succeeded_hosts)
    else:
        assert_that(succeeded_hosts).is_empty()


def test_wait_nodes_initialization(mocker, test_datadir):
    pbsnodes_output = read_text(test_datadir / "pbsnodes_output.xml")
    mock = mocker.patch(
        "common.schedulers.torque_commands.check_command_output", return_value=pbsnodes_output, autospec=True
    )

    hosts = ["ip-10-0-1-242", "ip-10-0-0-196"]
    result = wait_nodes_initialization(hosts)

    mock.assert_called_with("/opt/torque/bin/pbsnodes -x {0}".format(" ".join(hosts)))
    assert_that(result).is_true()


def test_get_compute_nodes_info(mocker, test_datadir):
    pbsnodes_output = read_text(test_datadir / "pbsnodes_output.xml")
    mock = mocker.patch(
        "common.schedulers.torque_commands.check_command_output", return_value=pbsnodes_output, autospec=True
    )

    nodes = get_compute_nodes_info(hostname_filter=["host1"])

    mock.assert_called_with("/opt/torque/bin/pbsnodes -x host1")
    assert_that(nodes).is_equal_to(
        {
            "ip-10-0-0-196": TorqueHost(name="ip-10-0-0-196", slots=1000, state="down,offline"),
            "ip-10-0-1-242": TorqueHost(name="ip-10-0-1-242", slots=4, state="free"),
        }
    )
