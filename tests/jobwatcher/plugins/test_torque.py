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
from common.schedulers.torque_commands import TorqueHost
from jobwatcher.plugins.torque import get_busy_nodes


@pytest.mark.parametrize(
    "compute_nodes, expected_busy_nodes",
    [
        ({"ip-10-0-0-196": TorqueHost(name="ip-10-0-0-196", slots=1000, state="down,offline", jobs=None)}, 0),
        (
            {
                "ip-10-0-0-196": TorqueHost(
                    name="ip-10-0-0-196",
                    slots=1000,
                    state="down,offline",
                    jobs="0/137.ip-10-0-0-196.eu-west-1.compute.internal",
                )
            },
            1,
        ),
        (
            {
                "ip-10-0-0-196": TorqueHost(name="ip-10-0-0-196", slots=1000, state="down,offline", jobs=None),
                "ip-10-0-1-242": TorqueHost(
                    name="ip-10-0-1-242", slots=4, state="free", jobs="0/137.ip-10-0-0-196.eu-west-1.compute.internal"
                ),
                "ip-10-0-1-237": TorqueHost(
                    name="ip-10-0-1-237",
                    slots=4,
                    state="job-exclusive",
                    jobs="1/136.ip-10-0-0-196.eu-west-1.compute.internal,2/137.ip-10-0-0-196.eu-west-1.compute.internal,"
                    "0,3/138.ip-10-0-0-196.eu-west-1.compute.internal",
                ),
            },
            2,
        ),
        ({}, 0),
    ],
    ids=["single_node_free", "single_node_busy", "multiple_nodes", "no_nodes"],
)
def test_get_busy_nodes(compute_nodes, expected_busy_nodes, mocker):
    mock = mocker.patch("jobwatcher.plugins.torque.get_compute_nodes_info", return_value=compute_nodes, autospec=True)

    count = get_busy_nodes()

    mock.assert_called_with()
    assert_that(count).is_equal_to(expected_busy_nodes)
