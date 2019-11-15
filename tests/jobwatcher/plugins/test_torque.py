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
from jobwatcher.plugins.torque import get_busy_nodes, get_required_nodes


@pytest.mark.parametrize(
    "compute_nodes, expected_busy_nodes",
    [
        ({"ip-10-0-0-196": TorqueHost(name="ip-10-0-0-196", slots=1000, state="free", jobs=None)}, 0),
        (
            {
                "ip-10-0-0-196": TorqueHost(
                    name="ip-10-0-0-196",
                    slots=1000,
                    state="job-exclusive",
                    jobs="0/137.ip-10-0-0-196.eu-west-1.compute.internal",
                )
            },
            1,
        ),
        (
            {
                "ip-10-0-0-196": TorqueHost(name="ip-10-0-0-196", slots=1000, state="free", jobs=None),
                "ip-10-0-1-242": TorqueHost(
                    name="ip-10-0-1-242", slots=4, state="free", jobs="0/137.ip-10-0-0-196.eu-west-1.compute.internal"
                ),
                "ip-10-0-1-237": TorqueHost(
                    name="ip-10-0-1-237",
                    slots=4,
                    state="job-exclusive",
                    jobs="1/136.ip-10-0-0-196.eu-west-1.compute.internal,"
                    "2/137.ip-10-0-0-196.eu-west-1.compute.internal,"
                    "0,3/138.ip-10-0-0-196.eu-west-1.compute.internal",
                ),
            },
            2,
        ),
        ({}, 0),
        ({"ip-10-0-0-196": TorqueHost(name="ip-10-0-0-196", slots=1000, state=["down", "offline"], jobs=None)}, 0),
        (
            {
                "ip-10-0-0-196": TorqueHost(
                    name="ip-10-0-0-196", slots=1000, state=["down", "offline", "MOM-list-not-sent"], jobs=None
                )
            },
            0,
        ),
    ],
    ids=["single_node_free", "single_node_busy", "multiple_nodes", "no_nodes", "offline", "initializing"],
)
def test_get_busy_nodes(compute_nodes, expected_busy_nodes, mocker):
    mock = mocker.patch("jobwatcher.plugins.torque.get_compute_nodes_info", return_value=compute_nodes, autospec=True)

    count = get_busy_nodes()

    mock.assert_called_with()
    assert_that(count).is_equal_to(expected_busy_nodes)


@pytest.mark.parametrize(
    "pending_jobs, expected_required_nodes",
    [
        (
            [
                TorqueJob(
                    id="149.ip-10-0-0-196.eu-west-1.compute.internal",
                    state="Q",
                    resources_list=TorqueResourceList(nodes_resources=[(1, 2)], nodes_count=1, ncpus=None),
                )
            ],
            1,
        ),
        (
            [
                # This requires 1 full node
                TorqueJob(
                    id="150.ip-10-0-0-196.eu-west-1.compute.internal",
                    state="Q",
                    resources_list=TorqueResourceList(nodes_resources=None, nodes_count=None, ncpus=4),
                ),
                # This requires 2 nodes
                TorqueJob(
                    id="151.ip-10-0-0-196.eu-west-1.compute.internal",
                    state="Q",
                    resources_list=TorqueResourceList(nodes_resources=[(1, 2), (1, 4)], nodes_count=2, ncpus=None),
                ),
                # This fits into existing node
                TorqueJob(
                    id="151.ip-10-0-0-196.eu-west-1.compute.internal",
                    state="Q",
                    resources_list=TorqueResourceList(nodes_resources=[(1, 2)], nodes_count=1, ncpus=None),
                ),
                # This fits into 2 node
                TorqueJob(
                    id="151.ip-10-0-0-196.eu-west-1.compute.internal",
                    state="Q",
                    resources_list=TorqueResourceList(nodes_resources=None, nodes_count=2, ncpus=None),
                ),
            ],
            5,
        ),
        ([], 0),
        (
            [
                TorqueJob(
                    id="149.ip-10-0-0-196.eu-west-1.compute.internal",
                    state="Q",
                    resources_list=TorqueResourceList(nodes_resources=[(2, 4)], nodes_count=2, ncpus=None),
                ),
                TorqueJob(
                    id="150.ip-10-0-0-196.eu-west-1.compute.internal",
                    state="Q",
                    resources_list=TorqueResourceList(nodes_resources=None, nodes_count=1, ncpus=None),
                ),
            ],
            3,
        ),
    ],
    ids=["single_job", "multiple_jobs", "no_jobs", "three_full_nodes"],
)
def test_get_required_nodes(pending_jobs, expected_required_nodes, mocker):
    mocker.patch("jobwatcher.plugins.torque.get_pending_jobs_info", return_value=pending_jobs, autospec=True)

    instance_properties = {"slots": 4}
    max_cluster_size = 10

    required_nodes = get_required_nodes(instance_properties, max_cluster_size)
    assert_that(required_nodes).is_equal_to(expected_required_nodes)
