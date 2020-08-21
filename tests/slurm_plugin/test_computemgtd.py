# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.


import os
from datetime import datetime, timedelta, timezone
from unittest.mock import mock_open

import pytest
from assertpy import assert_that

import slurm_plugin
from common.schedulers.slurm_commands import SlurmNode
from slurm_plugin.common import TIMESTAMP_FORMAT
from slurm_plugin.computemgtd import ComputemgtdConfig, _get_clustermgtd_heartbeat, _is_self_node_down


@pytest.mark.parametrize(
    ("config_file", "expected_attributes"),
    [
        (
            "default.conf",
            {
                "cluster_name": "hit",
                "region": "us-east-2",
                "_boto3_config": {"retries": {"max_attempts": 1, "mode": "standard"}},
                "clustermgtd_timeout": 180,
                "clustermgtd_heartbeat_file_path": "/home/ec2-user/clustermgtd_heartbeat",
                "disable_computemgtd_actions": False,
                "_slurm_nodename_file": "/etc/parallelcluster/slurm_plugin/slurm_nodename",
                "nodename": "some_nodename",
                "loop_time": 60,
                "logging_config": os.path.join(
                    os.path.dirname(slurm_plugin.__file__), "logging", "parallelcluster_computemgtd_logging.conf"
                ),
            },
        ),
        (
            "all_options.conf",
            {
                "cluster_name": "hit",
                "region": "us-east-2",
                "loop_time": 300,
                "clustermgtd_timeout": 30,
                "clustermgtd_heartbeat_file_path": "/home/ubuntu/clustermgtd_heartbeat",
                "_slurm_nodename_file": "/my/nodename/path",
                "nodename": "some_nodename",
                "disable_computemgtd_actions": True,
                "_boto3_config": {
                    "retries": {"max_attempts": 1, "mode": "standard"},
                    "proxies": {"https": "my.resume.proxy"},
                },
                "logging_config": "/path/to/logging/config",
            },
        ),
    ],
)
def test_computemgtd_config(config_file, expected_attributes, test_datadir, mocker):
    mocker.patch("slurm_plugin.computemgtd.ComputemgtdConfig._read_nodename_from_file", return_value="some_nodename")
    compute_config = ComputemgtdConfig(test_datadir / config_file)
    for key in expected_attributes:
        assert_that(compute_config.__dict__.get(key)).is_equal_to(expected_attributes.get(key))


@pytest.mark.parametrize(
    "time, expected_parsed_time",
    [
        (
            datetime(2020, 7, 30, 19, 34, 2, 613338, tzinfo=timezone.utc),
            datetime(2020, 7, 30, 19, 34, 2, 613338, tzinfo=timezone.utc),
        ),
        (
            datetime(2020, 7, 30, 10, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            datetime(2020, 7, 30, 10, 1, 1, tzinfo=timezone(timedelta(hours=1))),
        ),
    ],
)
def test_get_clustermgtd_heartbeat(time, expected_parsed_time, mocker):
    mocker.patch("slurm_plugin.computemgtd.open", mock_open(read_data=time.strftime(TIMESTAMP_FORMAT)))
    assert_that(_get_clustermgtd_heartbeat("some file path")).is_equal_to(expected_parsed_time)


@pytest.mark.parametrize(
    "mock_node_info, expected_result",
    [
        ([SlurmNode("nodename-1", "ip-1", "host-1", "DOWN*+CLOUD")], True,),
        ([SlurmNode("nodename-1", "ip-1", "host-1", "IDLE+CLOUD+DRAIN")], False,),
        ([SlurmNode("nodename-1", "ip-1", "host-1", "DOWN+CLOUD+DRAIN")], True,),
        (Exception, True,),
    ],
    ids=["node_down", "node_drained_idle", "node_drained_down", "cant_get_node_info"],
)
def test_is_self_node_down(mock_node_info, expected_result, mocker):
    if mock_node_info is Exception:
        mocker.patch("slurm_plugin.computemgtd._get_nodes_info_with_retry", side_effect=Exception())
    else:
        mocker.patch("slurm_plugin.computemgtd._get_nodes_info_with_retry", return_value=mock_node_info)

    assert_that(_is_self_node_down("nodename-1")).is_equal_to(expected_result)
