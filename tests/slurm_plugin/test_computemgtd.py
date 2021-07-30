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


import logging
import os

import pytest
import slurm_plugin
from assertpy import assert_that
from slurm_plugin.computemgtd import ComputemgtdConfig, _is_self_node_down, _self_terminate
from slurm_plugin.slurm_resources import DynamicNode


@pytest.mark.parametrize(
    ("config_file", "expected_attributes"),
    [
        (
            "default.conf",
            {
                "cluster_name": "hit",
                "region": "us-east-2",
                "_boto3_config": {"retries": {"max_attempts": 1, "mode": "standard"}},
                "clustermgtd_timeout": 600,
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
    mocker.patch("slurm_plugin.computemgtd.run_command")
    mocker.patch("slurm_plugin.computemgtd.open", return_value=open(test_datadir / config_file, "r"))
    compute_config = ComputemgtdConfig("mocked_config_path")
    for key in expected_attributes:
        assert_that(compute_config.__dict__.get(key)).is_equal_to(expected_attributes.get(key))


@pytest.mark.parametrize(
    "mock_node_info, expected_result",
    [
        (
            [DynamicNode("queue1-st-c5xlarge-1", "ip-1", "host-1", "DOWN*+CLOUD", "queue1")],
            True,
        ),
        (
            [DynamicNode("queue1-st-c5xlarge-1", "ip-1", "host-1", "IDLE+CLOUD+DRAIN", "queue1")],
            False,
        ),
        (
            [DynamicNode("queue1-st-c5xlarge-1", "ip-1", "host-1", "DOWN+CLOUD+DRAIN", "queue1")],
            True,
        ),
        (
            [DynamicNode("queue1-st-c5xlarge-1", "ip-1", "host-1", "IDLE+CLOUD+POWER", "queue1")],
            True,
        ),
        (
            Exception,
            True,
        ),
    ],
    ids=["node_down", "node_drained_idle", "node_drained_down", "node_power_save", "cant_get_node_info"],
)
def test_is_self_node_down(mock_node_info, expected_result, mocker):
    if mock_node_info is Exception:
        mocker.patch("slurm_plugin.computemgtd._get_nodes_info_with_retry", side_effect=Exception())
    else:
        mocker.patch("slurm_plugin.computemgtd._get_nodes_info_with_retry", return_value=mock_node_info)

    assert_that(_is_self_node_down("queue1-st-c5xlarge-1")).is_equal_to(expected_result)


def test_self_terminate(mocker, caplog):
    """Verify self-termination is implemented via a shutdown command rather than calling TerminateInstances."""
    run_command_patch = mocker.patch("slurm_plugin.computemgtd.run_command")
    sleep_patch = mocker.patch("slurm_plugin.computemgtd.time.sleep")
    with caplog.at_level(logging.INFO):
        _self_terminate()
    assert_that(caplog.text).contains("Preparing to self terminate the instance in 10 seconds!")
    assert_that(caplog.text).contains("Self terminating instance now!")
    run_command_patch.assert_called_with("sudo shutdown -h now")
    sleep_patch.assert_called_with(10)
