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
                "_boto3_config": {"retries": {"max_attempts": 5, "mode": "standard"}},
                "clustermgtd_timeout": 180,
                "clustermgtd_heartbeat_file_path": "/home/ec2-user/clustermgtd_heartbeat",
                "disable_computemgtd_actions": False,
                "slurm_nodename_file": "/opt/parallelcluster/configs/slurm/slurm_nodename",
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
                "slurm_nodename_file": "/my/nodename/path",
                "disable_computemgtd_actions": True,
                "_boto3_config": {
                    "retries": {"max_attempts": 5, "mode": "standard"},
                    "proxies": {"https": "my.resume.proxy"},
                },
                "logging_config": "/path/to/logging/config",
            },
        ),
    ],
)
def test_computemgtd_config(config_file, expected_attributes, test_datadir):
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
    "mock_nodename_content, mock_node_info, expected_result",
    [
        ("nodename-1", [SlurmNode("nodename-1", "ip-1", "host-1", "DOWN*+CLOUD")], True,),
        ("nodename-1", [SlurmNode("nodename-1", "ip-1", "host-1", "IDLE+CLOUD+DRAIN")], False,),
        ("nodename-1", [SlurmNode("nodename-1", "ip-1", "host-1", "DOWN+CLOUD+DRAIN")], True,),
        (IOError, [SlurmNode("nodename-1", "ip-1", "host-1", "IDLE+CLOUD")], True,),
        ("nodename-1", Exception, True,),
    ],
    ids=["node_down", "node_drained_idle", "node_drained_down", "cant_get_nodename", "cant_get_node_info"],
)
def test_is_self_node_down(mock_nodename_content, mock_node_info, expected_result, mocker):
    if mock_nodename_content is IOError:
        mocker.patch("slurm_plugin.computemgtd.open", side_effect=IOError())
    else:
        mocker.patch("slurm_plugin.computemgtd.open", mock_open(read_data=mock_nodename_content))
        if mock_node_info is Exception:
            mocker.patch("slurm_plugin.computemgtd.get_nodes_info", side_effect=Exception())
        else:
            mocker.patch("slurm_plugin.computemgtd.get_nodes_info", return_value=mock_node_info)

    assert_that(_is_self_node_down("some file path")).is_equal_to(expected_result)
