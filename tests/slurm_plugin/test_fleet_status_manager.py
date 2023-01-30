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
from types import SimpleNamespace
from unittest.mock import ANY

import botocore
import pytest
import slurm_plugin
from assertpy import assert_that
from slurm_plugin.clustermgtd import ComputeFleetStatus
from slurm_plugin.fleet_status_manager import (
    SlurmFleetManagerConfig,
    _get_computefleet_status,
    _manage_fleet_status_transition,
    _start_partitions,
    _stop_partitions,
)
from slurm_plugin.slurm_resources import PartitionStatus


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
    return "slurm_plugin.instance_manager.boto3"


@pytest.mark.parametrize(
    ("config_file", "expected_attributes"),
    [
        (
            "default.conf",
            {
                "cluster_name": "test",
                "region": "us-east-2",
                "terminate_max_batch_size": 1000,
                "_boto3_config": {"retries": {"max_attempts": 5, "mode": "standard"}},
                "logging_config": os.path.join(
                    os.path.dirname(slurm_plugin.__file__),
                    "logging",
                    "parallelcluster_fleet_status_manager_logging.conf",
                ),
            },
        ),
        (
            "all_options.conf",
            {
                "cluster_name": "test_again",
                "region": "us-east-1",
                "terminate_max_batch_size": 50,
                "_boto3_config": {
                    "retries": {"max_attempts": 10, "mode": "standard"},
                    "proxies": {"https": "my.resume.proxy"},
                },
                "logging_config": "/path/to/fleet_status_manager_logging/config",
            },
        ),
    ],
)
def test_fleet_status_manager_config(config_file, expected_attributes, test_datadir):
    resume_config = SlurmFleetManagerConfig(test_datadir / config_file)
    for key in expected_attributes:
        assert_that(resume_config.__dict__.get(key)).is_equal_to(expected_attributes.get(key))


@pytest.mark.parametrize(
    ("computefleet_status_data_path", "status", "action"),
    [
        ("path_to_file_1", ComputeFleetStatus.STOPPED, None),
        ("path_to_file_2", ComputeFleetStatus.RUNNING, None),
        ("path_to_file_3", ComputeFleetStatus.STOPPING, None),
        ("path_to_file_4", ComputeFleetStatus.STARTING, None),
        ("path_to_file_5", ComputeFleetStatus.STOP_REQUESTED, "stop"),
        ("path_to_file_6", ComputeFleetStatus.START_REQUESTED, "start"),
        ("path_to_file_7", ComputeFleetStatus.PROTECTED, None),
    ],
)
def test_fleet_status_manager(mocker, test_datadir, computefleet_status_data_path, status, action):
    # mocks
    config = SimpleNamespace(some_key_1="some_value_1", some_key_2="some_value_2")
    get_computefleet_status_mocked = mocker.patch("slurm_plugin.fleet_status_manager._get_computefleet_status")
    get_computefleet_status_mocked.return_value = status
    stop_partitions_mocked = mocker.patch("slurm_plugin.fleet_status_manager._stop_partitions")
    start_partitions_mocked = mocker.patch("slurm_plugin.fleet_status_manager._start_partitions")

    # method to test
    _manage_fleet_status_transition(config, computefleet_status_data_path)

    # assertions
    get_computefleet_status_mocked.assert_called_once_with(computefleet_status_data_path)
    if action == "start":
        start_partitions_mocked.assert_called_once()
        stop_partitions_mocked.assert_not_called()
    elif action == "stop":
        stop_partitions_mocked.assert_called_once_with(config)
        start_partitions_mocked.assert_not_called()
    else:
        start_partitions_mocked.assert_not_called()
        stop_partitions_mocked.assert_not_called()


@pytest.mark.parametrize(
    ("config_file", "expected_status"),
    [
        ("correct_status.json", ComputeFleetStatus.RUNNING),
        ("no_status.json", ValueError),
        ("malformed_status.json", FileNotFoundError),
        ("wrong_status.json", ValueError),
        (None, TypeError),
    ],
)
def test_get_computefleet_status(test_datadir, config_file, expected_status):
    if isinstance(expected_status, ComputeFleetStatus):
        status = _get_computefleet_status(test_datadir / config_file)
        assert_that(status).is_equal_to(expected_status)
    else:
        with pytest.raises(expected_status):
            _get_computefleet_status(test_datadir / config_file)


def test_start_partitions(mocker):
    update_all_partitions_mocked = mocker.patch("slurm_plugin.fleet_status_manager.update_all_partitions")
    resume_powering_down_nodes_mocked = mocker.patch("slurm_plugin.fleet_status_manager.resume_powering_down_nodes")

    _start_partitions()

    update_all_partitions_mocked.assert_called_once_with(PartitionStatus.UP, reset_node_addrs_hostname=False)
    resume_powering_down_nodes_mocked.assert_called_once()


def test_stop_partitions(mocker):
    # mocks
    config = SimpleNamespace(
        terminate_max_batch_size="3", region="us-east-1", cluster_name="test", boto3_config=botocore.config.Config()
    )
    update_all_partitions_mocked = mocker.patch("slurm_plugin.fleet_status_manager.update_all_partitions")

    terminate_all_compute_nodes_mocked = mocker.patch.object(
        slurm_plugin.instance_manager.InstanceManager, "terminate_all_compute_nodes", autospec=True
    )

    # method to test
    _stop_partitions(config)

    # assertions
    update_all_partitions_mocked.assert_called_once_with(PartitionStatus.INACTIVE, reset_node_addrs_hostname=True)
    terminate_all_compute_nodes_mocked.assert_called_once_with(ANY, config.terminate_max_batch_size)
