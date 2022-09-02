# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import json
import logging
import os
import sys
from configparser import ConfigParser
from logging.config import fileConfig

from botocore.config import Config
from common.schedulers.slurm_commands import resume_powering_down_nodes, update_all_partitions
from slurm_plugin.clustermgtd import ComputeFleetStatus, ComputeFleetStatusManager
from slurm_plugin.common import log_exception
from slurm_plugin.instance_manager import InstanceManager
from slurm_plugin.slurm_resources import CONFIG_FILE_DIR, PartitionStatus

log = logging.getLogger(__name__)


class SlurmFleetManagerConfig:
    DEFAULTS = {
        "max_retry": 5,
        "terminate_max_batch_size": 1000,
        "proxy": "NONE",
        "logging_config": os.path.join(
            os.path.dirname(__file__), "logging", "parallelcluster_fleet_status_manager_logging.conf"
        ),
    }

    def __init__(self, config_file_path):
        self._get_config(config_file_path)

    def __repr__(self):
        attrs = ", ".join(["{key}={value}".format(key=key, value=repr(value)) for key, value in self.__dict__.items()])
        return "{class_name}({attrs})".format(class_name=self.__class__.__name__, attrs=attrs)

    @log_exception(log, "reading fleet status manager configuration file", catch_exception=IOError, raise_on_error=True)
    def _get_config(self, config_file_path):
        """Get fleetmanager configuration."""
        log.info("Reading %s", config_file_path)

        config = ConfigParser()
        try:
            config.read_file(open(config_file_path, "r"))
        except IOError:
            log.error("Cannot read slurm fleet manager configuration file: %s", config_file_path)
            raise

        self.region = config.get("slurm_fleet_status_manager", "region")
        self.cluster_name = config.get("slurm_fleet_status_manager", "cluster_name")
        self.terminate_max_batch_size = config.getint(
            "slurm_fleet_status_manager",
            "terminate_max_batch_size",
            fallback=self.DEFAULTS.get("terminate_max_batch_size"),
        )
        self._boto3_retry = config.getint(
            "slurm_fleet_status_manager", "boto3_retry", fallback=self.DEFAULTS.get("max_retry")
        )
        self._boto3_config = {"retries": {"max_attempts": self._boto3_retry, "mode": "standard"}}
        proxy = config.get("slurm_fleet_status_manager", "proxy", fallback=self.DEFAULTS.get("proxy"))
        if proxy != "NONE":
            self._boto3_config["proxies"] = {"https": proxy}
        self.boto3_config = Config(**self._boto3_config)

        self.logging_config = config.get(
            "slurm_fleet_status_manager", "logging_config", fallback=self.DEFAULTS.get("logging_config")
        )

        log.info(self.__repr__())


def _manage_fleet_status_transition(config, computefleet_status_data_path):
    computefleet_status = _get_computefleet_status(computefleet_status_data_path)

    if ComputeFleetStatus.is_stop_requested(computefleet_status):
        _stop_partitions(config)
    elif ComputeFleetStatus.is_start_requested(computefleet_status):
        _start_partitions()


def _start_partitions():
    log.info("Setting slurm partitions to UP and resuming nodes...")
    update_all_partitions(PartitionStatus.UP, reset_node_addrs_hostname=False)
    resume_powering_down_nodes()


def _stop_partitions(config):
    log.info("Setting slurm partitions to INACTIVE and terminating all compute nodes...")
    update_all_partitions(PartitionStatus.INACTIVE, reset_node_addrs_hostname=True)
    instance_manager = InstanceManager(
        config.region,
        config.cluster_name,
        config.boto3_config,
    )
    instance_manager.terminate_all_compute_nodes(config.terminate_max_batch_size)


def _get_computefleet_status(computefleet_status_data_path):
    try:
        with open(computefleet_status_data_path, "r", encoding="utf-8") as computefleet_status_data_file:
            computefleet_status = ComputeFleetStatus(
                json.load(computefleet_status_data_file).get(ComputeFleetStatusManager.COMPUTE_FLEET_STATUS_ATTRIBUTE)
            )
        log.info("ComputeFleet status is: %s", computefleet_status)
    except Exception as e:
        log.error("Cannot read compute fleet status data file: %s.\nException: %s", computefleet_status_data_path, e)
        raise

    return computefleet_status


def main():
    default_log_file = "/var/log/parallelcluster/slurm_fleet_status_manager.log"
    logging.basicConfig(
        filename=default_log_file,
        level=logging.INFO,
        format="%(asctime)s - [%(name)s:%(funcName)s] - %(levelname)s - %(message)s",
    )
    log.info("FleetManager startup.")
    args = _parse_arguments()
    try:
        config_file = os.environ.get(
            "CONFIG_FILE", os.path.join(CONFIG_FILE_DIR, "parallelcluster_slurm_fleet_status_manager.conf")
        )
        fleet_status_manager_config = SlurmFleetManagerConfig(config_file)
        try:
            # Configure root logger
            fileConfig(fleet_status_manager_config.logging_config, disable_existing_loggers=False)
        except Exception as e:
            log.warning(
                "Unable to configure logging from %s, using default settings and writing to %s.\nException: %s",
                fleet_status_manager_config.logging_config,
                default_log_file,
                e,
            )
        log.info("FleetManager config: %s", fleet_status_manager_config)
        _manage_fleet_status_transition(fleet_status_manager_config, args.computefleet_status_data)
        log.info("FleetManager finished.")
    except Exception as e:
        log.exception("Encountered exception when running fleet manager: %s", e)
        sys.exit(1)


def _parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-cf", "--computefleet-status-data", help="Path to compute fleet status data", required=True)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
