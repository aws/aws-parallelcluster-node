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
import time
from configparser import ConfigParser
from datetime import datetime, timezone
from io import StringIO
from logging.config import fileConfig
from subprocess import CalledProcessError

from botocore.config import Config
from common.schedulers.slurm_commands import get_nodes_info
from common.time_utils import seconds
from common.utils import check_command_output, run_command, sleep_remaining_loop_time
from retrying import retry
from slurm_plugin.common import (
    DEFAULT_COMMAND_TIMEOUT,
    expired_clustermgtd_heartbeat,
    get_clustermgtd_heartbeat,
    log_exception,
)
from slurm_plugin.slurm_resources import CONFIG_FILE_DIR

LOOP_TIME = 60
RELOAD_CONFIG_ITERATIONS = 10
# Computemgtd config is under /opt/slurm/etc/pcluster/.slurm_plugin/; all compute nodes share a config
SLURM_PLUGIN_DIR = "/opt/slurm/etc/pcluster/.slurm_plugin"
COMPUTEMGTD_CONFIG_PATH = f"{SLURM_PLUGIN_DIR}/parallelcluster_computemgtd.conf"
log = logging.getLogger(__name__)


class ComputemgtdConfig:
    DEFAULTS = {
        # Basic configs
        "max_retry": 1,
        "loop_time": LOOP_TIME,
        "proxy": "NONE",
        "disable_computemgtd_actions": False,
        "clustermgtd_timeout": 600,
        "slurm_nodename_file": os.path.join(CONFIG_FILE_DIR, "slurm_nodename"),
        "logging_config": os.path.join(
            os.path.dirname(__file__), "logging", "parallelcluster_computemgtd_logging.conf"
        ),
    }

    def __init__(self, config_file_path):
        self._get_config(config_file_path)

    def __repr__(self):
        attrs = ", ".join(["{key}={value}".format(key=key, value=repr(value)) for key, value in self.__dict__.items()])
        return "{class_name}({attrs})".format(class_name=self.__class__.__name__, attrs=attrs)

    @log_exception(log, "reading computemgtd config", catch_exception=Exception, raise_on_error=True)
    def _get_config(self, config_file_path):
        """Get computemgtd configuration."""
        log.info("Reading %s", config_file_path)
        config = ConfigParser()
        try:
            # Use subprocess based method to copy shared file to local to prevent hanging when NFS is down
            config_str = check_command_output(
                f"cat {config_file_path}",
                timeout=DEFAULT_COMMAND_TIMEOUT,
                shell=True,  # nosec
            )
            config.read_file(StringIO(config_str))
        except Exception:
            log.error("Cannot read computemgtd configuration file: %s", config_file_path)
            raise

        # Get config settings
        self.region = config.get("computemgtd", "region")
        self.cluster_name = config.get("computemgtd", "cluster_name")
        # Configure boto3 to retry 1 times by default
        self._boto3_retry = config.getint("clustermgtd", "boto3_retry", fallback=self.DEFAULTS.get("max_retry"))
        self._boto3_config = {"retries": {"max_attempts": self._boto3_retry, "mode": "standard"}}
        self.loop_time = config.getint("computemgtd", "loop_time", fallback=self.DEFAULTS.get("loop_time"))
        self.clustermgtd_timeout = config.getint(
            "computemgtd",
            "clustermgtd_timeout",
            fallback=self.DEFAULTS.get("clustermgtd_timeout"),
        )
        self.disable_computemgtd_actions = config.getboolean(
            "computemgtd",
            "disable_computemgtd_actions",
            fallback=self.DEFAULTS.get("disable_computemgtd_actions"),
        )
        self.clustermgtd_heartbeat_file_path = config.get("computemgtd", "clustermgtd_heartbeat_file_path")
        self._slurm_nodename_file = config.get(
            "computemgtd", "slurm_nodename_file", fallback=self.DEFAULTS.get("slurm_nodename_file")
        )
        self.nodename = ComputemgtdConfig._read_nodename_from_file(self._slurm_nodename_file)

        proxy = config.get("computemgtd", "proxy", fallback=self.DEFAULTS.get("proxy"))
        if proxy != "NONE":
            self._boto3_config["proxies"] = {"https": proxy}
        self.boto3_config = Config(**self._boto3_config)
        self.logging_config = config.get("computemgtd", "logging_config", fallback=self.DEFAULTS.get("logging_config"))
        # Log configuration
        log.info(self.__repr__())

    @staticmethod
    def _read_nodename_from_file(nodename_file_path):
        """Read self nodename from a file."""
        try:
            with open(nodename_file_path, "r") as nodename_file:
                nodename = nodename_file.read()
            return nodename
        except Exception as e:
            log.error("Unable to read self nodename from %s with exception: %s\n", nodename_file_path, e)
            raise


@log_exception(log, "self terminating compute instance", catch_exception=CalledProcessError, raise_on_error=False)
def _self_terminate():
    """Self terminate the instance."""
    # Sleep for 10 seconds so termination log entries are uploaded to CW logs
    log.info("Preparing to self terminate the instance in 10 seconds!")
    time.sleep(10)
    log.info("Self terminating instance now!")
    run_command("sudo shutdown -h now")


@retry(stop_max_attempt_number=3, wait_fixed=1500)
def _get_nodes_info_with_retry(nodes):
    return get_nodes_info(nodes)


def _is_self_node_down(self_nodename):
    """
    Check if self node is healthy according to the scheduler.

    Node is considered healthy if:
    1. Node is not in DOWN
    2. Node is not in POWER_SAVE
    Note: node that is incorrectly attached to the scheduler will be in DOWN* after SlurmdTimeout.
    """
    try:
        self_node = _get_nodes_info_with_retry(self_nodename)[0]
        log.info("Current self node state %s", self_node.__repr__())
        if self_node.is_down() or self_node.is_power():
            log.warning("Node is incorrectly attached to scheduler, preparing for self termination...")
            return True
        log.info("Node is correctly attached to scheduler, not terminating...")
        return False
    except Exception as e:
        # This could happen is slurmctld is down completely
        log.error("Unable to retrieve current node state from slurm with exception: %s\nConsidering node as down!", e)

    return True


def _load_daemon_config(config_file):
    # Get program config
    computemgtd_config = ComputemgtdConfig(config_file)
    # Configure root logger
    try:
        fileConfig(computemgtd_config.logging_config, disable_existing_loggers=False)
    except Exception as e:
        log.warning(
            "Unable to configure logging from %s, using default logging settings.\nException: %s",
            computemgtd_config.logging_config,
            e,
        )
    return computemgtd_config


def _run_computemgtd(config_file):
    """Run computemgtd actions."""
    # Initial default heartbeat time as computemgtd startup time
    last_heartbeat = datetime.now(tz=timezone.utc)
    log.info("Initializing clustermgtd heartbeat to be computemgtd startup time: %s", last_heartbeat)
    computemgtd_config = _load_daemon_config(config_file)
    reload_config_counter = RELOAD_CONFIG_ITERATIONS
    while True:
        # Get current time
        current_time = datetime.now(tz=timezone.utc)

        if reload_config_counter <= 0:
            try:
                computemgtd_config = _load_daemon_config(config_file)
                reload_config_counter = RELOAD_CONFIG_ITERATIONS
            except Exception as e:
                log.warning("Unable to reload daemon config, using previous one.\nException: %s", e)
        else:
            reload_config_counter -= 1

        # Check heartbeat
        try:
            last_heartbeat = get_clustermgtd_heartbeat(computemgtd_config.clustermgtd_heartbeat_file_path)
            log.info("Latest heartbeat from clustermgtd: %s", last_heartbeat)
        except Exception as e:
            log.warning(
                "Unable to retrieve clustermgtd heartbeat. Using last known heartbeat: %s with exception: %s",
                last_heartbeat,
                e,
            )
        if expired_clustermgtd_heartbeat(last_heartbeat, current_time, computemgtd_config.clustermgtd_timeout):
            if computemgtd_config.disable_computemgtd_actions:
                log.info("All computemgtd actions currently disabled")
            elif _is_self_node_down(computemgtd_config.nodename):
                _self_terminate()

        sleep_remaining_loop_time(computemgtd_config.loop_time, current_time)


@retry(wait_fixed=seconds(LOOP_TIME))
def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - [%(name)s:%(funcName)s] - %(levelname)s - %(message)s"
    )
    log.info("Computemgtd Startup")
    try:
        clustermgtd_config_file = os.environ.get("CONFIG_FILE", COMPUTEMGTD_CONFIG_PATH)
        _run_computemgtd(clustermgtd_config_file)
    except Exception as e:
        log.exception("An unexpected error occurred: %s", e)
        raise


if __name__ == "__main__":
    main()
