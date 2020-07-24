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
from logging.config import fileConfig

import argparse
from botocore.config import Config
from configparser import ConfigParser

from common.schedulers.slurm_commands import get_nodes_info, set_nodes_idle
from slurm_plugin.common import CONFIG_FILE_DIR, InstanceManager

log = logging.getLogger(__name__)


class SlurmSuspendConfig:
    DEFAULTS = {
        "max_retry": 5,
        # max boto3 terminate_instance call size is 1000
        "max_batch_size": 1000,
        "proxy": "NONE",
        "logging_config": os.path.join(os.path.dirname(__file__), "logging", "parallelcluster_suspend_logging.conf"),
    }

    def __init__(self, config_file_path):
        self._get_config(config_file_path)

    def __repr__(self):
        attrs = ", ".join(["{key}={value}".format(key=key, value=repr(value)) for key, value in self.__dict__.items()])
        return "{class_name}({attrs})".format(class_name=self.__class__.__name__, attrs=attrs)

    def _get_config(self, config_file_path):
        """Get resume program configuration."""
        log.info("Reading %s", config_file_path)

        config = ConfigParser()
        try:
            config.read_file(open(config_file_path, "r"))
        except IOError:
            log.error(f"Cannot read slurm cloud bursting scripts configuration file: {config_file_path}")
            raise

        self.region = config.get("slurm_suspend", "region")
        self.cluster_name = config.get("slurm_suspend", "cluster_name")
        self.max_batch_size = config.getint(
            "slurm_suspend", "max_batch_size", fallback=self.DEFAULTS.get("max_batch_size")
        )

        # Configure boto3 to retry 5 times by default
        self._boto3_config = {"retries": {"max_attempts": self.DEFAULTS.get("max_retry"), "mode": "standard"}}
        proxy = config.get("slurm_suspend", "proxy", fallback=self.DEFAULTS.get("proxy"))
        if proxy != "NONE":
            self._boto3_config["proxies"] = {"https": proxy}
        self.boto3_config = Config(**self._boto3_config)
        self.logging_config = config.get(
            "slurm_suspend", "logging_config", fallback=self.DEFAULTS.get("logging_config")
        )

        log.info(self.__repr__())


def _set_nodes_idle(slurm_nodenames):
    """
    Set POWER_DOWN nodes back to IDLE by resuming the node.

    This is also the fall back mechanism to handle failure when removing instances.
    When encountering a failure, update the node state to IDLE so the node can be used for future jobs.
    Cloud-sync daemon will be responsible for removing orphaned nodes.
    """
    log.info("Resuming the following nodes back to IDLE: %s", slurm_nodenames)
    set_nodes_idle(slurm_nodenames, reset_node_addrs_hostname=True)


def _suspend(arg_nodes, suspend_config):
    """Suspend and terminate nodes requested by slurm."""
    log.info("Suspending nodes: " + arg_nodes)

    # Retrieve SlurmNode objects from slurm nodelist notation
    slurm_nodes = get_nodes_info(arg_nodes)
    log.debug("Slurm_nodes = %s", slurm_nodes)

    instance_manager = InstanceManager(suspend_config.region, suspend_config.cluster_name, suspend_config.boto3_config)
    instance_manager.terminate_associated_instances(
        slurm_nodes, suspend_config.max_batch_size,
    )

    log.info("Finished removing instances for nodes %s", arg_nodes)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("nodes", help="Nodes to release")
    args = parser.parse_args()
    try:
        suspend_config = SlurmSuspendConfig(os.path.join(CONFIG_FILE_DIR, "parallelcluster_slurm_suspend.conf"))
        try:
            # Configure root logger
            fileConfig(suspend_config.logging_config, disable_existing_loggers=False)
        except Exception as e:
            default_log_file = "/var/log/parallelcluster/slurm_suspend.log"
            logging.basicConfig(
                filename=default_log_file,
                level=logging.INFO,
                format="%(asctime)s - [%(name)s:%(funcName)s] - %(levelname)s - %(message)s",
            )
            log.warning(
                "Unable to configure logging with %s, using default settings and writing to %s.\nException: %s",
                suspend_config.logging_config,
                default_log_file,
                e,
            )
        _suspend(args.nodes, suspend_config)
    except Exception as e:
        log.exception("Encountered exception when suspending instances for %s: %s", args.nodes, e)
    finally:
        _set_nodes_idle(args.nodes)


if __name__ == "__main__":
    main()
