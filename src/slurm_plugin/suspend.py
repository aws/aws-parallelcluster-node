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
from configparser import ConfigParser

from common.schedulers.slurm_commands import set_nodes_idle
from slurm_plugin.common import CONFIG_FILE_DIR

log = logging.getLogger(__name__)


class SlurmSuspendConfig:
    DEFAULTS = {
        "logging_config": os.path.join(os.path.dirname(__file__), "logging", "parallelcluster_suspend_logging.conf"),
    }

    def __init__(self, config_file_path):
        config = ConfigParser()
        try:
            config.read_file(open(config_file_path, "r"))
        except IOError:
            log.error(f"Cannot read slurm cloud bursting scripts configuration file: {config_file_path}")
            raise

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
                "Unable to configure logging from %s, using default settings and writing to %s.\nException: %s",
                suspend_config.logging_config,
                default_log_file,
                e,
            )
        log.info("Resetting following nodes into IDLE. Clustermgtd will cleanup orphaned instances: %s", args.nodes)
        _set_nodes_idle(args.nodes)
        log.info("SuspendProgram finished.")
    except Exception as e:
        log.exception("Encountered exception %s when setting following nodes to DOWN: %s", e, args.nodes)


if __name__ == "__main__":
    main()
