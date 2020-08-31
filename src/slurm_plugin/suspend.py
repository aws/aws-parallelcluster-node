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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("nodes", help="Nodes to release")
    args = parser.parse_args()
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
    log.info("Suspending following nodes. Clustermgtd will cleanup orphaned instances: %s", args.nodes)
    log.info("SuspendProgram finished. Nodes will be available after SuspendTimeout")


if __name__ == "__main__":
    main()
