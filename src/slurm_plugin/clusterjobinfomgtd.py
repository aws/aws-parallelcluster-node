# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
from configparser import ConfigParser
from datetime import datetime, timezone
from logging.config import fileConfig

from botocore.config import Config
from common.schedulers.slurm_commands import get_jobs_info
from common.time_utils import seconds
from common.utils import sleep_remaining_loop_time
from retrying import retry
from slurm_plugin.cluster_event_publisher import ClusterEventPublisher
from slurm_plugin.common import log_exception

CONFIG_FILE_DIR = "/etc/parallelcluster"
LOOP_TIME = 180
log = logging.getLogger(__name__)
event_logger = log.getChild("events")


# Utils
def _seconds(sec):
    """Convert seconds to milliseconds."""
    return sec * 1000


def _minutes(min):
    """Convert minutes to seconds."""
    return min * 60


class JobinfomgtdConfig:
    """Represents the job info management demon configuration."""

    DEFAULTS = {
        "max_retry": 5,
        "loop_time": LOOP_TIME,
        "proxy": "NONE",
        "logging_config": os.path.join(os.path.dirname(__file__), "logging", "jobinfomgtd_logging.conf"),
        "cpu_gpu_cost_ratio": 1,
        "custom_discount_rate": 0,
    }

    def __init__(self, config_file_path):
        self._get_config(config_file_path)

    def __repr__(self):  # noqa: D105
        attrs = ", ".join([f"{key}={repr(value)}" for key, value in self.__dict__.items()])
        return f"{self.__class__.__name__}({attrs})"

    def __eq__(self, other):  # noqa: D105
        if type(other) is type(self):
            return self._config == other._config
        return False

    def __ne__(self, other):  # noqa: D105
        return not self.__eq__(other)

    @log_exception(log, "reading job info manger configuration file", catch_exception=Exception, raise_on_error=True)
    def _get_config(self, config_file_path):
        """Get jobinfomgtd configuration."""
        log.info("Reading %s", config_file_path)
        self._config = ConfigParser()
        with open(config_file_path, "r", encoding="utf-8") as config_file:
            self._config.read_file(config_file)

        # Get config settings
        self._get_basic_config(self._config)

    def _get_basic_config(self, config):
        """Get basic config options."""
        self.region = config.get("clusterjobinfomgtd", "region")
        self.cluster_name = config.get("clusterjobinfomgtd", "cluster_name")
        self.logging_config = config.get(
            "clusterjobinfomgtd", "logging_config", fallback=self.DEFAULTS.get("logging_config")
        )
        self.loop_time = config.getint("clusterjobinfomgtd", "loop_time", fallback=self.DEFAULTS.get("loop_time"))
        self.cpu_gpu_cost_ratio = config.getfloat(
            "clusterjobinfomgtd",
            "cpu_gpu_cost_ratio",
            fallback=self.DEFAULTS.get("cpu_gpu_cost_ratio"),
        )
        self.custom_discount_rate = config.getfloat(
            "clusterjobinfomgtd",
            "custom_discount_rate",
            fallback=self.DEFAULTS.get("custom_discount_rate"),
        )

        # Configure boto3 to retry 1 times by default
        self._boto3_retry = config.getint("clusterjobinfomgtd", "boto3_retry", fallback=self.DEFAULTS.get("max_retry"))
        self._boto3_config = {"retries": {"max_attempts": self._boto3_retry, "mode": "standard"}}
        # Configure proxy
        proxy = config.get("clusterjobinfomgtd", "proxy", fallback=self.DEFAULTS.get("proxy"))
        if proxy != "NONE":
            self._boto3_config["proxies"] = {"https": proxy}
        self.boto3_config = Config(**self._boto3_config)
        self.head_node_instance_id = config.get("clusterjobinfomgtd", "instance_id", fallback="unknown")


class JobInfoManager:
    """The job info manager."""

    def __init__(self, config):
        """Initialize JobInfoManager."""
        self._config = None
        self._current_time = None
        self.set_config(config)

    def process_job_info(self):
        """Get job information from scontrol and publish job information as json log events."""
        jobs = get_jobs_info()
        if jobs:
            log.info("Find jobs information. Publishing job information to log...")
        self._event_publisher.publish_job_info_events(jobs)

    def set_config(self, config):
        if self._config != config:
            log.info("Applying new jobinfomgtd config: %s", config)
            self._config = config
            self._event_publisher = ClusterEventPublisher.create_with_default_publisher(
                event_logger, config.cluster_name, "HeadNode", "clusterjobinfomgtd", config.head_node_instance_id
            )


def _run_clusterjobinfomgtd(config_file):
    config = JobinfomgtdConfig(config_file)
    job_info_manager = JobInfoManager(config=config)
    while True:
        # Get loop start time
        start_time = datetime.now(tz=timezone.utc)
        # Get program config
        try:
            config = JobinfomgtdConfig(config_file)
            job_info_manager.set_config(config)
        except Exception as e:
            log.warning("Unable to reload daemon config from %s, using previous one.\nException: %s", config_file, e)
        # Configure root logger
        try:
            fileConfig(config.logging_config, disable_existing_loggers=False)
        except Exception as e:
            log.warning(
                "Unable to configure logging from %s, using default logging settings.\nException: %s",
                config.logging_config,
                e,
            )
        job_info_manager.process_job_info()
        sleep_remaining_loop_time(config.loop_time, start_time)


@retry(wait_fixed=seconds(LOOP_TIME))
def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - [%(module)s:%(funcName)s] - %(levelname)s - %(message)s"
    )
    log.info("Clusterjobinfomgtd Startup")
    try:
        jobinfomgtd_config_file = os.environ.get(
            "CONFIG_FILE", os.path.join(CONFIG_FILE_DIR, "parallelcluster_jobinfomgtd.conf")
        )
        _run_clusterjobinfomgtd(jobinfomgtd_config_file)
    except Exception as e:
        log.exception("An unexpected error occurred: %s.\nRestarting in %s seconds...", e, LOOP_TIME)
        raise


if __name__ == "__main__":
    main()
