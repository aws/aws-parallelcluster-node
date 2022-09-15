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


import argparse
import logging
import os
from configparser import ConfigParser
from datetime import datetime, timezone
from logging.config import fileConfig

from botocore.config import Config
from common.schedulers.slurm_commands import get_nodes_info, set_nodes_down
from common.utils import read_json
from slurm_plugin.common import is_clustermgtd_heartbeat_valid, print_with_count
from slurm_plugin.instance_manager import InstanceManager
from slurm_plugin.slurm_resources import CONFIG_FILE_DIR

log = logging.getLogger(__name__)


class SlurmResumeConfig:
    DEFAULTS = {
        "max_retry": 1,
        "max_batch_size": 500,
        "update_node_address": True,
        "clustermgtd_timeout": 300,
        "proxy": "NONE",
        "logging_config": os.path.join(os.path.dirname(__file__), "logging", "parallelcluster_resume_logging.conf"),
        "hosted_zone": None,
        "dns_domain": None,
        "use_private_hostname": False,
        "run_instances_overrides": "/opt/slurm/etc/pcluster/run_instances_overrides.json",
        "create_fleet_overrides": "/opt/slurm/etc/pcluster/create_fleet_overrides.json",
        "fleet_config_file": "/etc/parallelcluster/slurm_plugin/fleet-config.json",
        "all_or_nothing_batch": False,
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
            log.error("Cannot read slurm cloud bursting scripts configuration file: %s", config_file_path)
            raise

        self.region = config.get("slurm_resume", "region")
        self.cluster_name = config.get("slurm_resume", "cluster_name")
        self.dynamodb_table = config.get("slurm_resume", "dynamodb_table")
        self.hosted_zone = config.get("slurm_resume", "hosted_zone", fallback=self.DEFAULTS.get("hosted_zone"))
        self.dns_domain = config.get("slurm_resume", "dns_domain", fallback=self.DEFAULTS.get("dns_domain"))
        self.use_private_hostname = config.getboolean(
            "slurm_resume", "use_private_hostname", fallback=self.DEFAULTS.get("use_private_hostname")
        )
        self.head_node_private_ip = config.get("slurm_resume", "head_node_private_ip")
        self.head_node_hostname = config.get("slurm_resume", "head_node_hostname")
        self.max_batch_size = config.getint(
            "slurm_resume", "max_batch_size", fallback=self.DEFAULTS.get("max_batch_size")
        )
        self.update_node_address = config.getboolean(
            "slurm_resume", "update_node_address", fallback=self.DEFAULTS.get("update_node_address")
        )
        self.all_or_nothing_batch = config.getboolean(
            "slurm_resume", "all_or_nothing_batch", fallback=self.DEFAULTS.get("all_or_nothing_batch")
        )
        fleet_config_file = config.get(
            "slurm_resume", "fleet_config_file", fallback=self.DEFAULTS.get("fleet_config_file")
        )
        self.fleet_config = read_json(fleet_config_file)

        # run_instances_overrides_file and create_fleet_overrides_file contain a json with the following format:
        # {
        #     "queue_name": {
        #         "compute_resource_name": {
        #             <arbitrary-json-with-boto3-api-params-to-override>
        #         },
        #         ...
        #     },
        #     ...
        # }
        run_instances_overrides_file = config.get(
            "slurm_resume", "run_instances_overrides", fallback=self.DEFAULTS.get("run_instances_overrides")
        )
        self.run_instances_overrides = read_json(run_instances_overrides_file, default={})
        create_fleet_overrides_file = config.get(
            "slurm_resume", "create_fleet_overrides", fallback=self.DEFAULTS.get("create_fleet_overrides")
        )
        self.create_fleet_overrides = read_json(create_fleet_overrides_file, default={})

        self.clustermgtd_timeout = config.getint(
            "slurm_resume",
            "clustermgtd_timeout",
            fallback=self.DEFAULTS.get("clustermgtd_timeout"),
        )
        self.clustermgtd_heartbeat_file_path = config.get("slurm_resume", "clustermgtd_heartbeat_file_path")

        # Configure boto3 to retry 1 times by default
        self._boto3_retry = config.getint("slurm_resume", "boto3_retry", fallback=self.DEFAULTS.get("max_retry"))
        self._boto3_config = {"retries": {"max_attempts": self._boto3_retry, "mode": "standard"}}
        proxy = config.get("slurm_resume", "proxy", fallback=self.DEFAULTS.get("proxy"))
        if proxy != "NONE":
            self._boto3_config["proxies"] = {"https": proxy}
        self.boto3_config = Config(**self._boto3_config)
        self.logging_config = config.get("slurm_resume", "logging_config", fallback=self.DEFAULTS.get("logging_config"))

        log.info(self.__repr__())


def _handle_failed_nodes(node_list, reason="Failure when resuming nodes"):
    """
    Fall back mechanism to handle failure when launching instances.

    When encountering a failure, want slurm to deallocate current nodes,
    and re-queue job to be run automatically by new nodes.
    To do this, set node to DOWN, so slurm will automatically re-queue job.
    Then set node to POWER_DOWN so suspend program will be run and node can be reset back to power saving.

    If this process is not done explicitly, slurm will wait until ResumeTimeout,
    then execute this process of setting nodes to DOWN then POWER_DOWN.
    To save time, should explicitly set nodes to DOWN in ResumeProgram so clustermgtd can maintain failed nodes.
    Clustermgtd will be responsible for running full DOWN -> POWER_DOWN process.
    """
    try:
        log.info("Setting following failed nodes into DOWN state: %s", print_with_count(node_list))
        set_nodes_down(node_list, reason=reason)
    except Exception as e:
        log.error("Failed to place nodes %s into down with exception: %s", print_with_count(node_list), e)


def _resume(arg_nodes, resume_config):
    """Launch new EC2 nodes according to nodes requested by slurm."""
    # Check heartbeat
    current_time = datetime.now(tz=timezone.utc)
    if not is_clustermgtd_heartbeat_valid(
        current_time, resume_config.clustermgtd_timeout, resume_config.clustermgtd_heartbeat_file_path
    ):
        log.error(
            "No valid clustermgtd heartbeat detected, clustermgtd is down!\n"
            "Please check clustermgtd log for error.\n"
            "Not launching nodes %s",
            arg_nodes,
        )
        _handle_failed_nodes(arg_nodes)
        return
    log.info("Launching EC2 instances for the following Slurm nodes: %s", arg_nodes)
    node_list = [node.name for node in get_nodes_info(arg_nodes)]
    log.debug("Retrieved nodelist: %s", node_list)

    instance_manager = InstanceManager(
        resume_config.region,
        resume_config.cluster_name,
        resume_config.boto3_config,
        table_name=resume_config.dynamodb_table,
        hosted_zone=resume_config.hosted_zone,
        dns_domain=resume_config.dns_domain,
        use_private_hostname=resume_config.use_private_hostname,
        head_node_private_ip=resume_config.head_node_private_ip,
        head_node_hostname=resume_config.head_node_hostname,
        fleet_config=resume_config.fleet_config,
        run_instances_overrides=resume_config.run_instances_overrides,
        create_fleet_overrides=resume_config.create_fleet_overrides,
    )
    instance_manager.add_instances_for_nodes(
        node_list=node_list,
        launch_batch_size=resume_config.max_batch_size,
        update_node_address=resume_config.update_node_address,
        all_or_nothing_batch=resume_config.all_or_nothing_batch,
    )
    failed_nodes = set().union(*instance_manager.failed_nodes.values())
    success_nodes = [node for node in node_list if node not in failed_nodes]
    log.info("Successfully launched nodes %s", print_with_count(success_nodes))

    if failed_nodes:
        log.error(
            "Failed to launch following nodes, setting nodes to down: %s",
            print_with_count(failed_nodes),
        )
        for error_code, node_list in instance_manager.failed_nodes.items():
            _handle_failed_nodes(node_list, reason=f"(Code:{error_code})Failure when resuming nodes")


def main():
    default_log_file = "/var/log/parallelcluster/slurm_resume.log"
    logging.basicConfig(
        filename=default_log_file,
        level=logging.INFO,
        format="%(asctime)s - [%(name)s:%(funcName)s] - %(levelname)s - %(message)s",
    )
    log.info("ResumeProgram startup.")
    parser = argparse.ArgumentParser()
    parser.add_argument("nodes", help="Nodes to burst")
    args = parser.parse_args()
    try:
        config_file = os.environ.get("CONFIG_FILE", os.path.join(CONFIG_FILE_DIR, "parallelcluster_slurm_resume.conf"))
        resume_config = SlurmResumeConfig(config_file)
        try:
            # Configure root logger
            fileConfig(resume_config.logging_config, disable_existing_loggers=False)
        except Exception as e:
            log.warning(
                "Unable to configure logging from %s, using default settings and writing to %s.\nException: %s",
                resume_config.logging_config,
                default_log_file,
                e,
            )
        log.info("ResumeProgram config: %s", resume_config)
        _resume(args.nodes, resume_config)
        log.info("ResumeProgram finished.")
    except Exception as e:
        log.exception("Encountered exception when requesting instances for %s: %s", args.nodes, e)
        _handle_failed_nodes(args.nodes)


if __name__ == "__main__":
    main()
