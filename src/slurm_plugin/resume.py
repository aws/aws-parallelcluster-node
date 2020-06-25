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

import collections
import logging
import os
import re
import subprocess
from logging.config import fileConfig

import argparse
import boto3
from botocore.config import Config
from configparser import ConfigParser
from retrying import retry

from common.schedulers.slurm_commands import get_nodes_info, set_nodes_down, set_nodes_power_down, update_nodes
from common.utils import grouper
from slurm_plugin.common import CONFIG_FILE_PATH

log = logging.getLogger(__name__)

failed_nodes = []


class SlurmResumeConfig:
    DEFAULTS = {
        "max_retry": 5,
        "max_batch_size": 100,
        "update_node_address": True,
        "proxy": "NONE",
        "logging_config": os.path.join(os.path.dirname(__file__), "logging", "parallelcluster_resume_logging.conf"),
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

        self.region = config.get("slurm_resume", "region")
        self.cluster_name = config.get("slurm_resume", "cluster_name")
        self.max_batch_size = config.getint(
            "slurm_resume", "max_batch_size", fallback=self.DEFAULTS.get("max_batch_size")
        )
        self.update_node_address = config.getboolean(
            "slurm_resume", "update_node_address", fallback=self.DEFAULTS.get("update_node_address")
        )

        # Configure boto3 to retry 5 times by default
        self._boto3_config = {"retries": {"max_attempts": self.DEFAULTS.get("max_retry"), "mode": "standard"}}
        proxy = config.get("slurm_resume", "proxy", fallback=self.DEFAULTS.get("proxy"))
        if proxy != "NONE":
            self._boto3_config["proxies"] = {"https": proxy}
        self.boto3_config = Config(**self._boto3_config)
        self.logging_config = config.get("slurm_resume", "logging_config", fallback=self.DEFAULTS.get("logging_config"))

        log.info(self.__repr__())


@retry(stop_max_attempt_number=3, wait_fixed=5000)
def _set_nodes_down_and_power_save(node_list):
    set_nodes_down(node_list, reason="Failure when resuming nodes")
    set_nodes_power_down(node_list, reason="Failure when resuming nodes")


def _handle_failed_nodes(node_list):
    """
    Fall back mechanism to handle failure when launching instances.

    When encountering a failure, want slurm to deallocate current nodes,
    and re-queue job to be run automatically by new nodes.
    To do this, set node to DOWN, so slurm will automatically re-queue job.
    Then set node to POWER_DOWN so suspend program will be run.
    Suspend program needs to properly clean up instances(if any) and set node back to IDLE in all cases.

    If this process is not done explicitly, slurm will wait until ResumeTimeout,
    then execute this process of setting nodes to DOWN then POWER_DOWN.
    To save time, should explicitly set nodes to DOWN then POWER_DOWN after encountering failure.
    """
    try:
        log.info("Following nodes marked as down and placed into power_down: %s", node_list)
        _set_nodes_down_and_power_save(node_list)
    except Exception as e:
        log.error("Failed to place nodes %s into down/power_down with exception: %s", node_list, e)


def _update_slurm_node_addrs(slurm_nodes, instance_ids, instance_ips, instance_hostnames):
    """Update node information in slurm with info from launched EC2 instance."""
    try:
        update_nodes(slurm_nodes, nodeaddrs=instance_ips, nodehostnames=instance_hostnames, raise_on_error=True)
        log.info(
            "Nodes %s are now configured with instance=%s private_ip=%s nodehostname=%s",
            slurm_nodes,
            instance_ids,
            instance_ips,
            instance_hostnames,
        )
    except subprocess.CalledProcessError:
        log.error(
            "Encountered error when updating node %s with instance=%s private_ip=%s nodehostname=%s",
            slurm_nodes,
            instance_ids,
            instance_ips,
            instance_hostnames,
        )
        failed_nodes.extend(slurm_nodes)


def _validate_nodename(nodename):
    """
    Check and validate nodename format.

    Valid NodeName format: {queue_name}-{static/dynamic}-{instance_type}-{number}
    Sample NodeName: queue1-static-c5.xlarge-2
    Nodename will be parsed on '-'
    Verify there are no extra '-' in parts of the nodename
    """
    if not re.match(r"^[^\-]+-[^\-]+-[^\-]+-[\d]+$", nodename):
        log.error("Invalid nodename format for node %s", nodename)
        failed_nodes.append(nodename)
        return False

    return True


def _parse_requested_instances(node_list):
    """
    Parse out instance type from slurm NodeName.

    Valid NodeName format: {queue_name}-{static/dynamic}-{instance_type}-{number}
    Sample NodeName: queue1-static-c5.xlarge-2
    """
    instances_to_launch = collections.defaultdict(lambda: collections.defaultdict(list))
    for node in node_list:
        if _validate_nodename(node):
            queue_name, _, instance_type = node.split("-")[0:3]
            instances_to_launch[queue_name][instance_type].append(node)
        else:
            log.warning("Discarding NodeName with invalid format: %s", node)
    log.info("instances_to_launch = %s", instances_to_launch)

    return instances_to_launch


def _launch_ec2_instances(ec2_client, cluster_name, queue, instance_type, current_batch_size):
    """Launch a batch of ec2 instances."""
    result = ec2_client.run_instances(
        # To-do, evaluate best effort scaling for future
        MinCount=current_batch_size,
        MaxCount=current_batch_size,
        # LaunchTemplate is different for every instance type in every queue
        # LaunchTemplate name format: {cluster_name}-{queue_name}-{instance_type}
        # Sample LT name: hit-queue1-c5.xlarge
        LaunchTemplate={"LaunchTemplateName": f"{cluster_name}-{queue}-{instance_type}"},
    )

    return result["Instances"]


def _parse_launched_instances(launched_instances):
    """Parse run_instance output."""
    instance_ids = []
    instance_ips = []
    instance_hostnames = []
    for instance in launched_instances:
        instance_ids.append(instance["InstanceId"])
        instance_ips.append(instance["PrivateIpAddress"])
        instance_hostnames.append(instance["PrivateDnsName"].split(".")[0])
    return instance_ids, instance_ips, instance_hostnames


def _add_instances(node_list, resume_config):
    """Launch EC2 instances for cloud nodes."""
    ec2_client = boto3.client("ec2", region_name=resume_config.region, config=resume_config.boto3_config)

    instances_to_launch = _parse_requested_instances(node_list)
    for queue, queue_instances in instances_to_launch.items():
        for instance_type, slurm_node_list in queue_instances.items():
            log.info("Launching instances for slurm nodes %s", slurm_node_list)
            for batch_nodes in grouper(slurm_node_list, resume_config.max_batch_size):
                try:
                    launched_instances = _launch_ec2_instances(
                        ec2_client, resume_config.cluster_name, queue, instance_type, len(batch_nodes)
                    )
                    if resume_config.update_node_address:
                        instance_ids, instance_ips, instance_hostnames = _parse_launched_instances(launched_instances)
                        _update_slurm_node_addrs(list(batch_nodes), instance_ids, instance_ips, instance_hostnames)
                except Exception as e:
                    log.error("Encountered exception when launching instances for nodes %s: %s", list(batch_nodes), e)
                    failed_nodes.extend(batch_nodes)


def _resume(arg_nodes, resume_config):
    """Launch new EC2 nodes according to nodes requested by slurm."""
    log.info("Launching EC2 instances for the following Slurm nodes: %s", arg_nodes)
    node_list = [node.name for node in get_nodes_info(arg_nodes)]
    log.info("Retrieved nodelist: %s", node_list)

    _add_instances(node_list, resume_config)
    success_nodes = [node for node in node_list if node not in failed_nodes]
    log.info("Successfully launched nodes %s", success_nodes)
    if failed_nodes:
        log.info("Failed to launch following nodes, powering down: %s", failed_nodes)
        _handle_failed_nodes(failed_nodes)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("nodes", help="Nodes to burst")
    args = parser.parse_args()
    try:
        resume_config = SlurmResumeConfig(CONFIG_FILE_PATH)
        try:
            # Configure root logger
            fileConfig(resume_config.logging_config, disable_existing_loggers=False)
        except Exception as e:
            default_log_file = "/var/log/parallelcluster/slurm_resume.log"
            logging.basicConfig(
                filename=default_log_file,
                level=logging.INFO,
                format="%(asctime)s - [%(name)s:%(funcName)s] - %(levelname)s - %(message)s",
            )
            log.warning(
                "Unable to configure logging from %s, using default settings and writing to %s.\nException: %s",
                resume_config.logging_config,
                default_log_file,
                e,
            )
        _resume(args.nodes, resume_config)
    except Exception as e:
        log.exception("Encountered exception when requesting instances for %s: %s", args.nodes, e)
        _handle_failed_nodes(args.nodes)


if __name__ == "__main__":
    main()
