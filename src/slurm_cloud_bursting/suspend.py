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
from logging.config import fileConfig

import argparse
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from configparser import ConfigParser
from retrying import retry

from common.schedulers.slurm_commands import get_nodes_info, set_nodes_idle
from common.utils import grouper
from slurm_cloud_bursting.utils import CONFIG_FILE_PATH

LOG_CONFIG_FILE = "/opt/parallelcluster/configs/slurm/parallelcluster_suspend_logging.conf"
log = logging.getLogger(__name__)


class SlurmSuspendConfig:
    DEFAULT_MAX_RETRY = 5
    DEFAULT_MAX_INSTANCES_BATCH_SIZE = 100

    def __init__(self, config_file_path=None, **kwargs):
        if config_file_path:
            self._get_config(config_file_path)
        else:
            self.region = kwargs.get("region")
            self.cluster_name = kwargs.get("cluster_name")
            self.max_batch_size = kwargs.get("max_batch_size")
            self.boto3_config = kwargs.get("boto3_config")

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

        self.region = config.get("slurm_cb_config", "region")
        self.cluster_name = config.get("slurm_cb_config", "cluster_name")
        self.max_batch_size = int(
            config.get("slurm_cb_config", "max_batch_size", fallback=self.DEFAULT_MAX_INSTANCES_BATCH_SIZE)
        )

        # Configure boto3 to retry 5 times by default
        self._boto3_config = {"retries": {"max_attempts": self.DEFAULT_MAX_RETRY, "mode": "standard"}}
        proxy = config.get("slurm_cb_config", "proxy", fallback="NONE")
        if proxy != "NONE":
            self._boto3_config["proxies"] = {"https": proxy}
        self.boto3_config = Config(**self._boto3_config)

        log.info(self.__repr__())


def _delete_instances(instance_ids_to_nodename, region, boto3_config, batch_size):
    """Terminate corresponding EC2 instances."""
    ec2_client = boto3.client("ec2", region_name=region, config=boto3_config)
    log.info("Terminating instances %s", list(instance_ids_to_nodename.keys()))
    for instances in grouper(instance_ids_to_nodename.keys(), batch_size):
        try:
            # Boto3 clients retries on connection errors only
            # Adding extra layer of retry on all exceptions to try to terminate instances
            retry(stop_max_attempt_number=3, wait_fixed=5000)(ec2_client.terminate_instances)(InstanceIds=instances,)
        except ClientError as e:
            log.error("Failed when terminating instances %s with error %s", instances, e)


def _get_instance_ids_to_nodename(slurm_nodes, region, cluster_name, boto3_config):
    """Retrieve dict that maps from instance ids to slurm nodenames."""
    node_ip_to_name = {node.nodeaddr: node.name for node in slurm_nodes}
    ec2_client = boto3.client("ec2", region_name=region, config=boto3_config)
    paginator = ec2_client.get_paginator("describe_instances")
    response_iterator = paginator.paginate(
        Filters=[
            {"Name": "private-ip-address", "Values": list(node_ip_to_name.keys())},
            {"Name": "tag:ClusterName", "Values": [cluster_name]},
        ],
    )
    filtered_iterator = response_iterator.search("Reservations[].Instances[]")
    return {
        instance_info["InstanceId"]: node_ip_to_name[instance_info["PrivateIpAddress"]]
        for instance_info in filtered_iterator
    }


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
    log.info("Suspending nodes:" + arg_nodes)

    # Retrieve SlurmNode objects from slurm nodelist notation
    slurm_nodes = get_nodes_info(arg_nodes)
    log.debug("Slurm_nodes = %s", slurm_nodes)

    instance_ids_to_nodename = _get_instance_ids_to_nodename(
        slurm_nodes, suspend_config.region, suspend_config.cluster_name, suspend_config.boto3_config
    )
    log.debug("instance_ids_to_nodename = %s", instance_ids_to_nodename)

    _delete_instances(
        instance_ids_to_nodename, suspend_config.region, suspend_config.boto3_config, suspend_config.max_batch_size,
    )
    _set_nodes_idle([node.name for node in slurm_nodes])

    log.info("Finished removing instances for nodes %s", arg_nodes)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("nodes", help="Nodes to release")
    args = parser.parse_args()
    try:
        # Configure root logger
        fileConfig(LOG_CONFIG_FILE, disable_existing_loggers=False)
        suspend_config = SlurmSuspendConfig(CONFIG_FILE_PATH)
        _suspend(args.nodes, suspend_config)
    except Exception as e:
        log.exception("Encountered exception when suspending instances for %s: %s", args.nodes, e)
        _set_nodes_idle(args.nodes)


if __name__ == "__main__":
    main()
