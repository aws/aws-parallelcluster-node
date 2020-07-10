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
import re
import subprocess

import boto3
from botocore.exceptions import ClientError

from common.schedulers.slurm_commands import update_nodes
from common.utils import grouper

CONFIG_FILE_DIR = "/opt/parallelcluster/configs/slurm"

log = logging.getLogger(__name__)


def terminate_associated_instances(slurm_nodes, region, cluster_name, boto3_config, batch_size):
    """Terminate instances associated with given nodes in batches."""
    instance_ids_to_nodename = _get_instance_ids_to_nodename(slurm_nodes, region, cluster_name, boto3_config)
    log.info("Terminating the following instances for respective associated nodes: %s", instance_ids_to_nodename)
    if instance_ids_to_nodename:
        delete_instances(
            list(instance_ids_to_nodename.keys()), region, boto3_config, batch_size,
        )


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


def delete_instances(instance_ids_to_terminate, region, boto3_config, batch_size):
    """Terminate corresponding EC2 instances."""
    ec2_client = boto3.client("ec2", region_name=region, config=boto3_config)
    log.info("Terminating instances %s", instance_ids_to_terminate)
    for instances in grouper(instance_ids_to_terminate, batch_size):
        try:
            # Boto3 clients retries on connection errors only
            ec2_client.terminate_instances(InstanceIds=list(instances),)
        except ClientError as e:
            log.error("Failed when terminating instances %s with error %s", instances, e)


class InstanceLauncher:
    """
    InstanceLauncher class.

    Class to manage instance launching action.
    Should generally be used when launching instances for slurm integration.
    """

    class InvalidNodenameError(ValueError):
        r"""
        Exception raised when encountering a NodeName that is invalid/incorrectly formatted.

        Valid NodeName format: {queue_name}-{static/dynamic}-{instance_type}-{number}
        And match: ^([a-z0-9\-_]+)-(static|dynamic)-([a-z0-9-]+.[a-z0-9-]+)-\d+$
        Sample NodeName: queue1-static-c5.xlarge-2
        """

        pass

    def __init__(self, node_list, region, cluster_name, boto3_config, max_batch_size, update_node_address):
        """Initialize InstanceLauncher with required attributes."""
        self._node_list = node_list
        self._region = region
        self._cluster_name = cluster_name
        self._boto3_config = boto3_config
        self._max_batch_size = max_batch_size
        self._update_node_address = update_node_address
        self.failed_nodes = []

    def add_instances_for_nodes(self):
        """Launch requested EC2 instances for nodes."""
        ec2_client = boto3.client("ec2", region_name=self._region, config=self._boto3_config)

        instances_to_launch = self._parse_requested_instances()
        for queue, queue_instances in instances_to_launch.items():
            for instance_type, slurm_node_list in queue_instances.items():
                log.info("Launching instances for slurm nodes %s", slurm_node_list)
                for batch_nodes in grouper(slurm_node_list, self._max_batch_size):
                    try:
                        launched_instances = InstanceLauncher._launch_ec2_instances(
                            ec2_client, self._cluster_name, queue, instance_type, len(batch_nodes)
                        )
                        if self._update_node_address:
                            instance_ids, instance_ips, instance_hostnames = InstanceLauncher._parse_launched_instances(
                                launched_instances
                            )
                            self._update_slurm_node_addrs(
                                list(batch_nodes), instance_ids, instance_ips, instance_hostnames
                            )
                    except Exception as e:
                        log.error(
                            "Encountered exception when launching instances for nodes %s: %s", list(batch_nodes), e
                        )
                        self.failed_nodes.extend(batch_nodes)

    def _update_slurm_node_addrs(self, slurm_nodes, instance_ids, instance_ips, instance_hostnames):
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
            self.failed_nodes.extend(slurm_nodes)

    @staticmethod
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

    def _parse_requested_instances(self):
        """
        Parse out which launch configurations (queue/instance type) are requested by slurm nodes from NodeName.

        Valid NodeName format: {queue_name}-{static/dynamic}-{instance_type}-{number}
        Sample NodeName: queue1-static-c5.xlarge-2
        """
        instances_to_launch = collections.defaultdict(lambda: collections.defaultdict(list))
        for node in self._node_list:
            try:
                capture = self._parse_nodename(node)
                queue_name, instance_type = capture
                instances_to_launch[queue_name][instance_type].append(node)
            except self.InvalidNodenameError:
                log.warning("Discarding NodeName with invalid format: %s", node)
                self.failed_nodes.append(node)
        log.info("Launch configuration requested by nodes = %s", instances_to_launch)

        return instances_to_launch

    @staticmethod
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

    def _parse_nodename(self, nodename):
        """Parse queue_name and instance_type from nodename."""
        nodename_capture = re.match(r"^([a-z0-9\-_]+)-(static|dynamic)-([a-z0-9-]+.[a-z0-9-]+)-\d+$", nodename)
        if not nodename_capture:
            raise self.InvalidNodenameError

        return nodename_capture.group(1, 3)
