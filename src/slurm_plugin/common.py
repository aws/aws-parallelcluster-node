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
import functools
import logging
import re
import subprocess
from datetime import timezone

import boto3
from botocore.exceptions import ClientError

from common.schedulers.slurm_commands import update_nodes
from common.utils import grouper

CONFIG_FILE_DIR = "/etc/parallelcluster/slurm_plugin"
EC2Instance = collections.namedtuple("EC2Instance", ["id", "private_ip", "hostname", "launch_time"])
EC2InstanceHealthState = collections.namedtuple(
    "EC2InstanceHealthState", ["id", "state", "instance_status", "system_status", "scheduled_events"]
)
# Possible ec2 health status: 'ok'|'impaired'|'insufficient-data'|'not-applicable'|'initializing'
EC2_HEALTH_STATUS_UNHEALTHY_STATES = {"impaired"}
# Possible instance states: 'pending'|'running'|'shutting-down'|'terminated'|'stopping'|'stopped'
EC2_INSTANCE_HEALTHY_STATES = {"pending", "running"}
EC2_INSTANCE_STOP_STATES = {"stopping", "stopped"}
EC2_INSTANCE_ALIVE_STATES = EC2_INSTANCE_HEALTHY_STATES | EC2_INSTANCE_STOP_STATES
EC2_SCHEDULED_EVENT_CODES = [
    "instance-reboot",
    "system-reboot",
    "system-maintenance",
    "instance-retirement",
    "instance-stop",
]
# PageSize parameter used for Boto3 paginated calls
# Corresponds to MaxResults in describe_instances and describe_instance_status API
BOTO3_PAGINATION_PAGE_SIZE = 1000
# timestamp used by clustermgtd and computemgtd should be in default ISO format
# YYYY-MM-DDTHH:MM:SS.ffffff+HH:MM[:SS[.ffffff]]
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f%z"

logger = logging.getLogger(__name__)


def log_exception(
    logger,
    action_desc,
    log_level=logging.ERROR,
    catch_exception=Exception,
    raise_on_error=True,
    exception_to_raise=None,
):
    def _log_exception(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except catch_exception as e:
                logger.log(log_level, "Failed when %s with exception %s", action_desc, e)
                if raise_on_error:
                    if exception_to_raise:
                        raise exception_to_raise
                    else:
                        raise

        return wrapper

    return _log_exception


def print_with_count(resource_list):
    """Print resource list with the len of the list."""
    resource_list = list(resource_list)
    return f"(x{len(resource_list)}) {resource_list}"


class InstanceManager:
    """
    InstanceManager class.

    Class implementing instance management actions.
    Used when launching instance, terminating instance, and retrieving instance info for slurm integration.
    """

    class InvalidNodenameError(ValueError):
        r"""
        Exception raised when encountering a NodeName that is invalid/incorrectly formatted.

        Valid NodeName format: {queue_name}-{static/dynamic}-{instance_type}-{number}
        And match: ^([a-z0-9\-_]+)-(static|dynamic)-([a-z0-9-]+.[a-z0-9-]+)-\d+$
        Sample NodeName: queue1-static-c5.xlarge-2
        """

        pass

    def __init__(self, region, cluster_name, boto3_config):
        """Initialize InstanceLauncher with required attributes."""
        self._region = region
        self._cluster_name = cluster_name
        self._boto3_config = boto3_config
        self.failed_nodes = []

    def _clear_failed_nodes(self):
        """Clear and reset failed nodes list."""
        self.failed_nodes = []

    def add_instances_for_nodes(self, node_list, launch_batch_size, update_node_address=True):
        """Launch requested EC2 instances for nodes."""
        # Reset failed_nodes
        self._clear_failed_nodes()
        instances_to_launch = self._parse_requested_instances(node_list)
        for queue, queue_instances in instances_to_launch.items():
            for instance_type, slurm_node_list in queue_instances.items():
                logger.info("Launching instances for slurm nodes %s", print_with_count(slurm_node_list))
                for batch_nodes in grouper(slurm_node_list, launch_batch_size):
                    try:
                        launched_instances = self._launch_ec2_instances(queue, instance_type, len(batch_nodes))
                        if update_node_address:
                            self._update_slurm_node_addrs(list(batch_nodes), launched_instances)
                    except Exception as e:
                        logger.error(
                            "Encountered exception when launching instances for nodes %s: %s",
                            print_with_count(batch_nodes),
                            e,
                        )
                        self.failed_nodes.extend(batch_nodes)

    def _update_slurm_node_addrs(self, slurm_nodes, launched_instances):
        """Update node information in slurm with info from launched EC2 instance."""
        try:
            # There could be fewer launched instances than nodes requested to be launched if best-effort scaling
            # Group nodes into successfully launched and failed to launch based on number of launched instances
            # fmt: off
            launched_nodes = slurm_nodes[:len(launched_instances)]
            fail_launch_nodes = slurm_nodes[len(launched_instances):]
            # fmt: on
            if launched_nodes:
                update_nodes(
                    launched_nodes,
                    nodeaddrs=[instance.private_ip for instance in launched_instances],
                    nodehostnames=[instance.hostname for instance in launched_instances],
                    raise_on_error=True,
                )
                logger.info(
                    "Nodes are now configured with instances: %s",
                    print_with_count(zip(launched_nodes, launched_instances)),
                )
            if fail_launch_nodes:
                logger.info("Failed to launch instances for following nodes: %s", print_with_count(fail_launch_nodes))
                self.failed_nodes.extend(fail_launch_nodes)
        except subprocess.CalledProcessError:
            logger.info(
                "Encountered error when updating node %s with instance %s",
                print_with_count(slurm_nodes),
                print_with_count(launched_instances),
            )
            self.failed_nodes.extend(slurm_nodes)

    def _parse_requested_instances(self, node_list):
        """
        Parse out which launch configurations (queue/instance type) are requested by slurm nodes from NodeName.

        Valid NodeName format: {queue_name}-{static/dynamic}-{instance_type}-{number}
        Sample NodeName: queue1-static-c5.xlarge-2
        """
        instances_to_launch = collections.defaultdict(lambda: collections.defaultdict(list))
        for node in node_list:
            try:
                capture = self._parse_nodename(node)
                queue_name, instance_type = capture
                instances_to_launch[queue_name][instance_type].append(node)
            except self.InvalidNodenameError:
                logger.warning("Discarding NodeName with invalid format: %s", node)
                self.failed_nodes.append(node)
        logger.debug("Launch configuration requested by nodes = %s", instances_to_launch)

        return instances_to_launch

    def _launch_ec2_instances(self, queue, instance_type, current_batch_size, best_effort=True):
        """Launch a batch of ec2 instances."""
        ec2_client = boto3.client("ec2", region_name=self._region, config=self._boto3_config)
        result = ec2_client.run_instances(
            # If best_effort scaling, set MinCount=1
            # so run_instances call will succeed even if entire count cannot be satisfied
            # Otherwise set MinCount=current_batch_size so run_instances will fail unless all are launched
            MinCount=1 if best_effort else current_batch_size,
            MaxCount=current_batch_size,
            # LaunchTemplate is different for every instance type in every queue
            # LaunchTemplate name format: {cluster_name}-{queue_name}-{instance_type}
            # Sample LT name: hit-queue1-c5.xlarge
            LaunchTemplate={"LaunchTemplateName": f"{self._cluster_name}-{queue}-{instance_type}"},
        )

        return [
            EC2Instance(
                instance_info["InstanceId"],
                instance_info["PrivateIpAddress"],
                instance_info["PrivateDnsName"].split(".")[0],
                instance_info["LaunchTime"],
            )
            for instance_info in result["Instances"]
        ]

    def _parse_nodename(self, nodename):
        """Parse queue_name and instance_type from nodename."""
        nodename_capture = re.match(r"^([a-z0-9\-_]+)-(static|dynamic)-([a-z0-9-]+.[a-z0-9-]+)-\d+$", nodename)
        if not nodename_capture:
            raise self.InvalidNodenameError

        return nodename_capture.group(1, 3)

    def delete_instances(self, instance_ids_to_terminate, terminate_batch_size):
        """Terminate corresponding EC2 instances."""
        ec2_client = boto3.client("ec2", region_name=self._region, config=self._boto3_config)
        logger.info("Terminating instances %s", print_with_count(instance_ids_to_terminate))
        for instances in grouper(instance_ids_to_terminate, terminate_batch_size):
            try:
                # Boto3 clients retries on connection errors only
                ec2_client.terminate_instances(InstanceIds=list(instances),)
            except ClientError as e:
                logger.error("Failed when terminating instances %s with error %s", print_with_count(instances), e)

    @log_exception(
        logger, "getting health status for unhealthy EC2 instances", catch_exception=Exception, raise_on_error=True
    )
    def get_unhealthy_cluster_instance_status(self, cluster_instance_ids):
        """
        Get health status for unhealthy EC2 instances.

        Retrieve instance status with 3 separate paginated calls filtering on different health check attributes
        Rather than doing call with instance ids
        Reason being number of unhealthy instances is in general lower than number of instances in cluster
        In addition, while specifying instance ids, the max result returned by 1 API call is 100
        As opposed to 1000 when not specifying instance ids and using filters
        """
        instance_health_states = {}
        health_check_filters = {
            "instance_status": {
                "Filters": [{"Name": "instance-status.status", "Values": list(EC2_HEALTH_STATUS_UNHEALTHY_STATES)}]
            },
            "system_status": {
                "Filters": [{"Name": "system-status.status", "Values": list(EC2_HEALTH_STATUS_UNHEALTHY_STATES)}]
            },
            "scheduled_events": {"Filters": [{"Name": "event.code", "Values": EC2_SCHEDULED_EVENT_CODES}]},
        }
        for health_check_type in health_check_filters:
            ec2_client = boto3.client("ec2", region_name=self._region, config=self._boto3_config)
            paginator = ec2_client.get_paginator("describe_instance_status")
            response_iterator = paginator.paginate(
                PaginationConfig={"PageSize": BOTO3_PAGINATION_PAGE_SIZE}, **health_check_filters[health_check_type]
            )
            filtered_iterator = response_iterator.search("InstanceStatuses")
            for instance_status in filtered_iterator:
                instance_id = instance_status.get("InstanceId")
                if instance_id in cluster_instance_ids and instance_id not in instance_health_states:
                    instance_health_states[instance_id] = EC2InstanceHealthState(
                        instance_id,
                        instance_status.get("InstanceState").get("Name"),
                        instance_status.get("InstanceStatus"),
                        instance_status.get("SystemStatus"),
                        instance_status.get("Events"),
                    )

        return list(instance_health_states.values())

    @log_exception(logger, "getting cluster instances from EC2", catch_exception=Exception, raise_on_error=True)
    def get_cluster_instances(self, include_master=False, alive_states_only=True):
        """Get instances that are associated with the cluster."""
        ec2_client = boto3.client("ec2", region_name=self._region, config=self._boto3_config)
        paginator = ec2_client.get_paginator("describe_instances")
        args = {
            "Filters": [{"Name": "tag:ClusterName", "Values": [self._cluster_name]}],
        }
        if alive_states_only:
            args["Filters"].append({"Name": "instance-state-name", "Values": list(EC2_INSTANCE_ALIVE_STATES)})
        if not include_master:
            args["Filters"].append({"Name": "tag:aws-parallelcluster-node-type", "Values": ["Compute"]})
        response_iterator = paginator.paginate(PaginationConfig={"PageSize": BOTO3_PAGINATION_PAGE_SIZE}, **args)
        filtered_iterator = response_iterator.search("Reservations[].Instances[]")
        return [
            EC2Instance(
                instance_info["InstanceId"],
                instance_info["PrivateIpAddress"],
                instance_info["PrivateDnsName"].split(".")[0],
                instance_info["LaunchTime"],
            )
            for instance_info in filtered_iterator
        ]

    def terminate_all_compute_nodes(self, terminate_batch_size):
        try:
            compute_nodes = self.get_cluster_instances()
            self.delete_instances([instance.id for instance in compute_nodes], terminate_batch_size)
            return True
        except Exception as e:
            logging.error("Failed when terminating compute fleet with error %s", e)
            return False


def time_is_up(initial_time, current_time, grace_time):
    """Check if timeout is exceeded."""
    # Localize datetime objects to UTC if not previously localized
    # All timestamps used in this function should be already localized
    # Assume timestamp was taken from UTC is there is no localization info
    if not initial_time.tzinfo:
        logger.warning("Timestamp %s is not localized. Please double check that this is expected, localizing to UTC.")
        initial_time = initial_time.replace(tzinfo=timezone.utc)
    if not current_time.tzinfo:
        logger.warning("Timestamp %s is not localized. Please double check that this is expected, localizing to UTC")
        current_time = current_time.replace(tzinfo=timezone.utc)
    time_diff = (current_time - initial_time).total_seconds()
    return time_diff >= grace_time
