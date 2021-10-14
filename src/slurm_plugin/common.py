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
import json
import logging
import subprocess
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from common.schedulers.slurm_commands import InvalidNodenameError, parse_nodename, update_nodes
from common.utils import check_command_output, grouper

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
DEFAULT_COMMAND_TIMEOUT = 30


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
    if isinstance(resource_list, str):
        return resource_list
    resource_list = [str(elem) for elem in resource_list]
    return f"(x{len(resource_list)}) {str(resource_list)}"


class InstanceManager:
    """
    InstanceManager class.

    Class implementing instance management actions.
    Used when launching instance, terminating instance, and retrieving instance info for slurm integration.
    """

    def __init__(
        self,
        region,
        cluster_name,
        boto3_config,
        table_name=None,
        hosted_zone=None,
        dns_domain=None,
        use_private_hostname=False,
        head_node_private_ip=None,
        head_node_hostname=None,
        instance_name_type_mapping=None,
        run_instances_overrides=None,
    ):
        """Initialize InstanceLauncher with required attributes."""
        self._region = region
        self._cluster_name = cluster_name
        self._boto3_config = boto3_config
        self.failed_nodes = []
        self._ddb_resource = boto3.resource("dynamodb", region_name=region, config=boto3_config)
        self._table = self._ddb_resource.Table(table_name) if table_name else None
        self._hosted_zone = hosted_zone
        self._dns_domain = dns_domain
        self._use_private_hostname = use_private_hostname
        self._head_node_private_ip = head_node_private_ip
        self._head_node_hostname = head_node_hostname
        self._instance_name_type_mapping = instance_name_type_mapping or {}
        self._run_instances_overrides = run_instances_overrides or {}

    def _clear_failed_nodes(self):
        """Clear and reset failed nodes list."""
        self.failed_nodes = []

    def add_instances_for_nodes(
        self, node_list, launch_batch_size, update_node_address=True, all_or_nothing_batch=False
    ):
        """Launch requested EC2 instances for nodes."""
        # Reset failed_nodes
        self._clear_failed_nodes()
        instances_to_launch = self._parse_requested_instances(node_list)
        for queue, queue_instances in instances_to_launch.items():
            for instance_type, slurm_node_list in queue_instances.items():
                logger.info("Launching instances for slurm nodes %s", print_with_count(slurm_node_list))
                for batch_nodes in grouper(slurm_node_list, launch_batch_size):
                    try:
                        run_instances_overrides = self._run_instances_overrides.get(queue, {}).get(instance_type, {})
                        launched_instances = self._launch_ec2_instances(
                            queue,
                            instance_type,
                            len(batch_nodes),
                            all_or_nothing_batch=all_or_nothing_batch,
                            run_instances_overrides=run_instances_overrides,
                        )
                        if update_node_address:
                            assigned_nodes = self._update_slurm_node_addrs(list(batch_nodes), launched_instances)
                            try:
                                self._store_assigned_hostnames(assigned_nodes)
                                self._update_dns_hostnames(assigned_nodes)
                            except Exception:
                                self.failed_nodes.extend(list(assigned_nodes.keys()))
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
                # When using a cluster DNS domain we don't need to pass nodehostnames
                # because they are equal to node names.
                # It is possible to force the use of private hostnames by setting
                # use_private_hostname = "true" as extra json parameter
                node_hostnames = (
                    None if not self._use_private_hostname else [instance.hostname for instance in launched_instances]
                )
                update_nodes(
                    launched_nodes,
                    nodeaddrs=[instance.private_ip for instance in launched_instances],
                    nodehostnames=node_hostnames,
                )
                logger.info(
                    "Nodes are now configured with instances: %s",
                    print_with_count(zip(launched_nodes, launched_instances)),
                )
            if fail_launch_nodes:
                logger.info("Failed to launch instances for following nodes: %s", print_with_count(fail_launch_nodes))
                self.failed_nodes.extend(fail_launch_nodes)

            return dict(zip(launched_nodes, launched_instances))

        except subprocess.CalledProcessError:
            logger.info(
                "Encountered error when updating node %s with instance %s",
                print_with_count(slurm_nodes),
                print_with_count(launched_instances),
            )
            self.failed_nodes.extend(slurm_nodes)

    @log_exception(logger, "saving assigned hostnames in DynamoDB", raise_on_error=True)
    def _store_assigned_hostnames(self, nodes):
        logger.info("Saving assigned hostnames in DynamoDB")
        if not self._table:
            raise Exception("Empty table name configuration parameter.")

        if nodes:
            with self._table.batch_writer() as batch_writer:
                for nodename, instance in nodes.items():
                    # Note: These items will be never removed, but the put_item method
                    # will replace old items if the hostnames are already associated with an old instance_id.
                    batch_writer.put_item(
                        Item={
                            "Id": nodename,
                            "InstanceId": instance.id,
                            "MasterPrivateIp": self._head_node_private_ip,
                            "MasterHostname": self._head_node_hostname,
                        }
                    )

        logger.info("Database update: COMPLETED")

    @log_exception(logger, "updating DNS records", raise_on_error=True)
    def _update_dns_hostnames(self, nodes):
        logger.info("Updating DNS records for %s - %s", self._hosted_zone, self._dns_domain)
        if not self._hosted_zone or not self._dns_domain:
            logger.info("Empty DNS domain name or hosted zone configuration parameter.")
            return

        changes = []
        for hostname, instance in nodes.items():
            changes.append(
                {
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": f"{hostname}.{self._dns_domain}",
                        "ResourceRecords": [{"Value": instance.private_ip}],
                        "Type": "A",
                        "TTL": 120,
                    },
                }
            )

        if changes:
            # Submit calls to change_resource_record_sets in batches of 500 elements each.
            # change_resource_record_sets API call has limit of 1000 changes,
            # but the UPSERT action counts for 2 calls
            # Also pick the number of retries to be the max between the globally configured one and 3.
            # This is done to address Route53 API throttling without changing the configured retries for all API calls.
            configured_retry = self._boto3_config.retries.get("max_attempts", 0) if self._boto3_config.retries else 0
            boto3_config = self._boto3_config.merge(
                Config(retries={"max_attempts": max([configured_retry, 3]), "mode": "standard"})
            )
            route53_client = boto3.client("route53", region_name=self._region, config=boto3_config)
            changes_batch_size = 500
            for changes_batch in grouper(changes, changes_batch_size):
                route53_client.change_resource_record_sets(
                    HostedZoneId=self._hosted_zone, ChangeBatch={"Changes": list(changes_batch)}
                )
        logger.info("DNS records update: COMPLETED")

    def _parse_requested_instances(self, node_list):
        """
        Parse out which launch configurations (queue/instance type) are requested by slurm nodes from NodeName.

        Valid NodeName format: {queue_name}-{st/dy}-{instance_type}-{number}
        Sample NodeName: queue1-st-c5_xlarge-2
        """
        instances_to_launch = collections.defaultdict(lambda: collections.defaultdict(list))
        for node in node_list:
            try:
                queue_name, node_type, instance_name = parse_nodename(node)
                instance_type = self._instance_name_type_mapping[instance_name]
                instances_to_launch[queue_name][instance_type].append(node)
            except (InvalidNodenameError, KeyError):
                logger.warning("Discarding NodeName with invalid format: %s", node)
                self.failed_nodes.append(node)
        logger.debug("Launch configuration requested by nodes = %s", instances_to_launch)

        return instances_to_launch

    def _launch_ec2_instances(
        self, queue, instance_type, current_batch_size, all_or_nothing_batch=False, run_instances_overrides=None
    ):
        """Launch a batch of ec2 instances."""
        try:
            ec2_client = boto3.client("ec2", region_name=self._region, config=self._boto3_config)
            run_instances_params = {
                # If not all_or_nothing_batch scaling, set MinCount=1
                # so run_instances call will succeed even if entire count cannot be satisfied
                # Otherwise set MinCount=current_batch_size so run_instances will fail unless all are launched
                "MinCount": 1 if not all_or_nothing_batch else current_batch_size,
                "MaxCount": current_batch_size,
                # LaunchTemplate is different for every instance type in every queue
                # LaunchTemplate name format: {cluster_name}-{queue_name}-{instance_type}
                # Sample LT name: hit-queue1-c5.xlarge
                "LaunchTemplate": {
                    "LaunchTemplateName": f"{self._cluster_name}-{queue}-{instance_type}",
                    "Version": "$Latest",
                },
            }
            run_instances_params.update(run_instances_overrides)
            if run_instances_overrides:
                logger.info(
                    "Found RunInstances parameters override. Launching instances with: %s", run_instances_params
                )
            result = ec2_client.run_instances(**run_instances_params)

            return [
                EC2Instance(
                    instance_info["InstanceId"],
                    instance_info["PrivateIpAddress"],
                    instance_info["PrivateDnsName"].split(".")[0],
                    instance_info["LaunchTime"],
                )
                for instance_info in result["Instances"]
            ]
        except ClientError as e:
            logger.error("Failed RunInstances request: %s", e.response.get("ResponseMetadata").get("RequestId"))
            raise

    def delete_instances(self, instance_ids_to_terminate, terminate_batch_size):
        """Terminate corresponding EC2 instances."""
        ec2_client = boto3.client("ec2", region_name=self._region, config=self._boto3_config)
        logger.info("Terminating instances %s", print_with_count(instance_ids_to_terminate))
        for instances in grouper(instance_ids_to_terminate, terminate_batch_size):
            try:
                # Boto3 clients retries on connection errors only
                ec2_client.terminate_instances(
                    InstanceIds=list(instances),
                )
            except ClientError as e:
                logger.error(
                    "Failed TerminateInstances request: %s", e.response.get("ResponseMetadata").get("RequestId")
                )
                logger.error("Failed when terminating instances %s with error %s", print_with_count(instances), e)

    @log_exception(logger, "getting health status for unhealthy EC2 instances", raise_on_error=True)
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

    @log_exception(logger, "getting cluster instances from EC2", raise_on_error=True)
    def get_cluster_instances(self, include_head_node=False, alive_states_only=True):
        """Get instances that are associated with the cluster."""
        ec2_client = boto3.client("ec2", region_name=self._region, config=self._boto3_config)
        paginator = ec2_client.get_paginator("describe_instances")
        args = {
            "Filters": [{"Name": "tag:ClusterName", "Values": [self._cluster_name]}],
        }
        if alive_states_only:
            args["Filters"].append({"Name": "instance-state-name", "Values": list(EC2_INSTANCE_ALIVE_STATES)})
        if not include_head_node:
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
    # Assume timestamp was taken from system local time if there is no localization info
    if not initial_time.tzinfo:
        logger.warning("Timestamp %s is not localized. Please double check that this is expected, localizing to UTC.")
        initial_time = initial_time.astimezone(tz=timezone.utc)
    if not current_time.tzinfo:
        logger.warning("Timestamp %s is not localized. Please double check that this is expected, localizing to UTC")
        current_time = current_time.astimezone(tz=timezone.utc)
    time_diff = (current_time - initial_time).total_seconds()
    return time_diff >= grace_time


def read_json(file_path, default=None):
    """Read json file into a dict."""
    try:
        with open(file_path) as mapping_file:
            return json.load(mapping_file)
    except Exception as e:
        if default is None:
            logging.error(
                "Unable to read file from '%s'. Failed with exception: %s",
                file_path,
                e,
            )
            raise
        else:
            logging.info("Unable to read file '%s'. Using default: %s", file_path, default)
            return default


def get_clustermgtd_heartbeat(clustermgtd_heartbeat_file_path):
    """Get clustermgtd's last heartbeat."""
    # Use subprocess based method to read shared file to prevent hanging when NFS is down
    # Do not copy to local. Different users need to access the file, but file should be writable by root only
    # Only use last line of output to avoid taking unexpected output in stdout
    heartbeat = (
        check_command_output(
            f"cat {clustermgtd_heartbeat_file_path}",
            timeout=DEFAULT_COMMAND_TIMEOUT,
            shell=True,  # nosec
        )
        .splitlines()[-1]
        .strip()
    )
    # Note: heartbeat must be written with datetime.strftime to convert localized datetime into str
    # datetime.strptime will not work with str(datetime)
    # Example timestamp written to heartbeat file: 2020-07-30 19:34:02.613338+00:00
    return datetime.strptime(heartbeat, TIMESTAMP_FORMAT)


def expired_clustermgtd_heartbeat(last_heartbeat, current_time, clustermgtd_timeout):
    """Test if clustermgtd heartbeat is expired."""
    if time_is_up(last_heartbeat, current_time, clustermgtd_timeout):
        logger.error(
            "Clustermgtd has been offline since %s. Current time is %s. Timeout of %s seconds has expired!",
            last_heartbeat,
            current_time,
            clustermgtd_timeout,
        )
        return True
    return False


def is_clustermgtd_heartbeat_valid(current_time, clustermgtd_timeout, clustermgtd_heartbeat_file_path):
    try:
        last_heartbeat = get_clustermgtd_heartbeat(clustermgtd_heartbeat_file_path)
        logger.info("Latest heartbeat from clustermgtd: %s", last_heartbeat)
        return not expired_clustermgtd_heartbeat(last_heartbeat, current_time, clustermgtd_timeout)
    except Exception as e:
        logger.error("Unable to retrieve clustermgtd heartbeat with exception: %s", e)
        return False
