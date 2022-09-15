# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
import collections
import logging
import subprocess

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from common.schedulers.slurm_commands import update_nodes
from common.utils import grouper
from slurm_plugin.common import log_exception, print_with_count
from slurm_plugin.fleet_manager import EC2Instance, FleetManagerFactory
from slurm_plugin.slurm_resources import (
    EC2_HEALTH_STATUS_UNHEALTHY_STATES,
    EC2_INSTANCE_ALIVE_STATES,
    EC2_SCHEDULED_EVENT_CODES,
    EC2InstanceHealthState,
    InvalidNodenameError,
    parse_nodename,
)

logger = logging.getLogger(__name__)

# PageSize parameter used for Boto3 paginated calls
# Corresponds to MaxResults in describe_instances and describe_instance_status API
BOTO3_PAGINATION_PAGE_SIZE = 1000


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
        fleet_config=None,
        run_instances_overrides=None,
        create_fleet_overrides=None,
    ):
        """Initialize InstanceLauncher with required attributes."""
        self._region = region
        self._cluster_name = cluster_name
        self._boto3_config = boto3_config
        self.failed_nodes = {}
        self._ddb_resource = boto3.resource("dynamodb", region_name=region, config=boto3_config)
        self._table = self._ddb_resource.Table(table_name) if table_name else None
        self._hosted_zone = hosted_zone
        self._dns_domain = dns_domain
        self._use_private_hostname = use_private_hostname
        self._head_node_private_ip = head_node_private_ip
        self._head_node_hostname = head_node_hostname
        self._fleet_config = fleet_config
        self._run_instances_overrides = run_instances_overrides or {}
        self._create_fleet_overrides = create_fleet_overrides or {}

    def _clear_failed_nodes(self):
        """Clear and reset failed nodes list."""
        self.failed_nodes = {}

    def add_instances_for_nodes(
        self, node_list, launch_batch_size, update_node_address=True, all_or_nothing_batch=False
    ):
        """Launch requested EC2 instances for nodes."""
        # Reset failed_nodes
        self._clear_failed_nodes()
        instances_to_launch = self._parse_requested_instances(node_list)
        for queue, compute_resources in instances_to_launch.items():
            for compute_resource, slurm_node_list in compute_resources.items():
                logger.info("Launching instances for slurm nodes %s", print_with_count(slurm_node_list))

                # each compute resource can be configured to use create_fleet or run_instances
                fleet_manager = FleetManagerFactory.get_manager(
                    self._cluster_name,
                    self._region,
                    self._boto3_config,
                    self._fleet_config,
                    queue,
                    compute_resource,
                    all_or_nothing_batch,
                    self._run_instances_overrides,
                    self._create_fleet_overrides,
                )
                for batch_nodes in grouper(slurm_node_list, launch_batch_size):
                    try:
                        launched_instances = fleet_manager.launch_ec2_instances(len(batch_nodes))

                        if update_node_address:
                            assigned_nodes = self._update_slurm_node_addrs(list(batch_nodes), launched_instances)
                            try:
                                self._store_assigned_hostnames(assigned_nodes)
                                self._update_dns_hostnames(assigned_nodes)
                            except Exception:
                                self._update_failed_nodes(set(assigned_nodes.keys()))
                    except ClientError as e:
                        logger.error(
                            "Encountered exception when launching instances for nodes %s: %s",
                            print_with_count(batch_nodes),
                            e,
                        )
                        error_code = e.response.get("Error", {}).get("Code")
                        self._update_failed_nodes(set(batch_nodes), error_code)
                    except Exception as e:
                        logger.error(
                            "Encountered exception when launching instances for nodes %s: %s",
                            print_with_count(batch_nodes),
                            e,
                        )
                        self._update_failed_nodes(set(batch_nodes))

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
                logger.warning(
                    "Failed to launch instances due to limited EC2 capacity for following nodes: %s",
                    print_with_count(fail_launch_nodes),
                )
                self._update_failed_nodes(set(fail_launch_nodes), "LimitedInstanceCapacity")

            return dict(zip(launched_nodes, launched_instances))

        except subprocess.CalledProcessError:
            logger.error(
                "Encountered error when updating nodes %s with instances %s",
                print_with_count(slurm_nodes),
                print_with_count(launched_instances),
            )
            self._update_failed_nodes(set(slurm_nodes))

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
                            "HeadNodePrivateIp": self._head_node_private_ip,
                            "HeadNodeHostname": self._head_node_hostname,
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
        Parse out which launch configurations (queue/compute resource) are requested by slurm nodes from NodeName.

        Valid NodeName format: {queue_name}-{st/dy}-{compute_resource_name}-{number}
        Sample NodeName: queue1-st-computeres1-2
        """
        instances_to_launch = collections.defaultdict(lambda: collections.defaultdict(list))
        for node in node_list:
            try:
                queue_name, node_type, compute_resource_name = parse_nodename(node)
                instances_to_launch[queue_name][compute_resource_name].append(node)
            except (InvalidNodenameError, KeyError):
                logger.warning("Discarding NodeName with invalid format: %s", node)
                self._update_failed_nodes({node}, "InvalidNodenameError")
        logger.debug("Launch configuration requested by nodes = %s", instances_to_launch)

        return instances_to_launch

    def delete_instances(self, instance_ids_to_terminate, terminate_batch_size):
        """Terminate corresponding EC2 instances."""
        ec2_client = boto3.client("ec2", region_name=self._region, config=self._boto3_config)
        logger.info("Terminating instances %s", print_with_count(instance_ids_to_terminate))
        for instances in grouper(instance_ids_to_terminate, terminate_batch_size):
            try:
                # Boto3 clients retries on connection errors only
                ec2_client.terminate_instances(InstanceIds=list(instances))
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
        """
        Get instances that are associated with the cluster.

        Instances without all the info set are ignored and not returned
        """
        ec2_client = boto3.client("ec2", region_name=self._region, config=self._boto3_config)
        paginator = ec2_client.get_paginator("describe_instances")
        args = {
            "Filters": [{"Name": "tag:parallelcluster:cluster-name", "Values": [self._cluster_name]}],
        }
        if alive_states_only:
            args["Filters"].append({"Name": "instance-state-name", "Values": list(EC2_INSTANCE_ALIVE_STATES)})
        if not include_head_node:
            args["Filters"].append({"Name": "tag:parallelcluster:node-type", "Values": ["Compute"]})
        response_iterator = paginator.paginate(PaginationConfig={"PageSize": BOTO3_PAGINATION_PAGE_SIZE}, **args)
        filtered_iterator = response_iterator.search("Reservations[].Instances[]")

        instances = []
        for instance_info in filtered_iterator:
            try:
                instances.append(
                    EC2Instance(
                        instance_info["InstanceId"],
                        instance_info["PrivateIpAddress"],
                        instance_info["PrivateDnsName"].split(".")[0],
                        instance_info["LaunchTime"],
                    )
                )
            except Exception as e:
                logger.warning(
                    "Ignoring instance %s because not all EC2 info are available, exception: %s, message: %s",
                    instance_info["InstanceId"],
                    type(e).__name__,
                    e,
                )

        return instances

    def terminate_all_compute_nodes(self, terminate_batch_size):
        try:
            compute_nodes = self.get_cluster_instances()
            self.delete_instances([instance.id for instance in compute_nodes], terminate_batch_size)
            return True
        except Exception as e:
            logger.error("Failed when terminating compute fleet with error %s", e)
            return False

    def _update_failed_nodes(self, nodeset, error_code="Exception"):
        """Update failed nodes dict with error code as key and nodeset value."""
        self.failed_nodes[error_code] = self.failed_nodes.get(error_code, set()).union(nodeset)
