# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import time
from configparser import ConfigParser
from datetime import datetime, timezone
from enum import Enum
from logging.config import fileConfig

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.config import Config
from common.schedulers.slurm_commands import (
    PartitionStatus,
    get_nodes_info,
    get_partition_info,
    reset_nodes,
    set_nodes_down,
    set_nodes_down_and_power_save,
    set_nodes_drain,
    update_all_partitions,
)
from common.time_utils import seconds
from common.utils import sleep_remaining_loop_time
from retrying import retry
from slurm_plugin.common import (
    CONFIG_FILE_DIR,
    EC2_HEALTH_STATUS_UNHEALTHY_STATES,
    TIMESTAMP_FORMAT,
    InstanceManager,
    log_exception,
    print_with_count,
    read_json,
    time_is_up,
)

LOOP_TIME = 60
log = logging.getLogger(__name__)


class ComputeFleetStatus(Enum):
    """Represents the status of the cluster compute fleet."""

    STOPPED = "STOPPED"  # Fleet is stopped, partitions are inactive.
    RUNNING = "RUNNING"  # Fleet is running, partitions are active.
    STOPPING = "STOPPING"  # clustermgtd is handling the stop request.
    STARTING = "STARTING"  # clustermgtd is handling the start request.
    STOP_REQUESTED = "STOP_REQUESTED"  # A request to stop the fleet has been submitted.
    START_REQUESTED = "START_REQUESTED"  # A request to start the fleet has been submitted.

    def __str__(self):
        return str(self.value)

    @staticmethod
    def is_stop_status(status):
        return status in {ComputeFleetStatus.STOP_REQUESTED, ComputeFleetStatus.STOPPING, ComputeFleetStatus.STOPPED}

    @staticmethod
    def is_start_in_progress(status):
        return status in {ComputeFleetStatus.START_REQUESTED, ComputeFleetStatus.STARTING}

    @staticmethod
    def is_stop_in_progress(status):
        return status in {ComputeFleetStatus.STOP_REQUESTED, ComputeFleetStatus.STOPPING}


class ComputeFleetStatusManager:
    COMPUTE_FLEET_STATUS_KEY = "COMPUTE_FLEET"
    COMPUTE_FLEET_STATUS_ATTRIBUTE = "Status"

    class ConditionalStatusUpdateFailed(Exception):
        """Raised when there is a failure in updating the status due to a change occurred after retrieving its value."""

        pass

    def __init__(self, table_name, boto3_config, region):
        self._table_name = table_name
        self._boto3_config = boto3_config
        self.__region = region
        self._ddb_resource = boto3.resource("dynamodb", region_name=region, config=boto3_config)
        self._table = self._ddb_resource.Table(table_name)

    def get_status(self, fallback=None):
        try:
            compute_fleet_status = self._table.get_item(ConsistentRead=True, Key={"Id": self.COMPUTE_FLEET_STATUS_KEY})
            if not compute_fleet_status or "Item" not in compute_fleet_status:
                raise Exception("COMPUTE_FLEET status not found in db table")
            return ComputeFleetStatus(compute_fleet_status["Item"][self.COMPUTE_FLEET_STATUS_ATTRIBUTE])
        except Exception as e:
            log.error(
                "Failed when retrieving fleet status from DynamoDB with error %s, using fallback value %s", e, fallback
            )
            return fallback

    def update_status(self, current_status, next_status):
        try:
            self._table.put_item(
                Item={"Id": self.COMPUTE_FLEET_STATUS_KEY, self.COMPUTE_FLEET_STATUS_ATTRIBUTE: str(next_status)},
                ConditionExpression=Attr(self.COMPUTE_FLEET_STATUS_ATTRIBUTE).eq(str(current_status)),
            )
        except self._ddb_resource.meta.client.exceptions.ConditionalCheckFailedException as e:
            raise ComputeFleetStatusManager.ConditionalStatusUpdateFailed(e)


class ClustermgtdConfig:
    DEFAULTS = {
        # Basic configs
        "max_retry": 1,
        "loop_time": LOOP_TIME,
        "proxy": "NONE",
        "logging_config": os.path.join(
            os.path.dirname(__file__), "logging", "parallelcluster_clustermgtd_logging.conf"
        ),
        "instance_type_mapping": "/opt/slurm/etc/pcluster/instance_name_type_mappings.json",
        "run_instances_overrides": "/opt/slurm/etc/pcluster/run_instances_overrides.json",
        # Launch configs
        "launch_max_batch_size": 500,
        "update_node_address": True,
        # Terminate configs
        "terminate_max_batch_size": 1000,
        # Timeout to wait for node initialization, should be the same as ResumeTimeout
        "node_replacement_timeout": 3600,
        "terminate_drain_nodes": True,
        "terminate_down_nodes": True,
        "orphaned_instance_timeout": 120,
        # Health check configs
        "disable_ec2_health_check": False,
        "disable_scheduled_event_health_check": False,
        "disable_all_cluster_management": False,
        "health_check_timeout": 180,
        # DNS domain configs
        "hosted_zone": None,
        "dns_domain": None,
        "use_private_hostname": False,
    }

    def __init__(self, config_file_path):
        self._get_config(config_file_path)

    def __repr__(self):
        attrs = ", ".join(["{key}={value}".format(key=key, value=repr(value)) for key, value in self.__dict__.items()])
        return "{class_name}({attrs})".format(class_name=self.__class__.__name__, attrs=attrs)

    def __eq__(self, other):
        if type(other) is type(self):
            return (
                self._config == other._config
                and self.instance_name_type_mapping == other.instance_name_type_mapping
                and self.run_instances_overrides == other.run_instances_overrides
            )
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def _get_basic_config(self, config):
        """Get basic config options."""
        self.region = config.get("clustermgtd", "region")
        self.cluster_name = config.get("clustermgtd", "cluster_name")
        self.dynamodb_table = config.get("clustermgtd", "dynamodb_table")
        self.head_node_private_ip = config.get("clustermgtd", "master_private_ip")
        self.head_node_hostname = config.get("clustermgtd", "master_hostname")
        instance_name_type_mapping_file = config.get(
            "clustermgtd", "instance_type_mapping", fallback=self.DEFAULTS.get("instance_type_mapping")
        )
        self.instance_name_type_mapping = read_json(instance_name_type_mapping_file)

        # run_instances_overrides_file contains a json with the following format:
        # {
        #     "queue_name": {
        #         "instance_type": {
        #             "RunInstancesCallParam": "Value"
        #         },
        #         ...
        #     },
        #     ...
        # }
        run_instances_overrides_file = config.get(
            "clustermgtd", "run_instances_overrides", fallback=self.DEFAULTS.get("run_instances_overrides")
        )
        self.run_instances_overrides = read_json(run_instances_overrides_file, default={})

        # Configure boto3 to retry 1 times by default
        self._boto3_retry = config.getint("clustermgtd", "boto3_retry", fallback=self.DEFAULTS.get("max_retry"))
        self._boto3_config = {"retries": {"max_attempts": self._boto3_retry, "mode": "standard"}}
        self.loop_time = config.getint("clustermgtd", "loop_time", fallback=self.DEFAULTS.get("loop_time"))
        self.disable_all_cluster_management = config.getboolean(
            "clustermgtd",
            "disable_all_cluster_management",
            fallback=self.DEFAULTS.get("disable_all_cluster_management"),
        )
        self.heartbeat_file_path = config.get("clustermgtd", "heartbeat_file_path")

        proxy = config.get("clustermgtd", "proxy", fallback=self.DEFAULTS.get("proxy"))
        if proxy != "NONE":
            self._boto3_config["proxies"] = {"https": proxy}
        self.boto3_config = Config(**self._boto3_config)
        self.logging_config = config.get("clustermgtd", "logging_config", fallback=self.DEFAULTS.get("logging_config"))

    def _get_launch_config(self, config):
        """Get config options related to launching instances."""
        self.launch_max_batch_size = config.getint(
            "clustermgtd", "launch_max_batch_size", fallback=self.DEFAULTS.get("launch_max_batch_size")
        )
        self.update_node_address = config.getboolean(
            "clustermgtd", "update_node_address", fallback=self.DEFAULTS.get("update_node_address")
        )

    def _get_health_check_config(self, config):
        self.disable_ec2_health_check = config.getboolean(
            "clustermgtd", "disable_ec2_health_check", fallback=self.DEFAULTS.get("disable_ec2_health_check")
        )
        self.disable_scheduled_event_health_check = config.getboolean(
            "clustermgtd",
            "disable_scheduled_event_health_check",
            fallback=self.DEFAULTS.get("disable_scheduled_event_health_check"),
        )
        self.health_check_timeout = config.getint(
            "clustermgtd", "health_check_timeout", fallback=self.DEFAULTS.get("health_check_timeout")
        )
        self.disable_all_health_checks = config.getboolean(
            "clustermgtd",
            "disable_all_health_checks",
            fallback=(self.disable_ec2_health_check and self.disable_scheduled_event_health_check),
        )

    def _get_terminate_config(self, config):
        """Get config option related to instance termination and node replacement."""
        self.terminate_max_batch_size = config.getint(
            "clustermgtd", "terminate_max_batch_size", fallback=self.DEFAULTS.get("terminate_max_batch_size")
        )
        self.node_replacement_timeout = config.getint(
            "clustermgtd", "node_replacement_timeout", fallback=self.DEFAULTS.get("node_replacement_timeout")
        )
        self.terminate_drain_nodes = config.getboolean(
            "clustermgtd", "terminate_drain_nodes", fallback=self.DEFAULTS.get("terminate_drain_nodes")
        )
        self.terminate_down_nodes = config.getboolean(
            "clustermgtd", "terminate_down_nodes", fallback=self.DEFAULTS.get("terminate_down_nodes")
        )
        self.orphaned_instance_timeout = config.getint(
            "clustermgtd", "orphaned_instance_timeout", fallback=self.DEFAULTS.get("orphaned_instance_timeout")
        )

    def _get_dns_config(self, config):
        """Get config option related to Route53 DNS domain."""
        self.hosted_zone = config.get("clustermgtd", "hosted_zone", fallback=self.DEFAULTS.get("hosted_zone"))
        self.dns_domain = config.get("clustermgtd", "dns_domain", fallback=self.DEFAULTS.get("dns_domain"))
        self.use_private_hostname = config.getboolean(
            "clustermgtd", "use_private_hostname", fallback=self.DEFAULTS.get("use_private_hostname")
        )

    @log_exception(log, "reading cluster manager configuration file", catch_exception=IOError, raise_on_error=True)
    def _get_config(self, config_file_path):
        """Get clustermgtd configuration."""
        log.info("Reading %s", config_file_path)
        self._config = ConfigParser()
        self._config.read_file(open(config_file_path, "r"))

        # Get config settings
        self._get_basic_config(self._config)
        self._get_health_check_config(self._config)
        self._get_launch_config(self._config)
        self._get_terminate_config(self._config)
        self._get_dns_config(self._config)


class ClusterManager:
    """Class for all cluster management related actions."""

    class HealthCheckTypes(Enum):
        """Enum for health check types."""

        scheduled_event = "scheduled_events_check"
        ec2_health = "ec2_health_check"

        def __str__(self):
            return self.value

    class SchedulerUnavailable(Exception):
        """Exception raised when unable to retrieve partition/node info from slurm."""

        pass

    class EC2InstancesInfoUnavailable(Exception):
        """Exception raised when unable to retrieve cluster instance info from EC2."""

        pass

    def __init__(self, config):
        """
        Initialize ClusterManager.

        self.static_nodes_in_replacement is persistent across multiple iteration of manage_cluster
        This state is required because we need to ignore static nodes that might have long bootstrap time
        """
        self._static_nodes_in_replacement = set()
        self._compute_fleet_status = ComputeFleetStatus.RUNNING
        self._current_time = None
        self._config = None
        self._compute_fleet_status_manager = None
        self._instance_manager = None
        self.set_config(config)

    def set_config(self, config):
        if self._config != config:
            logging.info("Applying new clustermgtd config: %s", config)
            self._config = config
            self._compute_fleet_status_manager = self._initialize_compute_fleet_status_manager(config)
            self._instance_manager = self._initialize_instance_manager(config)

    @staticmethod
    def _initialize_instance_manager(config):
        """Initialize instance manager class that will be used to launch/terminate/describe instances."""
        return InstanceManager(
            config.region,
            config.cluster_name,
            config.boto3_config,
            table_name=config.dynamodb_table,
            hosted_zone=config.hosted_zone,
            dns_domain=config.dns_domain,
            use_private_hostname=config.use_private_hostname,
            head_node_private_ip=config.head_node_private_ip,
            head_node_hostname=config.head_node_hostname,
            instance_name_type_mapping=config.instance_name_type_mapping,
            run_instances_overrides=config.run_instances_overrides,
        )

    @staticmethod
    def _initialize_compute_fleet_status_manager(config):
        return ComputeFleetStatusManager(
            table_name=config.dynamodb_table, boto3_config=config.boto3_config, region=config.region
        )

    def _update_compute_fleet_status(self, status):
        log.info("Updating compute fleet status from %s to %s", self._compute_fleet_status, status)
        self._compute_fleet_status_manager.update_status(current_status=self._compute_fleet_status, next_status=status)
        self._compute_fleet_status = status

    @log_exception(log, "handling compute fleet status transitions", catch_exception=Exception, raise_on_error=False)
    def _manage_compute_fleet_status_transitions(self):
        """
        Handle compute fleet status transitions.

        When running pcluster start/stop command the fleet status is set to START_REQUESTED/STOP_REQUESTED.
        The function fetches the current fleet status and performs the following transitions:
          - START_REQUESTED -> STARTING -> RUNNING
          - STOP_REQUESTED -> STOPPING -> STOPPED
        STARTING/STOPPING states are only used to communicate that the request is being processed by clustermgtd.
        The following actions are applied to the cluster based on the current status:
          - START_REQUESTED|STARTING: all Slurm partitions are enabled
          - STOP_REQUESTED|STOPPING|STOPPED: all Slurm partitions are disabled and EC2 instances terminated. These
            actions are executed also when the status is stopped to take into account changes that can be manually
            applied by the user by re-activating Slurm partitions.
        """
        self._compute_fleet_status = self._compute_fleet_status_manager.get_status(fallback=self._compute_fleet_status)
        log.info("Current compute fleet status: %s", self._compute_fleet_status)
        try:
            if ComputeFleetStatus.is_stop_status(self._compute_fleet_status):
                # Since Slurm partition status might have been manually modified, when STOPPED we want to keep checking
                # partitions and EC2 instances
                if self._compute_fleet_status == ComputeFleetStatus.STOP_REQUESTED:
                    self._update_compute_fleet_status(ComputeFleetStatus.STOPPING)
                # When setting partition to INACTIVE, always try to reset nodeaddr/nodehostname to avoid issue
                partitions_deactivated_successfully = update_all_partitions(
                    PartitionStatus.INACTIVE, reset_node_addrs_hostname=True
                )
                nodes_terminated = self._instance_manager.terminate_all_compute_nodes(
                    self._config.terminate_max_batch_size
                )
                if partitions_deactivated_successfully and nodes_terminated:
                    if self._compute_fleet_status == ComputeFleetStatus.STOPPING:
                        self._update_compute_fleet_status(ComputeFleetStatus.STOPPED)
            elif ComputeFleetStatus.is_start_in_progress(self._compute_fleet_status):
                if self._compute_fleet_status == ComputeFleetStatus.START_REQUESTED:
                    self._update_compute_fleet_status(ComputeFleetStatus.STARTING)
                # When setting partition to UP, DO NOT reset nodeaddr/nodehostname to avoid breaking nodes already up
                partitions_activated_successfully = update_all_partitions(
                    PartitionStatus.UP, reset_node_addrs_hostname=False
                )
                if partitions_activated_successfully:
                    self._update_compute_fleet_status(ComputeFleetStatus.RUNNING)
        except ComputeFleetStatusManager.ConditionalStatusUpdateFailed:
            log.warning(
                "Cluster status was updated while handling a transition from %s. "
                "Status transition will be retried at the next iteration",
                self._compute_fleet_status,
            )

    def manage_cluster(self):
        """Manage cluster by syncing scheduler states with EC2 states and performing node maintenance actions."""
        # Initialization
        log.info("Managing cluster...")
        self._current_time = datetime.now(tz=timezone.utc)

        self._manage_compute_fleet_status_transitions()

        if not self._config.disable_all_cluster_management and self._compute_fleet_status in {
            None,
            ComputeFleetStatus.RUNNING,
        }:
            # Get node states for nodes in inactive and active partitions
            try:
                log.info("Retrieving nodes info from the scheduler")
                active_nodes, inactive_nodes = self._get_node_info_from_partition()
                log.debug("Current active slurm nodes in scheduler: %s", active_nodes)
                log.debug("Current inactive slurm nodes in scheduler: %s", inactive_nodes)
            except ClusterManager.SchedulerUnavailable:
                log.error("Unable to get partition/node info from slurm, no other action can be performed. Sleeping...")
                return
            # Get all non-terminating instances in EC2
            try:
                # After reading Slurm nodes wait for 5 seconds to let instances appear in EC2 describe_instances call
                time.sleep(5)
                log.info("Retrieving list of EC2 instances associated with the cluster")
                cluster_instances = self._get_ec2_instances()
            except ClusterManager.EC2InstancesInfoUnavailable:
                log.error("Unable to get instances info from EC2, no other action can be performed. Sleeping...")
                return
            # Initialize mapping data structures
            ip_to_slurm_node_map = {node.nodeaddr: node for node in active_nodes}
            log.debug("ip_to_slurm_node_map %s", ip_to_slurm_node_map)
            # Handle inactive partition and terminate backing instances
            if inactive_nodes:
                cluster_instances = self._clean_up_inactive_partition(inactive_nodes, cluster_instances)
            log.debug("Current cluster instances in EC2: %s", cluster_instances)
            private_ip_to_instance_map = {instance.private_ip: instance for instance in cluster_instances}
            log.debug("private_ip_to_instance_map %s", private_ip_to_instance_map)
            # Clean up orphaned instances and skip all other operations if no active node
            if active_nodes:
                # Perform health check actions
                if not self._config.disable_all_health_checks:
                    self._perform_health_check_actions(cluster_instances, ip_to_slurm_node_map)
                # Maintain slurm nodes
                self._maintain_nodes(private_ip_to_instance_map, active_nodes)
            # Clean up orphaned instances
            self._terminate_orphaned_instances(cluster_instances, ips_used_by_slurm=list(ip_to_slurm_node_map.keys()))
        # Write clustermgtd heartbeat to file
        self._write_timestamp_to_file()

    def _write_timestamp_to_file(self):
        """Write timestamp into shared file so compute nodes can determine if head node is online."""
        # Make clustermgtd heartbeat readable to all users
        with open(os.open(self._config.heartbeat_file_path, os.O_WRONLY | os.O_CREAT, 0o644), "w") as timestamp_file:
            # Note: heartbeat must be written with datetime.strftime to convert localized datetime into str
            # datetime.strptime will not work with str(datetime)
            timestamp_file.write(datetime.now(tz=timezone.utc).strftime(TIMESTAMP_FORMAT))

    @staticmethod
    @retry(stop_max_attempt_number=2, wait_fixed=1000)
    def _get_node_info_with_retry(nodes=""):
        return get_nodes_info(nodes)

    @staticmethod
    @retry(stop_max_attempt_number=2, wait_fixed=1000)
    def _get_partition_info_with_retry():
        return get_partition_info(get_all_nodes=True)

    @staticmethod
    def _get_node_info_from_partition():
        """
        Retrieve node belonging in UP/INACTIVE partitions.

        Return list of nodes in active parition, list of nodes in in active partition
        """
        try:
            inactive_nodes = []
            active_nodes = []
            ignored_nodes = []
            partitions = {partition.name: partition for partition in ClusterManager._get_partition_info_with_retry()}
            log.debug("Partitions: %s", partitions)
            nodes = ClusterManager._get_node_info_with_retry()
            log.debug("Nodes: %s", nodes)
            for node in nodes:
                if not node.partitions or any(p not in partitions for p in node.partitions):
                    # ignore nodes not belonging to any partition
                    ignored_nodes.append(node)
                elif any("INACTIVE" not in partitions[p].state for p in node.partitions):
                    active_nodes.append(node)
                else:
                    inactive_nodes.append(node)

            if ignored_nodes:
                log.warning("Ignoring following nodes because they do not belong to any partition: %s", ignored_nodes)
            return active_nodes, inactive_nodes
        except Exception as e:
            log.error("Failed when getting partition/node states from scheduler with exception %s", e)
            raise ClusterManager.SchedulerUnavailable

    def _clean_up_inactive_partition(self, inactive_nodes, cluster_instances):
        """Terminate all other instances associated with nodes in INACTIVE partition directly through EC2."""
        try:
            log.info("Cleaning up INACTIVE partitions.")
            private_ip_to_instance_map = {instance.private_ip: instance for instance in cluster_instances}
            instances_to_terminate = ClusterManager._get_backing_instance_ids(
                inactive_nodes, private_ip_to_instance_map
            )
            log.info(
                "Clean up instances associated with nodes in INACTIVE partitions: %s",
                print_with_count(instances_to_terminate),
            )
            if instances_to_terminate:
                self._instance_manager.delete_instances(
                    instances_to_terminate, terminate_batch_size=self._config.terminate_max_batch_size
                )

            self._reset_nodes_in_inactive_partitions(inactive_nodes)

            instances_still_in_cluster = []
            for instance in cluster_instances:
                if instance.id not in instances_to_terminate:
                    instances_still_in_cluster.append(instance)

            return instances_still_in_cluster
        except Exception as e:
            log.error("Failed to clean up INACTIVE nodes %s with exception %s", print_with_count(inactive_nodes), e)
            return cluster_instances

    @staticmethod
    def _reset_nodes_in_inactive_partitions(inactive_nodes):
        # Try to reset nodeaddr if possible to avoid potential problems
        nodes_to_reset = []
        for node in inactive_nodes:
            if node.is_nodeaddr_set() or (
                not node.is_static and not (node.is_power() or node.is_powering_down() or node.is_down())
            ):
                nodes_to_reset.append(node.name)
        if nodes_to_reset:
            # Setting to down and not power_down cause while inactive power_down doesn't seem to be applied
            log.info(
                "Resetting nodeaddr/nodehostname and setting to down the following nodes: %s",
                print_with_count(nodes_to_reset),
            )
            try:
                reset_nodes(
                    nodes_to_reset,
                    raise_on_error=False,
                    state="down",
                    reason="inactive partition",
                )
            except Exception as e:
                log.error(
                    "Encountered exception when resetting nodeaddr for INACTIVE nodes %s: %s",
                    print_with_count(nodes_to_reset),
                    e,
                )

    def _get_ec2_instances(self):
        """
        Get EC2 instance by describe_instances API.

        Call is made by filtering on tags and includes non-terminating instances only
        Instances returned will not contain instances previously terminated in _clean_up_inactive_partition
        """
        try:
            return self._instance_manager.get_cluster_instances(include_head_node=False, alive_states_only=True)
        except Exception as e:
            log.error("Failed when getting instance info from EC2 with exception %s", e)
            raise ClusterManager.EC2InstancesInfoUnavailable

    @log_exception(log, "performing health check action", catch_exception=Exception, raise_on_error=False)
    def _perform_health_check_actions(self, cluster_instances, ip_to_slurm_node_map):
        """Run health check actions."""
        log.info("Performing instance health check actions")
        id_to_instance_map = {instance.id: instance for instance in cluster_instances}
        # Get health states for instances that might be considered unhealthy
        unhealthy_instance_status = self._instance_manager.get_unhealthy_cluster_instance_status(
            list(id_to_instance_map.keys())
        )
        log.debug("Cluster instances that might be considered unhealthy: %s", unhealthy_instance_status)
        if unhealthy_instance_status:
            # Perform EC2 health check actions
            if not self._config.disable_ec2_health_check:
                self._handle_health_check(
                    unhealthy_instance_status,
                    id_to_instance_map,
                    ip_to_slurm_node_map,
                    health_check_type=ClusterManager.HealthCheckTypes.ec2_health,
                )
            # Perform scheduled event actions
            if not self._config.disable_scheduled_event_health_check:
                self._handle_health_check(
                    unhealthy_instance_status,
                    id_to_instance_map,
                    ip_to_slurm_node_map,
                    health_check_type=ClusterManager.HealthCheckTypes.scheduled_event,
                )

    @staticmethod
    def _fail_ec2_health_check(instance_health_state, current_time, health_check_timeout):
        """Check if instance is failing any EC2 health check for more than health_check_timeout."""
        try:
            if (
                # Check instance status
                instance_health_state.instance_status.get("Status") in EC2_HEALTH_STATUS_UNHEALTHY_STATES
                and time_is_up(
                    instance_health_state.instance_status.get("Details")[0].get("ImpairedSince"),
                    current_time,
                    health_check_timeout,
                )
            ) or (
                # Check system status
                instance_health_state.system_status.get("Status") in EC2_HEALTH_STATUS_UNHEALTHY_STATES
                and time_is_up(
                    instance_health_state.system_status.get("Details")[0].get("ImpairedSince"),
                    current_time,
                    health_check_timeout,
                )
            ):
                return True
        except Exception as e:
            log.warning("Error when parsing instance health status %s, with exception: %s", instance_health_state, e)
            return False

        return False

    @staticmethod
    def _fail_scheduled_events_check(instance_health_state):
        """Check if instance has EC2 scheduled maintenance event."""
        if instance_health_state.scheduled_events:
            return True
        return False

    @log_exception(log, "handling health check", catch_exception=Exception, raise_on_error=False)
    def _handle_health_check(
        self, unhealthy_instance_status, id_to_instance_map, ip_to_slurm_node_map, health_check_type
    ):
        """
        Perform supported health checks action.

        Place nodes failing health check into DRAIN, so they can be maintained when possible
        """
        health_check_functions = {
            ClusterManager.HealthCheckTypes.scheduled_event: {
                "function": ClusterManager._fail_scheduled_events_check,
                "default_kwargs": {},
            },
            ClusterManager.HealthCheckTypes.ec2_health: {
                "function": ClusterManager._fail_ec2_health_check,
                "default_kwargs": {
                    "current_time": self._current_time,
                    "health_check_timeout": self._config.health_check_timeout,
                },
            },
        }
        log.info("Performing actions for health check type: %s", health_check_type)
        nodes_failing_health_check = []
        # Pick corresponding health check function
        is_instance_unhealthy = health_check_functions.get(health_check_type).get("function")
        default_kwargs = health_check_functions.get(health_check_type).get("default_kwargs")
        for instance in unhealthy_instance_status:
            if is_instance_unhealthy(instance_health_state=instance, **default_kwargs):
                unhealthy_node = ip_to_slurm_node_map.get(id_to_instance_map.get(instance.id).private_ip)
                if unhealthy_node:
                    log.warning(
                        "Node %s(%s) is associated with instance %s that is failing %s. EC2 health state: %s",
                        unhealthy_node.name,
                        unhealthy_node.nodeaddr,
                        instance.id,
                        health_check_type,
                        instance,
                    )
                    if unhealthy_node.name in self._static_nodes_in_replacement:
                        log.warning(
                            "Detected failed health check for static node in replacement. "
                            "No longer considering node %s(%s) as in replacement process. "
                            "Will attempt to replace node again immediately.",
                            unhealthy_node.name,
                            unhealthy_node.nodeaddr,
                        )
                        self._static_nodes_in_replacement.remove(unhealthy_node.name)
                    nodes_failing_health_check.append(unhealthy_node.name)
        if nodes_failing_health_check:
            # Place unhealthy node into drain, this operation is idempotent
            log.warning(
                "Setting nodes failing health check type %s to DRAIN: %s", health_check_type, nodes_failing_health_check
            )
            set_nodes_drain(nodes_failing_health_check, reason=f"Node failing {health_check_type}")

    def _update_static_nodes_in_replacement(self, slurm_nodes):
        """Update self.static_nodes_in_replacement by removing nodes that finished replacement and is up."""
        nodename_to_slurm_nodes_map = {node.name: node for node in slurm_nodes}
        nodes_still_in_replacement = set()
        for nodename in self._static_nodes_in_replacement:
            node = nodename_to_slurm_nodes_map.get(nodename)
            # Remove nodename from static_nodes_in_replacement if node is no longer an active node or node is up
            if node and not node.is_up():
                nodes_still_in_replacement.add(nodename)
        self._static_nodes_in_replacement = nodes_still_in_replacement

    def _find_unhealthy_slurm_nodes(self, slurm_nodes, private_ip_to_instance_map):
        """Check and return slurm nodes with unhealthy scheduler state, grouping by node type (static/dynamic)."""
        unhealthy_static_nodes = []
        unhealthy_dynamic_nodes = []
        for node in slurm_nodes:
            if not self._is_node_healthy(node, private_ip_to_instance_map):
                if node.is_static:
                    unhealthy_static_nodes.append(node)
                else:
                    unhealthy_dynamic_nodes.append(node)

        return unhealthy_dynamic_nodes, unhealthy_static_nodes

    def _is_node_being_replaced(self, node, private_ip_to_instance_map):
        """Check if a node is currently being replaced and within node_replacement_timeout."""
        backing_instance = private_ip_to_instance_map.get(node.nodeaddr)
        if (
            backing_instance
            and node.name in self._static_nodes_in_replacement
            and not time_is_up(
                backing_instance.launch_time, self._current_time, grace_time=self._config.node_replacement_timeout
            )
        ):
            return True
        return False

    @staticmethod
    def _is_static_node_configuration_valid(node):
        """Check if static node is configured with a private IP."""
        if not node.is_nodeaddr_set():
            log.warning("Node state check: static node without nodeaddr set node %s", node)
            return False
        return True

    @staticmethod
    def _is_backing_instance_valid(node, instance_ips_in_cluster):
        """Check if a slurm node's addr is set, it points to a valid instance in EC2."""
        if node.is_nodeaddr_set():
            if node.nodeaddr not in instance_ips_in_cluster:
                log.warning("Node state check: no corresponding instance in EC2 for node %s", node)
                return False
        return True

    def _is_node_state_healthy(self, node, private_ip_to_instance_map):
        """Check if a slurm node's scheduler state is considered healthy."""
        # Check to see if node is in DRAINED, ignoring any node currently being replaced
        if node.is_drained() and self._config.terminate_drain_nodes:
            if self._is_node_being_replaced(node, private_ip_to_instance_map):
                log.debug("Node state check: node %s in DRAINED but is currently being replaced, ignoring", node)
                return True
            else:
                log.warning("Node state check: node %s in DRAINED, replacing node", node)
                return False
        # Check to see if node is in DOWN, ignoring any node currently being replaced
        if node.is_down() and self._config.terminate_down_nodes:
            if self._is_node_being_replaced(node, private_ip_to_instance_map):
                log.debug("Node state check: node %s in DOWN but is currently being replaced, ignoring.", node)
                return True
            else:
                if not node.is_static and not node.is_nodeaddr_set():
                    # Silently handle failed to launch dynamic node to clean up normal logging
                    log.debug("Node state check: node %s in DOWN, replacing node", node)
                else:
                    log.warning("Node state check: node %s in DOWN, replacing node", node)
                return False
        return True

    def _is_node_healthy(self, node, private_ip_to_instance_map):
        """Check if a slurm node is considered healthy."""
        if node.is_static:
            return (
                ClusterManager._is_static_node_configuration_valid(node)
                and ClusterManager._is_backing_instance_valid(
                    node, instance_ips_in_cluster=list(private_ip_to_instance_map.keys())
                )
                and self._is_node_state_healthy(node, private_ip_to_instance_map)
            )
        else:
            return ClusterManager._is_backing_instance_valid(
                node, instance_ips_in_cluster=list(private_ip_to_instance_map.keys())
            ) and self._is_node_state_healthy(node, private_ip_to_instance_map)

    @log_exception(log, "maintaining unhealthy dynamic nodes", raise_on_error=False)
    def _handle_unhealthy_dynamic_nodes(self, unhealthy_dynamic_nodes, private_ip_to_instance_map):
        """
        Maintain any unhealthy dynamic node.

        Terminate instances backing dynamic nodes.
        Setting node to down will let slurm requeue jobs allocated to node.
        Setting node to power_down will terminate backing instance and reset dynamic node for future use.
        """
        instances_to_terminate = ClusterManager._get_backing_instance_ids(
            unhealthy_dynamic_nodes, private_ip_to_instance_map
        )
        if instances_to_terminate:
            log.info("Terminating instances that are backing unhealthy dynamic nodes")
            self._instance_manager.delete_instances(
                instances_to_terminate, terminate_batch_size=self._config.terminate_max_batch_size
            )
        log.info("Setting unhealthy dynamic nodes to down and power_down.")
        set_nodes_down_and_power_save(
            [node.name for node in unhealthy_dynamic_nodes], reason="Scheduler health check failed"
        )

    @log_exception(log, "maintaining powering down nodes", raise_on_error=False)
    def _handle_powering_down_nodes(self, slurm_nodes, private_ip_to_instance_map):
        """
        Handle nodes that are powering down.

        Terminate instances backing the powering down node if any.
        Reset the nodeaddr for the powering down node. Node state is not changed.
        """
        powering_down_nodes = []
        for node in slurm_nodes:
            if not node.is_static and node.is_nodeaddr_set() and (node.is_power() or node.is_powering_down()):
                powering_down_nodes.append(node)

        if powering_down_nodes:
            log.info("Resetting powering down nodes: %s", print_with_count(powering_down_nodes))
            reset_nodes(nodes=[node.name for node in powering_down_nodes])
            instances_to_terminate = ClusterManager._get_backing_instance_ids(
                powering_down_nodes, private_ip_to_instance_map
            )
            if instances_to_terminate:
                log.info("Terminating instances that are backing powering down nodes")
                self._instance_manager.delete_instances(
                    instances_to_terminate, terminate_batch_size=self._config.terminate_max_batch_size
                )

    @staticmethod
    def _get_backing_instance_ids(slurm_nodes, private_ip_to_instance_map):
        backing_instances = set()
        for node in slurm_nodes:
            instance = private_ip_to_instance_map.get(node.nodeaddr)
            if instance:
                backing_instances.add(instance.id)

        return list(backing_instances)

    @log_exception(log, "maintaining unhealthy static nodes", raise_on_error=False)
    def _handle_unhealthy_static_nodes(self, unhealthy_static_nodes, private_ip_to_instance_map):
        """
        Maintain any unhealthy static node.

        Set node to down, terminate backing instance, and launch new instance for static node.
        """
        node_list = [node.name for node in unhealthy_static_nodes]
        # Set nodes into down state so jobs can be requeued immediately
        try:
            log.info("Setting unhealthy static nodes to DOWN")
            set_nodes_down(node_list, reason="Static node maintenance: unhealthy node is being replaced")
        except Exception as e:
            log.error("Encountered exception when setting unhealthy static nodes into down state: %s", e)
        instances_to_terminate = ClusterManager._get_backing_instance_ids(
            unhealthy_static_nodes, private_ip_to_instance_map
        )
        if instances_to_terminate:
            log.info("Terminating instances backing unhealthy static nodes")
            self._instance_manager.delete_instances(
                instances_to_terminate, terminate_batch_size=self._config.terminate_max_batch_size
            )
        log.info("Launching new instances for unhealthy static nodes")
        self._instance_manager.add_instances_for_nodes(
            node_list, self._config.launch_max_batch_size, self._config.update_node_address
        )
        # Add launched nodes to list of nodes being replaced, excluding any nodes that failed to launch
        launched_nodes = set(node_list) - set(self._instance_manager.failed_nodes)
        self._static_nodes_in_replacement |= launched_nodes
        log.info(
            "After node maintenance, following nodes are currently in replacement: %s",
            print_with_count(self._static_nodes_in_replacement),
        )

    @log_exception(log, "maintaining slurm nodes", catch_exception=Exception, raise_on_error=False)
    def _maintain_nodes(self, private_ip_to_instance_map, slurm_nodes):
        """
        Call functions to maintain unhealthy nodes.

        This function needs to handle the case that 2 slurm nodes have the same IP/nodeaddr.
        A list of slurm nodes is passed in and slurm node map with IP/nodeaddr as key should be avoided.
        """
        log.info("Performing node maintenance actions")
        self._handle_powering_down_nodes(slurm_nodes, private_ip_to_instance_map)

        # Update self.static_nodes_in_replacement by removing any up nodes from the set
        self._update_static_nodes_in_replacement(slurm_nodes)
        log.info(
            "Following nodes are currently in replacement: %s", print_with_count(self._static_nodes_in_replacement)
        )
        unhealthy_dynamic_nodes, unhealthy_static_nodes = self._find_unhealthy_slurm_nodes(
            slurm_nodes, private_ip_to_instance_map
        )
        if unhealthy_dynamic_nodes:
            log.info("Found the following unhealthy dynamic nodes: %s", print_with_count(unhealthy_dynamic_nodes))
            self._handle_unhealthy_dynamic_nodes(unhealthy_dynamic_nodes, private_ip_to_instance_map)
        if unhealthy_static_nodes:
            log.info("Found the following unhealthy static nodes: %s", print_with_count(unhealthy_static_nodes))
            self._handle_unhealthy_static_nodes(unhealthy_static_nodes, private_ip_to_instance_map)

    @log_exception(log, "terminating orphaned instances", catch_exception=Exception, raise_on_error=False)
    def _terminate_orphaned_instances(self, cluster_instances, ips_used_by_slurm):
        """Terminate instance not associated with any node and running longer than orphaned_instance_timeout."""
        log.info("Checking for orphaned instance")
        instances_to_terminate = []
        for instance in cluster_instances:
            if instance.private_ip not in ips_used_by_slurm and time_is_up(
                instance.launch_time, self._current_time, self._config.orphaned_instance_timeout
            ):
                instances_to_terminate.append(instance.id)
        if instances_to_terminate:
            log.info("Terminating orphaned instances")
            self._instance_manager.delete_instances(
                instances_to_terminate, terminate_batch_size=self._config.terminate_max_batch_size
            )


def _run_clustermgtd():
    """Run clustermgtd actions."""
    clustermgtd_config_file = os.path.join(CONFIG_FILE_DIR, "parallelcluster_clustermgtd.conf")
    config = ClustermgtdConfig(clustermgtd_config_file)
    cluster_manager = ClusterManager(config=config)
    while True:
        # Get loop start time
        start_time = datetime.now(tz=timezone.utc)
        # Get program config
        try:
            config = ClustermgtdConfig(clustermgtd_config_file)
            cluster_manager.set_config(config)
        except Exception as e:
            log.warning(
                "Unable to reload daemon config from %s, using previous one.\nException: %s",
                clustermgtd_config_file,
                e,
            )
        # Configure root logger
        try:
            fileConfig(config.logging_config, disable_existing_loggers=False)
        except Exception as e:
            log.warning(
                "Unable to configure logging from %s, using default logging settings.\nException: %s",
                config.logging_config,
                e,
            )
        # Manage cluster
        cluster_manager.manage_cluster()
        sleep_remaining_loop_time(config.loop_time, start_time)


@retry(wait_fixed=seconds(LOOP_TIME))
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s")
    log.info("ClusterManager Startup")
    try:
        _run_clustermgtd()
    except Exception as e:
        log.exception("An unexpected error occurred: %s", e)
        raise


if __name__ == "__main__":
    main()
