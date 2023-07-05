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

import json
import logging
import os
import time
from configparser import ConfigParser
from datetime import datetime, timezone
from enum import Enum
from functools import partial
from logging.config import fileConfig

# A nosec comment is appended to the following line in order to disable the B404 check.
# In this file the input of the module subprocess is trusted.
from subprocess import CalledProcessError  # nosec B404
from typing import Dict, List

from botocore.config import Config
from common.schedulers.slurm_commands import (
    PartitionNodelistMapping,
    get_nodes_info,
    get_partitions_info,
    reset_nodes,
    set_nodes_down,
    set_nodes_drain,
    set_nodes_power_down,
    update_all_partitions,
    update_partitions,
)
from common.time_utils import seconds
from common.utils import check_command_output, read_json, sleep_remaining_loop_time, time_is_up, wait_remaining_time
from retrying import retry
from slurm_plugin.cluster_event_publisher import ClusterEventPublisher
from slurm_plugin.common import TIMESTAMP_FORMAT, log_exception, print_with_count
from slurm_plugin.console_logger import ConsoleLogger
from slurm_plugin.instance_manager import InstanceManagerFactory
from slurm_plugin.slurm_resources import (
    CONFIG_FILE_DIR,
    ComputeResourceFailureEvent,
    DynamicNode,
    EC2InstanceHealthState,
    PartitionStatus,
    SlurmNode,
    SlurmPartition,
    StaticNode,
)
from slurm_plugin.task_executor import TaskExecutor

LOOP_TIME = 60
CONSOLE_OUTPUT_WAIT_TIME = 5 * 60
MAXIMUM_TASK_BACKLOG = 100
log = logging.getLogger(__name__)
compute_logger = log.getChild("console_output")
event_logger = log.getChild("events")


class ComputeFleetStatus(Enum):
    """Represents the status of the cluster compute fleet."""

    STOPPED = "STOPPED"  # Fleet is stopped, partitions are inactive.
    RUNNING = "RUNNING"  # Fleet is running, partitions are active.
    STOPPING = "STOPPING"  # clustermgtd is handling the stop request.
    STARTING = "STARTING"  # clustermgtd is handling the start request.
    STOP_REQUESTED = "STOP_REQUESTED"  # A request to stop the fleet has been submitted.
    START_REQUESTED = "START_REQUESTED"  # A request to start the fleet has been submitted.
    # PROTECTED indicates that some partitions have consistent bootstrap failures. Affected partitions are inactive.
    PROTECTED = "PROTECTED"

    def __str__(self):
        return str(self.value)

    @staticmethod
    def is_start_requested(status):
        return status == ComputeFleetStatus.START_REQUESTED

    @staticmethod
    def is_stop_requested(status):
        return status == ComputeFleetStatus.STOP_REQUESTED

    @staticmethod
    def is_protected(status):
        return status == ComputeFleetStatus.PROTECTED


class ComputeFleetStatusManager:
    COMPUTE_FLEET_STATUS_ATTRIBUTE = "status"
    COMPUTE_FLEET_LAST_UPDATED_TIME_ATTRIBUTE = "lastStatusUpdatedTime"

    @staticmethod
    def get_status(fallback=None):
        try:
            compute_fleet_raw_data = check_command_output("get-compute-fleet-status.sh")
            log.debug("Retrieved compute fleet data: %s", compute_fleet_raw_data)
            return ComputeFleetStatus(
                json.loads(compute_fleet_raw_data).get(ComputeFleetStatusManager.COMPUTE_FLEET_STATUS_ATTRIBUTE)
            )
        except Exception as e:
            if isinstance(e, CalledProcessError):
                error = e.stdout.rstrip()
            else:
                error = e
            log.error(
                "Failed when retrieving fleet status with error: %s, using fallback value %s",
                error,
                fallback,
            )

            return fallback

    @staticmethod
    def update_status(status):
        try:
            check_command_output(f"update-compute-fleet-status.sh --status {status}")
        except Exception as e:
            log.error("Failed when updating fleet status to status %s, with error: %s", status, e.stdout.rstrip())
            raise


class ClustermgtdConfig:
    DEFAULTS = {
        # Basic configs
        "max_retry": 1,
        "loop_time": LOOP_TIME,
        "proxy": "NONE",
        "logging_config": os.path.join(
            os.path.dirname(__file__), "logging", "parallelcluster_clustermgtd_logging.conf"
        ),
        # Launch configs
        "launch_max_batch_size": 500,
        "update_node_address": True,
        "run_instances_overrides": "/opt/slurm/etc/pcluster/run_instances_overrides.json",
        "create_fleet_overrides": "/opt/slurm/etc/pcluster/create_fleet_overrides.json",
        "fleet_config_file": "/etc/parallelcluster/slurm_plugin/fleet-config.json",
        # Terminate configs
        "terminate_max_batch_size": 1000,
        # Timeout to wait for node initialization, should be the same as ResumeTimeout
        "node_replacement_timeout": 1800,
        "terminate_drain_nodes": True,
        "terminate_down_nodes": True,
        "orphaned_instance_timeout": 120,
        # Health check configs
        "disable_ec2_health_check": False,
        "disable_scheduled_event_health_check": False,
        "disable_all_cluster_management": False,
        "health_check_timeout": 180,
        "health_check_timeout_after_slurmdstarttime": 180,
        # DNS domain configs
        "hosted_zone": None,
        "dns_domain": None,
        "use_private_hostname": False,
        "protected_failure_count": 10,
        "insufficient_capacity_timeout": 600,
        # Compute console logging configs
        "compute_console_logging_enabled": True,
        "compute_console_logging_max_sample_size": 1,
        "compute_console_wait_time": CONSOLE_OUTPUT_WAIT_TIME,
        # Task executor configs
        "worker_pool_size": 5,
        "worker_pool_max_backlog": MAXIMUM_TASK_BACKLOG,
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
                and self.fleet_config == other.fleet_config
                and self.run_instances_overrides == other.run_instances_overrides
                and self.create_fleet_overrides == other.create_fleet_overrides
            )
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def _get_basic_config(self, config):
        """Get basic config options."""
        self.region = config.get("clustermgtd", "region")
        self.cluster_name = config.get("clustermgtd", "cluster_name")
        self.dynamodb_table = config.get("clustermgtd", "dynamodb_table")
        self.head_node_private_ip = config.get("clustermgtd", "head_node_private_ip")
        self.head_node_hostname = config.get("clustermgtd", "head_node_hostname")
        self.head_node_instance_id = config.get("clustermgtd", "instance_id", fallback="unknown")

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
        fleet_config_file = config.get(
            "clustermgtd", "fleet_config_file", fallback=self.DEFAULTS.get("fleet_config_file")
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
            "clustermgtd", "run_instances_overrides", fallback=self.DEFAULTS.get("run_instances_overrides")
        )
        self.run_instances_overrides = read_json(run_instances_overrides_file, default={})
        create_fleet_overrides_file = config.get(
            "clustermgtd", "create_fleet_overrides", fallback=self.DEFAULTS.get("create_fleet_overrides")
        )
        self.create_fleet_overrides = read_json(create_fleet_overrides_file, default={})

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
        self.health_check_timeout_after_slurmdstarttime = config.getint(
            "clustermgtd",
            "health_check_timeout_after_slurmdstarttime",
            fallback=self.DEFAULTS.get("health_check_timeout_after_slurmdstarttime"),
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
        self.protected_failure_count = config.getint(
            "clustermgtd", "protected_failure_count", fallback=self.DEFAULTS.get("protected_failure_count")
        )
        self.insufficient_capacity_timeout = config.getfloat(
            "clustermgtd", "insufficient_capacity_timeout", fallback=self.DEFAULTS.get("insufficient_capacity_timeout")
        )
        self.disable_nodes_on_insufficient_capacity = self.insufficient_capacity_timeout > 0

    def _get_dns_config(self, config):
        """Get config option related to Route53 DNS domain."""
        self.hosted_zone = config.get("clustermgtd", "hosted_zone", fallback=self.DEFAULTS.get("hosted_zone"))
        self.dns_domain = config.get("clustermgtd", "dns_domain", fallback=self.DEFAULTS.get("dns_domain"))
        self.use_private_hostname = config.getboolean(
            "clustermgtd", "use_private_hostname", fallback=self.DEFAULTS.get("use_private_hostname")
        )

    def _get_compute_console_output_config(self, config):
        """Get config options related to logging console output from compute nodes."""
        self.compute_console_logging_enabled = config.getboolean(
            "clustermgtd",
            "compute_console_logging_enabled",
            fallback=self.DEFAULTS.get("compute_console_logging_enabled"),
        )
        self.compute_console_logging_max_sample_size = config.getint(
            "clustermgtd",
            "compute_console_logging_max_sample_size",
            fallback=self.DEFAULTS.get("compute_console_logging_max_sample_size"),
        )
        self.compute_console_wait_time = config.getint(
            "clustermgtd", "compute_console_wait_time", fallback=self.DEFAULTS.get("compute_console_wait_time")
        )

    def _get_worker_pool_config(self, config):
        self.worker_pool_size = config.getint(
            "clustermgtd", "worker_pool_size", fallback=self.DEFAULTS.get("worker_pool_size")
        )
        self.worker_pool_max_backlog = config.getint(
            "clustermgtd", "worker_pool_max_backlog", fallback=self.DEFAULTS.get("worker_pool_max_backlog")
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
        self._get_compute_console_output_config(self._config)
        self._get_worker_pool_config(self._config)


class ClusterManager:
    """Class for all cluster management related actions."""

    _config: ClustermgtdConfig

    class HealthCheckTypes(Enum):
        """Enum for health check types."""

        scheduled_event = "scheduled_events_check"
        ec2_health = "ec2_health_check"

        def __str__(self):
            return self.value

    class EC2InstancesInfoUnavailable(Exception):
        """Exception raised when unable to retrieve cluster instance info from EC2."""

        pass

    def __init__(self, config):
        """
        Initialize ClusterManager.

        self.static_nodes_in_replacement is persistent across multiple iteration of manage_cluster
        This state is required because we need to ignore static nodes that might have long bootstrap time
        """
        self._insufficient_capacity_compute_resources = {}
        self._static_nodes_in_replacement = set()
        self._partitions_protected_failure_count_map = {}
        self._compute_fleet_status = ComputeFleetStatus.RUNNING
        self._current_time = None
        self._config = None
        self._compute_fleet_status_manager = None
        self._instance_manager = None
        self._task_executor = None
        self._console_logger = None
        self._event_publisher = None
        self._partition_nodelist_mapping_instance = None
        self.set_config(config)

    def set_config(self, config: ClustermgtdConfig):
        if self._config != config:
            log.info("Applying new clustermgtd config: %s", config)

            # If a new task executor is needed, the old one will be shutdown.
            # This should cause any pending tasks to be cancelled (Py 3.9) and
            # any executing tasks to raise an exception as long as they test
            # the executor for shutdown. If tracking of cancelled or failed
            # tasks is needed, then the Future object returned from task_executor
            # can be queried to determine how the task exited.
            # The shutdown on the task_executor is by default, non-blocking, so
            # it is possible for some tasks to continue executing even after the
            # shutdown request has returned.
            self._task_executor = self._initialize_executor(config)

            self._config = config
            self._event_publisher = ClusterEventPublisher.create_with_default_publisher(
                event_logger, config.cluster_name, "HeadNode", "clustermgtd", config.head_node_instance_id
            )
            self._compute_fleet_status_manager = ComputeFleetStatusManager()
            self._instance_manager = self._initialize_instance_manager(config)
            self._console_logger = self._initialize_console_logger(config)

    def shutdown(self):
        if self._task_executor:
            self._task_executor.shutdown()
            self._task_executor = None

    @staticmethod
    def _initialize_instance_manager(config):
        """Initialize instance manager class that will be used to launch/terminate/describe instances."""
        return InstanceManagerFactory.get_manager(
            config.region,
            config.cluster_name,
            config.boto3_config,
            table_name=config.dynamodb_table,
            hosted_zone=config.hosted_zone,
            dns_domain=config.dns_domain,
            use_private_hostname=config.use_private_hostname,
            head_node_private_ip=config.head_node_private_ip,
            head_node_hostname=config.head_node_hostname,
            run_instances_overrides=config.run_instances_overrides,
            create_fleet_overrides=config.create_fleet_overrides,
            fleet_config=config.fleet_config,
        )

    def _initialize_executor(self, config):
        if (
            self._config is None
            or self._config.worker_pool_size != config.worker_pool_size
            or self._config.worker_pool_max_backlog != config.worker_pool_max_backlog
        ):
            if self._task_executor:
                self._task_executor.shutdown()
            return TaskExecutor(
                worker_pool_size=config.worker_pool_size,
                max_backlog=config.worker_pool_max_backlog,
            )
        return self._task_executor

    @staticmethod
    def _initialize_console_logger(config):
        return ConsoleLogger(
            region=config.region,
            enabled=config.compute_console_logging_enabled,
            console_output_consumer=lambda name, instance_id, output: compute_logger.info(
                "Console output for node %s (Instance Id %s):\r%s", name, instance_id, output
            ),
        )

    def _update_compute_fleet_status(self, status):
        log.info("Updating compute fleet status from %s to %s", self._compute_fleet_status, status)
        self._compute_fleet_status_manager.update_status(status)
        self._compute_fleet_status = status

    def _handle_successfully_launched_nodes(self, partitions_name_map):
        """
        Handle nodes have been failed in bootstrap are launched successfully during this iteration.

        Includes resetting partition bootstrap failure count for these nodes type and updating the
        bootstrap_failure_nodes_types.
        If a node has been failed successfully launched, the partition count of this node will be reset.
        So the successfully launched nodes type will be removed from bootstrap_failure_nodes_types.
        If there's node types failed in this partition later, it will keep count again.
        """
        # Find nodes types which have been failed during bootstrap now become online.
        partitions_protected_failure_count_map = self._partitions_protected_failure_count_map.copy()
        for partition, failures_per_compute_resource in partitions_protected_failure_count_map.items():
            partition_online_compute_resources = partitions_name_map[partition].get_online_node_by_type(
                self._config.terminate_drain_nodes, self._config.terminate_down_nodes
            )
            for compute_resource in failures_per_compute_resource.keys():
                if compute_resource in partition_online_compute_resources:
                    self._reset_partition_failure_count(partition)
                    break

    def manage_cluster(self):
        """Manage cluster by syncing scheduler states with EC2 states and performing node maintenance actions."""
        # Initialization
        log.info("Managing cluster...")
        self._current_time = datetime.now(tz=timezone.utc)

        self._compute_fleet_status = self._compute_fleet_status_manager.get_status(fallback=self._compute_fleet_status)
        log.info("Current compute fleet status: %s", self._compute_fleet_status)

        if not self._config.disable_all_cluster_management:
            if self._compute_fleet_status in {
                None,
                ComputeFleetStatus.RUNNING,
                ComputeFleetStatus.PROTECTED,
            }:
                # Get partition_nodelist_mapping between PC-managed Slurm partitions and PC-managed Slurm nodelists
                # Initialize PartitionNodelistMapping singleton
                self._partition_nodelist_mapping_instance = PartitionNodelistMapping.instance()

                # Get node states for nodes in inactive and active partitions
                # Initialize nodes
                try:
                    log.info("Retrieving nodes info from the scheduler")
                    nodes = self._get_node_info_with_retry()
                    log.debug("Nodes: %s", nodes)
                    partitions_name_map, compute_resource_nodes_map = self._parse_scheduler_nodes_data(nodes)
                except Exception as e:
                    log.error(
                        "Unable to get partition/node info from slurm, no other action can be performed. Sleeping... "
                        "Exception: %s",
                        e,
                    )
                    return

                # Get all non-terminating instances in EC2
                try:
                    cluster_instances = self._get_ec2_instances()
                except ClusterManager.EC2InstancesInfoUnavailable:
                    log.error("Unable to get instances info from EC2, no other action can be performed. Sleeping...")
                    return
                log.debug("Current cluster instances in EC2: %s", cluster_instances)
                partitions = list(partitions_name_map.values())
                self._update_slurm_nodes_with_ec2_info(nodes, cluster_instances)
                self._event_publisher.publish_compute_node_events(nodes, cluster_instances)
                # Handle inactive partition and terminate backing instances
                self._clean_up_inactive_partition(partitions)
                # Perform health check actions
                if not self._config.disable_all_health_checks:
                    self._perform_health_check_actions(partitions)
                # Maintain slurm nodes
                self._maintain_nodes(partitions_name_map, compute_resource_nodes_map)
                # Clean up orphaned instances
                self._terminate_orphaned_instances(cluster_instances)
            elif self._compute_fleet_status in {
                ComputeFleetStatus.STOPPED,
            }:
                # Since Slurm partition status might have been manually modified, when STOPPED we want to keep checking
                # partitions and EC2 instances to take into account changes that can be manually
                # applied by the user by re-activating Slurm partitions.
                # When partition are INACTIVE, always try to reset nodeaddr/nodehostname to avoid issue.
                self._maintain_nodes_down()

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
    def _get_partitions_info_with_retry() -> Dict[str, SlurmPartition]:
        return {part.name: part for part in get_partitions_info()}

    def _clean_up_inactive_partition(self, partitions):
        """Terminate all other instances associated with nodes in INACTIVE partition directly through EC2."""
        inactive_instance_ids, inactive_nodes = ClusterManager._get_inactive_instances_and_nodes(partitions)
        if inactive_nodes:
            try:
                log.info("Cleaning up INACTIVE partitions.")
                if inactive_instance_ids:
                    log.info(
                        "Clean up instances associated with nodes in INACTIVE partitions: %s",
                        print_with_count(inactive_instance_ids),
                    )
                    self._instance_manager.delete_instances(
                        inactive_instance_ids, terminate_batch_size=self._config.terminate_max_batch_size
                    )

                self._reset_nodes_in_inactive_partitions(list(inactive_nodes))
            except Exception as e:
                log.error("Failed to clean up INACTIVE nodes %s with exception %s", print_with_count(inactive_nodes), e)

    @staticmethod
    def _reset_nodes_in_inactive_partitions(inactive_nodes):
        # Try to reset nodeaddr if possible to avoid potential problems
        nodes_to_reset = set()
        for node in inactive_nodes:
            if node.needs_reset_when_inactive():
                nodes_to_reset.add(node.name)
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
        # After reading Slurm nodes wait for 5 seconds to let instances appear in EC2 describe_instances call
        time.sleep(5)
        log.info("Retrieving list of EC2 instances associated with the cluster")
        try:
            return self._instance_manager.get_cluster_instances(include_head_node=False, alive_states_only=True)
        except Exception as e:
            log.error("Failed when getting instance info from EC2 with exception %s", e)
            raise ClusterManager.EC2InstancesInfoUnavailable

    @log_exception(log, "maintaining slurm nodes down", catch_exception=Exception, raise_on_error=False)
    def _maintain_nodes_down(self):
        update_all_partitions(PartitionStatus.INACTIVE, reset_node_addrs_hostname=True)
        self._instance_manager.terminate_all_compute_nodes(self._config.terminate_max_batch_size)

    @log_exception(log, "performing health check action", catch_exception=Exception, raise_on_error=False)
    def _perform_health_check_actions(self, partitions: List[SlurmPartition]):
        """Run health check actions."""
        log.info("Performing instance health check actions")
        instance_id_to_active_node_map = ClusterManager.get_instance_id_to_active_node_map(partitions)
        if not instance_id_to_active_node_map:
            return
        # Get health states for instances that might be considered unhealthy
        unhealthy_instances_status = self._instance_manager.get_unhealthy_cluster_instance_status(
            list(instance_id_to_active_node_map.keys())
        )
        log.debug("Cluster instances that might be considered unhealthy: %s", unhealthy_instances_status)
        if unhealthy_instances_status:
            # Perform EC2 health check actions
            if not self._config.disable_ec2_health_check:
                self._handle_health_check(
                    unhealthy_instances_status,
                    instance_id_to_active_node_map,
                    health_check_type=ClusterManager.HealthCheckTypes.ec2_health,
                )
            # Perform scheduled event actions
            if not self._config.disable_scheduled_event_health_check:
                self._handle_health_check(
                    unhealthy_instances_status,
                    instance_id_to_active_node_map,
                    health_check_type=ClusterManager.HealthCheckTypes.scheduled_event,
                )

    def _get_nodes_failing_health_check(
        self, unhealthy_instances_status, instance_id_to_active_node_map, health_check_type
    ):
        """Get nodes fail health check."""
        log.info("Performing actions for health check type: %s", health_check_type)
        nodes_failing_health_check = []
        for instance_status in unhealthy_instances_status:
            unhealthy_node = instance_id_to_active_node_map.get(instance_status.id)
            if unhealthy_node and self._is_instance_unhealthy(instance_status, health_check_type):
                nodes_failing_health_check.append(unhealthy_node)
                unhealthy_node.is_failing_health_check = True
                log.warning(
                    "Node %s(%s) is associated with instance %s that is failing %s. EC2 health state: %s",
                    unhealthy_node.name,
                    unhealthy_node.nodeaddr,
                    instance_status.id,
                    health_check_type,
                    [
                        instance_status.id,
                        instance_status.state,
                        instance_status.instance_status,
                        instance_status.system_status,
                        instance_status.scheduled_events,
                    ],
                )
        return nodes_failing_health_check

    def _is_instance_unhealthy(self, instance_status: EC2InstanceHealthState, health_check_type):
        """Check if instance status is unhealthy based on picked corresponding health check function."""
        is_instance_status_unhealthy = False
        if health_check_type == ClusterManager.HealthCheckTypes.scheduled_event:
            is_instance_status_unhealthy = instance_status.fail_scheduled_events_check()
        elif health_check_type == ClusterManager.HealthCheckTypes.ec2_health:
            is_instance_status_unhealthy = instance_status.fail_ec2_health_check(
                self._current_time, self._config.health_check_timeout
            )
        return is_instance_status_unhealthy

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
        for node in slurm_nodes:
            node.is_static_nodes_in_replacement = node.name in self._static_nodes_in_replacement
            node.is_being_replaced = self._is_node_being_replaced(node)
            node._is_replacement_timeout = self._is_node_replacement_timeout(node)

    def _find_unhealthy_slurm_nodes(self, slurm_nodes):
        """
        Find unhealthy static slurm nodes and dynamic slurm nodes.

        Check and return slurm nodes with unhealthy and healthy scheduler state, grouping unhealthy nodes
        by node type (static/dynamic).
        """
        unhealthy_static_nodes = []
        unhealthy_dynamic_nodes = []
        ice_compute_resources_and_nodes_map = {}
        all_unhealthy_nodes = []
        for node in slurm_nodes:
            if not node.is_healthy(self._config.terminate_drain_nodes, self._config.terminate_down_nodes):
                all_unhealthy_nodes.append(node)

                if isinstance(node, StaticNode):
                    unhealthy_static_nodes.append(node)
                elif self._config.disable_nodes_on_insufficient_capacity and node.is_ice():
                    ice_compute_resources_and_nodes_map.setdefault(node.queue_name, {}).setdefault(
                        node.compute_resource_name, []
                    ).append(node)
                else:
                    unhealthy_dynamic_nodes.append(node)
        self._event_publisher.publish_unhealthy_node_events(all_unhealthy_nodes)
        return (
            unhealthy_dynamic_nodes,
            unhealthy_static_nodes,
            ice_compute_resources_and_nodes_map,
        )

    def _increase_partitions_protected_failure_count(self, bootstrap_failure_nodes):
        """Keep count of boostrap failures."""
        for node in bootstrap_failure_nodes:
            compute_resource = node.compute_resource_name
            queue_name = node.queue_name
            if queue_name in self._partitions_protected_failure_count_map:
                self._partitions_protected_failure_count_map[queue_name][compute_resource] = (
                    self._partitions_protected_failure_count_map[queue_name].get(compute_resource, 0) + 1
                )
            else:
                self._partitions_protected_failure_count_map[queue_name] = {}
                self._partitions_protected_failure_count_map[queue_name][compute_resource] = 1

    @log_exception(log, "maintaining unhealthy dynamic nodes", raise_on_error=False)
    def _handle_unhealthy_dynamic_nodes(self, unhealthy_dynamic_nodes):
        """
        Maintain any unhealthy dynamic node.

        Terminate instances backing dynamic nodes.
        Setting node to down will let slurm requeue jobs allocated to node.
        Setting node to power_down will terminate backing instance and reset dynamic node for future use.
        """
        instances_to_terminate = [node.instance.id for node in unhealthy_dynamic_nodes if node.instance]
        if instances_to_terminate:
            log.info("Terminating instances that are backing unhealthy dynamic nodes")
            self._instance_manager.delete_instances(
                instances_to_terminate, terminate_batch_size=self._config.terminate_max_batch_size
            )
        log.info("Setting unhealthy dynamic nodes to down and power_down.")
        set_nodes_power_down([node.name for node in unhealthy_dynamic_nodes], reason="Scheduler health check failed")

    @log_exception(log, "maintaining powering down nodes", raise_on_error=False)
    def _handle_powering_down_nodes(self, slurm_nodes):
        """
        Handle nodes that are powering down and not already being replaced.

        Terminate instances backing the powering down node if any.
        Reset the nodeaddr for the powering down node. Node state is not changed.
        """
        powering_down_nodes = [
            node for node in slurm_nodes if node.is_powering_down_with_nodeaddr() and not node.is_being_replaced
        ]
        if powering_down_nodes:
            log.info("Resetting powering down nodes: %s", print_with_count(powering_down_nodes))
            reset_nodes(nodes=[node.name for node in powering_down_nodes])
            instances_to_terminate = [node.instance.id for node in powering_down_nodes if node.instance]
            log.info("Terminating instances that are backing powering down nodes")
            self._instance_manager.delete_instances(
                instances_to_terminate, terminate_batch_size=self._config.terminate_max_batch_size
            )

    @log_exception(log, "maintaining unhealthy static nodes", raise_on_error=False)
    def _handle_unhealthy_static_nodes(self, unhealthy_static_nodes):
        """
        Maintain any unhealthy static node.

        Set node to down, terminate backing instance, and launch new instance for static node.
        """
        try:
            wait_function = partial(
                wait_remaining_time,
                wait_start_time=datetime.now(tz=timezone.utc),
                total_wait_time=self._config.compute_console_wait_time,
                wait_function=self._task_executor.wait_unless_shutdown,
            )
            self._console_logger.report_console_output_from_nodes(
                compute_instances=self._instance_manager.get_compute_node_instances(
                    unhealthy_static_nodes,
                    self._config.compute_console_logging_max_sample_size,
                ),
                task_controller=self._task_executor,
                task_wait_function=wait_function,
            )
        except Exception as e:
            log.error("Encountered exception when retrieving console output from unhealthy static nodes: %s", e)

        node_list = [node.name for node in unhealthy_static_nodes]
        # Set nodes into down state so jobs can be requeued immediately
        try:
            log.info("Setting unhealthy static nodes to DOWN")
            reset_nodes(node_list, state="down", reason="Static node maintenance: unhealthy node is being replaced")
        except Exception as e:
            log.error("Encountered exception when setting unhealthy static nodes into down state: %s", e)

        instances_to_terminate = [node.instance.id for node in unhealthy_static_nodes if node.instance]

        if instances_to_terminate:
            log.info("Terminating instances backing unhealthy static nodes")
            self._instance_manager.delete_instances(
                instances_to_terminate, terminate_batch_size=self._config.terminate_max_batch_size
            )
        log.info("Launching new instances for unhealthy static nodes")
        self._instance_manager.add_instances(
            node_list=node_list,
            launch_batch_size=self._config.launch_max_batch_size,
            update_node_address=self._config.update_node_address,
        )
        # Add launched nodes to list of nodes being replaced, excluding any nodes that failed to launch
        failed_nodes = set().union(*self._instance_manager.failed_nodes.values())
        launched_nodes = set(node_list) - failed_nodes
        self._static_nodes_in_replacement |= launched_nodes
        log.info(
            "After node maintenance, following nodes are currently in replacement: %s",
            print_with_count(self._static_nodes_in_replacement),
        )

        self._event_publisher.publish_unhealthy_static_node_events(
            unhealthy_static_nodes,
            self._static_nodes_in_replacement,
            launched_nodes,
            self._instance_manager.failed_nodes,
        )

    @log_exception(log, "maintaining slurm nodes", catch_exception=Exception, raise_on_error=False)
    def _maintain_nodes(self, partitions_name_map, compute_resource_nodes_map):
        """
        Call functions to maintain unhealthy nodes.

        This function needs to handle the case that 2 slurm nodes have the same IP/nodeaddr.
        A list of slurm nodes is passed in and slurm node map with IP/nodeaddr as key should be avoided.
        """
        log.info("Performing node maintenance actions")
        active_nodes = self._find_active_nodes(partitions_name_map)
        # Update self.static_nodes_in_replacement by removing any up nodes from the set
        self._update_static_nodes_in_replacement(active_nodes)
        log.info(
            "Following nodes are currently in replacement: %s", print_with_count(self._static_nodes_in_replacement)
        )
        self._handle_powering_down_nodes(active_nodes)
        (
            unhealthy_dynamic_nodes,
            unhealthy_static_nodes,
            ice_compute_resources_and_nodes_map,
        ) = self._find_unhealthy_slurm_nodes(active_nodes)
        if unhealthy_dynamic_nodes:
            log.info("Found the following unhealthy dynamic nodes: %s", print_with_count(unhealthy_dynamic_nodes))
            self._handle_unhealthy_dynamic_nodes(unhealthy_dynamic_nodes)
        if unhealthy_static_nodes:
            log.info("Found the following unhealthy static nodes: %s", print_with_count(unhealthy_static_nodes))
            self._handle_unhealthy_static_nodes(unhealthy_static_nodes)
        if self._is_protected_mode_enabled():
            self._handle_protected_mode_process(active_nodes, partitions_name_map)
        if self._config.disable_nodes_on_insufficient_capacity:
            self._handle_ice_nodes(ice_compute_resources_and_nodes_map, compute_resource_nodes_map)
        self._handle_failed_health_check_nodes_in_replacement(active_nodes)

    @log_exception(log, "terminating orphaned instances", catch_exception=Exception, raise_on_error=False)
    def _terminate_orphaned_instances(self, cluster_instances):
        """Terminate instance not associated with any node and running longer than orphaned_instance_timeout."""
        log.info("Checking for orphaned instance")
        instances_to_terminate = []
        for instance in cluster_instances:
            if not instance.slurm_node and time_is_up(
                instance.launch_time, self._current_time, self._config.orphaned_instance_timeout
            ):
                instances_to_terminate.append(instance.id)

        if instances_to_terminate:
            log.info("Terminating orphaned instances")
            self._instance_manager.delete_instances(
                instances_to_terminate, terminate_batch_size=self._config.terminate_max_batch_size
            )

    def _enter_protected_mode(self, partitions_to_disable):
        """Entering protected mode if no active running job in queue."""
        # Place partitions into inactive
        log.info("Placing bootstrap failure partitions to INACTIVE: %s", partitions_to_disable)
        update_partitions(partitions_to_disable, PartitionStatus.INACTIVE)
        # Change compute fleet status to protected
        if not ComputeFleetStatus.is_protected(self._compute_fleet_status):
            log.warning(
                "Setting cluster into protected mode due to failures detected in node provisioning. "
                "Please investigate the issue and then use 'pcluster update-compute-fleet --status START_REQUESTED' "
                "command to re-enable the fleet."
            )
            self._update_compute_fleet_status(ComputeFleetStatus.PROTECTED)

    def _reset_partition_failure_count(self, partition):
        """Reset bootstrap failure count for partition which has bootstrap failure nodes successfully launched."""
        log.info("Find successfully launched node in partition %s, reset partition protected failure count", partition)
        self._partitions_protected_failure_count_map.pop(partition, None)

    def _handle_bootstrap_failure_nodes(self, active_nodes):
        """
        Find bootstrap failure nodes and increase partition failure count.

        There are two kinds of bootstrap failure nodes:
        Nodes fail in bootstrap during health check,
        Nodes fail in bootstrap during maintain nodes.
        """
        # Find bootstrap failure nodes in unhealthy nodes
        bootstrap_failure_nodes = self._find_bootstrap_failure_nodes(active_nodes)
        # Find bootstrap failure nodes failing health check
        if bootstrap_failure_nodes:
            log.warning("Found the following bootstrap failure nodes: %s", print_with_count(bootstrap_failure_nodes))
            # Increase the partition failure count and add failed nodes to the set of bootstrap failure nodes.
            self._increase_partitions_protected_failure_count(bootstrap_failure_nodes)
        self._event_publisher.publish_bootstrap_failure_events(bootstrap_failure_nodes)

    def _is_protected_mode_enabled(self):
        """When protected_failure_count is set to -1, disable protected mode."""
        if self._config.protected_failure_count <= 0:
            return False
        return True

    @log_exception(log, "handling protected mode process", catch_exception=Exception, raise_on_error=False)
    def _handle_protected_mode_process(self, active_nodes, partitions_name_map):
        """Handle the process of entering protected mode."""
        # Handle successfully launched nodes
        if self._partitions_protected_failure_count_map:
            self._handle_successfully_launched_nodes(partitions_name_map)
        self._handle_bootstrap_failure_nodes(active_nodes)

        # Enter protected mode
        # We will put a partition into inactive state only if the partition satisfies the following:
        # Partition is not INACTIVE
        # Partition bootstrap failure count above threshold
        # Partition does not have job running
        if self._partitions_protected_failure_count_map:
            log.info(
                "Partitions bootstrap failure count: %s, cluster will be set into protected mode if "
                "protected failure count reaches threshold %s",
                self._partitions_protected_failure_count_map,
                self._config.protected_failure_count,
            )

        partitions_to_disable = []
        bootstrap_failure_partitions_have_jobs = []
        for part_name, failures in self._partitions_protected_failure_count_map.items():
            part = partitions_name_map.get(part_name)
            if part and not part.is_inactive() and sum(failures.values()) >= self._config.protected_failure_count:
                if part.has_running_job():
                    bootstrap_failure_partitions_have_jobs.append(part_name)
                else:
                    partitions_to_disable.append(part_name)

        if bootstrap_failure_partitions_have_jobs:
            log.info(
                "Bootstrap failure partitions %s currently have jobs running, not disabling them",
                bootstrap_failure_partitions_have_jobs,
            )
            if not partitions_to_disable:
                log.info("Not entering protected mode since active jobs are running in bootstrap failure partitions")
        elif partitions_to_disable:
            self._enter_protected_mode(partitions_to_disable)
        if ComputeFleetStatus.is_protected(self._compute_fleet_status):
            log.warning(
                "Cluster is in protected mode due to failures detected in node provisioning. "
                "Please investigate the issue and then use 'pcluster update-compute-fleet --status START_REQUESTED' "
                "command to re-enable the fleet."
            )

    def _handle_nodes_failing_health_check(self, nodes_failing_health_check: List[SlurmNode], health_check_type: str):
        # Place unhealthy node into drain, this operation is idempotent
        nodes_name_failing_health_check = set()
        nodes_name_recently_rebooted = set()
        if nodes_failing_health_check:
            for node in nodes_failing_health_check:
                # Do not consider nodes failing health checks as unhealthy if:
                # 1. the node is still rebooting, OR
                # 2. slurmd was recently restarted (less than health_check_timeout_after_slurmdstarttime seconds ago).
                # In the implementation the logic is reversed to exploit the `and` in the if clause
                if not node.is_reboot_issued() and time_is_up(
                    node.slurmdstarttime, self._current_time, self._config.health_check_timeout_after_slurmdstarttime
                ):
                    nodes_name_failing_health_check.add(node.name)
                else:
                    nodes_name_recently_rebooted.add(node.name)
            if len(nodes_name_failing_health_check) > 0:
                log.warning(
                    "Setting nodes failing health check type %s to DRAIN: %s",
                    health_check_type,
                    nodes_name_failing_health_check,
                )
                set_nodes_drain(nodes_name_failing_health_check, reason=f"Node failing {health_check_type}")
            if len(nodes_name_recently_rebooted) > 0:
                log.info(
                    "Ignoring health check failure due to reboot for nodes: %s",
                    nodes_name_recently_rebooted,
                )
        self._event_publisher.publish_nodes_failing_health_check_events(
            health_check_type, nodes_name_failing_health_check
        )

    def _handle_failed_health_check_nodes_in_replacement(self, active_nodes):
        failed_health_check_nodes_in_replacement = []
        for node in active_nodes:
            if node.is_static_nodes_in_replacement and node.is_failing_health_check:
                failed_health_check_nodes_in_replacement.append(node.name)

        if failed_health_check_nodes_in_replacement:
            log.warning(
                "Detected failed health check for static node in replacement. "
                "No longer considering nodes %s as in replacement process. "
                "Will attempt to replace node again immediately.",
                failed_health_check_nodes_in_replacement,
            )
            self._static_nodes_in_replacement -= set(failed_health_check_nodes_in_replacement)

    @log_exception(log, "handling health check", catch_exception=Exception, raise_on_error=False)
    def _handle_health_check(self, unhealthy_instances_status, instance_id_to_active_node_map, health_check_type):
        """
        Perform supported health checks action.

        Place nodes failing health check into DRAIN, so they can be maintained when possible
        """
        nodes_failing_health_check = self._get_nodes_failing_health_check(
            unhealthy_instances_status,
            instance_id_to_active_node_map,
            health_check_type=health_check_type,
        )
        self._handle_nodes_failing_health_check(nodes_failing_health_check, health_check_type)

    @staticmethod
    def _parse_scheduler_nodes_data(nodes):
        try:
            compute_resource_nodes_map = {}
            partitions_name_map = ClusterManager._get_partitions_info_with_retry()
            log.debug("Partitions: %s", partitions_name_map)
            for node in nodes:
                partitions_name_map[node.queue_name].slurm_nodes.append(node)
                compute_resource_nodes_map.setdefault(node.queue_name, {}).setdefault(
                    node.compute_resource_name, []
                ).append(node)
            return partitions_name_map, compute_resource_nodes_map
        except Exception as e:
            log.error("Failed when getting partition/node states from scheduler with exception %s", e)
            raise

    @staticmethod
    def _update_slurm_nodes_with_ec2_info(nodes, cluster_instances):
        if cluster_instances:
            ip_to_slurm_node_map = {node.nodeaddr: node for node in nodes}
            for instance in cluster_instances:
                if instance.private_ip in ip_to_slurm_node_map:
                    slurm_node = ip_to_slurm_node_map.get(instance.private_ip)
                    slurm_node.instance = instance
                    instance.slurm_node = slurm_node

    @staticmethod
    def get_instance_id_to_active_node_map(partitions: List[SlurmPartition]) -> Dict:
        instance_id_to_active_node_map = {}
        for partition in partitions:
            if not partition.is_inactive():
                for node in partition.slurm_nodes:
                    if node.instance:
                        instance_id_to_active_node_map[node.instance.id] = node
        return instance_id_to_active_node_map

    @staticmethod
    def _get_inactive_instances_and_nodes(partitions):
        inactive_instance_ids = set()
        inactive_nodes = set()
        try:
            for partition in partitions:
                if partition.is_inactive():
                    inactive_nodes |= set(partition.slurm_nodes)
                    inactive_instance_ids |= {node.instance.id for node in partition.slurm_nodes if node.instance}
        except Exception as e:
            log.error("Unable to get inactive instances and nodes. Exception: %s", e)
        return inactive_instance_ids, inactive_nodes

    def _is_node_being_replaced(self, node):
        """Check if a node is currently being replaced and within node_replacement_timeout."""
        return self._is_node_in_replacement_valid(node, check_node_is_valid=True)

    def _is_node_replacement_timeout(self, node):
        """Check if a static node is in replacement but replacement time is expired."""
        return self._is_node_in_replacement_valid(node, check_node_is_valid=False)

    @staticmethod
    def _find_bootstrap_failure_nodes(slurm_nodes):
        bootstrap_failure_nodes = []
        for node in slurm_nodes:
            if node.is_bootstrap_failure():
                bootstrap_failure_nodes.append(node)
        return bootstrap_failure_nodes

    @staticmethod
    def _find_active_nodes(partitions_name_map):
        active_nodes = []
        for partition in partitions_name_map.values():
            if partition.state != "INACTIVE":
                active_nodes += partition.slurm_nodes
        return list(dict.fromkeys(active_nodes))

    def _is_node_in_replacement_valid(self, node, check_node_is_valid):
        """
        Check node is replacement timeout or in replacement.

        If check_node_is_valid=True, check whether a node is in replacement,
        If check_node_is_valid=False, check whether a node is replacement timeout.
        """
        if node.instance and node.name in self._static_nodes_in_replacement:
            time_is_expired = time_is_up(
                node.instance.launch_time, self._current_time, grace_time=self._config.node_replacement_timeout
            )
            return not time_is_expired if check_node_is_valid else time_is_expired
        return False

    @log_exception(
        log, "handling nodes failed due to insufficient capacity", catch_exception=Exception, raise_on_error=False
    )
    def _handle_ice_nodes(
        self,
        ice_compute_resources_and_nodes_map: Dict[str, Dict[str, List[DynamicNode]]],
        compute_resource_nodes_map: Dict[str, Dict[str, List[SlurmNode]]],
    ):
        """Handle nodes failed with insufficient capacity."""
        if ice_compute_resources_and_nodes_map:
            self._update_insufficient_capacity_compute_resources(ice_compute_resources_and_nodes_map)
            self._reset_timeout_expired_compute_resources(ice_compute_resources_and_nodes_map)
        self._set_ice_compute_resources_to_down(compute_resource_nodes_map)

    def _update_insufficient_capacity_compute_resources(
        self, ice_compute_resources_and_nodes_map: Dict[str, Dict[str, List[SlurmNode]]]
    ):
        """Add compute resource to insufficient_capacity_compute_resources if node is ICE node."""
        for queue_name, compute_resources in ice_compute_resources_and_nodes_map.items():
            for compute_resource, nodes in compute_resources.items():
                if not self._insufficient_capacity_compute_resources.get(queue_name, {}).get(compute_resource):
                    self._insufficient_capacity_compute_resources.setdefault(queue_name, {})[
                        compute_resource
                    ] = ComputeResourceFailureEvent(self._current_time, nodes[0].error_code)

    def _reset_timeout_expired_compute_resources(
        self, ice_compute_resources_and_nodes_map: Dict[str, Dict[str, List[SlurmNode]]]
    ):
        """Reset compute resources which insufficient_capacity_timeout expired."""
        # Find insufficient_capacity_timeout compute resources
        if not self._insufficient_capacity_compute_resources:
            return
        log.info(
            "The following compute resources are in down state due to insufficient capacity: %s, "
            "compute resources will be reset after insufficient capacity timeout (%s seconds) expired",
            self._insufficient_capacity_compute_resources,
            self._config.insufficient_capacity_timeout,
        )
        timeout_expired_compute_resources = self._find_insufficient_capacity_timeout_expired_compute_resources()

        # Reset nodes which insufficient capacity timeout expired
        if timeout_expired_compute_resources:
            self._reset_insufficient_capacity_timeout_expired_nodes(
                timeout_expired_compute_resources, ice_compute_resources_and_nodes_map
            )

    def _set_ice_compute_resources_to_down(self, compute_resource_nodes_map: Dict[str, Dict[str, List[SlurmNode]]]):
        """Set powered_down nodes which belong to insufficient capacity compute resources to down."""
        if not self._insufficient_capacity_compute_resources:
            return
        nodes_to_down = {}
        for queue_name, compute_resources in self._insufficient_capacity_compute_resources.items():
            for compute_resource, event in compute_resources.items():
                nodes = compute_resource_nodes_map.get(queue_name, {}).get(compute_resource, [])
                for node in nodes:
                    if node.is_power() and not node.is_nodeaddr_set():
                        error_code = event.error_code
                        nodes_to_down.setdefault(error_code, []).append(node.name)
        if nodes_to_down:
            for error_code, node_list in nodes_to_down.items():
                set_nodes_down(
                    node_list,
                    reason=f"(Code:{error_code})Temporarily disabling node due to insufficient capacity",
                )

    def _find_insufficient_capacity_timeout_expired_compute_resources(
        self,
    ) -> Dict[str, Dict[str, ComputeResourceFailureEvent]]:
        """Find compute resources which insufficient_capacity_timeout expired."""
        timeout_expired_cr = dict()
        for queue_name, compute_resources in self._insufficient_capacity_compute_resources.copy().items():
            for compute_resource, event in compute_resources.copy().items():
                if time_is_up(event.timestamp, self._current_time, self._config.insufficient_capacity_timeout):
                    self._insufficient_capacity_compute_resources[queue_name].pop(compute_resource)
                    timeout_expired_cr.setdefault(queue_name, []).append(compute_resource)
                    if not self._insufficient_capacity_compute_resources.get(queue_name):
                        self._insufficient_capacity_compute_resources.pop(queue_name)
        return timeout_expired_cr

    def _reset_insufficient_capacity_timeout_expired_nodes(
        self,
        timeout_expired_cr: Dict[str, Dict[str, ComputeResourceFailureEvent]],
        ice_compute_resources_and_nodes_map: Dict[str, Dict[str, List[SlurmNode]]],
    ):
        """Reset nodes in the compute resource which insufficient_capacity_timeout expired."""
        logging.info(
            "Reset the following compute resources because insufficient capacity timeout expired: %s",
            timeout_expired_cr,
        )
        nodes_to_power_down = []
        for queue, compute_resources in timeout_expired_cr.items():
            for compute_resource in compute_resources:
                nodes = ice_compute_resources_and_nodes_map.get(queue, {}).get(compute_resource, [])
                nodes_to_power_down += nodes

        if nodes_to_power_down:
            node_names = [node.name for node in nodes_to_power_down]
            set_nodes_power_down(
                node_names,
                reason="Enabling node since insufficient capacity timeout expired",
            )


def _run_clustermgtd(config_file):
    """Run clustermgtd actions."""
    config = ClustermgtdConfig(config_file)
    cluster_manager = ClusterManager(config=config)
    try:
        while True:
            # Get loop start time
            start_time = datetime.now(tz=timezone.utc)
            # Get program config
            try:
                config = ClustermgtdConfig(config_file)
                cluster_manager.set_config(config)
            except Exception as e:
                log.warning(
                    "Unable to reload daemon config from %s, using previous one.\nException: %s",
                    config_file,
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
    finally:
        cluster_manager.shutdown()


@retry(wait_fixed=seconds(LOOP_TIME))
def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - [%(name)s:%(funcName)s] - %(levelname)s - %(message)s"
    )
    log.info("ClusterManager Startup")
    try:
        clustermgtd_config_file = os.environ.get(
            "CONFIG_FILE", os.path.join(CONFIG_FILE_DIR, "parallelcluster_clustermgtd.conf")
        )
        _run_clustermgtd(clustermgtd_config_file)
    except Exception as e:
        log.exception("An unexpected error occurred: %s", e)
        raise


if __name__ == "__main__":
    main()
