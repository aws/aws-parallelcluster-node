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
from datetime import datetime, timezone
from enum import Enum
from logging.config import fileConfig

from botocore.config import Config
from configparser import ConfigParser
from retrying import retry

from common.schedulers.slurm_commands import (
    get_nodes_info,
    get_partition_info,
    set_nodes_down,
    set_nodes_down_and_power_save,
    set_nodes_drain,
)
from common.time_utils import seconds
from common.utils import sleep_remaining_loop_time
from slurm_plugin.common import CONFIG_FILE_DIR, EC2_HEALTH_STATUS_UNHEALTHY_STATES, InstanceManager

LOOP_TIME = 30
log = logging.getLogger(__name__)


class ClustermgtdConfig:
    DEFAULTS = {
        # Basic configs
        "max_retry": 5,
        "loop_time": LOOP_TIME,
        "proxy": "NONE",
        "logging_config": os.path.join(
            os.path.dirname(__file__), "logging", "parallelcluster_clustermgtd_logging.conf"
        ),
        # Launch configs
        "launch_max_batch_size": 100,
        "update_node_address": True,
        # Terminate configs
        "terminate_max_batch_size": 1000,
        # Timeout to wait for node initialization, should be the same as ResumeTimeout
        "node_replacement_timeout": 600,
        "terminate_drain_nodes": True,
        "terminate_down_nodes": True,
        "orphaned_instance_timeout": 180,
        # Health check configs
        "disable_ec2_health_check": False,
        "disable_scheduled_event_health_check": False,
        "disable_all_cluster_management": False,
        "health_check_timeout": 180,
    }

    def __init__(self, config_file_path):
        self._get_config(config_file_path)

    def __repr__(self):
        attrs = ", ".join(["{key}={value}".format(key=key, value=repr(value)) for key, value in self.__dict__.items()])
        return "{class_name}({attrs})".format(class_name=self.__class__.__name__, attrs=attrs)

    def _get_basic_config(self, config):
        """Get basic config options."""
        self.region = config.get("clustermgtd", "region")
        self.cluster_name = config.get("clustermgtd", "cluster_name")
        # Configure boto3 to retry 5 times by default
        self._boto3_config = {"retries": {"max_attempts": self.DEFAULTS.get("max_retry"), "mode": "standard"}}
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

    def _get_config(self, config_file_path):
        """Get clustermgtd configuration."""
        log.info("Reading %s", config_file_path)
        config = ConfigParser()
        try:
            config.read_file(open(config_file_path, "r"))
        except IOError:
            log.error(f"Cannot read cluster manager configuration file: {config_file_path}")
            raise

        # Get config settings
        self._get_basic_config(config)
        self._get_health_check_config(config)
        self._get_launch_config(config)
        self._get_terminate_config(config)

        # Log configuration
        log.info(self.__repr__())


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

    def __init__(self):
        """
        Initialize ClusterManager.

        self.static_nodes_in_replacement is persistent across multiple iteration of manage_cluster
        This state is required because we need to ignore static nodes that might have long bootstrap time
        """
        self.static_nodes_in_replacement = set()

    def _set_static_nodes_in_replacement(self, node_list):
        """Setter function used for testing purposes only; not used in production."""
        self.static_nodes_in_replacement = set(node_list)

    def _set_current_time(self, current_time):
        self.current_time = current_time

    def _set_sync_config(self, sync_config, initialize_instance_manager=True):
        self.sync_config = sync_config
        if initialize_instance_manager:
            self._initialize_instance_manager()

    def _initialize_instance_manager(self):
        """Initialize instance manager class that will be used to launch/terminate/describe instances."""
        self.instance_manager = InstanceManager(
            self.sync_config.region, self.sync_config.cluster_name, self.sync_config.boto3_config
        )

    def manage_cluster(self, sync_config):
        """Manage cluster by syncing scheduler states with EC2 states and performing node maintenance actions."""
        # Initialization
        self._set_sync_config(sync_config)
        self._set_current_time(datetime.now(tz=timezone.utc))
        if not sync_config.disable_all_cluster_management:
            # Get node states for nodes in inactive and active partitions
            try:
                active_nodes, inactive_nodes = ClusterManager._get_node_info_from_partition()
            except ClusterManager.SchedulerUnavailable:
                log.error("Unable to get partition/node info from slurm, no other action can be performed. Sleeping...")
                return
            # Initialize mapping data structures
            ip_to_slurm_node_map = {node.nodeaddr: node for node in active_nodes}
            # Handle inactive partition and terminate backing instances
            if inactive_nodes:
                self._clean_up_inactive_partition(inactive_nodes)
            try:
                # Get all instances in EC2
                cluster_instances = self._get_ec2_instances()
            except ClusterManager.EC2InstancesInfoUnavailable:
                log.error("Unable to get instances info from EC2, no other action can be performed. Sleeping...")
                return
            log.info("Current cluster instances in EC2: %s", cluster_instances)
            # Clean up orphaned instances and skip all other operations if no active node
            if active_nodes:
                log.info("Current active slurm nodes in scheduler: %s", active_nodes)
                # Perform health check actions
                if not sync_config.disable_all_health_checks:
                    self._perform_health_check_actions(cluster_instances, ip_to_slurm_node_map)
                # Maintain slurm nodes
                self._maintain_nodes(cluster_instances, active_nodes)
            # Clean up orphaned instances
            self._terminate_orphaned_instances(cluster_instances, ips_used_by_slurm=list(ip_to_slurm_node_map.keys()))
        # Write clustermgtd heartbeat to file
        self._write_timestamp_to_file()

    def _write_timestamp_to_file(self):
        """Write timestamp into shared file so compute nodes can determine if head node is online."""
        with open(self.sync_config.heartbeat_file_path, "w") as timestamp_file:
            timestamp_file.write(f"{self.current_time}")

    @staticmethod
    def _get_node_info_from_partition():
        """
        Retrieve node belonging in UP/INACTIVE partitions.

        Return list of nodes in active parition, list of nodes in in active partition
        """
        try:
            inactive_nodes = []
            active_nodes = []
            partitions = get_partition_info()
            for part in partitions:
                nodes = get_nodes_info(part.nodes)
                if "INACTIVE" in part.state:
                    inactive_nodes.extend(nodes)
                else:
                    active_nodes.extend(nodes)

            return active_nodes, inactive_nodes
        except Exception as e:
            log.error("Exception when getting partition/node states from scheduler: %s", e)
            raise ClusterManager.SchedulerUnavailable

    def _clean_up_inactive_partition(self, inactive_nodes):
        """
        Terminate all other instances associated with nodes directly through EC2.

        This operation will not update node states, nodes will retain their states
        Nodes will be placed into down* by slurm after SlurmdTimeout
        If dynamic, nodes will be power_saved after SuspendTime
        describe_instances call is made with filter on private IPs
        """
        try:
            log.info("Clean up instances associated with nodes in INACTIVE partitions: %s", inactive_nodes)
            self.instance_manager.terminate_associated_instances(
                inactive_nodes, terminate_batch_size=self.sync_config.terminate_max_batch_size
            )
        except Exception as e:
            log.error(
                "Unable to clean up nodes in INACTIVE partitions with exception: %s\nContinuing other operations...", e
            )

    def _get_ec2_instances(self):
        """
        Get EC2 instance by describe_instances API.

        Call is made by filtering on tags and includes non-terminating instances only
        Instances returned will not contain instances previously terminated in _clean_up_inactive_partition
        """
        try:
            return self.instance_manager.get_cluster_instances(include_master=False, alive_states_only=True)
        except Exception as e:
            log.error("Exception when retrieving instances from EC2: %s", e)
            raise ClusterManager.EC2InstancesInfoUnavailable

    def _perform_health_check_actions(self, cluster_instances, ip_to_slurm_node_map):
        """Run health check actions."""
        log.info("Performing instance health")
        try:
            id_to_instance_map = {instance.id: instance for instance in cluster_instances}
            # Get instance health states
            unhealthy_instance_status = self.instance_manager.get_unhealthy_cluster_instance_status(
                list(id_to_instance_map.keys())
            )
            # Perform EC2 health check actions
            if not self.sync_config.disable_ec2_health_check:
                self._handle_health_check(
                    unhealthy_instance_status,
                    id_to_instance_map,
                    ip_to_slurm_node_map,
                    health_check_type=ClusterManager.HealthCheckTypes.ec2_health,
                )
            # Perform scheduled event actions
            if not self.sync_config.disable_scheduled_event_health_check:
                self._handle_health_check(
                    unhealthy_instance_status,
                    id_to_instance_map,
                    ip_to_slurm_node_map,
                    health_check_type=ClusterManager.HealthCheckTypes.scheduled_event,
                )
        except Exception as e:
            log.error(
                "Unable to perform instance health check actions with exception: %s\nContinuing other operations...", e
            )

    @staticmethod
    def _fail_ec2_health_check(instance_health_state, current_time, health_check_timeout):
        """Check if instance is failing any EC2 health check for more than health_check_timeout."""
        try:
            if (
                # Check instance status
                instance_health_state.instance_status.get("Status") in EC2_HEALTH_STATUS_UNHEALTHY_STATES
                and ClusterManager._time_is_up(
                    instance_health_state.instance_status.get("Details")[0].get("ImpairedSince"),
                    current_time,
                    health_check_timeout,
                )
            ) or (
                # Check system status
                instance_health_state.system_status.get("Status") in EC2_HEALTH_STATUS_UNHEALTHY_STATES
                and ClusterManager._time_is_up(
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

    def _handle_health_check(
        self, unhealthy_instance_status, id_to_instance_map, ip_to_slurm_node_map, health_check_type
    ):
        """
        Perform health check action for all types of supported health checks.

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
                    "current_time": self.current_time,
                    "health_check_timeout": self.sync_config.health_check_timeout,
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
                    nodes_failing_health_check.append(unhealthy_node.name)
        if nodes_failing_health_check:
            # Place unhealthy node into drain, this operation is idempotent
            set_nodes_drain(nodes_failing_health_check, reason=f"Node failing {health_check_type}")

    def _update_static_nodes_in_replacement(self, slurm_nodes):
        """Update self.static_nodes_in_replacement by removing nodes that finished replacement and is up."""
        nodename_to_slurm_nodes_map = {node.name: node for node in slurm_nodes}
        nodes_still_in_replacement = set()
        for nodename in self.static_nodes_in_replacement:
            node = nodename_to_slurm_nodes_map.get(nodename)
            # Remove nodename from static_nodes_in_replacement if node is no longer an active node or node is up
            if node and not node.is_up():
                nodes_still_in_replacement.add(nodename)
        self.static_nodes_in_replacement = nodes_still_in_replacement

    def _find_unhealthy_slurm_nodes(self, slurm_nodes, private_ip_to_instance_map):
        """Check and return slurm nodes with unhealthy scheduler state, grouping by node type (static/dynamic)."""
        unhealthy_static_nodes = []
        unhealthy_dynamic_nodes = []
        for node in slurm_nodes:
            if not self._is_node_healthy(node, private_ip_to_instance_map):
                if node.is_static_node():
                    unhealthy_static_nodes.append(node)
                else:
                    unhealthy_dynamic_nodes.append(node)

        return unhealthy_dynamic_nodes, unhealthy_static_nodes

    def _is_node_being_replaced(self, node, private_ip_to_instance_map):
        """Check if a node is currently being replaced and within node_replacement_timeout."""
        backing_instance = private_ip_to_instance_map.get(node.nodeaddr)
        if (
            backing_instance
            and node.name in self.static_nodes_in_replacement
            and not ClusterManager._time_is_up(
                backing_instance.launch_time, self.current_time, grace_time=self.sync_config.node_replacement_timeout
            )
        ):
            return True
        return False

    @staticmethod
    def _is_static_node_configuration_valid(node):
        """Check if static node is configured with a private IP."""
        if not node.is_nodeaddr_set():
            log.warning("Node state check: static node without nodeaddr set node %s", node.name)
            return False
        return True

    @staticmethod
    def _is_backing_instance_valid(node, instance_ips_in_cluster):
        """Check if a slurm node's addr is set, it points to a valid instance in EC2."""
        if node.is_nodeaddr_set():
            if node.nodeaddr not in instance_ips_in_cluster:
                log.warning("Node state check: no corresponding instance in EC2 for node %s", node.name)
                return False
        return True

    def _is_node_state_healthy(self, node, private_ip_to_instance_map):
        """Check if a slurm node's scheduler state is considered healthy."""
        # Check to see if node is in DRAINED, ignoring any node currently being replaced
        if node.is_drained() and self.sync_config.terminate_drain_nodes:
            if self._is_node_being_replaced(node, private_ip_to_instance_map):
                log.info("Node state check: node %s in DRAINED but is currently being replaced, ignoring", node.name)
                return True
            else:
                log.warning("Node state check: node %s in DRAINED, replacing node", node.name)
                return False
        # Check to see if node is in DOWN, ignoring any node currently being replaced
        if node.is_down() and self.sync_config.terminate_down_nodes:
            if self._is_node_being_replaced(node, private_ip_to_instance_map):
                log.info("Node state check: node %s in DOWN but is currently being replaced, ignoring.", node.name)
                return True
            else:
                log.warning("Node state check: node %s in DOWN, replacing node", node.name)
                return False
        return True

    def _is_node_healthy(self, node, private_ip_to_instance_map):
        """Check if a slurm node is considered healthy."""
        if node.is_static_node():
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

    @staticmethod
    def _handle_unhealthy_dynamic_nodes(unhealthy_dynamic_nodes):
        """
        Maintain any unhealthy dynamic node.

        Setting node to down will let slurm requeue jobs allocated to node.
        Setting node to power_down will terminate backing instance and reset dynamic node for future use.
        """
        log.info("Setting unhealthy dynamic nodes to down and power_down: %s", unhealthy_dynamic_nodes)
        set_nodes_down_and_power_save(
            [node.name for node in unhealthy_dynamic_nodes], reason="Schduler health check failed"
        )

    def _handle_unhealthy_static_nodes(self, unhealthy_static_nodes, private_ip_to_instance_map):
        """
        Maintain any unhealthy static node.

        Set node to down, terminate backing instance, and launch new instance for static node.
        """
        log.info(
            "Setting following unhealthy static nodes to DOWN and terminating associated instances: %s",
            unhealthy_static_nodes,
        )
        node_list = [node.name for node in unhealthy_static_nodes]
        try:
            # Set nodes into down state so jobs can be requeued immediately
            set_nodes_down(node_list, reason="Static node maintenance: unhealthy node is being replaced")
        except Exception as e:
            logging.error(
                "Error setting unhealthy static nodes into down state, continuing with instance replacement: %s", e,
            )
        instances_to_terminate = []
        for node in unhealthy_static_nodes:
            backing_instance = private_ip_to_instance_map.get(node.nodeaddr)
            if backing_instance:
                instances_to_terminate.append(backing_instance.id)
        log.info(
            "Terminating instances %s that are backing unhealthy static nodes: %s",
            instances_to_terminate,
            unhealthy_static_nodes,
        )
        self.instance_manager.delete_instances(
            instances_to_terminate, terminate_batch_size=self.sync_config.terminate_max_batch_size
        )
        log.info("Launching new instances for unhealthy static nodes: %s", unhealthy_static_nodes)
        self.instance_manager.add_instances_for_nodes(
            node_list, self.sync_config.launch_max_batch_size, self.sync_config.update_node_address
        )
        # Add node to list of nodes being replaced
        self.static_nodes_in_replacement |= set(node_list)
        log.info(
            "After node maintenance, following nodes are currently in replacement: %s", self.static_nodes_in_replacement
        )

    def _maintain_nodes(self, cluster_instances, slurm_nodes):
        """
        Call functions to maintain unhealthy nodes.

        This function needs to handle the case that 2 slurm nodes have the same IP/nodeaddr.
        Hence the a list of nodes is passed in and slurm node dict with IP/nodeaddr as key should be avoided.
        """
        try:
            # Update self.static_nodes_in_replacement by removing any up nodes from the set
            self._update_static_nodes_in_replacement(slurm_nodes)
            log.info("Following nodes are currently in replacement: %s", self.static_nodes_in_replacement)
            private_ip_to_instance_map = {instance.private_ip: instance for instance in cluster_instances}
            unhealthy_dynamic_nodes, unhealthy_static_nodes = self._find_unhealthy_slurm_nodes(
                slurm_nodes, private_ip_to_instance_map
            )
            log.info(
                "Found the following unhealthy static nodes: %s\nFound the following unhealthy dynamic nodes: %s",
                unhealthy_static_nodes,
                unhealthy_dynamic_nodes,
            )
            if unhealthy_dynamic_nodes:
                ClusterManager._handle_unhealthy_dynamic_nodes(unhealthy_dynamic_nodes)
            if unhealthy_static_nodes:
                self._handle_unhealthy_static_nodes(unhealthy_static_nodes, private_ip_to_instance_map)
        except Exception as e:
            log.error(
                "Error when maintaining nodes with unhealthy scheduler states: %s\nContinuing other operations...", e,
            )

    def _terminate_orphaned_instances(self, cluster_instances, ips_used_by_slurm):
        """Terminate instance not associated with any node and running longer than orphaned_instance_timeout."""
        try:
            instances_to_terminate = []
            for instance in cluster_instances:
                if instance.private_ip not in ips_used_by_slurm and ClusterManager._time_is_up(
                    instance.launch_time, self.current_time, self.sync_config.orphaned_instance_timeout
                ):
                    instances_to_terminate.append(instance.id)
            log.info("Terminating the following orphaned instances: %s", instances_to_terminate)
            if instances_to_terminate:
                self.instance_manager.delete_instances(
                    instances_to_terminate, terminate_batch_size=self.sync_config.terminate_max_batch_size
                )
        except Exception as e:
            log.error("Error when terminating orphaned instances with exception: %s\nContinuing other operations...", e)

    @staticmethod
    def _time_is_up(initial_time, current_time, grace_time):
        """Check if timeout is exceeded."""
        # Localize datetime objects to UTC if not previously localized
        # All timestamps used in this function should be already localized
        # Assume timestamp was taken from UTC is there is no localization info
        if not initial_time.tzinfo:
            log.warning("Timestamp %s is not localized. Please double check that this is expected, localizing to UTC.")
            initial_time = initial_time.replace(tzinfo=timezone.utc)
        if not current_time.tzinfo:
            log.warning("Timestamp %s is not localized. Please double check that this is expected, localizing to UTC")
            current_time = current_time.replace(tzinfo=timezone.utc)
        time_diff = (current_time - initial_time).total_seconds()
        return time_diff >= grace_time


def _run_clustermgtd():
    """Run clustermgtd actions."""
    cluster_manager = ClusterManager()
    while True:
        # Get loop start time
        start_time = datetime.now(tz=timezone.utc)
        # Get program config
        sync_config = ClustermgtdConfig(os.path.join(CONFIG_FILE_DIR, "parallelcluster_clustermgtd.conf"))
        # Configure root logger
        try:
            fileConfig(sync_config.logging_config, disable_existing_loggers=False)
        except Exception as e:
            log.warning(
                "Unable to configure logging from %s, using default logging settings.\nException: %s",
                sync_config.logging_config,
                e,
            )
        # Manage cluster
        cluster_manager.manage_cluster(sync_config)
        sleep_remaining_loop_time(sync_config.loop_time, start_time)


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
