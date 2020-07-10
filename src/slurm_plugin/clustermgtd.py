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
from slurm_plugin.common import CONFIG_FILE_DIR, EC2_INSTANCE_HEALTHY_STATUSES, InstanceManager

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

        # Initialize instance manager class that will be used to launch/terminate instance
        self.instance_manager = InstanceManager(self.region, self.cluster_name, self.boto3_config)
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

    def __init__(self):
        self.replacing_nodes = set()

    def _set_replacing_nodes(self, node_list):
        """Setter function used for testing purposes only."""
        self.replacing_nodes = set(node_list)

    def set_current_time(self, current_time):
        self.current_time = current_time

    def set_sync_config(self, sync_config):
        self.sync_config = sync_config

    def manage_cluster(self, sync_config, current_time):
        """Manage cluster by syncing scheduler states with EC2 states and performing node maintenance actions."""
        # Initialization
        self.set_sync_config(sync_config)
        self.set_current_time(current_time)
        self._write_timestamp_to_file()
        if sync_config.disable_all_cluster_management:
            log.info("All cluster management actions currently disabled.")
            return
        # Get node states for nodes in inactive and active partitions
        active_nodes, inactive_nodes = ClusterManager._get_node_info_from_partition()
        # Handle inactive partition and terminate backing instances
        if inactive_nodes:
            self._clean_up_inactive_partition(inactive_nodes)
        # Get all instances in EC2
        cluster_instances = self._get_ec2_states()
        log.info("Current cluster instances in EC2: %s", cluster_instances)
        # Clean up orphaned instances and skip all other operations if no active node
        if not active_nodes:
            self._terminate_orphaned_instances(cluster_instances, ips_used_by_slurm=[])
            log.info("No active node in scheduler, skipping all other maintenance operation")
            return
        log.info("Current active slurm nodes in scheduler: %s", active_nodes)
        # Initialize mapping data structures
        slurm_nodes_ip_phonebook = {node.nodeaddr: node for node in active_nodes}
        # Perform health check actions
        if not sync_config.disable_all_health_checks:
            self._perform_health_check_actions(cluster_instances, slurm_nodes_ip_phonebook)
        # Maintain slurm nodes
        self._maintain_nodes(cluster_instances, active_nodes)
        # Clean up orphaned instances
        self._terminate_orphaned_instances(cluster_instances, ips_used_by_slurm=list(slurm_nodes_ip_phonebook.keys()))

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

    def _clean_up_inactive_partition(self, inactive_nodes):
        """
        Terminate all other instances associated with nodes directly through EC2.

        This operation will not update node states, nodes will retain their states
        Nodes will be placed into down* by slurm after SlurmdTimeout
        If dynamic, nodes will be power_saved after SuspendTime
        """
        log.info("Clean up instances associated with nodes in INACTIVE partitions: %s", inactive_nodes)
        self.sync_config.instance_manager.terminate_associated_instances(
            inactive_nodes, terminate_batch_size=self.sync_config.terminate_max_batch_size
        )

    def _get_ec2_states(self):
        """Get EC2 instance states by describe_instances API."""
        return self.sync_config.instance_manager.get_cluster_instances(include_master=False, alive_states_only=True)

    def _perform_health_check_actions(self, cluster_instances, slurm_nodes_ip_phonebook):
        """Run health check actions."""
        instances_id_phonebook = {instance.id: instance for instance in cluster_instances}
        # Get instance health states
        instance_health_states = self.sync_config.instance_manager.get_instance_health_states(
            list(instances_id_phonebook.keys())
        )
        # Perform EC2 health check actions
        if not self.sync_config.disable_ec2_health_check:
            self._handle_health_check(
                instance_health_states,
                instances_id_phonebook,
                slurm_nodes_ip_phonebook,
                health_check_type=ClusterManager.HealthCheckTypes.ec2_health,
            )
        # Perform scheduled event actions
        if not self.sync_config.disable_scheduled_event_health_check:
            self._handle_health_check(
                instance_health_states,
                instances_id_phonebook,
                slurm_nodes_ip_phonebook,
                health_check_type=ClusterManager.HealthCheckTypes.scheduled_event,
            )

    def _fail_ec2_health_check(self, instance_health_state):
        """Check if instance is failing any EC2 health check for more than health_check_timeout."""
        try:
            if (
                # Check instance status
                instance_health_state.instance_status.get("Status") not in EC2_INSTANCE_HEALTHY_STATUSES
                and ClusterManager._time_is_up(
                    instance_health_state.instance_status.get("Details")[0].get("ImpairedSince"),
                    self.current_time,
                    self.sync_config.health_check_timeout,
                )
            ) or (
                # Check system status
                instance_health_state.system_status.get("Status") not in EC2_INSTANCE_HEALTHY_STATUSES
                and ClusterManager._time_is_up(
                    instance_health_state.instance_status.get("Details")[0].get("ImpairedSince"),
                    self.current_time,
                    self.sync_config.health_check_timeout,
                )
            ):
                return True
        except Exception as e:
            log.warning("Error when parsing instance health status %s, with exception: %s", instance_health_state, e)
            return True

        return False

    def _fail_scheduled_events_check(self, instance_health_state):
        """Check if instance has EC2 scheduled maintenance event."""
        if instance_health_state.scheduled_events:
            return True
        return False

    def _handle_health_check(
        self, instance_health_states, instances_id_phonebook, slurm_nodes_ip_phonebook, health_check_type
    ):
        """
        Perform health check action for all types of supported health checks.

        Place nodes failing health check into DRAIN, so they can be maintained when possible
        """
        health_check_functions = {
            ClusterManager.HealthCheckTypes.scheduled_event: self._fail_scheduled_events_check,
            ClusterManager.HealthCheckTypes.ec2_health: self._fail_ec2_health_check,
        }
        # Check if health check type is supported
        if health_check_type not in health_check_functions:
            log.error("Unable to perform action for unsupported health check type: %s", health_check_type)
            return
        log.info("Performing actions for health check type: %s", health_check_type)
        nodes_failing_health_check = []
        for instance in instance_health_states:
            # Pick corresponding health check function
            is_instance_unhealthy = health_check_functions.get(health_check_type)
            if is_instance_unhealthy(instance_health_state=instance):
                unhealthy_node = slurm_nodes_ip_phonebook.get(instances_id_phonebook.get(instance.id).private_ip)
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

    def _update_replacing_nodes(self, slurm_nodes):
        """Update self.replacing_nodes by removing nodes that finished replacement and is up."""
        for node in slurm_nodes:
            if node.is_up():
                self.replacing_nodes.discard(node.name)

    def _find_unhealthy_slurm_nodes(self, slurm_nodes, instances_ip_phonebook):
        """Check and return slurm nodes with unhealthy scheduler state, grouping by node type (static/dynamic)."""
        unhealthy_static_nodes = []
        unhealthy_dynamic_nodes = []
        for node in slurm_nodes:
            if not self._is_node_healthy(node, instances_ip_phonebook):
                if node.is_static_node():
                    unhealthy_static_nodes.append(node)
                else:
                    unhealthy_dynamic_nodes.append(node)

        return unhealthy_dynamic_nodes, unhealthy_static_nodes

    def _is_node_being_replaced(self, node, instances_ip_phonebook):
        """Check if a node is currently being replaced and within node_replacement_timeout."""
        backing_instance = instances_ip_phonebook.get(node.nodeaddr)
        if (
            backing_instance
            and node.name in self.replacing_nodes
            and not ClusterManager._time_is_up(
                backing_instance.launch_time, self.current_time, grace_time=self.sync_config.node_replacement_timeout
            )
        ):
            return True
        return False

    @staticmethod
    def _is_node_addr_valid(node, instance_ips_in_cluster):
        """Check if a slurm node's addr setting is considered valid."""
        # Check if static node has a backing instance
        if node.is_static_node():
            if not node.is_nodeaddr_set():
                log.warning("Node state check: static node without nodeaddr set node %s", node.name)
                return False
        # Check if backing instance is still in EC2
        if node.is_nodeaddr_set():
            if node.nodeaddr not in instance_ips_in_cluster:
                log.warning("Node state check: no corresponding instance in EC2 for node %s", node.name)
                return False
        return True

    def _is_node_state_healthy(self, node, instances_ip_phonebook):
        """Check if a slurm node's scheduler state is considered healthy."""
        # Check to see if node is in DRAINED, ignoring any node currently being replaced
        if node.is_drained() and self.sync_config.terminate_drain_nodes:
            if self._is_node_being_replaced(node, instances_ip_phonebook):
                log.info("Node state check: node %s in DRAINED but is currently being replaced, ignoring", node.name)
                return True
            else:
                log.warning("Node state check: node %s in DRAINED, replacing node", node.name)
                return False
        # Check to see if node is in DOWN, ignoring any node currently being replaced
        if node.is_down() and self.sync_config.terminate_down_nodes:
            if self._is_node_being_replaced(node, instances_ip_phonebook):
                log.info("Node state check: node %s in DOWN but is currently being replaced, ignoring.", node.name)
                return True
            else:
                log.warning("Node state check: node %s in DOWN, replacing node", node.name)
                return False
        return True

    def _is_node_healthy(self, node, instances_ip_phonebook):
        """Check if a slurm node is considered healthy."""
        return ClusterManager._is_node_addr_valid(
            node, instance_ips_in_cluster=list(instances_ip_phonebook.keys())
        ) and self._is_node_state_healthy(node, instances_ip_phonebook)

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

    def _handle_unhealthy_static_nodes(self, unhealthy_static_nodes):
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
        self.sync_config.instance_manager.terminate_associated_instances(
            unhealthy_static_nodes, terminate_batch_size=self.sync_config.terminate_max_batch_size
        )
        log.info("Launching new instances for unhealthy static nodes: %s", unhealthy_static_nodes)
        self.sync_config.instance_manager.add_instances_for_nodes(
            node_list, self.sync_config.launch_max_batch_size, self.sync_config.update_node_address
        )
        # Add node to list of nodes being replaced
        self.replacing_nodes |= set(node_list)
        log.info("After node maintenance, following nodes are currently in replacement: %s", self.replacing_nodes)

    def _maintain_nodes(self, cluster_instances, slurm_nodes):
        """
        Call functions to maintain unhealthy nodes.

        This function needs to handle the case that 2 slurm nodes have the same IP/nodeaddr.
        Hence the a list of nodes is passed in and slurm node dict with IP/nodeaddr as key should be avoided.
        """
        self._update_replacing_nodes(slurm_nodes)
        log.info("Following nodes are currently in replacement: %s", self.replacing_nodes)
        instances_ip_phonebook = {instance.private_ip: instance for instance in cluster_instances}
        unhealthy_dynamic_nodes, unhealthy_static_nodes = self._find_unhealthy_slurm_nodes(
            slurm_nodes, instances_ip_phonebook
        )
        log.info(
            "Found the following unhealthy static nodes: %s\nFound the following unhealthy dynamic nodes: %s",
            unhealthy_static_nodes,
            unhealthy_dynamic_nodes,
        )
        if unhealthy_dynamic_nodes:
            ClusterManager._handle_unhealthy_dynamic_nodes(unhealthy_dynamic_nodes)
        if unhealthy_static_nodes:
            self._handle_unhealthy_static_nodes(unhealthy_static_nodes)

    def _terminate_orphaned_instances(self, cluster_instances, ips_used_by_slurm):
        """Terminate instance not associated with any node and running longer than orphaned_instance_timeout."""
        instances_to_terminate = []
        for instance in cluster_instances:
            if instance.private_ip not in ips_used_by_slurm and ClusterManager._time_is_up(
                instance.launch_time, self.current_time, self.sync_config.orphaned_instance_timeout
            ):
                instances_to_terminate.append(instance.id)
        log.info("Terminating the following orphaned instances: %s", instances_to_terminate)
        if instances_to_terminate:
            self.sync_config.instance_manager.delete_instances(
                instances_to_terminate, terminate_batch_size=self.sync_config.terminate_max_batch_size
            )

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
        cluster_manager.manage_cluster(sync_config, start_time)
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
