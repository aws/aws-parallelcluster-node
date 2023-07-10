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

import logging
import re
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List

from common.utils import convert_range_to_list, time_is_up

logger = logging.getLogger(__name__)

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


CONFIG_FILE_DIR = "/etc/parallelcluster/slurm_plugin"


class PartitionStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    INACTIVE = "INACTIVE"
    DRAIN = "DRAIN"

    def __str__(self):
        return str(self.value)


class SlurmPartition:
    def __init__(self, name, nodenames, state):
        """Initialize slurm partition with attributes."""
        self.name = name
        self.nodenames = nodenames
        self.state = state
        self.slurm_nodes = []

    def is_inactive(self):
        return self.state == "INACTIVE"

    def has_running_job(self):
        return any(node.is_running_job() for node in self.slurm_nodes)

    def get_online_node_by_type(self, terminate_drain_nodes, terminate_down_nodes):
        online_compute_resources = set()
        if not self.state == "INACTIVE":
            for node in self.slurm_nodes:
                if (
                    node.is_healthy(terminate_drain_nodes, terminate_down_nodes, log_warn_if_unhealthy=False)
                    and node.is_online()
                ):
                    logger.debug("Currently online node: %s, node state: %s", node.name, node.state_string)
                    online_compute_resources.add(node.compute_resource_name)
        return online_compute_resources

    def __eq__(self, other):
        """Compare 2 SlurmPartition objects."""
        if isinstance(other, SlurmPartition):
            return self.__dict__ == other.__dict__
        return False


class JobOversubscribe(Enum):
    YES = "YES"
    NO = "NO"
    USER = "USER"
    MCS = "MCS"
    OK = "OK"

    @classmethod
    # Default to OK when Oversubscribe value cannot be parsed
    def _missing_(cls, value):
        return cls.OK

    def __str__(self):
        return str(self.value)


@dataclass
class SlurmJob(metaclass=ABCMeta):
    job_id: int


class SlurmResumeJob(SlurmJob):
    def __init__(
        self,
        job_id,
        nodes_alloc,
        nodes_resume,
        oversubscribe,
        partition="",
        reservation="",
        features="",
        extra="",
    ):
        """Initialize Slurm job with attributes."""
        super().__init__(
            job_id,
        )

        self.nodes_alloc = get_node_list(nodes_alloc)
        self.nodes_resume = get_node_list(nodes_resume)
        self.oversubscribe = JobOversubscribe(oversubscribe)
        self.partition = partition
        self.reservation = reservation
        self.features = features
        self.extra = extra

    def is_exclusive(self):
        return self.oversubscribe == JobOversubscribe.NO

    def __eq__(self, other):
        """Compare 2 SlurmJob objects."""
        if isinstance(other, SlurmJob):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        attrs = ", ".join(["{key}={value}".format(key=key, value=repr(value)) for key, value in self.__dict__.items()])
        return "{class_name}({attrs})".format(class_name=self.__class__.__name__, attrs=attrs)

    def __str__(self):
        return f"{self.job_id}"

    def __hash__(self):
        return hash(self.job_id)


@dataclass
class SlurmResumeData:
    # List of exclusive job allocated to 1 node each
    jobs_single_node_no_oversubscribe: List[SlurmResumeJob]
    # List of exclusive job allocated to more than 1 node each
    jobs_multi_node_no_oversubscribe: List[SlurmResumeJob]
    # List of non-exclusive job
    jobs_oversubscribe: List[SlurmResumeJob]
    # List of node allocated to single node exclusive job
    single_node_no_oversubscribe: List[str]
    # List of node allocated to multiple node exclusive job
    multi_node_no_oversubscribe: List[str]
    # List of node allocated to non-exclusive job
    nodes_oversubscribe: List[str]


class SlurmNode(metaclass=ABCMeta):
    SLURM_SCONTROL_COMPLETING_STATE = "COMPLETING"
    SLURM_SCONTROL_BUSY_STATES = {"MIXED", "ALLOCATED", SLURM_SCONTROL_COMPLETING_STATE}
    SLURM_SCONTROL_IDLE_STATE = "IDLE"
    SLURM_SCONTROL_DOWN_STATE = "DOWN"
    SLURM_SCONTROL_DRAIN_STATE = "DRAIN"
    SLURM_SCONTROL_POWERING_DOWN_STATE = "POWERING_DOWN"
    SLURM_SCONTROL_POWER_DOWN_STATE = "POWER_DOWN"
    SLURM_SCONTROL_POWERED_DOWN_STATE = "POWERED_DOWN"
    SLURM_SCONTROL_POWER_UP_STATE = "POWERING_UP"
    SLURM_SCONTROL_ONLINE_STATES = {"IDLE+CLOUD", "MIXED+CLOUD", "ALLOCATED+CLOUD", "COMPLETING+CLOUD"}
    SLURM_SCONTROL_POWER_WITH_JOB_STATE = {"MIXED", "CLOUD", "POWERED_DOWN"}
    SLURM_SCONTROL_RESUME_FAILED_STATE = {"DOWN", "CLOUD", "POWERED_DOWN", "NOT_RESPONDING"}
    SLURM_SCONTROL_NODE_DOWN_NOT_RESPONDING_STATE = {"DOWN", "CLOUD", "NOT_RESPONDING"}
    # Due to a bug in Slurm a powered down node can enter IDLE+CLOUD+POWER_DOWN+POWERED_DOWN state
    SLURM_SCONTROL_POWER_STATES = [{"IDLE", "CLOUD", "POWERED_DOWN"}, {"IDLE", "CLOUD", "POWERED_DOWN", "POWER_DOWN"}]
    SLURM_SCONTROL_REBOOT_REQUESTED_STATE = "REBOOT_REQUESTED"
    SLURM_SCONTROL_REBOOT_ISSUED_STATE = "REBOOT_ISSUED"
    SLURM_SCONTROL_INVALID_REGISTRATION_STATE = "INVALID_REG"

    SLURM_SCONTROL_NODE_DOWN_NOT_RESPONDING_REASON = re.compile(r"Not responding \[slurm@.+\]")

    EC2_ICE_ERROR_CODES = {
        "InsufficientInstanceCapacity",
        "InsufficientHostCapacity",
        "InsufficientReservedInstanceCapacity",
        "MaxSpotInstanceCountExceeded",
        "Unsupported",
        "SpotMaxPriceTooLow",
    }

    def __init__(
        self,
        name,
        nodeaddr,
        nodehostname,
        state,
        partitions=None,
        reason=None,
        instance=None,
        slurmdstarttime: datetime = None,
        lastbusytime: datetime = None,
    ):
        """Initialize slurm node with attributes."""
        self.name = name
        self.nodeaddr = nodeaddr
        self.nodehostname = nodehostname
        self.state_string = state
        self.states = set(state.split("+"))
        self.partitions = partitions.strip().split(",") if partitions else None
        self.reason = reason
        self.instance = instance
        self.slurmdstarttime = slurmdstarttime
        self.lastbusytime = lastbusytime
        self.is_static_nodes_in_replacement = False
        self.is_being_replaced = False
        self._is_replacement_timeout = False
        self.is_failing_health_check = False
        self.error_code = self._parse_error_code()
        self.queue_name, self._node_type, self.compute_resource_name = parse_nodename(name)

    def is_nodeaddr_set(self):
        """Check if nodeaddr(private ip) for the node is set."""
        return self.nodeaddr != self.name

    def has_job(self):
        """Check if slurm node is in a working state."""
        return any(working_state in self.states for working_state in self.SLURM_SCONTROL_BUSY_STATES)

    def _is_drain(self):
        """Check if slurm node is in any drain(draining, drained) states."""
        return self.SLURM_SCONTROL_DRAIN_STATE in self.states

    def is_drained(self):
        """
        Check if slurm node is in drained state.

        drained(sinfo) is equivalent to IDLE+DRAIN(scontrol) or DOWN+DRAIN(scontrol)
        """
        return (
            self._is_drain()
            and (self.SLURM_SCONTROL_IDLE_STATE in self.states or self.is_down())
            and not self.is_completing()
        )

    def is_completing(self):
        """Check if slurm node is in COMPLETING state."""
        return self.SLURM_SCONTROL_COMPLETING_STATE in self.states

    def is_power_down(self):
        """Check if slurm node is in power down state."""
        return self.SLURM_SCONTROL_POWER_DOWN_STATE in self.states

    def is_powering_down(self):
        """Check if slurm node is in powering down state."""
        return self.SLURM_SCONTROL_POWERING_DOWN_STATE in self.states

    def is_powered_down(self):
        """Check if slurm node is in powered down state."""
        return self.SLURM_SCONTROL_POWERED_DOWN_STATE in self.states

    def is_idle(self):
        """
        Determine if node as idle.

        A node is idle if it has a backing instance, LastBusyTime has a value from scontrol, and is in IDLE state.
        """
        return self.instance and self.lastbusytime and self.SLURM_SCONTROL_IDLE_STATE in self.states

    def is_power(self):
        """Check if slurm node is in power state."""
        return self.states in self.SLURM_SCONTROL_POWER_STATES

    def is_down(self):
        """Check if slurm node is in a down state."""
        return (
            self.SLURM_SCONTROL_DOWN_STATE in self.states
            and not self.is_powering_down()
            and (not self.is_power_down() or self.is_powered_down())
        )

    def is_up(self):
        """Check if slurm node is in a healthy state."""
        return not self._is_drain() and not self.is_down() and not self.is_powering_down()

    def is_powering_up(self):
        """Check if slurm node is in powering up state."""
        return self.SLURM_SCONTROL_POWER_UP_STATE in self.states

    def is_online(self):
        """Check if slurm node is online with backing instance."""
        return self.state_string in self.SLURM_SCONTROL_ONLINE_STATES

    def is_configuring_job(self):
        """Check if slurm node is configuring with job and haven't begun to run a job."""
        return self.is_powering_up() and self.has_job()

    def is_power_with_job(self):
        """Dynamic nodes allocated a job but power up process has not started yet."""
        return self.states == self.SLURM_SCONTROL_POWER_WITH_JOB_STATE

    def is_running_job(self):
        """Check if slurm node is running a job but not in configuring job state."""
        return not self.is_powering_up() and self.has_job() and not self.is_power_with_job()

    def is_resume_failed(self):
        """Check if node resume timeout expires."""
        return self.states == self.SLURM_SCONTROL_RESUME_FAILED_STATE

    def is_down_not_responding(self):
        """Check if node was set to down by Slurm because it was not responding."""
        return (
            self.states == self.SLURM_SCONTROL_NODE_DOWN_NOT_RESPONDING_STATE
            and self.SLURM_SCONTROL_NODE_DOWN_NOT_RESPONDING_REASON.match(self.reason)
            if self.reason
            else False
        )

    def is_powering_up_idle(self):
        """Check if node is in IDLE# state."""
        return self.SLURM_SCONTROL_IDLE_STATE in self.states and self.is_powering_up()

    def is_ice(self):
        return self.error_code in self.EC2_ICE_ERROR_CODES

    def is_reboot_requested(self):
        return self.SLURM_SCONTROL_REBOOT_REQUESTED_STATE in self.states

    def is_reboot_issued(self):
        return self.SLURM_SCONTROL_REBOOT_ISSUED_STATE in self.states

    def is_rebooting(self):
        """
        Check if the node is rebooting via scontrol reboot.

        Check that the node is in a state consistent with the scontrol reboot request.
        """
        cond = False
        if self.is_reboot_issued() or self.is_reboot_requested():
            logger.debug(
                "Node state check: node %s is currently rebooting, ignoring, node state: %s",
                self,
                self.state_string,
            )
            cond = True
        return cond

    def is_invalid_slurm_registration(self):
        """Check if a slurm node has failed registration with the Slurm management daemon."""
        return self.SLURM_SCONTROL_INVALID_REGISTRATION_STATE in self.states

    @abstractmethod
    def is_state_healthy(self, terminate_drain_nodes, terminate_down_nodes, log_warn_if_unhealthy=True):
        """Check if a slurm node's scheduler state is considered healthy."""
        pass

    @abstractmethod
    def is_bootstrap_failure(self):
        """
        Check if a slurm node has boostrap failure.

        Here's the cases of bootstrap error we are checking:
        Bootstrap error that causes instance to self terminate.
        Bootstrap error that prevents instance from joining cluster but does not cause self termination.
        """
        pass

    @abstractmethod
    def is_bootstrap_timeout(self):
        """Check if slurm node timed out while waiting for backing instance to bootstrap."""
        pass

    @abstractmethod
    def is_healthy(self, terminate_drain_nodes, terminate_down_nodes, log_warn_if_unhealthy=True):
        """Check if a slurm node is considered healthy."""
        pass

    def is_powering_down_with_nodeaddr(self):
        """Check if a slurm node is a powering down node with instance backing."""
        # Node in POWERED_DOWN with nodeaddr still set, may have not been seen during the POWERING_DOWN transition
        # for example because of a short SuspendTimeout
        return self.is_nodeaddr_set() and (self.is_power() or self.is_powering_down())

    def is_backing_instance_valid(self, log_warn_if_unhealthy=True):
        """Check if a slurm node's addr is set, it points to a valid instance in EC2."""
        if self.is_nodeaddr_set():
            if not self.instance:
                if log_warn_if_unhealthy:
                    logger.warning(
                        "Node state check: no corresponding instance in EC2 for node %s, node state: %s",
                        self,
                        self.state_string,
                    )
                return False
        return True

    @abstractmethod
    def needs_reset_when_inactive(self):
        """Check if the node need to be reset if node is inactive."""
        pass

    def idle_time(self, current_time: datetime) -> float:
        return (current_time - self.lastbusytime).total_seconds() if self.lastbusytime else 0

    def _parse_error_code(self):
        """Parse RunInstance error code from node reason."""
        if self.reason and self.reason.startswith("(Code:"):
            index_of_bracket = self.reason.find(")")
            error_code = self.reason[len("(Code:") : index_of_bracket]  # noqa E203: whitespace before ':'
            return error_code
        return None

    def __eq__(self, other):
        """Compare 2 SlurmNode objects."""
        if isinstance(other, SlurmNode):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        attrs = ", ".join(["{key}={value}".format(key=key, value=repr(value)) for key, value in self.__dict__.items()])
        return "{class_name}({attrs})".format(class_name=self.__class__.__name__, attrs=attrs)

    def __str__(self):
        return f"{self.name}({self.nodeaddr})"

    def __hash__(self):
        return hash(self.name)


class StaticNode(SlurmNode):
    def __init__(
        self,
        name,
        nodeaddr,
        nodehostname,
        state,
        partitions=None,
        reason=None,
        instance=None,
        slurmdstarttime=None,
        lastbusytime=None,
    ):
        """Initialize slurm node with attributes."""
        super().__init__(
            name,
            nodeaddr,
            nodehostname,
            state,
            partitions,
            reason,
            instance,
            slurmdstarttime,
            lastbusytime=lastbusytime,
        )

    def is_healthy(self, terminate_drain_nodes, terminate_down_nodes, log_warn_if_unhealthy=True):
        """Check if a slurm node is considered healthy."""
        return (
            self._is_static_node_ip_configuration_valid(log_warn_if_unhealthy=log_warn_if_unhealthy)
            and self.is_backing_instance_valid(log_warn_if_unhealthy=log_warn_if_unhealthy)
            and self.is_state_healthy(
                terminate_drain_nodes, terminate_down_nodes, log_warn_if_unhealthy=log_warn_if_unhealthy
            )
        )

    def is_state_healthy(self, terminate_drain_nodes, terminate_down_nodes, log_warn_if_unhealthy=True):
        """Check if a slurm node's scheduler state is considered healthy."""
        # Check if node is rebooting: if so, the node is healthy
        if self.is_rebooting():
            return True
        # Check to see if node is in DRAINED, ignoring any node currently being replaced or in POWER_DOWN
        if self.is_drained() and not self.is_power_down() and terminate_drain_nodes:
            if self.is_being_replaced:
                logger.debug(
                    "Node state check: node %s in DRAINED but is currently being replaced, ignoring, node state: %s",
                    self,
                    self.state_string,
                )
                return True
            else:
                if log_warn_if_unhealthy:
                    logger.warning("Node state check: node %s in DRAINED, node state: %s", self, self.state_string)
                return False
        # Check to see if node is in DOWN, ignoring any node currently being replaced
        elif self.is_down() and terminate_down_nodes:
            if self.is_being_replaced:
                logger.debug(
                    "Node state check: node %s in DOWN but is currently being replaced, ignoring. Node state: ",
                    self,
                    self.state_string,
                )
                return True
            else:
                if log_warn_if_unhealthy:
                    logger.warning("Node state check: node %s in DOWN, node state: %s", self, self.state_string)
                return False
        return True

    def _is_static_node_ip_configuration_valid(self, log_warn_if_unhealthy=True):
        """Check if static node is configured with a private IP."""
        if not self.is_nodeaddr_set():
            if log_warn_if_unhealthy:
                logger.warning(
                    "Node state check: static node without nodeaddr set, node %s, node state %s:",
                    self,
                    self.state_string,
                )
            return False
        return True

    def is_bootstrap_failure(self):
        """Check if a slurm node has boostrap failure."""
        if self.is_static_nodes_in_replacement and not self.is_backing_instance_valid(log_warn_if_unhealthy=False):
            # Node is currently in replacement and no backing instance
            logger.warning(
                "Node bootstrap error: Node %s is currently in replacement and no backing instance, node state %s:",
                self,
                self.state_string,
            )
            return True
            # Replacement timeout expires for node in replacement
        elif self.is_bootstrap_timeout():
            logger.warning(
                "Node bootstrap error: Replacement timeout expires for node %s in replacement, node state %s:",
                self,
                self.state_string,
            )
            return True
        elif self.is_failing_health_check and self.is_static_nodes_in_replacement:
            logger.warning(
                "Node bootstrap error: Node %s failed during bootstrap when performing health check, node state %s:",
                self,
                self.state_string,
            )
            return True
        return False

    def is_bootstrap_timeout(self):
        """Check if slurm node timed out waiting for backing instance to bootstrap."""
        return self._is_replacement_timeout

    def needs_reset_when_inactive(self):
        """Check if the node need to be reset if node is inactive."""
        return self.is_nodeaddr_set()


class DynamicNode(SlurmNode):
    def __init__(
        self,
        name,
        nodeaddr,
        nodehostname,
        state,
        partitions=None,
        reason=None,
        instance=None,
        slurmdstarttime=None,
        lastbusytime=None,
    ):
        """Initialize slurm node with attributes."""
        super().__init__(
            name,
            nodeaddr,
            nodehostname,
            state,
            partitions,
            reason,
            instance,
            slurmdstarttime,
            lastbusytime=lastbusytime,
        )

    def is_state_healthy(self, terminate_drain_nodes, terminate_down_nodes, log_warn_if_unhealthy=True):
        """Check if a slurm node's scheduler state is considered healthy."""
        # Check if node is rebooting: if so, the node is healthy
        if self.is_rebooting():
            return True
        # Check to see if node is in DRAINED, ignoring any node currently being replaced or in POWER_DOWN
        if self.is_drained() and not self.is_power_down() and terminate_drain_nodes:
            if log_warn_if_unhealthy:
                logger.warning("Node state check: node %s in DRAINED, node state: %s", self, self.state_string)
            return False
        # Check to see if node is in DOWN, ignoring any node currently being replaced
        elif self.is_down() and terminate_down_nodes:
            if not self.is_nodeaddr_set():
                # Silently handle failed to launch dynamic node to clean up normal logging
                logger.debug("Node state check: node %s in DOWN, node state: %s", self, self.state_string)
            else:
                if log_warn_if_unhealthy:
                    logger.warning("Node state check: node %s in DOWN, node state: %s", self, self.state_string)
            return False
        return True

    def is_healthy(self, terminate_drain_nodes, terminate_down_nodes, log_warn_if_unhealthy=True):
        """Check if a slurm node is considered healthy."""
        return self.is_backing_instance_valid(log_warn_if_unhealthy=log_warn_if_unhealthy) and self.is_state_healthy(
            terminate_drain_nodes, terminate_down_nodes, log_warn_if_unhealthy=log_warn_if_unhealthy
        )

    def is_bootstrap_failure(self):
        """Check if a slurm node has boostrap failure."""
        # no backing instance + [working state]# in node state
        if (self.is_configuring_job() or self.is_powering_up_idle()) and not self.is_backing_instance_valid(
            log_warn_if_unhealthy=False
        ):
            logger.warning(
                "Node bootstrap error: Node %s is in power up state without valid backing instance, node state: %s",
                self,
                self.state_string,
            )
            return True
        # Dynamic node in DOWN+CLOUD+POWERED_DOWN+NOT_RESPONDING state
        elif self.is_bootstrap_timeout():
            # We need to check if nodeaddr is set to avoid counting powering up nodes as bootstrap failure nodes during
            # cluster start/stop.
            logger.warning(
                "Node bootstrap error: Resume timeout expires for node %s, node state: %s", self, self.state_string
            )
            return True
        elif self.is_failing_health_check and self.is_powering_up():
            logger.warning(
                "Node bootstrap error: Node %s failed during bootstrap when performing health check, node state: %s",
                self,
                self.state_string,
            )
            return True
        # Consider the invalid registration as a bootstrap failure event, but only the first time it is registered.
        # After this, clustermgtd will mark the node as unhealthy and power it down.
        # This does not clear the INVALID_REG flag immediately: this will happen only when the node is fully powered
        # down. Therefore we exclude the nodes that are still pending powering down from this check.
        elif self.is_invalid_slurm_registration() and not (self.is_power_down() or self.is_powering_down()):
            logger.warning(
                "Node bootstrap error: Node %s failed to register to the Slurm management daemon, node state: %s",
                self,
                self.state_string,
            )
            return True
        return False

    def is_bootstrap_timeout(self):
        """Check if slurm node timed out waiting for backing instance to bootstrap."""
        return self.is_resume_failed() and self.is_nodeaddr_set()

    def needs_reset_when_inactive(self):
        """Check if the node need to be reset if node is inactive."""
        return self.is_nodeaddr_set() or (not (self.is_power() or self.is_powering_down() or self.is_down()))


class EC2InstanceHealthState:
    def __init__(self, id, state, instance_status, system_status, scheduled_events):
        """Initialize slurm node with attributes."""
        self.id = id
        self.state = state
        self.instance_status = instance_status
        self.system_status = system_status
        self.scheduled_events = scheduled_events

    def fail_ec2_health_check(self, current_time, health_check_timeout):
        """Check if instance is failing any EC2 health check for more than health_check_timeout."""
        try:
            if (
                # Check instance status
                self.instance_status.get("Status") in EC2_HEALTH_STATUS_UNHEALTHY_STATES
                and time_is_up(
                    self.instance_status.get("Details")[0].get("ImpairedSince"),
                    current_time,
                    health_check_timeout,
                )
            ) or (
                # Check system status
                self.system_status.get("Status") in EC2_HEALTH_STATUS_UNHEALTHY_STATES
                and time_is_up(
                    self.system_status.get("Details")[0].get("ImpairedSince"),
                    current_time,
                    health_check_timeout,
                )
            ):
                return True
        except Exception as e:
            logger.warning("Error when parsing instance health status %s, with exception: %s", self, e)
            return False

        return False

    def fail_scheduled_events_check(self):
        """Check if instance has EC2 scheduled maintenance event."""
        if self.scheduled_events:
            return True
        return False


class InvalidNodenameError(ValueError):
    r"""
    Exception raised when encountering a NodeName that is invalid/incorrectly formatted.

    Valid NodeName format: {queue-name}-{st/dy}-{compute-resource}-{number}
    And match: ^([a-z0-9\-]+)-(st|dy)-([a-z0-9\-]+)-\d+$
    Sample NodeName: queue-1-st-computeresource-2
    """

    pass


@dataclass
class ComputeResourceFailureEvent:
    timestamp: datetime
    error_code: str


def parse_nodename(nodename):
    """Parse queue_name, node_type (st vs dy) and instance_type from nodename."""
    nodename_capture = re.match(r"^([a-z0-9\-]+)-(st|dy)-([a-z0-9\-]+)-\d+$", nodename)
    if not nodename_capture:
        raise InvalidNodenameError

    queue_name, node_type, compute_resource_name = nodename_capture.groups()
    return queue_name, node_type, compute_resource_name


def get_node_list(nodenames):
    """
    Convert nodenames to a list of nodes.

    Example input nodenames: "queue1-st-c5xlarge-[1,3,4-5],queue1-st-c5large-20"
    Example output [queue1-st-c5xlarge-1, queue1-st-c5xlarge-3, queue1-st-c5xlarge-4, queue1-st-c5xlarge-5,
    queue1-st-c5large-20]
    """
    matches = []
    if type(nodenames) is str:
        matches = re.findall(r"((([a-z0-9\-]+)-(st|dy)-([a-z0-9\-]+)-)(\[[\d+,-]+\]|\d+))(,|$)", nodenames)
        # [('queue1-st-c5xlarge-[1,3,4-5]', 'queue1-st-c5xlarge-', 'queue1', 'st', 'c5xlarge', '[1,3,4-5]'),
        # ('queue1-st-c5large-20', 'queue1-st-c5large-', 'queue1', 'st', 'c5large', '20')]
    node_list = []
    if not matches:
        raise InvalidNodenameError
    for match in matches:
        nodename, prefix, _, _, _, nodes, _ = match
        if "[" not in nodes:
            # Single nodename
            node_list.append(nodename)
        else:
            # Multiple nodenames
            try:
                node_range = convert_range_to_list(nodes.strip("[]"))
            except ValueError:
                raise InvalidNodenameError
            node_list += [prefix + str(n) for n in node_range]
    return node_list
