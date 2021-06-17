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

from common.utils import check_command_output, grouper, run_command
from retrying import retry
from slurm_plugin.slurm_resources import (
    DynamicNode,
    InvalidNodenameError,
    PartitionStatus,
    SlurmPartition,
    StaticNode,
    parse_nodename,
)

log = logging.getLogger(__name__)

PENDING_RESOURCES_REASONS = [
    "Resources",
    "Nodes required for job are DOWN, DRAINED or reserved for jobs in higher priority partitions",
    "BeginTime",
    "NodeDown",
    "Priority",
    "ReqNodeNotAvail, May be reserved for other job",
]

SQUEUE_FIELD_SIZE = 200
_SQUEUE_FIELDS = [
    "jobid",
    "statecompact",
    "numnodes",
    "numcpus",
    "numtasks",
    "cpus-per-task",
    "mincpus",
    "reason",
    "tres-per-job",
    "tres-per-task",
    "tres-per-node",
    "cpus-per-tres",
]
SQUEUE_FIELD_STRING = ",".join([field + ":{size}" for field in _SQUEUE_FIELDS]).format(size=SQUEUE_FIELD_SIZE)
SCONTROL = "sudo /opt/slurm/bin/scontrol"
SINFO = "/opt/slurm/bin/sinfo"

# Set default timeouts for running different slurm commands.
# These timeouts might be needed when running on large scale
DEFAULT_GET_INFO_COMMAND_TIMEOUT = 30
DEFAULT_UPDATE_COMMAND_TIMEOUT = 60


def is_static_node(nodename):
    """
    Check if the node is static or dynamic.

    Valid NodeName format: {queue_name}-{st/dy}-{instancetype}-{number}
    """
    _, node_type, _ = parse_nodename(nodename)
    return "st" == node_type


def update_nodes(
    nodes,
    nodeaddrs=None,
    nodehostnames=None,
    state=None,
    reason=None,
    raise_on_error=True,
    command_timeout=DEFAULT_UPDATE_COMMAND_TIMEOUT,
):
    """
    Update slurm nodes with scontrol call.

    Slurm can process 10000 nodes in range format.
    Max range is somewhere below 100000, then we see the following error:
    fatal: _parse_single_range: Too many hosts in range '1-100000'

    To safely execute update command, run in batches of 100.
    Inputs can be string or other iterables.

    When there is an error with scontrol update, slurm will try to update as much as it can.
    For example, if one node in a batch failed, the rest of the nodes will still be updated.
    With the node that failed, slurm will try to update attributes that do not have error.
    For example, if updating a state cause failure, but updating nodeaddr cause no failure.
    if we run scontrol update state=fail_state nodeaddr=good_addr nodename=name,
    the scontrol command will fail but nodeaddr will be updated to good_addr.
    """
    batched_node_info = _batch_node_info(nodes, nodeaddrs, nodehostnames, batch_size=100)

    update_cmd = f"{SCONTROL} update"
    if state:
        update_cmd += f" state={state}"
    if reason:
        update_cmd += f' reason="{reason}"'
    for nodenames, addrs, hostnames in batched_node_info:
        node_info = f"nodename={nodenames}"
        if addrs:
            node_info += f" nodeaddr={addrs}"
        if hostnames:
            node_info += f" nodehostname={hostnames}"
        run_command(  # nosec
            f"{update_cmd} {node_info}", raise_on_error=raise_on_error, timeout=command_timeout, shell=True
        )


def update_partitions(partitions, state):
    succeeded_partitions = []
    for partition in partitions:
        try:
            run_command(  # nosec
                f"{SCONTROL} update partitionname={partition} state={state}", raise_on_error=True, shell=True
            )
            succeeded_partitions.append(partition)
        except Exception as e:
            log.error("Failed when setting partition %s to %s with error %s", partition, state, e)

    return succeeded_partitions


def update_all_partitions(state, reset_node_addrs_hostname):
    """Update partitions to a state and reset nodesaddr/nodehostname if needed."""
    try:
        # Get all nodes from partition as opposed to ignoring power_down nodes
        partitions = get_partition_info(get_all_nodes=True)
        partition_to_update = []
        for part in partitions:
            if PartitionStatus(part.state) != PartitionStatus(state):
                log.info(f"Setting partition {part.name} state from {part.state} to {state}")
                if reset_node_addrs_hostname:
                    log.info(f"Resetting partition nodes {part.nodenames}")
                    reset_nodes(part.nodenames, state="power_down", reason="stopping cluster")
                partition_to_update.append(part.name)
        succeeded_partitions = update_partitions(partition_to_update, state)
        return succeeded_partitions == partition_to_update
    except Exception as e:
        log.error("Failed when updating partitions with error %s", e)
        return False


def _batch_attribute(attribute, batch_size, expected_length=None):
    """Parse an attribute into batches."""
    if type(attribute) is str:
        attribute = re.split("(?<=]),", attribute)
    if expected_length and len(attribute) != expected_length:
        raise ValueError

    return [",".join(batch) for batch in grouper(attribute, batch_size)]


def _batch_node_info(nodenames, nodeaddrs, nodehostnames, batch_size):
    """Group nodename, nodeaddrs, nodehostnames into batches."""
    if type(nodenames) is str:
        # Only split on , if there is ] before
        # For ex. "node-[1,3,4-5],node-[20,30]" should split into ["node-[1,3,4-5]","node-[20,30]"]
        nodenames = re.split("(?<=]),", nodenames)
    nodename_batch = _batch_attribute(nodenames, batch_size)
    nodeaddrs_batch = [None] * len(nodename_batch)
    nodehostnames_batch = [None] * len(nodename_batch)
    if nodeaddrs:
        try:
            nodeaddrs_batch = _batch_attribute(nodeaddrs, batch_size, expected_length=len(nodenames))
        except ValueError:
            log.error("Nodename %s and NodeAddr %s contain different number of entries", nodenames, nodeaddrs)
            raise
    if nodehostnames:
        try:
            nodehostnames_batch = _batch_attribute(nodehostnames, batch_size, expected_length=len(nodenames))
        except ValueError:
            log.error("Nodename %s and NodeHostname %s contain different number of entries", nodenames, nodehostnames)
            raise

    return zip(nodename_batch, nodeaddrs_batch, nodehostnames_batch)


def set_nodes_down(nodes, reason):
    """Place slurm node into down state, reason is required."""
    update_nodes(nodes, state="down", reason=reason)


def set_nodes_drain(nodes, reason):
    """Place slurm node into down state, reason is required."""
    update_nodes(nodes, state="drain", reason=reason)


def set_nodes_power_down(nodes, reason=None):
    """
    Place slurm node into power_down state.

    Do not reset the nodeaddr/nodehostname manually.
    Nodeaddr/nodehostname will be reset automatically after power_down with cloud_reg_addrs.
    """
    update_nodes(nodes=nodes, state="power_down", reason=reason, raise_on_error=True)


def reset_nodes(nodes, state=None, reason=None, raise_on_error=False):
    """Reset nodeaddr and nodehostname to be equal to nodename."""
    update_nodes(
        nodes=nodes, nodeaddrs=nodes, nodehostnames=nodes, state=state, reason=reason, raise_on_error=raise_on_error
    )


def set_nodes_idle(nodes, reason=None, reset_node_addrs_hostname=False):
    """
    Place slurm node into idle state.

    Do not raise on error.
    Failure for resume command will fail if node already in IDLE, ignore failure.
    """
    if reset_node_addrs_hostname:
        # slurm supports updating multiple nodeaddr/nodehostname at the same time
        # however if number of nodeaddr/nodehostname entries != number of nodes update will fail
        # works: scontrol update nodename=c5.2xlarge-[1-2] nodeaddr=c5.2xlarge-[1-2]
        # works: scontrol update nodename=c5.2xlarge-[1-2] nodeaddr="some ip","some ip"
        # fails: scontrol update nodename=c5.2xlarge-[1-2] nodeaddr="some ip"
        reset_nodes(nodes, state="resume", reason=reason, raise_on_error=False)
    else:
        update_nodes(nodes=nodes, state="resume", reason=reason, raise_on_error=False)


@retry(stop_max_attempt_number=3, wait_fixed=1500)
def set_nodes_down_and_power_save(node_list, reason):
    """
    Set slurm nodes into down -> power_down.

    This is the standard failure recovery procedure to reset a CLOUD node.
    """
    set_nodes_down(node_list, reason=reason)
    set_nodes_power_down(node_list, reason=reason)


def get_nodes_info(nodes="", command_timeout=DEFAULT_GET_INFO_COMMAND_TIMEOUT):
    """
    Retrieve SlurmNode list from slurm nodelist notation.

    Sample slurm nodelist notation: queue1-dy-c5_xlarge-[1-3],queue2-st-t2_micro-5.
    """
    # awk is used to replace the \n\n record separator with '---\n'
    # Note: In case the node does not belong to any partition the Partitions field is missing from Slurm output
    show_node_info_command = (
        f'{SCONTROL} show nodes {nodes} | awk \'BEGIN{{RS="\\n\\n" ; ORS="---\\n";}} {{print}}\' | '
        'grep -oP "^NodeName=\\K(\\S+)| NodeAddr=\\K(\\S+)| NodeHostName=\\K(\\S+)| State=\\K(\\S+)|'
        ' Partitions=\\K(\\S+)|(---)"'
    )
    nodeinfo_str = check_command_output(show_node_info_command, timeout=command_timeout, shell=True)  # nosec

    return _parse_nodes_info(nodeinfo_str)


def get_partition_info(command_timeout=DEFAULT_GET_INFO_COMMAND_TIMEOUT, get_all_nodes=True):
    """Retrieve slurm partition info from scontrol."""
    show_partition_info_command = f'{SCONTROL} show partitions | grep -oP "^PartitionName=\\K(\\S+)| State=\\K(\\S+)"'
    partition_info_str = check_command_output(show_partition_info_command, timeout=command_timeout, shell=True)  # nosec
    partitions_info = _parse_partition_name_and_state(partition_info_str)
    return [
        SlurmPartition(
            partition_name,
            _get_all_partition_nodes(partition_name) if get_all_nodes else _get_partition_nodes(partition_name),
            partition_state,
        )
        for partition_name, partition_state in partitions_info
    ]


def _parse_partition_name_and_state(partition_info):
    """Parse partition name and state from scontrol output."""
    return grouper(partition_info.splitlines(), 2)


def _get_all_partition_nodes(partition_name, command_timeout=DEFAULT_GET_INFO_COMMAND_TIMEOUT):
    """Get all nodes in partition."""
    show_all_nodes_command = f"{SINFO} -h -p {partition_name} -o %N"
    return check_command_output(show_all_nodes_command, timeout=command_timeout, shell=True).strip()  # nosec


def _get_partition_nodes(partition_name, command_timeout=DEFAULT_GET_INFO_COMMAND_TIMEOUT):
    """Get up nodes in a parition by querying sinfo, and filtering out power_down nodes."""
    show_all_nodes_command = f"{SINFO} -h -p {partition_name} -N -o %N"
    show_power_down_nodes_command = f"{SINFO} -h -p {partition_name} -t power_down,powering_down -N -o %N"
    show_down_nodes_command = f"{SINFO} -h -p {partition_name} -t down -N -o %N"
    # Every node is print on a separate line
    all_nodes = check_command_output(show_all_nodes_command, timeout=command_timeout, shell=True).splitlines()  # nosec
    power_down_nodes = check_command_output(  # nosec
        show_power_down_nodes_command, timeout=command_timeout, shell=True
    ).splitlines()
    down_nodes = check_command_output(  # nosec
        show_down_nodes_command, timeout=command_timeout, shell=True
    ).splitlines()
    nodes = []
    for nodename in all_nodes:
        # Always try to maintain the following nodes:
        # Static nodes
        # Any node in down
        # Any node not in power_saving mode
        if "-st-" in nodename or nodename in down_nodes or (nodename not in power_down_nodes and nodename != "n/a"):
            nodes.append(nodename)
    return ",".join(nodes)


def _parse_nodes_info(slurm_node_info):
    """Parse slurm node info into SlurmNode objects."""
    # [ec2-user@ip-10-0-0-58 ~]$ /opt/slurm/bin/scontrol show nodes compute-dy-c5xlarge-[1-3],compute-dy-c5xlarge-50001\
    # | awk 'BEGIN{RS="\n\n" ; ORS="---\n";} {print}' | grep -oP "^NodeName=\K(\S+)| NodeAddr=\K(\S+)|\
    # NodeHostName=\K(\S+)| State=\K(\S+)| Partitions=\K(\S+)|(---)"
    # compute-dy-c5xlarge-1
    # 1.2.3.4
    # compute-dy-c5xlarge-1
    # IDLE+CLOUD+POWER
    # compute,compute2
    # ---
    # compute-dy-c5xlarge-2
    # 1.2.3.4
    # compute-dy-c5xlarge-2
    # IDLE+CLOUD+POWER
    # compute,compute2
    # ---
    # compute-dy-c5xlarge-3
    # 1.2.3.4
    # compute-dy-c5xlarge-3
    # IDLE+CLOUD+POWER
    # compute,compute2
    # ---
    # compute-dy-c5xlarge-50001
    # 1.2.3.4
    # compute-dy-c5xlarge-50001
    # IDLE+CLOUD+POWER
    # ---
    node_info = slurm_node_info.split("---")
    slurm_nodes = []
    for node in node_info:
        lines = node.strip().splitlines()
        if lines:
            try:
                if is_static_node(lines[0]):
                    node = StaticNode(*lines)
                    slurm_nodes.append(node)
                else:
                    node = DynamicNode(*lines)
                    slurm_nodes.append(node)
            except InvalidNodenameError:
                log.warning("Ignoring node %s because it has an invalid name", lines[0])

    return slurm_nodes
