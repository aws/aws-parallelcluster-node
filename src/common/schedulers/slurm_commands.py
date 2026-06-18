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

import json
import logging
import os
import re
import shlex

# A nosec comment is appended to the following line in order to disable the B404 check.
# In this file the input of the module subprocess is trusted.
import subprocess  # nosec B404
from datetime import datetime, timezone
from typing import Dict, List

from common.utils import check_command_output, grouper, run_command, validate_subprocess_argument
from retrying import retry
from slurm_plugin.slurm_resources import (
    DynamicNode,
    InvalidNodenameError,
    PartitionStatus,
    SlurmNode,
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
SLURM_BINARIES_DIR = os.environ.get("SLURM_BINARIES_DIR", "/opt/slurm/bin")
SLURM_CONF_DIR = os.path.join(os.path.split(SLURM_BINARIES_DIR)[0], "etc")
SCONTROL = f"sudo {SLURM_BINARIES_DIR}/scontrol"
SINFO = f"{SLURM_BINARIES_DIR}/sinfo"

SCONTROL_NODE_INFO_FIELD_REGEX = re.compile(
    r"^(NodeName=\S+)"
    r"|(NodeAddr=\S+)"
    r"|(NodeHostName=\S+)"
    r"|(?<!Next)(State=\S+)"
    r"|(Partitions=\S+)"
    r"|(SlurmdStartTime=\S+)"
    r"|(LastBusyTime=\S+)"
    r"|(ReservationName=\S+)"
    r"|(InstanceId=\S+)"
    r"|(Reason=.*)",
    re.MULTILINE,
)

# Fields extracted from `scontrol show partitions` output. `(?<!Next)` ensures `State` is matched but
# `NextState` is not.
SCONTROL_PARTITION_INFO_FIELD_REGEX = re.compile(
    r"^(PartitionName=\S+)" r"|(?<!Next)(State=\S+)",
    re.MULTILINE,
)

# Set default timeouts for running different slurm commands.
# These timeouts might be needed when running on large scale
DEFAULT_SCONTROL_COMMAND_TIMEOUT = 30
DEFAULT_GET_INFO_COMMAND_TIMEOUT = 30
DEFAULT_UPDATE_COMMAND_TIMEOUT = 60


class PartitionNodelistMapping:
    """
    Singleton class to represent the partition-nodelist mapping between PC-managed partitions and PC-managed nodes.

    The information contained in this singleton is used to make sure ParallelCluster only handles partitions and nodes
    it is supposed to manage (via Slurm commands).
    """

    partition_nodelist_mapping: Dict[str, str]
    _instance = None

    def __init__(self):
        self.partition_nodelist_mapping = {}

    def get_partition_nodelist_mapping(self) -> Dict[str, str]:
        """Retrieve partition-nodelist mapping from JSON file."""
        if not self.partition_nodelist_mapping:
            partition_nodelist_json = os.path.join(
                SLURM_CONF_DIR,
                "pcluster/parallelcluster_partition_nodelist_mapping.json",
            )
            with open(partition_nodelist_json, "r", encoding="utf-8") as file:
                self.partition_nodelist_mapping = json.load(file)
        return self.partition_nodelist_mapping

    def get_partitions(self) -> List[str]:
        """Retrieve partitions from JSON file."""
        return list(self.get_partition_nodelist_mapping().keys())

    @staticmethod
    def instance():
        """Return the singleton PartitionNodelistMapping instance."""
        if not PartitionNodelistMapping._instance:
            PartitionNodelistMapping._instance = PartitionNodelistMapping()
        return PartitionNodelistMapping._instance

    @staticmethod
    def reset():
        """Reset the instance to clear all caches."""
        PartitionNodelistMapping._instance = None


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
    instance_ids=None,
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

    InstanceId is set in the same batched command as NodeAddr so that the node and its backing
    instance are associated atomically.
    """
    batched_node_info = _batch_node_info(nodes, nodeaddrs, nodehostnames, instance_ids, batch_size=100)

    update_cmd = f"{SCONTROL} update"
    if state:
        validate_subprocess_argument(state)
        update_cmd += f" state={state}"
    if reason:
        validate_subprocess_argument(reason)
        update_cmd += f' reason="{reason}"'
    for nodenames, addrs, hostnames, ids in batched_node_info:
        validate_subprocess_argument(nodenames)
        node_info = f"nodename={nodenames}"
        if addrs:
            validate_subprocess_argument(addrs)
            node_info += f" nodeaddr={addrs}"
        if hostnames:
            validate_subprocess_argument(hostnames)
            node_info += f" nodehostname={hostnames}"
        if ids:
            validate_subprocess_argument(ids)
            node_info += f" instanceid={ids}"
        # It's safe to use the function affected by B604 since the command is fully built in this code
        run_command(  # nosec B604
            f"{update_cmd} {node_info}", raise_on_error=raise_on_error, timeout=command_timeout, shell=True
        )


def update_partitions(partitions, state):
    succeeded_partitions = []
    # Validation to sanitize the input argument and make it safe to use the function affected by B604
    validate_subprocess_argument(state)
    for partition in partitions:
        try:
            # Validation to sanitize the input argument and make it safe to use the function affected by B604
            validate_subprocess_argument(partition)
            run_command(  # nosec B604
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
        partitions = get_partitions_info()
        partition_to_update = []
        for part in partitions:
            if PartitionStatus(part.state) != PartitionStatus(state):
                log.info("Setting partition %s state from %s to %s", part.name, part.state, state)
                if reset_node_addrs_hostname:
                    log.info("Resetting partition nodes %s", part.nodenames)
                    set_nodes_power_down(part.nodenames, reason="stopping cluster")
                partition_to_update.append(part.name)
        succeeded_partitions = update_partitions(partition_to_update, state)
        return succeeded_partitions == partition_to_update
    except Exception as e:
        log.error("Failed when updating partitions with error %s", e)
        return False


def reset_nodes_in_inactive_partitions():
    """
    Reset nodeaddr/nodehostname and set to down the nodes belonging to INACTIVE partitions.

    This is meant to be called when starting the compute fleet, before bringing partitions back UP, to clean up nodes
    that an INACTIVE partition may have left behind (e.g. dynamic nodes that were powering up without a backing
    instance when protected mode disabled the partition). If not reset, those nodes would be detected as bootstrap
    failures right after start and send the cluster back into protected mode. Only INACTIVE partitions are considered,
    so nodes of active partitions (which may be running jobs) are not affected.
    """
    try:
        # Collect the node names of INACTIVE partitions, skipping partitions with no nodes to avoid building a
        # malformed (e.g. empty or with consecutive commas) node list.
        inactive_partition_nodes = [
            part.nodenames
            for part in get_partitions_info()
            if PartitionStatus(part.state) == PartitionStatus.INACTIVE and part.nodenames
        ]
        if not inactive_partition_nodes:
            log.info("No nodes in INACTIVE partitions, nothing to reset.")
            return
        # Reset only the nodes that actually need it (e.g. nodeaddr still set), mirroring clustermgtd's inactive
        # cleanup.
        nodes_to_reset = {
            node.name for node in get_nodes_info(",".join(inactive_partition_nodes)) if node.needs_reset_when_inactive()
        }
        if nodes_to_reset:
            log.info(
                "Resetting nodeaddr/nodehostname and setting to down the following nodes of INACTIVE partitions: %s",
                sorted(nodes_to_reset),
            )
            reset_nodes(nodes_to_reset, state="down", reason="inactive partition", raise_on_error=False)
        else:
            log.info("No nodes of INACTIVE partitions need to be reset.")
    except Exception as e:
        log.error("Failed when resetting nodes of INACTIVE partitions with error %s", e)


def _batch_attribute(attribute, batch_size, expected_length=None):
    """Parse an attribute into batches."""
    if type(attribute) is str:
        attribute = re.split("(?<=]),", attribute)
    if expected_length and len(attribute) != expected_length:
        raise ValueError

    return [",".join(batch) for batch in grouper(attribute, batch_size)]


def _batch_optional_attribute(attribute, attribute_label, nodenames, default_batch, batch_size):
    """Batch an optional per-node attribute, raising if its entry count does not match the nodes."""
    if not attribute:
        return default_batch
    try:
        return _batch_attribute(attribute, batch_size, expected_length=len(nodenames))
    except ValueError:
        log.error("Nodename %s and %s %s contain different number of entries", nodenames, attribute_label, attribute)
        raise


def _batch_node_info(nodenames, nodeaddrs, nodehostnames, instance_ids, batch_size):
    """Group nodename, nodeaddrs, nodehostnames, instance_ids into batches."""
    if type(nodenames) is str:
        # Only split on , if there is ] before
        # For ex. "node-[1,3,4-5],node-[20,30]" should split into ["node-[1,3,4-5]","node-[20,30]"]
        nodenames = re.split("(?<=]),", nodenames)
    nodename_batch = _batch_attribute(nodenames, batch_size)
    default_batch = [None] * len(nodename_batch)
    nodeaddrs_batch = _batch_optional_attribute(nodeaddrs, "NodeAddr", nodenames, default_batch, batch_size)
    nodehostnames_batch = _batch_optional_attribute(nodehostnames, "NodeHostName", nodenames, default_batch, batch_size)
    instance_ids_batch = _batch_optional_attribute(instance_ids, "InstanceId", nodenames, default_batch, batch_size)

    return zip(nodename_batch, nodeaddrs_batch, nodehostnames_batch, instance_ids_batch)


def set_nodes_down(nodes, reason):
    """Place slurm node into down state, reason is required."""
    update_nodes(nodes, state="down", reason=reason)


def set_nodes_drain(nodes, reason):
    """Place slurm node into down state, reason is required."""
    update_nodes(nodes, state="drain", reason=reason)


@retry(stop_max_attempt_number=3, wait_fixed=1500)
def set_nodes_power_down(nodes, reason=None):
    """Place slurm node into power_down state and reset nodeaddr/nodehostname."""
    reset_nodes(nodes=nodes, state="power_down_force", reason=reason, raise_on_error=True)


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


def _run_scontrol_command(
    scontrol_args: str,
    command_timeout: int = DEFAULT_GET_INFO_COMMAND_TIMEOUT,
    raise_on_error: bool = True,
) -> str:
    """
    Run a `scontrol <args>` command as a standalone subprocess and return its stdout.

    scontrol's exit code, stderr and raw stdout are logged. When scontrol exits non-zero but still returns
    output (e.g. one of the requested nodes does not exist: Slurm prints "Node <x> not found" and exits 1
    while still returning the other nodes), the output is returned and a warning is logged. When it exits
    non-zero with no output, an error is logged and, when raise_on_error, a CalledProcessError is raised.
    """
    command = f"{SCONTROL} {scontrol_args}"
    log.debug("Executing scontrol command: %s", command)
    try:
        # nosec B603: command is built from trusted/validated input and run without a shell.
        result = subprocess.run(  # nosec B603
            shlex.split(command),
            timeout=command_timeout,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
    except subprocess.TimeoutExpired:
        log.error("scontrol command timed out after %s seconds: '%s'", command_timeout, command)
        raise
    except OSError as e:
        log.error("Unable to execute scontrol command '%s'. Failed with exception: %s", command, e)
        raise

    stdout = result.stdout or ""
    stderr = (result.stderr or "").strip()
    # Raw scontrol output is logged at DEBUG only, so it is not emitted at the default INFO level.
    log.debug("scontrol command '%s' exited with code %s. Raw output:\n%s", command, result.returncode, stdout)

    if result.returncode != 0:
        if stdout.strip():
            log.warning(
                "scontrol command '%s' returned non-zero exit code %s but produced output; proceeding with "
                "the output. stderr: '%s'",
                command,
                result.returncode,
                stderr,
            )
        else:
            log.error(
                "scontrol command '%s' failed with exit code %s and produced no output. stderr: '%s'",
                command,
                result.returncode,
                stderr,
            )
            if raise_on_error:
                raise subprocess.CalledProcessError(result.returncode, command, output=stdout, stderr=result.stderr)

    return stdout


def _extract_scontrol_records(raw_output: str, field_regex) -> List[Dict[str, str]]:
    """
    Split raw scontrol output into records and extract the relevant `key=value` fields of each record.

    Records are separated by blank lines. For each record, only the fields matched by field_regex are kept,
    returned as a dict mapping the Slurm field name to its value. Records with no matching field are skipped.
    """
    records = []
    for record in re.split(r"\n\n+", raw_output.strip()):
        fields = {}
        for match in field_regex.finditer(record):
            key, _, value = match.group(0).partition("=")
            fields[key] = value
        if fields:
            records.append(fields)
    return records


def get_nodes_info(nodes: str = "", command_timeout=DEFAULT_GET_INFO_COMMAND_TIMEOUT) -> List[SlurmNode]:
    """
    Retrieve SlurmNode list from slurm nodelist notation.

    Sample slurm nodelist notation: queue1-dy-c5_xlarge-[1-3],queue2-st-t2_micro-5.
    If no nodes argument is provided, this function considers only nodes in partitions managed by ParallelCluster.
    It is responsibility of the caller to pass a nodes argument with only nodes in partitions managed by
    ParallelCluster.

    TODO: we can consider building a filter to be used in case a nodes argument is passed, in order to exclude nodes
     not managed by ParallelCluster.
    """
    if nodes == "":
        nodes = _get_all_partition_nodes(",".join(PartitionNodelistMapping.instance().get_partitions()))

    validate_subprocess_argument(nodes)

    # Note: In case the node does not belong to any partition the Partitions field is missing from Slurm output
    raw_node_info = _run_scontrol_command(f"show nodes {nodes}", command_timeout=command_timeout)
    return _parse_nodes_info(_extract_scontrol_records(raw_node_info, SCONTROL_NODE_INFO_FIELD_REGEX))


def get_partitions_info(command_timeout=DEFAULT_GET_INFO_COMMAND_TIMEOUT) -> List[SlurmPartition]:
    """
    Retrieve slurm partition info from scontrol.

    This function considers only partitions managed by ParallelCluster.
    """
    partitions = set(PartitionNodelistMapping.instance().get_partitions())
    raw_partition_info = _run_scontrol_command("show partitions", command_timeout=command_timeout)
    partition_records = _extract_scontrol_records(raw_partition_info, SCONTROL_PARTITION_INFO_FIELD_REGEX)
    partitions_info = _parse_partitions_info(partition_records, managed_partitions=partitions or None)
    return [
        SlurmPartition(
            partition_name,
            _get_all_partition_nodes(partition_name),
            partition_state,
        )
        for partition_name, partition_state in partitions_info
    ]


def resume_powering_down_nodes():
    """Resume nodes that are powering_down so that are set in power state right away."""
    # TODO: This function was added due to Slurm ticket 12915. The bug is not reproducible and the ticket was then
    #  closed. This operation may now be useless: we need to check this.
    log.info("Resuming powering down nodes.")
    powering_down_nodes = _get_slurm_nodes(states="powering_down")
    update_nodes(nodes=powering_down_nodes, state="resume", raise_on_error=False)


def _parse_partitions_info(partition_records, managed_partitions=None):
    """
    Extract (partition_name, partition_state) tuples from scontrol partition records.

    Only partitions in managed_partitions are returned; if managed_partitions is None, all partitions are
    returned. Records missing the name or state are skipped.
    """
    partitions_info = []
    for fields in partition_records:
        partition_name = fields.get("PartitionName")
        partition_state = fields.get("State")
        if not partition_name or not partition_state:
            continue
        if managed_partitions is not None and partition_name not in managed_partitions:
            continue
        partitions_info.append((partition_name, partition_state))
    return partitions_info


def _get_all_partition_nodes(partition_name, command_timeout=DEFAULT_GET_INFO_COMMAND_TIMEOUT):
    """Get all nodes in partition."""
    # Validation to sanitize the input argument and make it safe to use the function affected by B604
    validate_subprocess_argument(partition_name)

    show_all_nodes_command = f"{SINFO} -h -p {partition_name} -o %N"
    return check_command_output(show_all_nodes_command, timeout=command_timeout, shell=True).strip()  # nosec B604


def _get_slurm_nodes(states=None, partition_name=None, command_timeout=DEFAULT_GET_INFO_COMMAND_TIMEOUT):
    sinfo_command = f"{SINFO} -h -N -o %N"
    partition_name = partition_name or ",".join(PartitionNodelistMapping.instance().get_partitions())
    validate_subprocess_argument(partition_name)
    sinfo_command += f" -p {partition_name}"
    if states:
        validate_subprocess_argument(states)
        sinfo_command += f" -t {states}"
    # Every node is print on a separate line
    # It's safe to use the function affected by B604 since the command is fully built in this code
    return check_command_output(sinfo_command, timeout=command_timeout, shell=True).splitlines()  # nosec B604


def _parse_nodes_info(node_records: List[Dict[str, str]]) -> List[SlurmNode]:
    """
    Build SlurmNode objects from scontrol node records extracted by _extract_scontrol_records.

    Each record is a dict mapping Slurm field names to their values, e.g.:
    {"NodeName": "compute-dy-c5xlarge-1", "NodeAddr": "1.2.3.4", "NodeHostName": "compute-dy-c5xlarge-1",
     "State": "IDLE+CLOUD+POWER", "Partitions": "compute,compute2", "SlurmdStartTime": "2023-01-26T09:57:15"}
    """
    map_slurm_key_to_arg = {
        "NodeName": "name",
        "NodeAddr": "nodeaddr",
        "NodeHostName": "nodehostname",
        "State": "state",
        "Partitions": "partitions",
        "Reason": "reason",
        "SlurmdStartTime": "slurmdstarttime",
        "LastBusyTime": "lastbusytime",
        "ReservationName": "reservation_name",
        "InstanceId": "instance_id",
    }

    date_fields = ["SlurmdStartTime", "LastBusyTime"]

    slurm_nodes = []
    for fields in node_records:
        if "NodeName" not in fields:
            continue
        kwargs = {}
        for key, value in fields.items():
            if key in date_fields:
                if value not in ["None", "Unknown"]:
                    value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").astimezone(tz=timezone.utc)
                else:
                    value = None
            elif key == "InstanceId" and value == "(null)":
                # Slurm reports an unset InstanceId as "(null)"
                value = None
            kwargs[map_slurm_key_to_arg[key]] = value
        try:
            if is_static_node(kwargs["name"]):
                slurm_nodes.append(StaticNode(**kwargs))
            else:
                slurm_nodes.append(DynamicNode(**kwargs))
        except InvalidNodenameError:
            log.warning("Ignoring node %s because it has an invalid name", kwargs["name"])

    return slurm_nodes
