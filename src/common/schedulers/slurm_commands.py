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
import math
import re
from enum import Enum
from textwrap import wrap

from retrying import retry

from common.schedulers.converters import ComparableObject, from_table_to_obj_list
from common.utils import check_command_output, grouper, run_command

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
SCONTROL = "/opt/slurm/bin/scontrol"
SINFO = "/opt/slurm/bin/sinfo"

SlurmPartition = collections.namedtuple("SlurmPartition", ["name", "nodes", "state"])


class PartitionStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    INACTIVE = "INACTIVE"
    DRAIN = "DRAIN"

    def __str__(self):
        return str(self.value)


class SlurmNode:
    SLURM_SCONTROL_BUSY_STATES = {"MIXED", "ALLOCATED", "COMPLETING"}
    SLURM_SCONTROL_IDLE_STATE = "IDLE"
    SLURM_SCONTROL_DOWN_STATE = "DOWN"
    SLURM_SCONTROL_DRAIN_STATE = "DRAIN"
    SLURM_SCONTROL_POWERING_DOWN_STATE = "POWERING_DOWN"
    SLURM_SCONTROL_POWER_STATE = "IDLE+CLOUD+POWER"

    def __init__(self, name, nodeaddr, nodehostname, state):
        """Initialize slurm node with attributes."""
        self.name = name
        self.is_static = is_static_node(name)
        self.nodeaddr = nodeaddr
        self.nodehostname = nodehostname
        self.state = state

    def is_nodeaddr_set(self):
        """Check if nodeaddr(private ip) for the node is set."""
        return self.nodeaddr != self.name

    def has_job(self):
        """Check if slurm node is in a working state."""
        return any(working_state in self.state for working_state in self.SLURM_SCONTROL_BUSY_STATES)

    def _is_drain(self):
        """Check if slurm node is in any drain(draining, drained) states."""
        return self.SLURM_SCONTROL_DRAIN_STATE in self.state

    def is_drained(self):
        """
        Check if slurm node is in drained state.

        drained(sinfo) is equivalent to IDLE+DRAIN(scontrol) or DOWN+DRAIN(scontrol)
        """
        return self._is_drain() and (
            self.SLURM_SCONTROL_IDLE_STATE in self.state or self.SLURM_SCONTROL_DOWN_STATE in self.state
        )

    def is_powering_down(self):
        """Check if slurm node is in powering down state."""
        return self.SLURM_SCONTROL_POWERING_DOWN_STATE in self.state

    def is_power(self):
        """Check if slurm node is in power state."""
        return self.SLURM_SCONTROL_POWER_STATE == self.state

    def is_down(self):
        """Check if slurm node is in a down state."""
        return self.SLURM_SCONTROL_DOWN_STATE in self.state and not self.is_powering_down()

    def is_up(self):
        """Check if slurm node is in a healthy state."""
        return not self._is_drain() and not self.is_down() and not self.is_powering_down()

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


class InvalidNodenameError(ValueError):
    r"""
    Exception raised when encountering a NodeName that is invalid/incorrectly formatted.

    Valid NodeName format: {queue-name}-{st/dy}-{instancetype}-{number}
    And match: ^([a-z0-9\-]+)-(st|dy)-([a-z0-9-]+)-\d+$
    Sample NodeName: queue-1-st-c5xlarge-2
    """

    pass


def parse_nodename(nodename):
    """Parse queue_name, node_type (st vs dy) and instance_type from nodename."""
    nodename_capture = re.match(r"^([a-z0-9\-]+)-(st|dy)-([a-z0-9]+)-\d+$", nodename)
    if not nodename_capture:
        raise InvalidNodenameError

    queue_name, node_type, instance_name = nodename_capture.groups()
    return queue_name, node_type, instance_name


def is_static_node(nodename):
    """
    Check if the node is static or dynamic.

    Valid NodeName format: {queue_name}-{st/dy}-{instancetype}-{number}
    """
    _, node_type, _ = parse_nodename(nodename)
    return "st" == node_type


def update_nodes(
    nodes, nodeaddrs=None, nodehostnames=None, state=None, reason=None, raise_on_error=True, command_timeout=60
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
        run_command(f"{update_cmd} {node_info}", raise_on_error=raise_on_error, timeout=command_timeout, shell=True)


def update_partitions(partitions, state):
    succeeded_partitions = []
    for partition in partitions:
        try:
            run_command(f"{SCONTROL} update partitionname={partition} state={state}", raise_on_error=True, shell=True)
            succeeded_partitions.append(partition)
        except Exception as e:
            logging.error("Failed when setting partition %s to %s with error %s", partition, state, e)

    return succeeded_partitions


def update_all_partitions(state, reset_node_addrs_hostname):
    """Update partitions to a state and reset nodesaddr/nodehostname if needed."""
    try:
        # Get all nodes from partition as opposed to ignoring power_down nodes
        partitions = get_partition_info(get_all_nodes=True)
        partition_to_update = []
        for part in partitions:
            if PartitionStatus(part.state) != PartitionStatus(state):
                logging.info(f"Setting partition {part.name} state from {part.state} to {state}")
                if reset_node_addrs_hostname:
                    logging.info(f"Resetting partition nodes {part.nodes}")
                    reset_nodes(part.nodes, state="power_down", reason="stopping cluster")
                partition_to_update.append(part.name)
        succeeded_partitions = update_partitions(partition_to_update, state)
        return succeeded_partitions == partition_to_update
    except Exception as e:
        logging.error("Failed when updating partitions with error %s", e)
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
            logging.error("Nodename %s and NodeAddr %s contain different number of entries", nodenames, nodeaddrs)
            raise
    if nodehostnames:
        try:
            nodehostnames_batch = _batch_attribute(nodehostnames, batch_size, expected_length=len(nodenames))
        except ValueError:
            logging.error(
                "Nodename %s and NodeHostname %s contain different number of entries", nodenames, nodehostnames
            )
            raise

    return zip(nodename_batch, nodeaddrs_batch, nodehostnames_batch)


def set_nodes_down(nodes, reason):
    """Place slurm node into down state, reason is required."""
    update_nodes(nodes, state="down", reason=reason)


def set_nodes_drain(nodes, reason):
    """Place slurm node into down state, reason is required."""
    update_nodes(nodes, state="drain", reason=reason)


def set_nodes_power_down(nodes, reason=None):
    """Place slurm node into power_down state and reset nodeaddr/nodehostname."""
    reset_nodes(nodes=nodes, state="power_down", reason=reason, raise_on_error=True)


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


def get_nodes_info(nodes, command_timeout=5):
    """
    Retrieve SlurmNode list from slurm nodelist notation.

    Sample slurm nodelist notation: queue1-dy-c5_xlarge-[1-3],queue2-st-t2_micro-5.
    """
    show_node_info_command = (
        f'{SCONTROL} show nodes {nodes} | grep -oP "^NodeName=\\K(\\S+)| '
        'NodeAddr=\\K(\\S+)| NodeHostName=\\K(\\S+)| State=\\K(\\S+)"'
    )
    nodeinfo_str = check_command_output(show_node_info_command, timeout=command_timeout, shell=True)

    return _parse_nodes_info(nodeinfo_str)


def get_partition_info(command_timeout=5, get_all_nodes=True):
    """Retrieve slurm partition info from scontrol."""
    show_partition_info_command = f'{SCONTROL} show partitions | grep -oP "^PartitionName=\\K(\\S+)| State=\\K(\\S+)"'
    partition_info_str = check_command_output(show_partition_info_command, timeout=command_timeout, shell=True)
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


def _get_all_partition_nodes(partition_name, command_timeout=5):
    """Get all nodes in partition."""
    show_all_nodes_command = f"{SINFO} -h -p {partition_name} -o %N"
    return check_command_output(show_all_nodes_command, timeout=command_timeout, shell=True).strip()


def _get_partition_nodes(partition_name, command_timeout=5):
    """Get up nodes in a parition by querying sinfo, and filtering out power_down nodes."""
    show_all_nodes_command = f"{SINFO} -h -p {partition_name} -N -o %N"
    show_power_down_nodes_command = f"{SINFO} -h -p {partition_name} -t power_down,powering_down -N -o %N"
    show_down_nodes_command = f"{SINFO} -h -p {partition_name} -t down -N -o %N"
    # Every node is print on a separate line
    all_nodes = check_command_output(show_all_nodes_command, timeout=command_timeout, shell=True).splitlines()
    power_down_nodes = check_command_output(
        show_power_down_nodes_command, timeout=command_timeout, shell=True
    ).splitlines()
    down_nodes = check_command_output(show_down_nodes_command, timeout=command_timeout, shell=True).splitlines()
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
    return [SlurmNode(*node) for node in grouper(slurm_node_info.splitlines(), 4)]


def get_jobs_info(job_state_filter=None):
    """
    Retrieve the list of submitted jobs.

    :param job_state_filter: filter jobs by the given state
    :return: a list of SlurmJob objects representing the submitted jobs.
    """
    command = "/opt/slurm/bin/squeue -r -O '{0}'".format(SQUEUE_FIELD_STRING)
    if job_state_filter:
        command += " --states {0}".format(job_state_filter)

    output = check_command_output(command)
    return SlurmJob.from_table(output)


def get_pending_jobs_info(
    instance_properties=None, max_nodes_filter=None, filter_by_pending_reasons=None, log_pending_jobs=True
):
    """
    Retrieve the list of pending jobs from the Slurm scheduler.

    The list is filtered based on the args passed to the function.
    Moreover the number of required nodes is recomputed by taking into account the number of
    CPUs required by each task and the number of nodes requested by the user. See _recompute_required_nodes_per_job
    for additional details.

    :param max_slots_filter: max number of slots in a compute node.
    :param max_nodes_filter: max number of nodes in the cluster.
    :param filter_by_pending_reasons: retrieve only jobs with the following pending reasons.
    :param log_pending_jobs: log the actual list of pending jobs (rather than just a count)
    :return: array of filtered SlurmJobs
    """
    pending_jobs = get_jobs_info(job_state_filter="PD")
    logging.info("Retrieved {0} pending jobs".format(len(pending_jobs)))
    if log_pending_jobs:
        logging.info("The pending jobs are: {0}".format(pending_jobs))
    if instance_properties:
        _recompute_required_nodes_by_slots_reservation(pending_jobs, instance_properties["slots"])
        _recompute_required_nodes_by_gpu_reservation(pending_jobs, instance_properties["gpus"])
    if instance_properties or filter_by_pending_reasons or max_nodes_filter:
        filtered_jobs = []
        for job in pending_jobs:
            job_resources = {
                "slots": max(job.cpus_min_per_node, -(-job.cpus_total // job.nodes)),
                "gpus": -(-process_gpus_total_for_job(job) // job.nodes),
            }
            if instance_properties and not job_runnable_on_given_node(job_resources, instance_properties):
                continue
            elif max_nodes_filter and job.nodes > max_nodes_filter:
                logging.info(
                    "Skipping job %s since required nodes (%d) exceed max nodes in cluster (%d)",
                    job.id,
                    job.nodes,
                    max_nodes_filter,
                )
            elif filter_by_pending_reasons and job.pending_reason not in filter_by_pending_reasons:
                logging.info("Skipping pending job %s due to pending reason: %s", job.id, job.pending_reason)
            else:
                filtered_jobs.append(job)

        return filtered_jobs
    else:
        return pending_jobs


def _recompute_required_nodes_by_slots_reservation(pending_jobs, node_slots):
    """
    Adjust the number of required nodes if necessary based on the required slots.

    According to squeue manual page:
    %D    Number of nodes allocated to the job or the minimum number of nodes required by a pending job.
    The actual number of nodes allocated to a pending job may exceed this number if the job specified a node range count
    (e.g.  minimum and maximum node counts) or the job specifies a processor count instead of a node count and the
    cluster contains nodes with varying processor counts.

    For example, when submitting a job with: sbatch -c 3 -n 5 in a cluster with 4-slot nodes, the output of the squeue
    command is:
        /opt/slurm/bin/squeue -r -o '%i|%t|%D|%C|%c|%r'
        JOBID|ST|NODES|CPUS|MIN_CPUS|REASON
        91|PD|4|15|3|Nodes required for job are DOWN, DRAINED or reserved for jobs in higher priority partitions

    The following function, based on the number of slots required per task, computes the necessary amount of nodes
    to run the job by considering the number of tasks that can fit on a single node.

    :param pending_jobs: array of SlurmJob
    :param node_slots: max slots per compute node
    """
    for job in pending_jobs:
        _recompute_cpus_total_with_cpus_per_gpu(job)
        if node_slots >= job.cpus_min_per_node:
            # check the max number of slots I can fill with tasks of size cpus_min_per_node
            usable_slots_per_node = node_slots - (node_slots % job.cpus_min_per_node)
            # compute number of nodes by considering nodes of size usable_slots_per_node
            required_nodes = int(math.ceil(float(job.cpus_total) / usable_slots_per_node))
            # setting nodes to the max between the computed required nodes and the number of nodes given by
            # the squeue output. This is done to handle job submissions with the -N option where the user can
            # specify a number of nodes that is bigger than the min required.
            # e.g. sbatch -c 1 -N 2 -n 2 => (cpus_min_per_node=1, cpus_total=2, nodes=2)
            job.nodes = max(required_nodes, job.nodes)


def _recompute_required_nodes_by_gpu_reservation(pending_jobs, gpus_per_node):
    """
    Adjust the number of required nodes if necessary based on the required GPUs.

    When using --gpus option or --gpus-per-task without explicitly specifying the number of nodes Slurm will not
    display the correct number of required nodes in the squeue output. The actual number needs to be recomputed
    based on TRES_PER_JOB and TRES_PER_TASK data.

    See examples below (Note output has been compressed):
    ubuntu@ip-10-0-0-64:~$ sbatch --wrap "sleep 100" --gpus=12
    Submitted batch job 79
    ubuntu@ip-10-0-0-64:~$ /opt/slurm/bin/squeue -r -O 'jobid,statecompact,numnodes,numcpus,cpus-per-task,reason,
        tres-per-job,tres-per-task'
    JOBID|ST|NODES|CPUS|CPUS_PER_TASK|REASON|TRES_PER_JOB|TRES_PER_TASK
    79|PD|1|1|1|ReqNodeNotAvail, Maygpu:12|N/A
    ubuntu@ip-10-0-0-64:~$ sbatch --wrap "sleep 100" --gpus-per-task=2 -n 3
    Submitted batch job 92
    ubuntu@ip-10-0-0-64:~$ /opt/slurm/bin/squeue -r -O 'jobid,statecompact,numnodes,numcpus,cpus-per-task,reason,
        tres-per-job,tres-per-task'
    JOBID|ST|NODES|CPUS|CPUS_PER_TASK|REASON|TRES_PER_JOB|TRES_PER_TASK
    92|PD|1|3|1|ReqNodeNotAvail, May|N/A|gpu:2

    :param pending_jobs: array of SlurmJob
    :param gpus_per_node: max gpus per compute node
    """
    if gpus_per_node <= 0:
        # do not process any filtering since we assume scheduler rejects all jobs that require GPUs
        return

    for job in pending_jobs:
        gpus_per_job = job.tres_per_job.get("gpu")
        if gpus_per_job:
            new_min_nodes = int(math.ceil(float(gpus_per_job) / gpus_per_node))
            _update_job_nodes_and_cpus_total(job, new_min_nodes)

        gpus_per_task = job.tres_per_task.get("gpu")
        if gpus_per_task:
            tasks_schedulable_per_node = float(gpus_per_node) // gpus_per_task
            new_min_nodes = int(math.ceil(float(job.tasks) / tasks_schedulable_per_node))
            _update_job_nodes_and_cpus_total(job, new_min_nodes)


def _recompute_cpus_total_with_cpus_per_gpu(job):
    """
    Need to recompute CPUs requested with GPU against total CPUs for a pending job.

    i.e. For this job: sbatch --wrap="sleep 1" -G 2 --cpus-per-gpu=2, we are requesting 4 CPUs total
    but when this job is pending, the cpus_total from scheduler is 1.
    """
    if job.cpus_per_tres and "gpu" in job.cpus_per_tres:
        num_gpus = process_gpus_total_for_job(job)
        cpus_requested_with_gpu = job.cpus_per_tres["gpu"] * num_gpus
        if cpus_requested_with_gpu > job.cpus_total:
            logging.info(
                (
                    "Number of CPUs requested with GPUs({0}) is greater than number of cpus_total({1}) "
                    "retrieved from job, setting cpus_total to {0}"
                ).format(cpus_requested_with_gpu, job.cpus_total)
            )
            job.cpus_total = cpus_requested_with_gpu


def _update_job_nodes_and_cpus_total(job, new_min_nodes):
    """
    Update job.nodes and check/change job.cpus_total.

    We need to do this check for GPU jobs.
    i.e. we have 2 GPUs per node, we submit "sbatch --wrap='sleep 1' -G 4".
    When we read in this job, job.nodes=1, job.cpus_total=1, job.cpus_min_per_node=1
    We update job.nodes=2 in the previous step, but we also need to change job.cpus_total from 1 to 2
    """
    if new_min_nodes > job.nodes:
        job.nodes = new_min_nodes
        if job.cpus_total < job.nodes * job.cpus_min_per_node:
            job.cpus_total = job.nodes * job.cpus_min_per_node


def transform_tres_to_dict(value):
    if value == "N/A":
        return {}

    tres_dict = {}
    for tres in value.split(","):
        resource, value = tres.split(":")
        tres_dict[resource] = int(value)
    return tres_dict


def process_gpus_total_for_job(job):
    """Calculate the total number of GPUs needed by a job."""
    if job.tres_per_node:
        return job.tres_per_node["gpu"] * job.nodes
    if job.tres_per_task:
        return job.tres_per_task["gpu"] * job.tasks
    if job.tres_per_job:
        return job.tres_per_job["gpu"]

    return 0


def job_runnable_on_given_node(job_resources_per_node, resources_available, existing_node=False):
    """Check to see if job can be run on a given node."""
    for resource_type in job_resources_per_node:
        try:
            if resources_available[resource_type] < job_resources_per_node[resource_type]:
                # Don't log an insufficient node resources message if the node exists.
                # Doing so pollutes the logs.
                if not existing_node:
                    logging.warning(
                        (
                            "Resource:{0} required per node ({1}) is greater than resources "
                            "available on single node ({2}), skipping job..."
                        ).format(
                            resource_type, job_resources_per_node[resource_type], resources_available[resource_type]
                        )
                    )
                return False
        except KeyError as e:
            logging.warning(e)
            return False

    return True


class SlurmJob(ComparableObject):
    # This is the format after being processed by reformat_table function
    # JOBID|ST|NODES|CPUS|TASKS|CPUS_PER_TASK|MIN_CPUS|REASON|TRES_PER_JOB|TRES_PER_TASK
    # 72|PD|2|5|5|N/A|1|Resources|N/A|N/A
    # 86|PD|10|40|4|4|4|PartitionConfig|gpu:12|N/A
    # 87|PD|10|10|10|1|1|PartitionNodeLimit|N/A|gpu:4
    MAPPINGS = {
        "JOBID": {"field": "id"},
        "ST": {"field": "state"},
        "NODES": {"field": "nodes", "transformation": int},
        "CPUS": {"field": "cpus_total", "transformation": int},
        "TASKS": {"field": "tasks", "transformation": int},
        # cpus_per_task will be N/A if -c is not specified
        "CPUS_PER_TASK": {
            "field": "cpus_per_task",
            "transformation": lambda value: 1 if value == "N/A" else int(value),
        },
        "MIN_CPUS": {"field": "cpus_min_per_node", "transformation": int},
        "REASON": {"field": "pending_reason"},
        "TRES_PER_JOB": {"field": "tres_per_job", "transformation": transform_tres_to_dict},
        "TRES_PER_TASK": {"field": "tres_per_task", "transformation": transform_tres_to_dict},
        "TRES_PER_NODE": {"field": "tres_per_node", "transformation": transform_tres_to_dict},
        "CPUS_PER_TRES": {"field": "cpus_per_tres", "transformation": transform_tres_to_dict},
    }

    def __init__(
        self,
        id=None,
        state="",
        nodes=0,
        cpus_total=0,
        tasks=0,
        cpus_per_task=0,
        cpus_min_per_node=0,
        pending_reason="",
        tres_per_job=None,
        tres_per_task=None,
        tres_per_node=None,
        cpus_per_tres=None,
    ):
        self.id = id
        self.state = state
        self.nodes = nodes
        self.cpus_total = cpus_total
        self.tasks = tasks
        self.cpus_per_task = cpus_per_task
        self.cpus_min_per_node = cpus_min_per_node
        self.pending_reason = pending_reason
        self.tres_per_job = tres_per_job or {}
        self.tres_per_task = tres_per_task or {}
        self.tres_per_node = tres_per_node or {}
        self.cpus_per_tres = cpus_per_tres or {}

    @staticmethod
    def reformat_table(table):
        """
        Reformat the output of squeue command.

        The -O option used with squeue only supports fixed width formatting and is not as flexible as -o.
        This function removes all empty spaces and compresses the table in a format that is suitable for
        from_table_to_obj_list function
        :param table: the output of squeue -O command
        :return: the compressed table with "|" used as delimiter
        """
        lines = table.splitlines()
        for i in range(0, len(lines)):
            lines[i] = "|".join(wrap(lines[i], SQUEUE_FIELD_SIZE))
        return "\n".join(lines)

    @staticmethod
    def from_table(table):
        return from_table_to_obj_list(SlurmJob.reformat_table(table), SlurmJob)
