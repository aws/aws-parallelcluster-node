import logging

from common.slurm import PENDING_RESOURCES_REASONS
from common.utils import check_command_output
from utils import get_optimal_nodes

log = logging.getLogger(__name__)


# get nodes requested from pending jobs
def get_required_nodes(instance_properties):
    log.info("Computing number of required nodes for submitted jobs")
    command = "/opt/slurm/bin/squeue -r -h -o '%i-%t-%D-%C-%r'"
    # Example output of squeue
    # 1-PD-1-24-Nodes required for job are DOWN, DRAINED or reserved for jobs in higher priority partitions
    # 2-PD-1-24-Licenses
    # 3-PD-1-24-PartitionNodeLimit
    # 4-R-1-24-
    output = check_command_output(command)
    slots_requested = []
    nodes_requested = []
    output = output.split("\n")
    for line in output:
        line_arr = line.split("-")
        if len(line_arr) == 5 and line_arr[1] == "PD":
            if line_arr[4] in PENDING_RESOURCES_REASONS:
                slots_requested.append(int(line_arr[3]))
                nodes_requested.append(int(line_arr[2]))
            else:
                log.info("Skipping pending job %s due to pending reason: %s", line_arr[0], line_arr[4])

    return get_optimal_nodes(nodes_requested, slots_requested, instance_properties)


# get nodes reserved by running jobs
def get_busy_nodes(instance_properties):
    command = "/opt/slurm/bin/sinfo -r -h -o '%D %t'"
    # Sample output:
    # 2 mix
    # 4 alloc
    # 10 idle
    output = check_command_output(command)
    nodes = 0
    output = output.split("\n")
    for line in output:
        line_arr = line.split()
        if len(line_arr) == 2 and (line_arr[1] in ["mix", "alloc", "drain", "drain*"]):
            nodes += int(line_arr[0])
    return nodes
