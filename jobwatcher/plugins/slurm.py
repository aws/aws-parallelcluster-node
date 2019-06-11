import logging

from common.schedulers.slurm_commands import PENDING_RESOURCES_REASONS, get_pending_jobs_info
from common.utils import check_command_output
from jobwatcher.plugins.utils import get_optimal_nodes

log = logging.getLogger(__name__)


# get nodes requested from pending jobs
def get_required_nodes(instance_properties, max_size):
    log.info("Computing number of required nodes for submitted jobs")
    pending_jobs = get_pending_jobs_info(
        max_slots_filter=instance_properties.get("slots"),
        max_nodes_filter=max_size,
        filter_by_pending_reasons=PENDING_RESOURCES_REASONS,
    )
    slots_requested = []
    nodes_requested = []
    for job in pending_jobs:
        slots_requested.append(job.cpus_total)
        nodes_requested.append(job.nodes)

    return get_optimal_nodes(nodes_requested, slots_requested, instance_properties)


# get nodes reserved by running jobs
def get_busy_nodes():
    command = "/opt/slurm/bin/sinfo -h -o '%D %t'"
    # Sample output:
    # 2 mix
    # 4 alloc
    # 10 idle
    # 1 down*
    output = check_command_output(command)
    nodes = 0
    output = output.split("\n")
    for line in output:
        line_arr = line.split()
        if len(line_arr) == 2 and (line_arr[1] in ["mix", "alloc", "drain", "drain*", "down", "down*"]):
            nodes += int(line_arr[0])
    return nodes
