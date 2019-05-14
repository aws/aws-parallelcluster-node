import logging

from common.schedulers.sge_commands import SGE_BUSY_STATES, SGE_HOLD_STATE, get_compute_nodes_info, get_jobs_info

log = logging.getLogger(__name__)


def _get_required_slots(instance_properties, max_size):
    """Compute the total number of slots required by pending jobs."""
    pending_jobs = get_jobs_info(job_state_filter="p")
    max_cluster_slots = max_size * instance_properties.get("slots")
    slots = 0
    for job in pending_jobs:
        if job.slots > max_cluster_slots:
            log.info(
                "Skipping job %s since required slots (%d) exceed max cluster size (%d)",
                job.number,
                job.slots,
                max_cluster_slots,
            )
        elif SGE_HOLD_STATE in job.state:
            log.info("Skipping job %s since in hold state (%s)", job.number, job.state)
        else:
            slots += job.slots

    return slots


# get nodes requested from pending jobs
def get_required_nodes(instance_properties, max_size):
    required_slots = _get_required_slots(instance_properties, max_size)
    vcpus = instance_properties.get("slots")
    return -(-required_slots // vcpus)


def get_busy_nodes():
    """
    Count nodes that have at least 1 job running or have a state that makes them unusable for jobs submission.
    """
    nodes = get_compute_nodes_info()
    busy_nodes = 0
    for node in nodes.values():
        if (
            any(busy_state in node.state for busy_state in SGE_BUSY_STATES)
            or int(node.slots_used) > 0
            or int(node.slots_reserved) > 0
        ):
            busy_nodes += 1

    return busy_nodes
