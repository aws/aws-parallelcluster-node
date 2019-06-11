import logging

from common.schedulers.sge_commands import (
    SGE_BUSY_STATES,
    SGE_HOLD_STATE,
    SGE_ORPHANED_STATE,
    get_compute_nodes_info,
    get_pending_jobs_info,
)

log = logging.getLogger(__name__)


def _get_required_slots(instance_properties, max_size):
    """Compute the total number of slots required by pending jobs."""
    max_cluster_slots = max_size * instance_properties.get("slots")
    pending_jobs = get_pending_jobs_info(max_slots_filter=max_cluster_slots, skip_if_state=SGE_HOLD_STATE)
    slots = 0
    for job in pending_jobs:
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
            if SGE_ORPHANED_STATE in node.state:
                logging.info(
                    "Skipping host %s since in orphaned state, hence not in ASG. "
                    "Host will disappear when assigned jobs are deleted.",
                    node.name,
                )
            else:
                busy_nodes += 1

    return busy_nodes
