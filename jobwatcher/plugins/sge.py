import logging

from common.schedulers.sge_commands import SGE_BUSY_STATES, get_compute_nodes_info
from common.sge import check_sge_command_output

log = logging.getLogger(__name__)


# get nodes requested from pending jobs
def get_required_nodes(instance_properties):
    command = "qstat -g d -s p -u '*'"
    _output = check_sge_command_output(command)
    slots = 0
    output = _output.split("\n")[2:]
    for line in output:
        line_arr = line.split()
        if len(line_arr) >= 8:
            slots += int(line_arr[7])
    vcpus = instance_properties.get("slots")
    return -(-slots // vcpus)


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
