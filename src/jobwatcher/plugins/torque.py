import logging
from functools import reduce

from common.schedulers.torque_commands import get_compute_nodes_info
from common.utils import check_command_output

from .utils import get_optimal_nodes

log = logging.getLogger(__name__)


# get nodes requested from pending jobs
def get_required_nodes(instance_properties, max_size):
    command = "/opt/torque/bin/qstat -at"

    # Example output of torque
    #                                                                                   Req'd       Req'd       Elap
    # Job ID                  Username    Queue    Jobname          SessID  NDS   TSK   Memory      Time    S   Time
    # ----------------------- ----------- -------- ---------------- ------ ----- ------ --------- --------- - ---------
    # 0.ip-172-31-11-1.ec2.i  centos      batch    job.sh             5343     5     30       --   01:00:00 Q  00:04:58
    # 1.ip-172-31-11-1.ec2.i  centos      batch    job.sh             5340     3      6       --   01:00:00 R  00:08:14
    # 2.ip-172-31-11-1.ec2.i  centos      batch    job.sh             5387     2      4       --   01:00:00 R  00:08:27

    status = ["Q"]
    _output = check_command_output(command)
    output = _output.split("\n")[5:]
    slots_requested = []
    nodes_requested = []
    for line in output:
        line_arr = line.split()
        if len(line_arr) >= 10 and line_arr[9] in status:
            # if a job has been looked at to account for pending nodes, don't look at it again
            slots_requested.append(int(line_arr[6]))
            nodes_requested.append(int(line_arr[5]))

    return get_optimal_nodes(nodes_requested, slots_requested, instance_properties)


# get nodes reserved by running jobs
def get_busy_nodes():
    nodes = get_compute_nodes_info()
    busy_nodes = 0
    for node in nodes.values():
        # when a node is added it transitions from down,offline,MOM-list-not-sent -> down -> free
        if node.jobs or (
            any(state in ["offline", "state-unknown"] for state in node.state) and "MOM-list-not-sent" not in node.state
        ):
            busy_nodes += 1

    return busy_nodes
