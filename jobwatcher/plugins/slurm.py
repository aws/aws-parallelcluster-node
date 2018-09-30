import logging
from utils import run_command, get_optimal_nodes


log = logging.getLogger(__name__)


# get nodes requested from pending jobs
def get_required_nodes(instance_properties):
    command = "/opt/slurm/bin/squeue -r -h -o '%i %t %D %C'"
    # Example output of squeue
    # 25 PD 1 24
    # 26 R 1 24
    _output = run_command(command, {})
    slots_requested = []
    nodes_requested = []
    output = _output.split("\n")
    for line in output:
        line_arr = line.split()
        if len(line_arr) == 4 and line_arr[1] == 'PD':
            slots_requested.append(int(line_arr[3]))
            nodes_requested.append(int(line_arr[2]))

    return get_optimal_nodes(nodes_requested, slots_requested, instance_properties)


# get nodes reserved by running jobs
def get_busy_nodes(instance_properties):
    command = "/opt/slurm/bin/sinfo -r -h -o '%D %t'"
    # Sample output:
    # 2 mix
    # 4 alloc
    # 10 idle
    _output = run_command(command, {})
    nodes = 0
    output = _output.split("\n")
    for line in output:
        line_arr = line.split()
        if len(line_arr) == 2 and (line_arr[1] == 'mix' or line_arr[1] == 'alloc'):
            nodes += int(line_arr[0])
    return nodes
