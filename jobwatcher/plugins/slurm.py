import logging
from run_command import run_command

log = logging.getLogger(__name__)

# get nodes requested from pending jobs
def get_required_nodes(instance_properties):
    command = "/opt/slurm/bin/squeue -r -h -o '%t %D'"
    _output = run_command(command, {})
    nodes = 0
    output = _output.split("\n")
    for line in output:
        line_arr = line.split()
        if len(line_arr) == 2 and line_arr[0] == 'PD':
            nodes += int(line_arr[1])
    return nodes

# get nodes reserved by running jobs
def get_busy_nodes(instance_properties):
    command = "/opt/slurm/bin/squeue -r -h -o '%t %D'"
    _output = run_command(command, {})
    nodes = 0
    output = _output.split("\n")
    for line in output:
        line_arr = line.split()
        if len(line_arr) == 2 and line_arr[0] == 'R':
            nodes += int(line_arr[1])
    return nodes
