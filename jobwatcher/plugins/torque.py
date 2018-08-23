import logging
from run_command import run_command

log = logging.getLogger(__name__)

# get nodes requested from pending jobs
def get_required_nodes(instance_properties):
    command = "/opt/torque/bin/qstat -a"
    status = ['Q']
    _output = run_command(command, {})
    output = _output.split("\n")[5:]
    nodes = 0
    for line in output:
        line_arr = line.split()
        if len(line_arr) >= 10 and line_arr[9] in status:
            nodes += int(line_arr[5])
    return nodes

# get nodes reserved by running jobs
def get_busy_nodes(instance_properties):
    command = "/opt/torque/bin/qstat -a"
    status = ['R']
    _output = run_command(command, {})
    output = _output.split("\n")[5:]
    nodes = 0
    for line in output:
        line_arr = line.split()
        if len(line_arr) >= 10 and line_arr[9] in status:
            nodes += int(line_arr[5])
    return nodes
