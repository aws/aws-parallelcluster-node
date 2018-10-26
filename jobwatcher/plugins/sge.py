import logging
import json
from utils import run_command

log = logging.getLogger(__name__)

# get nodes requested from pending jobs
def get_required_nodes(instance_properties):
    command = "/opt/sge/bin/lx-amd64/qstat -g d -s p -u '*'"
    _output = run_command(command, {'SGE_ROOT': '/opt/sge',
                                    'PATH': '/opt/sge/bin:/opt/sge/bin/lx-amd64:/bin:/usr/bin'})
    slots = 0
    output = _output.split("\n")[2:]
    for line in output:
        line_arr = line.split()
        if len(line_arr) >= 8:
            slots += int(line_arr[7])
    vcpus = instance_properties.get('slots')
    return -(-slots // vcpus)

# get nodes reserved by running jobs
# if a host has 1 or more job running on it, it'll be marked busy
def get_busy_nodes(instance_properties):
    command = "/opt/sge/bin/lx-amd64/qstat -f"
    _output = run_command(command, {'SGE_ROOT': '/opt/sge',
                                    'PATH': '/opt/sge/bin:/opt/sge/bin/lx-amd64:/bin:/usr/bin'})
    nodes = 0
    output = _output.split("\n")[2:]
    for line in output:
        line_arr = line.split()
        if len(line_arr) == 5:
            # resv/used/tot.
            (resv, used, total) = line_arr[2].split('/')
            if int(used) > 0 or int(resv) > 0:
                nodes += 1
    return nodes

