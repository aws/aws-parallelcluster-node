import logging

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


# get nodes reserved by running jobs
# if a host has 1 or more job running on it, it'll be marked busy
def get_busy_nodes(instance_properties):
    command = "qstat -f"
    _output = check_sge_command_output(command)
    nodes = 0
    output = _output.split("\n")[2:]
    for line in output:
        line_arr = line.split()
        if len(line_arr) == 5:
            # resv/used/tot.
            (resv, used, total) = line_arr[2].split("/")
            if int(used) > 0 or int(resv) > 0:
                nodes += 1
    return nodes
