import logging
import xml.etree.ElementTree as ET
from utils import run_command, get_optimal_nodes

log = logging.getLogger(__name__)

# get nodes requested from pending jobs
def get_required_nodes(instance_properties):
    command = "/opt/torque/bin/qstat -at"

    # Example output of torque
    #                                                                                   Req'd       Req'd       Elap
    # Job ID                  Username    Queue    Jobname          SessID  NDS   TSK   Memory      Time    S   Time
    # ----------------------- ----------- -------- ---------------- ------ ----- ------ --------- --------- - ---------
    # 0.ip-172-31-11-1.ec2.i  centos      batch    job.sh             5343     5     30       --   01:00:00 Q  00:04:58
    # 1.ip-172-31-11-1.ec2.i  centos      batch    job.sh             5340     3      6       --   01:00:00 R  00:08:14
    # 2.ip-172-31-11-1.ec2.i  centos      batch    job.sh             5387     2      4       --   01:00:00 R  00:08:27

    status = ['Q']
    _output = run_command(command, {})
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
def get_busy_nodes(instance_properties):
    command = "/opt/torque/bin/pbsnodes -x"
    # The output of the command
    #<?xml version="1.0" encoding="UTF-8"?>
    # <Data>
    #    <Node>
    #       <name>ip-172-31-11-1</name>
    #       <state>down</state>
    #       <power_state>Running</power_state>
    #       <np>1000</np>
    #       <ntype>cluster</ntype>
    #       <jobs>"job-id"</jobs>
    #       <note>MasterServer</note>
    #       <mom_service_port>15002</mom_service_port>
    #       <mom_manager_port>15003</mom_manager_port>
    #    </Node>
    # </Data>
    _output = run_command(command, {})
    root = ET.fromstring(_output)
    count = 0
    # See how many nodes have jobs
    for node in root.findall('Node'):
        if len(node.findall('jobs')) != 0:
            count += 1
    return count
