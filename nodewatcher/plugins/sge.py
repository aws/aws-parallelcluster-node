# Copyright 2013-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import logging
import socket
import subprocess

from common.schedulers.sge_commands import SGE_ERROR_STATES, get_compute_nodes_info, lock_host, unlock_host
from common.sge import check_sge_command_output
from common.utils import check_command_output

log = logging.getLogger(__name__)


def hasJobs(hostname):
    # Checking for running jobs on the node, with parallel job view expanded (-g t)
    command = "qstat -s rs -g t -l hostname={0} -u '*'".format(hostname)

    # Command output
    # job-ID  prior   name       user         state submit/start at     queue                          master ja-task-ID
    # ------------------------------------------------------------------------------------------------------------------
    # 16 0.6 0500 job.sh     ec2-user     r     02/06/2019 11:06:30 all.q@ip-172-31-68-26.ec2.inte SLAVE
    #                                                               all.q@ip-172-31-68-26.ec2.inte SLAVE
    #                                                               all.q@ip-172-31-68-26.ec2.inte SLAVE
    #                                                               all.q@ip-172-31-68-26.ec2.inte SLAVE
    # 17 0.50500 STDIN      ec2-user     r     02/06/2019 11:06:30 all.q@ip-172-31-68-26.ec2.inte MASTER 1
    # 17 0.50500 STDIN      ec2-user     r     02/06/2019 11:06:30 all.q@ip-172-31-68-26.ec2.inte MASTER 2

    try:
        output = check_sge_command_output(command)
        has_jobs = output != ""
    except subprocess.CalledProcessError:
        has_jobs = False

    return has_jobs


def hasPendingJobs():
    command = "qstat -g d -s p -u '*'"

    # Command outputs the pending jobs in the queue in the following format
    # job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID
    # -----------------------------------------------------------------------------------------------------------------
    #      70 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:24                                    1
    #      71 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:24                                    1
    #      72 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:25                                    1
    #      73 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:25                                    1

    try:
        output = check_sge_command_output(command)
        lines = filter(None, output.split("\n"))
        has_pending = True if len(lines) > 1 else False
        error = False
    except Exception as e:
        log.error("Failed when checking if node is down with exception %s. Reporting node as down.", e)
        error = True
        has_pending = False

    return has_pending, error


def lockHost(hostname, unlock=False):
    try:
        if unlock:
            unlock_host(hostname)
        else:
            lock_host(hostname)
    except subprocess.CalledProcessError:
        log.error("Error %s host %s", "unlocking" if unlock else "locking", hostname)


def is_node_down():
    """
    Check if node is down according to scheduler

    The node is considered as down if:
    - there is a failure contacting the scheduler
    - node is not reported in the compute nodes list
    - node is in one of the SGE_ERROR_STATES states
    """
    try:
        hostname = check_command_output("hostname").strip()
        host_fqdn = socket.getfqdn(hostname)
        nodes = get_compute_nodes_info(hostname_filter=hostname)
        if not any(host in nodes for host in ["all.q@" + hostname, "all.q@" + host_fqdn]):
            log.warning("Node is not attached to scheduler. Reporting as down")
            return True

        node = nodes.get("all.q@" + host_fqdn, nodes.get("all.q@" + hostname))
        log.info("Node is in state: '{0}'".format(node.state))
        if all(error_state not in node.state for error_state in SGE_ERROR_STATES):
            return False
    except Exception as e:
        log.error("Failed when checking if node is down with exception %s. Reporting node as down.", e)

    return True
