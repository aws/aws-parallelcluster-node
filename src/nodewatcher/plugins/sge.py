# Copyright 2013-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import logging
import socket
import subprocess

from common.schedulers.sge_commands import (
    SGE_ERROR_STATES,
    SGE_HOLD_STATE,
    get_compute_nodes_info,
    get_jobs_info,
    get_pending_jobs_info,
)
from common.schedulers.sge_commands import lock_host as sge_lock_host
from common.schedulers.sge_commands import unlock_host
from common.utils import check_command_output

log = logging.getLogger(__name__)


def has_jobs(hostname):
    try:
        # Checking for running or suspended jobs on the node
        # According to the manual (man sge_status) h(old) state only appears in conjunction with r(unning) or p(ending)
        jobs = get_jobs_info(hostname_filter=hostname, job_state_filter="rs")
        logging.info("Found the following running jobs:\n%s", jobs)
        return len(jobs) > 0
    except Exception as e:
        log.error("Failed when checking for running jobs with exception %s", e)
        return False


def has_pending_jobs(instance_properties, max_size):
    """
    Check if there is any pending job in the queue.

    :return: a pair (has_pending_job, has_error) where has_error communicates if there was
             an error when checking for pending jobs.
    """
    try:
        max_cluster_slots = max_size * instance_properties.get("slots")
        pending_jobs = get_pending_jobs_info(
            max_slots_filter=max_cluster_slots, skip_if_state=SGE_HOLD_STATE, log_pending_jobs=False
        )
        return len(pending_jobs) > 0, False
    except Exception as e:
        log.error("Failed when checking for pending jobs with exception %s. Reporting no pending jobs.", e)
        return False, True


def lock_host(hostname, unlock=False):
    try:
        if unlock:
            unlock_host(hostname)
        else:
            sge_lock_host(hostname)
    except subprocess.CalledProcessError:
        log.error("Error %s host %s", "unlocking" if unlock else "locking", hostname)


def is_node_down():
    """
    Check if node is down according to scheduler.

    The node is considered as down if:
    - there is a failure contacting the scheduler
    - node is not reported in the compute nodes list
    - node is in one of the SGE_ERROR_STATES states
    """
    try:
        hostname = check_command_output("hostname").strip()
        host_fqdn = socket.getfqdn(hostname)
        nodes = get_compute_nodes_info(hostname_filter=hostname)
        if not any(host in nodes for host in [hostname, host_fqdn]):
            log.warning("Node is not attached to scheduler. Reporting as down")
            return True

        node = nodes.get(host_fqdn, nodes.get(hostname))
        log.info("Node is in state: '{0}'".format(node.state))
        if all(error_state not in node.state for error_state in SGE_ERROR_STATES):
            return False
    except Exception as e:
        log.error("Failed when checking if node is down with exception %s. Reporting node as down.", e)

    return True
