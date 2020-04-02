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
import subprocess

from common.schedulers.torque_commands import (
    TORQUE_BIN_DIR,
    TORQUE_NODE_ERROR_STATES,
    TORQUE_RUNNING_JOB_STATE,
    TORQUE_SUSPENDED_JOB_STATE,
    get_compute_nodes_info,
    get_jobs_info,
    get_pending_jobs_info,
)
from common.utils import check_command_output, run_command

log = logging.getLogger(__name__)


def has_jobs(hostname):
    try:
        short_name = hostname.split(".")[0]
        # Checking for running jobs on the node
        jobs = get_jobs_info(
            filter_by_exec_hosts={short_name}, filter_by_states=[TORQUE_RUNNING_JOB_STATE, TORQUE_SUSPENDED_JOB_STATE]
        )
        logging.info("Found the following running jobs:\n%s", jobs)
        return len(jobs) > 0
    except Exception as e:
        log.error("Failed when checking for running jobs with exception %s. Reporting no running jobs.", e)
        return False


def has_pending_jobs(instance_properties, max_size):
    """
    Check if there is any pending job in the queue.

    :return: a pair (has_pending_job, has_error) where has_error communicates if there was
             an error when checking for pending jobs.
    """
    try:
        pending_jobs = get_pending_jobs_info(max_slots_filter=instance_properties.get("slots"), log_pending_jobs=False)
        return len(pending_jobs) > 0, False
    except Exception as e:
        log.error("Failed when checking for pending jobs with exception %s. Reporting no pending jobs.", e)
        return False, True


def lock_host(hostname, unlock=False):
    # hostname format: ip-10-0-0-114.eu-west-1.compute.internal
    hostname = hostname.split(".")[0]
    mod = unlock and "-c" or "-o"
    command = [TORQUE_BIN_DIR + "pbsnodes", mod, hostname]
    try:
        run_command(command)
    except subprocess.CalledProcessError:
        log.error("Error %s host %s", "unlocking" if unlock else "locking", hostname)


def is_node_down():
    """Check if node is down according to scheduler."""
    try:
        hostname = check_command_output("hostname").strip()
        node = get_compute_nodes_info(hostname_filter=[hostname]).get(hostname)
        if node:
            log.info("Node is in state: '{0}'".format(node.state))
            if all(error_state not in node.state for error_state in TORQUE_NODE_ERROR_STATES):
                return False
        else:
            log.warning("Node is not attached to scheduler. Reporting as down")
    except Exception as e:
        log.error("Failed when checking if node is down with exception %s. Reporting node as down.", e)

    return True
