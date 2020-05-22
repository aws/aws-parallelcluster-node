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

from common.schedulers.slurm_commands import (
    PENDING_RESOURCES_REASONS,
    SLURM_NODE_ERROR_STATES,
    get_node_state,
    get_pending_jobs_info,
    lock_node,
    unlock_node,
)
from common.utils import TREAT_DISABLED_AS_DOWN_WARNING, check_command_output

log = logging.getLogger(__name__)


def has_jobs(hostname):
    # Slurm won't use FQDN
    short_name = hostname.split(".")[0]
    # Checking for running jobs on the node
    command = ["/opt/slurm/bin/squeue", "-w", short_name, "-h"]
    try:
        output = check_command_output(command)
        logging.info("Found the following running jobs:\n%s", output.rstrip())
        has_jobs = output != ""
    except subprocess.CalledProcessError:
        has_jobs = False

    return has_jobs


def has_pending_jobs(instance_properties, max_size):
    """
    Check if there is any pending job in the queue.

    :return: a pair (has_pending_job, has_error) where has_error communicates if there was
             an error when checking for pending jobs.
    """
    try:
        pending_jobs = get_pending_jobs_info(
            instance_properties=instance_properties,
            max_nodes_filter=max_size,
            filter_by_pending_reasons=PENDING_RESOURCES_REASONS,
            log_pending_jobs=False,
        )
        return len(pending_jobs) > 0, False
    except Exception as e:
        log.error("Failed when checking if node is down with exception %s. Reporting no pending jobs.", e)
        return False, True


def lock_host(hostname, unlock=False):
    if unlock:
        unlock_node(hostname)
    else:
        lock_node(hostname)


def is_node_down():
    """Check if node is down according to scheduler."""
    try:
        hostname = check_command_output("hostname").strip()
        output = get_node_state(hostname)
        log.info("Node is in state: '{0}'".format(output))
        if output and all(state not in output for state in SLURM_NODE_ERROR_STATES):
            return False
        if output and "drained" in output:
            log.warning(TREAT_DISABLED_AS_DOWN_WARNING)
    except Exception as e:
        log.error("Failed when checking if node is down with exception %s. Reporting node as down.", e)

    return True


def _get_node_slots():
    hostname = check_command_output("hostname")
    # retrieves number of slots for a specific node in the cluster.
    # Output format:
    # 4
    command = "/opt/slurm/bin/sinfo -o '%c' -n {0} -h".format(hostname)
    output = check_command_output(command)
    return int(output)
