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
import subprocess

from common.slurm import PENDING_RESOURCES_REASONS
from common.utils import check_command_output, run_command

log = logging.getLogger(__name__)


def hasJobs(hostname):
    # Slurm won't use FQDN
    short_name = hostname.split(".")[0]
    # Checking for running jobs on the node
    command = ["/opt/slurm/bin/squeue", "-w", short_name, "-h"]
    try:
        output = check_command_output(command)
        has_jobs = output != ""
    except subprocess.CalledProcessError:
        has_jobs = False

    return has_jobs


def hasPendingJobs():
    command = "/opt/slurm/bin/squeue -t PD --noheader -o '%c-%r'"

    # Command outputs the pending jobs in the queue in the following format
    #  8-Resources
    #  8-Priority
    #  8-PartitionNodeLimit
    try:
        node_slots = _get_node_slots()
        output = check_command_output(command)
        has_pending = False
        for line in output.split("\n"):
            line_arr = line.split("-")
            if len(line_arr) == 2:
                required_slots = int(line_arr[0])
                pending_code = line_arr[1]
                log.info("required_slots %s pending_code %s", required_slots, pending_code)
                if pending_code in PENDING_RESOURCES_REASONS and required_slots <= node_slots:
                    has_pending = True
                    break

        error = False
    except Exception as e:
        log.warning("Failed when checking for pending jobs with exception %s. Assuming no pending jobs.", e)
        error = True
        has_pending = False

    return has_pending, error


def lockHost(hostname, unlock=False):
    # hostname format: ip-10-0-0-114.eu-west-1.compute.internal
    hostname = hostname.split(".")[0]
    if unlock:
        log.info("Unlocking host %s", hostname)
        command = [
            "/opt/slurm/bin/scontrol",
            "update",
            "NodeName={0}".format(hostname),
            "State=RESUME",
            'Reason="Unlocking"',
        ]
    else:
        log.info("Locking host %s", hostname)
        command = [
            "/opt/slurm/bin/scontrol",
            "update",
            "NodeName={0}".format(hostname),
            "State=DRAIN",
            'Reason="Shutting down"',
        ]
    try:
        run_command(command)
    except subprocess.CalledProcessError:
        log.error("Error %s host %s", "unlocking" if unlock else "locking", hostname)


def _get_node_slots():
    hostname = check_command_output("hostname")
    # retrieves number of slots for a specific node in the cluster.
    # Output format:
    # 4
    command = "/opt/slurm/bin/sinfo -o '%c' -n {0} -h".format(hostname)
    output = check_command_output(command)
    return int(output)
