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

from future.moves.subprocess import check_output

import logging
import os
import shlex
import subprocess

from common.slurm import PENDING_RESOURCES_REASONS

log = logging.getLogger(__name__)


def _run_command(command):
    try:
        if isinstance(command, str):
            command = shlex.split(command)
        return check_output(command, env=dict(os.environ), universal_newlines=True)
    except subprocess.CalledProcessError as e:
        # CalledProcessError.__str__ already produces a significant error message
        log.error(e)
        raise


def hasJobs(hostname):
    # Slurm won't use FQDN
    short_name = hostname.split('.')[0]
    # Checking for running jobs on the node
    command = ['/opt/slurm/bin/squeue', '-w', short_name, '-h']
    try:
        output = _run_command(command)
        has_jobs = output != ""
    except subprocess.CalledProcessError:
        has_jobs = False

    return has_jobs


def hasPendingJobs():
    command = "/opt/slurm/bin/squeue -t PD --noheader -o '%r'"

    # Command outputs the pending jobs in the queue in the following format
    #  Resources
    #  Priority
    #  PartitionNodeLimit
    try:
        output = _run_command(command)
        has_pending = len(filter(lambda reason: reason in PENDING_RESOURCES_REASONS, output.split("\n"))) > 0
        error = False
    except subprocess.CalledProcessError:
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
        subprocess.check_call(command, env=dict(os.environ))
    except subprocess.CalledProcessError as e:
        # CalledProcessError.__str__ already produces a significant error message
        log.error(e)
