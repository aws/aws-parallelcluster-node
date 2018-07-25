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

import subprocess
import logging
import shlex
import os

log = logging.getLogger(__name__)


def getJobs(hostname):
    # Slurm won't use FQDN
    short_name = hostname.split('.')[0]
    # Checking for running jobs on the node
    _command = ['/opt/slurm/bin/squeue', '-w', short_name, '-h']
    try:
        output = subprocess.Popen(_command, stdout=subprocess.PIPE).communicate()[0]
    except subprocess.CalledProcessError:
        log.error("Failed to run %s\n" % _command)

    if output == "":
        _jobs = False
    else:
        _jobs = True

    return _jobs

def queueHasPendingJobs():
    command = "/opt/slurm/bin/squeue -t PD"
    _command = shlex.split(command)
    error = False
    has_pending = False

    try:
        process = subprocess.Popen(_command, env=dict(os.environ),
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        log.error("Failed to run %s\n" % command)
        error = True

    output = process.communicate()[0]
    lines = output.split("\n")
    exit_code = process.poll()

    if exit_code != 0:
        log.error("Failed to run %s\n" % command)
        error = True
        return has_pending, error

    if len(lines) > 1:
        has_pending = True

    return has_pending, error

def lockHost(hostname, unlock=False):
    pass
