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

__author__ = 'dougalb'

import subprocess
import os
import logging
import shlex

log = logging.getLogger(__name__)


def has_jobs(hostname):
    # Checking for running jobs on the node
    command = ['/opt/sge/bin/idle-nodes']
    try:
       _output = subprocess.Popen(command, stdout=subprocess.PIPE,
                                 env=dict(os.environ, SGE_ROOT='/opt/sge',
                                          PATH='/opt/sge/bin:/opt/sge/bin/lx-amd64:/bin:/usr/bin')).communicate()[0]
    except subprocess.CalledProcessError:
        log.error("Failed to run %s\n" % command)

    _jobs = True
    for host in _output.split('\n'):
        if hostname.split('.')[0] in host:
            _jobs = False
            break

    return _jobs


def get_idle_nodes():
    command = ['/opt/sge/bin/idle-nodes']

    # Command outputs the idle nodes in the following format
    # ip-172-31-56-253
    # ip-172-31-56-25
    # ip-172-31-56-252
    # ip-172-31-56-251

    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                 env=dict(os.environ, SGE_ROOT='/opt/sge',
                                          PATH='/opt/sge/bin:/opt/sge/bin/lx-amd64:/bin:/usr/bin'))
    except subprocess.CalledProcessError:
        log.error("Failed to run %s\n" % command)
        return set()

    output = process.communicate()[0]
    idle_nodes = filter(None, output.split('\n'))
    return idle_nodes


def get_current_cluster_size():
    command = "/opt/sge/bin/lx-amd64/qconf -sel"

    # Command outputs the compute nodes in the following format
    # ip-172-31-56-253.ec2.internal
    # ip-172-31-56-252.ec2.internal
    # ip-172-31-56-251.ec2.internal

    _command = shlex.split(command)
    try:
        process = subprocess.Popen(_command, env=dict(os.environ, SGE_ROOT='/opt/sge',
                                                      PATH='/opt/sge/bin:/opt/sge/bin/lx-amd64:/bin:/usr/bin'),
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        log.error("Failed to run %s\n" % command)
        return -1

    output = process.communicate()[0]
    current_cluster_size = len(filter(None, output.split("\n")))
    log.info("Current cluster size as reported by scheduler: %s" % current_cluster_size)
    return current_cluster_size


def has_pending_jobs():
    command = "/opt/sge/bin/lx-amd64/qstat -g d -s p -u '*'"

    # Command outputs the pending jobs in the queue in the following format
    # job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID
    # -----------------------------------------------------------------------------------------------------------------
    #      70 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:24                                    1
    #      71 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:24                                    1
    #      72 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:25                                    1
    #      73 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:25                                    1

    _command = shlex.split(command)
    error = False
    has_pending = False

    try:
        process = subprocess.Popen(_command, env=dict(os.environ, SGE_ROOT='/opt/sge',
                                                      PATH='/opt/sge/bin:/opt/sge/bin/lx-amd64:/bin:/usr/bin'),
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        log.error("Failed to run %s\n" % command)
        error = True

    output = process.communicate()[0]
    lines = filter(None, output.split("\n"))

    if len(lines) > 1:
        has_pending = True

    return has_pending, error


def lock_host(hostname, unlock=False):
    _mod = unlock and '-e' or '-d'
    command = ['/opt/sge/bin/lx-amd64/qmod', _mod, 'all.q@%s' % hostname]
    try:
        subprocess.check_call(
            command,
            env=dict(os.environ, SGE_ROOT='/opt/sge',
                     PATH='/opt/sge/bin:/opt/sge/bin/lx-amd64:/bin:/usr/bin'))
    except subprocess.CalledProcessError:
        log.error("Failed to run %s\n" % command)

