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

def runPipe(cmds):
    try:
        p1 = subprocess.Popen(cmds[0].split(' '), stdin = None, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        prev = p1
        for cmd in cmds[1:]:
            p = subprocess.Popen(cmd.split(' '), stdin = prev.stdout, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            prev = p
        stdout, stderr = p.communicate()
        p.wait()
        returncode = p.returncode
    except Exception, e:
        stderr = str(e)
        returncode = -1
    if returncode == 0:
        return (True, stdout.strip().split('\n'))
    else:
        return (False, stderr)

def hasJobs(hostname):
    # Checking for running jobs on the node
    commands = ['/opt/torque/bin/qstat -r -t -n -1', ('grep ' + hostname.split('.')[0])]
    try:
        status, output = runPipe(commands)
    except subprocess.CalledProcessError:
        log.error("Failed to run %s\n" % commands)

    if output == "":
        _jobs = False
    else:
        _jobs = True

    return _jobs

def hasPendingJobs():
    command = "/opt/torque/bin/qstat -Q"

    # Command outputs the status of the queue in the following format
    # Queue              Max    Tot   Ena   Str   Que   Run   Hld   Wat   Trn   Ext T   Cpt
    # ----------------   ---   ----    --    --   ---   ---   ---   ---   ---   --- -   ---
    # batch                0     24   yes   yes    24     0     0     0     0     0 E     0
    # test1                0     26   yes   yes    26     0     0     0     0     0 E     0

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

    lines = filter(None, output.split("\n"))
    if len(lines) < 3:
        error = True
        return has_pending, error
    pending = 0
    for idx, line in enumerate(lines):
        if idx < 2:
            continue
        queue_status = line.split()
        pending += int(queue_status[5])

    has_pending = pending > 0

    return has_pending, error

def lockHost(hostname, unlock=False):
    # https://lists.sdsc.edu/pipermail/npaci-rocks-discussion/2007-November/027919.html
    _mod = unlock and '-c' or '-o'
    command = ['/opt/torque/bin/pbsnodes', _mod, hostname]
    try:
        subprocess.check_call(command)
    except subprocess.CalledProcessError:
        log.error("Failed to run %s\n" % command)

