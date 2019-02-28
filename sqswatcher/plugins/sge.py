# Copyright 2013-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import subprocess as sub
import paramiko
from tempfile import NamedTemporaryFile
import time
import os
import socket
import logging
import shlex
from sqswatcher.sqswatcher import HostRemovalError
from sqswatcher.sqswatcher import QueryConfigError

log = logging.getLogger(__name__)


def _is_host_configured(command, hostname):
    _command = shlex.split(command)
    log.debug(_command)
    host_configured = False

    try:
        output = sub.Popen(
            _command,
            stdout=sub.PIPE,
            env=dict(
                os.environ,
                SGE_ROOT='/opt/sge',
                PATH='/opt/sge/bin:/opt/sge/bin/lx-amd64:/bin:/usr/bin',
            ),
        ).communicate()[0]
    except:
        log.error("Failed to run %s\n" % command)
        raise QueryConfigError

    if output is not None:
        # Expected output
        # ip-172-31-66-16.ec2.internal
        # ip-172-31-74-69.ec2.internal
        match = list(filter(lambda x: hostname in x.split(".")[0], output.split("\n")))

        if len(match) > 0:
            host_configured = True

    return host_configured


def _run_sge_command(command, raise_exception=False):
    _command = shlex.split(str(command))
    log.debug(_command)

    try:
        sub.check_call(_command, env=dict(os.environ, SGE_ROOT='/opt/sge'))
    except sub.CalledProcessError:
        log.error("Failed to run %s\n" % _command)
        if raise_exception:
            raise HostRemovalError


def addHost(hostname, cluster_user, slots):
    log.info('Adding %s with %s slots' % (hostname,slots))

    # Adding host as administrative host
    command = ('/opt/sge/bin/lx-amd64/qconf -ah %s' % hostname)
    _run_sge_command(command)

    # Adding host as submit host
    command = ('/opt/sge/bin/lx-amd64/qconf -as %s' % hostname)
    _run_sge_command(command)

    # Setup template to add execution host
    qconf_Ae_template = """hostname              %s
load_scaling          NONE
complex_values        NONE
user_lists            NONE
xuser_lists           NONE
projects              NONE
xprojects             NONE
usage_scaling         NONE
report_variables      NONE
"""

    with NamedTemporaryFile() as t:
        temp_template = open(t.name,'w')
        temp_template.write(qconf_Ae_template % hostname)
        temp_template.flush()
        os.fsync(t.fileno())

        # Add host as an execution host
        command = ('/opt/sge/bin/lx-amd64/qconf -Ae %s' % t.name)
        _run_sge_command(command)

    # Connect and start SGE
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    hosts_key_file = os.path.expanduser("~" + cluster_user) + '/.ssh/known_hosts'
    user_key_file = os.path.expanduser("~" + cluster_user) + '/.ssh/id_rsa'
    iter=0
    connected=False
    while iter < 3 and connected == False:
        try:
            log.info('Connecting to host: %s iter: %d' % (hostname, iter))
            ssh.connect(hostname, username=cluster_user, key_filename=user_key_file)
            connected=True
        except socket.error, e:
            log.error('Socket error: %s' % e)
            time.sleep(10 + iter)
            iter = iter + 1
            if iter == 3:
               log.critical("Unable to provison host")
               return
    try:
        ssh.load_host_keys(hosts_key_file)
    except IOError:
        ssh._host_keys_filename = None
        pass
    ssh.save_host_keys(hosts_key_file)
    command = "sudo sh -c \'cd /opt/sge && /opt/sge/inst_sge -noremote -x -auto /opt/parallelcluster/templates/sge/sge_inst.conf\'"
    stdin, stdout, stderr = ssh.exec_command(command)
    while not stdout.channel.exit_status_ready():
        time.sleep(1)
    ssh.close()

    # Add the host to the all.q
    command = ('/opt/sge/bin/lx-amd64/qconf -aattr hostgroup hostlist %s @allhosts' % hostname)
    _run_sge_command(command)

    # Set the numbers of slots for the host
    command = ('/opt/sge/bin/lx-amd64/qconf -aattr queue slots ["%s=%s"] all.q' % (hostname,slots))
    _run_sge_command(command)


def removeHost(hostname, cluster_user):
    log.info('Removing %s', hostname)

    # Check if host is administrative host
    command = "/opt/sge/bin/lx-amd64/qconf -sh"
    if _is_host_configured(command, hostname):
        # Removing host as administrative host
        command = ("/opt/sge/bin/lx-amd64/qconf -dh %s" % hostname)
        _run_sge_command(command, raise_exception=True)
    else:
        log.info('Host %s is not administrative host', hostname)

    # Check if host is in all.q (qconf -sq all.q)
    # Purge hostname from all.q
    command = ("/opt/sge/bin/lx-amd64/qconf -purge queue '*' all.q@%s" % hostname)
    _run_sge_command(command)

    # Check if host is in @allhosts group (qconf -shgrp_resolved @allhosts)
    # Remove host from @allhosts group
    command = ("/opt/sge/bin/lx-amd64/qconf -dattr hostgroup hostlist %s @allhosts" % hostname)
    _run_sge_command(command)

    # Check if host is execution host
    command = "/opt/sge/bin/lx-amd64/qconf -sel"
    if _is_host_configured(command, hostname):
        # Removing host as execution host
        command = ("/opt/sge/bin/lx-amd64/qconf -de %s" % hostname)
        _run_sge_command(command, raise_exception=True)
    else:
        log.info('Host %s is not execution host', hostname)

    # Check if host is submission host
    command = "/opt/sge/bin/lx-amd64/qconf -ss"
    if _is_host_configured(command, hostname):
        # Removing host as submission host
        command = ("/opt/sge/bin/lx-amd64/qconf -ds %s" % hostname)
        _run_sge_command(command, raise_exception=True)
    else:
        log.info('Host %s is not submission host', hostname)
