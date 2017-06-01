# Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import subprocess as sub
import paramiko
from tempfile import NamedTemporaryFile
import time
import os
import socket
import logging
import shlex

log = logging.getLogger(__name__)

def __runSgeCommand(command):
    log.debug(repr(command))
    _command = shlex.split(str(command))
    log.debug(_command)
    try:
        sub.check_call(_command, env=dict(os.environ, SGE_ROOT='/opt/sge'))
    except sub.CalledProcessError:
        log.error("Failed to run %s\n" % _command)

def addHost(hostname, cluster_user, slots):
    log.info('Adding %s with %s slots' % (hostname,slots))

    # Adding host as administrative host
    command = ('/opt/sge/bin/lx-amd64/qconf -ah %s' % hostname)
    __runSgeCommand(command)

    # Adding host as submit host
    command = ('/opt/sge/bin/lx-amd64/qconf -as %s' % hostname)
    __runSgeCommand(command)

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
        __runSgeCommand(command)

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
    command = "sudo sh -c \'cd /opt/sge && /opt/sge/inst_sge -noremote -x -auto /opt/cfncluster/templates/sge/sge_inst.conf\'"
    stdin, stdout, stderr = ssh.exec_command(command)
    while not stdout.channel.exit_status_ready():
        time.sleep(1)
    ssh.close()

    # Add the host to the qll.q
    command = ('/opt/sge/bin/lx-amd64/qconf -aattr hostgroup hostlist %s @allhosts' % hostname)
    __runSgeCommand(command)

    # Set the numbers of slots for the host
    command = ('/opt/sge/bin/lx-amd64/qconf -aattr queue slots ["%s=%s"] all.q' % (hostname,slots))
    __runSgeCommand(command)

def removeHost(hostname,cluster_user):
    log.info('Removing %s', hostname)

    # Removing host as administrative host
    command = ("/opt/sge/bin/lx-amd64/qconf -dh %s" % hostname)
    __runSgeCommand(command)

    # Purge hostname from all.q
    command = ("/opt/sge/bin/lx-amd64/qconf -purge queue '*' all.q@%s" % hostname)
    __runSgeCommand(command)

    # Remove host from @allhosts group
    command = ("/opt/sge/bin/lx-amd64/qconf -dattr hostgroup hostlist %s @allhosts" % hostname)
    __runSgeCommand(command)

    # Removing host as execution host
    command = ("/opt/sge/bin/lx-amd64/qconf -de %s" % hostname)
    __runSgeCommand(command)

    # Removing host as submit host
    command = ("/opt/sge/bin/lx-amd64/qconf -ds %s" % hostname)
    __runSgeCommand(command)
