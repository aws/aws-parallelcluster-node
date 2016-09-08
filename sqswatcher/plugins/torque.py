# Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/asl/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

__author__ = 'dougalb'

import subprocess as sub
import os
import paramiko
import logging
import shlex

log = logging.getLogger(__name__)

def __runCommand(command):
    log.debug(repr(command))
    _command = shlex.split(str(command))
    log.debug(_command)
    try:
        sub.check_call(_command, env=dict(os.environ))
    except sub.CalledProcessError:
        log.error("Failed to run %s\n" % _command)

def addHost(hostname,cluster_user,slots, queue=None):
    log.info('Adding %s', hostname)

    command = ("/opt/torque/bin/qmgr -c 'create node %s np=%s'" % (hostname, slots))
    __runCommand(command)

    command = ('/opt/torque/bin/pbsnodes -c %s' % hostname)
    __runCommand(command)

    # Connect and hostkey
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
            log.info('Socket error: %s' % e)
            time.sleep(10 + iter)
            iter = iter + 1
            if iter == 3:
               log.info("Unable to provison host")
               return
    try:
        ssh.load_host_keys(hosts_key_file)
    except IOError:
        ssh._host_keys_filename = None
        pass
    ssh.save_host_keys(hosts_key_file)
    ssh.close()

    command = ('/etc/init.d/pbs_server restart')
    __runCommand(command)

def removeHost(hostname, cluster_user, queue=None):
    log.info('Removing %s', hostname)

    command = ('/opt/torque/bin/pbsnodes -o %s' % hostname)
    __runCommand(command)

    command = ("/opt/torque/bin/qmgr -c 'delete node %s'" % hostname)
    __runCommand(command)

    command = ('/etc/init.d/pbs_server restart')
    __runCommand(command)

