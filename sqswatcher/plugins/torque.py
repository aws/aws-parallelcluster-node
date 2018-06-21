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
import os
import paramiko
import logging
import shlex
import time
import xml.etree.ElementTree as xmltree
import socket

log = logging.getLogger(__name__)

def __runCommand(command):
    log.debug(repr(command))
    _command = shlex.split(str(command))
    log.debug(_command)

    DEV_NULL = open(os.devnull, "rb")
    try:
        process = sub.Popen(_command, env=dict(os.environ), stdout=sub.PIPE, stderr=sub.STDOUT, stdin=DEV_NULL)
        stdout = process.communicate()[0]
        exitcode = process.poll()
        if exitcode != 0:
            log.error("Failed to run %s:\n%s" % (_command, stdout))
        return stdout
    finally:
        DEV_NULL.close()


def isHostInitState(host_state):
    # Node states http://docs.adaptivecomputing.com/torque/6-0-2/adminGuide/help.htm#topics/torque/8-resources/resources.htm#nodeStates
    init_states = ("down", "offline", "unknown", str(None))
    return str(host_state).startswith(init_states)

def wakeupSchedOn(hostname):
    log.info('Waking up scheduler on host %s', hostname)
    command = ("/opt/torque/bin/pbsnodes -x %s" % (hostname))

    sleep_time = 3
    times = 20
    host_state = None
    while isHostInitState(host_state) and times > 0:
        output = __runCommand(command)
        try:
            # Ex.1: <Data><Node><name>ip-10-0-76-39</name><state>down,offline,MOM-list-not-sent</state><power_state>Running</power_state>
            #        <np>1</np><ntype>cluster</ntype><mom_service_port>15002</mom_service_port><mom_manager_port>15003</mom_manager_port></Node></Data>
            # Ex 2: <Data><Node><name>ip-10-0-76-39</name><state>free</state><power_state>Running</power_state><np>1</np><ntype>cluster</ntype>
            #        <status>rectime=1527799181,macaddr=02:e4:00:b0:b1:72,cpuclock=Fixed,varattr=,jobs=,state=free,netload=210647044,gres=,loadave=0.00,
            #        ncpus=1,physmem=1017208kb,availmem=753728kb,totmem=1017208kb,idletime=856,nusers=1,nsessions=1,sessions=19698,
            #        uname=Linux ip-10-0-76-39 4.9.75-25.55.amzn1.x86_64 #1 SMP Fri Jan 5 23:50:27 UTC 2018 x86_64,opsys=linux</status>
            #        <mom_service_port>15002</mom_service_port><mom_manager_port>15003</mom_manager_port></Node></Data>
            xmlnode = xmltree.XML(output)
            host_state = xmlnode.findtext("./Node/state")
        except:
            log.error("Error parsing XML from %s" % output)

        if isHostInitState(host_state):
            log.debug("Host %s is still in state %s" % (hostname, host_state))
            time.sleep(sleep_time)
            times -= 1

    if host_state == "free":
        command = "/opt/torque/bin/qmgr -c \"set server scheduling=true\""
        __runCommand(command)
    elif times == 0:
        log.error("Host %s is still in state %s" % (hostname, host_state))
    else:
        log.debug("Host %s is in state %s" % (hostname, host_state))

def addHost(hostname,cluster_user,slots):
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

    wakeupSchedOn(hostname)

def removeHost(hostname, cluster_user):
    log.info('Removing %s', hostname)

    command = ('/opt/torque/bin/pbsnodes -o %s' % hostname)
    __runCommand(command)

    command = ("/opt/torque/bin/qmgr -c 'delete node %s'" % hostname)
    __runCommand(command)

