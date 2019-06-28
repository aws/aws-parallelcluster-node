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

import logging
import os
import socket
import time
from xml.etree import ElementTree

import paramiko
from common.utils import check_command_output, run_command

log = logging.getLogger(__name__)


def isHostInitState(host_state):
    # Node states http://docs.adaptivecomputing.com/torque/6-0-2/adminGuide/help.htm#topics/torque/8-resources/resources.htm#nodeStates
    init_states = ("down", "offline", "unknown", str(None))
    return str(host_state).startswith(init_states)


def wakeupSchedOn(hostname):
    log.info("Waking up scheduler on host %s", hostname)
    command = "/opt/torque/bin/pbsnodes -x %s" % (hostname)

    sleep_time = 3
    times = 20
    host_state = None
    while isHostInitState(host_state) and times > 0:
        try:
            output = check_command_output(command)
            # Ex.1: <Data><Node><name>ip-10-0-76-39</name><state>down,offline,MOM-list-not-sent</state><power_state>Running</power_state>
            #        <np>1</np><ntype>cluster</ntype><mom_service_port>15002</mom_service_port><mom_manager_port>15003</mom_manager_port></Node></Data>
            # Ex 2: <Data><Node><name>ip-10-0-76-39</name><state>free</state><power_state>Running</power_state><np>1</np><ntype>cluster</ntype>
            #        <status>rectime=1527799181,macaddr=02:e4:00:b0:b1:72,cpuclock=Fixed,varattr=,jobs=,state=free,netload=210647044,gres=,loadave=0.00,
            #        ncpus=1,physmem=1017208kb,availmem=753728kb,totmem=1017208kb,idletime=856,nusers=1,nsessions=1,sessions=19698,
            #        uname=Linux ip-10-0-76-39 4.9.75-25.55.amzn1.x86_64 #1 SMP Fri Jan 5 23:50:27 UTC 2018 x86_64,opsys=linux</status>
            #        <mom_service_port>15002</mom_service_port><mom_manager_port>15003</mom_manager_port></Node></Data>
            xmlnode = ElementTree.XML(output)
            host_state = xmlnode.findtext("./Node/state")
        except:
            log.error("Error parsing XML from %s" % output)

        if isHostInitState(host_state):
            log.debug("Host %s is still in state %s" % (hostname, host_state))
            time.sleep(sleep_time)
            times -= 1

    if host_state == "free":
        command = '/opt/torque/bin/qmgr -c "set server scheduling=true"'
        run_command(command, raise_on_error=False)
    elif times == 0:
        log.error("Host %s is still in state %s" % (hostname, host_state))
    else:
        log.debug("Host %s is in state %s" % (hostname, host_state))


def addHost(hostname, cluster_user, slots, max_cluster_size):
    log.info("Adding %s with %s slots" % (hostname, slots))

    command = "/opt/torque/bin/qmgr -c 'create node %s np=%s'" % (hostname, slots)
    run_command(command, raise_on_error=False)

    command = "/opt/torque/bin/pbsnodes -c %s" % hostname
    run_command(command, raise_on_error=False)

    # Connect and hostkey
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    hosts_key_file = os.path.expanduser("~" + cluster_user) + "/.ssh/known_hosts"
    user_key_file = os.path.expanduser("~" + cluster_user) + "/.ssh/id_rsa"
    iter = 0
    connected = False
    while iter < 3 and connected == False:
        try:
            log.info("Connecting to host: %s iter: %d" % (hostname, iter))
            ssh.connect(hostname, username=cluster_user, key_filename=user_key_file)
            connected = True
        except socket.error as e:
            log.info("Socket error: %s" % e)
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


def removeHost(hostname, cluster_user, max_cluster_size):
    log.info("Removing %s", hostname)

    command = "/opt/torque/bin/pbsnodes -o %s" % hostname
    run_command(command, raise_on_error=False)

    command = "/opt/torque/bin/qmgr -c 'delete node %s'" % hostname
    run_command(command, raise_on_error=False)


def update_cluster(max_cluster_size, cluster_user, update_events, instance_properties):
    failed = []
    succeeded = []
    for event in update_events:
        try:
            if event.action == "REMOVE":
                removeHost(event.host.hostname, cluster_user, max_cluster_size)
            elif event.action == "ADD":
                addHost(event.host.hostname, cluster_user, event.host.slots, max_cluster_size)
            succeeded.append(event)
        except Exception as e:
            log.error(
                "Encountered error when processing %s event for host %s: %s", event.action, event.host.hostname, e
            )
            failed.append(event)

    return failed, succeeded
