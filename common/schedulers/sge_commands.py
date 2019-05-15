# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
import collections
import logging
import re
from xml.etree import ElementTree

from common import sge
from common.remote_command_executor import RemoteCommandExecutor
from common.sge import check_sge_command_output, run_sge_command

QConfCommand = collections.namedtuple("QConfCommand", ["command_flags", "successful_messages", "description"])

QCONF_COMMANDS = {
    "ADD_ADMINISTRATIVE_HOST": QConfCommand(
        command_flags="-ah",
        successful_messages=[r".* added to administrative host list", r'adminhost ".*" already exists'],
        description="add administrative hosts",
    ),
    "ADD_SUBMIT_HOST": QConfCommand(
        command_flags="-as",
        successful_messages=[r".* added to submit host list", r'submithost ".*" already exists'],
        description="add submit hosts",
    ),
    "REMOVE_ADMINISTRATIVE_HOST": QConfCommand(
        command_flags="-dh",
        successful_messages=[
            r".* removed .* from administrative host list",
            r'denied: administrative host ".*" does not exist',
        ],
        description="remove administrative hosts",
    ),
    "REMOVE_SUBMIT_HOST": QConfCommand(
        command_flags="-ds",
        successful_messages=[r".* removed .* from submit host list", r'denied: submit host ".*" does not exist'],
        description="remove submission hosts",
    ),
    "REMOVE_EXECUTION_HOST": QConfCommand(
        command_flags="-de",
        successful_messages=[r".* removed .* from execution host list", r'denied: execution host ".*" does not exist'],
        description="remove execution hosts",
    ),
}

# The state of the queue - one of u(nknown), a(larm), A(larm), C(alendar suspended), s(uspended),
# S(ubordinate), d(isabled), D(isabled), E(rror), c(configuration ambiguous), o(rphaned), P(reempted),
# or some combination thereof.
# Refer to qstat man page for additional details.
SGE_BUSY_STATES = ["u", "C", "s", "d", "D", "E", "o"]

# The states q(ueued)/w(aiting) and h(old) only appear for pending jobs. Pending, unheld job`s are displayed as qw.
# The h(old) state indicates that a job currently is not eligible for execution due to a hold state assigned to it
# via qhold(1), qalter(1) or the qsub(1) -h option, or that the job is waiting for completion of the jobs for which job
# dependencies have been assigned to it job via the -hold_jid or -hold_jid_ad options of qsub(1) or qalter(1).
SGE_HOLD_STATE = "h"

# The state of the queue - one of u(nknown), a(larm), A(larm), C(alendar suspended), s(uspended),
# S(ubordinate), d(isabled), D(isabled), E(rror), c(configuration ambiguous), o(rphaned), P(reempted),
# or some combination thereof.
# Refer to qstat man page for additional details.
SGE_ERROR_STATES = ["u", "E", "o"]


def exec_qconf_command(hosts, qhost_command):
    if not hosts:
        return []

    hostnames = ",".join([host.hostname for host in hosts])
    try:
        logging.info("Executing operation '%s' for hosts %s", qhost_command.description, hostnames)
        command = "qconf {flags} {hostnames}".format(flags=qhost_command.command_flags, hostnames=hostnames)
        # setting raise_on_error to False and evaluating command output to decide if the execution was successful
        output = check_sge_command_output(command, raise_on_error=False)
        succeeded_hosts = []
        # assuming output contains a message line for each node the command is executed for.
        for host, message in zip(hosts, output.split("\n")):
            if any(re.match(pattern, message) is not None for pattern in qhost_command.successful_messages):
                succeeded_hosts.append(host)

        return succeeded_hosts
    except Exception as e:
        logging.error(
            "Unable to execute operation '%s' for hosts %s. Failed with exception %s",
            qhost_command.description,
            hostnames,
            e,
        )
        return []


def add_hosts_to_group(hosts):
    logging.info("Adding %s to @allhosts group", ",".join([host.hostname for host in hosts]))
    command = "qconf -aattr hostgroup hostlist {hostname} @allhosts"
    return _run_sge_command_for_multiple_hosts(hosts, command)


def add_host_slots(hosts):
    logging.info("Adding %s to all.q queue", ",".join([host.hostname for host in hosts]))
    command = 'qconf -aattr queue slots ["{hostname}={slots}"] all.q'
    return _run_sge_command_for_multiple_hosts(hosts, command)


def remove_hosts_from_group(hosts):
    logging.info("Removing %s from @allhosts group", ",".join([host.hostname for host in hosts]))
    command = "qconf -dattr hostgroup hostlist {hostname} @allhosts"
    return _run_sge_command_for_multiple_hosts(hosts, command)


def remove_hosts_from_queue(hosts):
    logging.info("Removing %s from all.q queue", ",".join([host.hostname for host in hosts]))
    command = "qconf -purge queue '*' all.q@{hostname}"
    return _run_sge_command_for_multiple_hosts(hosts, command)


def install_sge_on_compute_nodes(hosts, cluster_user):
    """Start sge on compute nodes in parallel."""
    command = (
        "sudo sh -c 'cd {0} && {0}/inst_sge -noremote -x -auto /opt/parallelcluster/templates/sge/sge_inst.conf'"
    ).format(sge.SGE_ROOT)
    hostnames = [host.hostname for host in hosts]
    result = RemoteCommandExecutor.run_remote_command_on_multiple_hosts(command, hostnames, cluster_user)

    succeeded_hosts = []
    for host in hosts:
        if host.hostname in result and result[host.hostname]:
            succeeded_hosts.append(host)

    return succeeded_hosts


def _run_sge_command_for_multiple_hosts(hosts, command_template):
    """Sequentially run an sge command on the master node for the given hostnames."""
    succeeded_hosts = []
    for host in hosts:
        command = command_template.format(hostname=host.hostname, slots=host.slots)
        try:
            run_sge_command(command.format(hostname=host.hostname))
            succeeded_hosts.append(host)
        except Exception as e:
            logging.error("Failed when executing command %s with exception %s", command, e)
    return succeeded_hosts
