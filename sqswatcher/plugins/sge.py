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
import collections
import logging
import re

import common.sge as sge
from common.remote_command_executor import RemoteCommandExecutor
from common.sge import check_sge_command_output, run_sge_command

log = logging.getLogger(__name__)

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


def _exec_qconf_command(hosts, qhost_command):
    if not hosts:
        return []

    hostnames = ",".join([host.hostname for host in hosts])
    try:
        log.info("Executing operation '%s' for hosts %s", qhost_command.description, hostnames)
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
        log.error(
            "Unable to execute operation '%s' for hosts %s. Failed with exception %s",
            qhost_command.description,
            hostnames,
            e,
        )
        return []


def _run_sge_command_for_multiple_hosts(hosts, command_template):
    """Sequentially run an sge command on the master node for the given hostnames."""
    succeeded_hosts = []
    for host in hosts:
        command = command_template.format(hostname=host.hostname, slots=host.slots)
        try:
            run_sge_command(command.format(hostname=host.hostname))
            succeeded_hosts.append(host)
        except Exception as e:
            log.error("Failed when executing command %s with exception %s", command, e)
    return succeeded_hosts


def _add_hosts_to_group(hosts):
    log.info("Adding %s to @allhosts group", ",".join([host.hostname for host in hosts]))
    command = "qconf -aattr hostgroup hostlist {hostname} @allhosts"
    return _run_sge_command_for_multiple_hosts(hosts, command)


def _add_host_slots(hosts):
    log.info("Adding %s to all.q queue", ",".join([host.hostname for host in hosts]))
    command = 'qconf -aattr queue slots ["{hostname}={slots}"] all.q'
    return _run_sge_command_for_multiple_hosts(hosts, command)


def _remove_hosts_from_group(hosts):
    log.info("Removing %s from @allhosts group", ",".join([host.hostname for host in hosts]))
    command = "qconf -dattr hostgroup hostlist {hostname} @allhosts"
    return _run_sge_command_for_multiple_hosts(hosts, command)


def _remove_hosts_from_queue(hosts):
    log.info("Removing %s from all.q queue", ",".join([host.hostname for host in hosts]))
    command = "qconf -purge queue '*' all.q@{hostname}"
    return _run_sge_command_for_multiple_hosts(hosts, command)


def _install_compute(hosts, cluster_user):
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


def _add_hosts(hosts, cluster_user):
    """
    Add a list of compute nodes to the cluster.

    If one of the steps fails then the procedure for the failing host is stopped.
    All operations should be idempotent in order to allow retries in case the procedure
    fails at any of the steps.

    :return: the list of hostnames that were added correctly to the cluster
    """
    if not hosts:
        return []

    succeeded_hosts = _exec_qconf_command(hosts, QCONF_COMMANDS["ADD_ADMINISTRATIVE_HOST"])
    succeeded_hosts = _exec_qconf_command(succeeded_hosts, QCONF_COMMANDS["ADD_SUBMIT_HOST"])
    succeeded_hosts = _add_hosts_to_group(succeeded_hosts)
    succeeded_hosts = _add_host_slots(succeeded_hosts)
    succeeded_hosts = _install_compute(succeeded_hosts, cluster_user)
    return [host.hostname for host in succeeded_hosts]


def _remove_hosts(hosts):
    """
    Remove a list of compute nodes from the cluster.

    If one of the steps fails then the procedure for the failing host continues. This is done
    to clean up as much as possible the scheduler configuration in case a node is terminated
    but some of the steps are failing.
    All operations should be idempotent in order to allow retries in case the procedure
    fails at any of the steps.

    :return: the list of hostnames that were removed correctly from the cluster
    """
    if not hosts:
        return []

    succeeded_hosts = set(hosts)
    succeeded_hosts = succeeded_hosts.intersection(set(_remove_hosts_from_queue(hosts)))
    succeeded_hosts = succeeded_hosts.intersection(set(_remove_hosts_from_group(hosts)))
    succeeded_hosts = succeeded_hosts.intersection(
        set(_exec_qconf_command(hosts, QCONF_COMMANDS["REMOVE_ADMINISTRATIVE_HOST"]))
    )
    succeeded_hosts = succeeded_hosts.intersection(
        set(_exec_qconf_command(hosts, QCONF_COMMANDS["REMOVE_SUBMIT_HOST"]))
    )
    succeeded_hosts = succeeded_hosts.intersection(
        set(_exec_qconf_command(hosts, QCONF_COMMANDS["REMOVE_EXECUTION_HOST"]))
    )
    return [host.hostname for host in succeeded_hosts]


def update_cluster(max_cluster_size, cluster_user, update_events, instance_properties):
    if not update_events:
        return [], []

    hosts_to_add = []
    hosts_to_remove = []
    for event in update_events:
        if event.action == "REMOVE":
            hosts_to_remove.append(event.host)
        elif event.action == "ADD":
            hosts_to_add.append(event.host)

    added_hosts = _add_hosts(hosts_to_add, cluster_user)
    removed_hosts = _remove_hosts(hosts_to_remove)

    succeeded = []
    failed = []
    for event in update_events:
        if event.host.hostname in added_hosts or event.host.hostname in removed_hosts:
            succeeded.append(event)
        else:
            failed.append(event)

    return failed, succeeded
