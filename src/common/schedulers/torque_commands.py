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

# In Python 2, division of two ints produces an int. In Python 3, it produces a float. Uniforming the behaviour.
import logging
import re
import subprocess
from xml.etree import ElementTree

from retrying import retry

from common.schedulers.converters import ComparableObject, from_xml_to_obj
from common.time_utils import minutes, seconds
from common.utils import check_command_output, run_command

TORQUE_NODE_ERROR_STATES = ("down", "offline", "unknown")
TORQUE_NODE_STATES = (
    "free",
    "offline",
    "down",
    "reserve",
    "job-exclusive",
    "job-sharing",
    "busy",
    "time-shared",
    "state-unknown",
)


def _qmgr_manage_nodes(operation, hosts, error_messages_to_ignore, additional_qmgr_args=""):
    if not hosts:
        return []

    hostnames = ",".join(hosts)
    command = '/opt/torque/bin/qmgr -c "{operation} node {hostnames} {additional_args}"'.format(
        operation=operation, hostnames=hostnames, additional_args=additional_qmgr_args
    )
    try:
        output = check_command_output(command, log_error=False)
    except subprocess.CalledProcessError as e:
        if not hasattr(e, "output") or e.output == "":
            logging.error("Failed when executing operation %s on nodes %s with error %s", operation, hostnames, e)
            return set()

    succeeded_hosts = set(hosts)
    # analyze command output to understand if failure can be ignored (e.g. already existing node)
    for error_message in output.splitlines():
        match = re.match(r"qmgr obj=(?P<host>.*) svr=default: (?P<error>.*)", error_message)
        if not match:
            # assume unexpected error and mark all as failed
            logging.error("Failed when executing operation %s on nodes %s with error %s", operation, hostnames, output)
            return set()

        host, error = match.groups()
        if any(error == message_to_ignore for message_to_ignore in error_messages_to_ignore):
            logging.warning(
                "Marking %s operation on node % as succeeded because of ignored error message %s",
                operation,
                host,
                error,
            )
            continue

        try:
            succeeded_hosts.remove(host)
        except Exception as e:
            logging.error(
                "Failed to extract host from error message while adding nodes. Mark all as failed. Output was %s.\n"
                "Exception was %s",
                output,
                e,
            )

    return succeeded_hosts


def add_nodes(hosts, slots):
    return _qmgr_manage_nodes(
        operation="create",
        hosts=hosts,
        error_messages_to_ignore=["Node name already exists"],
        additional_qmgr_args="np={0}".format(slots),
    )


def delete_nodes(hosts):
    return _qmgr_manage_nodes(operation="delete", hosts=hosts, error_messages_to_ignore=["Unknown node"])


def update_cluster_limits(max_nodes, node_slots):
    try:
        logging.info("Updating cluster limits: max_nodes=%d, node_slots=%d", max_nodes, node_slots)
        run_command('/opt/torque/bin/qmgr -c "set queue batch resources_available.nodect={0}"'.format(max_nodes))
        run_command('/opt/torque/bin/qmgr -c "set server resources_available.nodect={0}"'.format(max_nodes))
        run_command('/opt/torque/bin/qmgr -c "set queue batch resources_max.ncpus={0}"'.format(node_slots))
        _update_master_np(max_nodes, node_slots)
    except Exception as e:
        logging.error("Failed when updating cluster limits with exception %s.", e)


def _update_master_np(max_nodes, node_slots):
    """Master np is dynamically based on the number of compute nodes that join the cluster."""
    current_nodes_count = (
        int(check_command_output("/bin/bash --login -c 'cat /var/spool/torque/server_priv/nodes | wc -l'")) - 1
    )
    # If cluster is at max size set the master np to 1 since 0 is not allowed.
    master_node_np = max(1, (max_nodes - current_nodes_count) * node_slots)
    master_hostname = check_command_output("hostname")
    logging.info("Setting master np to: %d", master_node_np)
    run_command(
        '/opt/torque/bin/qmgr -c "set node {hostname} np = {slots}"'.format(
            hostname=master_hostname, slots=master_node_np
        )
    )


def get_compute_nodes_info(hostname_filter=None):
    command = "/opt/torque/bin/pbsnodes -x"
    if hostname_filter:
        command += " {0}".format(" ".join(hostname_filter))

    output = check_command_output(command, raise_on_error=False)
    if output.startswith("<Data>"):
        root = ElementTree.fromstring(output)
        nodes = root.findall("./Node")
        nodes_list = [TorqueHost.from_xml(ElementTree.tostring(node)) for node in nodes]
        return dict((node.name, node) for node in nodes_list)
    else:
        if output != "":
            logging.warning("Failed when running command %s with error %s", command, output)
        return dict()


@retry(wait_fixed=seconds(3), retry_on_result=lambda result: result is False, stop_max_delay=minutes(1))
def wait_nodes_initialization(hosts):
    """Wait for at least one host from hosts to become active"""
    torque_hosts = get_compute_nodes_info(hosts).values()
    for node in torque_hosts:
        if not any(init_state in node.state for init_state in TORQUE_NODE_ERROR_STATES):
            return True
    return False


def wakeup_scheduler(added_hosts):
    torque_hosts = get_compute_nodes_info().values()
    for node in torque_hosts:
        if not any(init_state in node.state for init_state in TORQUE_NODE_ERROR_STATES):
            if node.name not in added_hosts:
                # Do not trigger scheduling cycle when there was already at least one active node.
                return

    try:
        # Before triggering a scheduling cycle wait for at least one node to become active
        wait_nodes_initialization(added_hosts)
    except Exception as e:
        logging.error("Failed while waiting for nodes initialization with exception %s", e)

    # Trigger a scheduling cycle. This is necessary when the first compute node gets added to the scheduler.
    logging.info("Triggering a scheduling cycle.")
    run_command('/opt/torque/bin/qmgr -c "set server scheduling=true"', raise_on_error=False)


class TorqueHost(ComparableObject):
    # <Node>
    #     <name>ip-10-0-1-242</name>
    #     <state>free</state>
    #     <power_state>Running</power_state>
    #     <np>4</np>
    #     <ntype>cluster</ntype>
    #     <status>opsys=linux,uname=Linux ip-10-0-1-242 4.4.111-1.el6.elrepo.x86_64...</status>
    #     <mom_service_port>15002</mom_service_port>
    #     <mom_manager_port>15003</mom_manager_port>
    # </Node>
    MAPPINGS = {"name": {"field": "name"}, "np": {"field": "slots", "transformation": int}, "state": {"field": "state"}}

    def __init__(self, name=None, slots=0, state=""):
        self.name = name
        self.slots = slots
        self.state = state

    @staticmethod
    def from_xml(xml):
        return from_xml_to_obj(xml, TorqueHost)
