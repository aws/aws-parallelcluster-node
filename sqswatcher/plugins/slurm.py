# Copyright 2013-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied.
# See the License for the specific language governing permissions and limitations under the License.

# This file has a special meaning for pytest. See https://docs.pytest.org/en/2.7.3/plugins.html for
# additional details.

import logging
import os
import os.path
from shutil import move
from tempfile import mkstemp

from retrying import retry

from common.remote_command_executor import RemoteCommandExecutor
from common.utils import run_command

log = logging.getLogger(__name__)

PCLUSTER_NODES_CONFIG = "/opt/slurm/etc/slurm_parallelcluster_nodes.conf"


@retry(stop_max_attempt_number=3, wait_fixed=10000)
def _restart_master_node():
    log.info("Restarting slurm on master node")
    if os.path.isfile("/etc/systemd/system/slurmctld.service"):
        command = ["sudo", "systemctl", "restart", "slurmctld.service"]
    else:
        command = ["/etc/init.d/slurm", "restart"]
    try:
        run_command(command)
    except Exception as e:
        log.error("Failed when restarting slurm daemon on master node with exception %s", e)
        raise


def _restart_multiple_compute_nodes(hostnames, cluster_user):
    command = (
        "if [ -f /etc/systemd/system/slurmd.service ]; "
        "then sudo systemctl restart slurmd.service; "
        'else sudo sh -c "/etc/init.d/slurm restart 2>&1 > /tmp/slurmdstart.log"; fi'
    )
    return RemoteCommandExecutor.run_remote_command_on_multiple_hosts(command, hostnames, cluster_user)


def _reconfigure_nodes():
    log.info("Reconfiguring slurm")
    command = ["/opt/slurm/bin/scontrol", "reconfigure"]
    try:
        run_command(command)
    except Exception as e:
        log.error("Failed when reconfiguring slurm daemon with exception %s", e)


def _read_node_list():
    nodes = []
    with open(PCLUSTER_NODES_CONFIG) as slurm_config:
        for line in slurm_config:
            if line.startswith("NodeName") and "dummy-compute" not in line:
                nodes.append(line)
    return nodes


def _write_node_list(node_list, max_cluster_size, instance_properties):
    dummy_nodes_count = max_cluster_size - len(node_list)
    fh, abs_path = mkstemp()
    if dummy_nodes_count > 0:
        os.write(
            fh,
            "NodeName=dummy-compute[1-{0}] CPUs={1} State=FUTURE\n".format(
                dummy_nodes_count, instance_properties["slots"]
            ),
        )
    for node in node_list:
        os.write(fh, "{0}".format(node))

    os.close(fh)
    # Update permissions on new file
    os.chmod(abs_path, 0o744)
    # Move new file
    move(abs_path, PCLUSTER_NODES_CONFIG)


def update_cluster(max_cluster_size, cluster_user, update_events, instance_properties):
    # Get the current node list
    node_list = _read_node_list()
    nodes_to_restart = []
    for event in update_events:
        if event.action == "REMOVE":
            node_name = "NodeName={0}".format(event.host.hostname)
            node_list = [node for node in node_list if node.split()[0] != node_name]
        elif event.action == "ADD":
            # Add new node
            new_node = "NodeName={nodename} CPUs={cpus} State=UNKNOWN\n".format(
                nodename=event.host.hostname, cpus=event.host.slots
            )
            if new_node not in node_list:
                node_list.append(new_node)
            # Restarting also if already in config cause it might have failed at the previous iteration
            nodes_to_restart.append(event.host.hostname)

    try:
        _write_node_list(node_list, max_cluster_size, instance_properties)
        _restart_master_node()
        results = _restart_multiple_compute_nodes(nodes_to_restart, cluster_user)
        _reconfigure_nodes()

        failed = []
        succeeded = []
        for event in update_events:
            if results.get(event.host.hostname, True):
                succeeded.append(event)
            else:
                failed.append(event)

        return failed, succeeded
    except Exception as e:
        log.error("Encountered error when processing events: %s", e)
        return update_events, []
