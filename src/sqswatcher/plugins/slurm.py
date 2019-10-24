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
import tempfile
from shutil import move

from retrying import retry

from common.remote_command_executor import RemoteCommandExecutor
from common.utils import run_command

log = logging.getLogger(__name__)

PCLUSTER_NODES_CONFIG = "/opt/slurm/etc/slurm_parallelcluster_nodes.conf"
# slurm_parallelcluster_gres.conf is included in gres.conf,
# so user can easily add to gres.conf without interfering with sqswatcher logic.
PCLUSTER_GRES_CONFIG = "/opt/slurm/etc/slurm_parallelcluster_gres.conf"


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


def _read_node_list(config_file):
    nodes = []
    if os.path.exists(config_file):
        with open(config_file) as slurm_config:
            for line in slurm_config:
                if line.startswith("NodeName") and "dummy-compute" not in line:
                    nodes.append(line)
    return nodes


def _write_node_list_to_file(generic_node_list, config_file_path):
    log.info("Writing updated {0} with the following nodes: {1}".format(config_file_path, generic_node_list))
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as file:
        abs_path = file.name
        # Adding a comment to the file so that Slurm does not display warnings in case of empty file
        file.write("# This file is automatically generated by ParallelCluster\n\n")
        for node in generic_node_list:
            file.write("{0}".format(node))

    # Update permissions on new file
    os.chmod(abs_path, 0o744)
    # Move new file
    move(abs_path, config_file_path)


def _update_gres_node_lists(update_events):
    """
    Process events and add new gres nodes to existing gres_node_list.

    Only returns updated gres_node_list; nodes_to_restart is handled by _update_node_lists function.
    """
    gres_node_list = _read_node_list(PCLUSTER_GRES_CONFIG)
    for event in update_events:
        if event.action == "REMOVE":
            node_name = "NodeName={0}".format(event.host.hostname)
            gres_node_list = [node for node in gres_node_list if node.split()[0] != node_name]
        elif event.action == "ADD":
            # Only add new node in gres.conf if instance has GPU
            if event.host.gpus != 0:
                new_gres_node = "NodeName={nodename} Name=gpu Type=tesla File=/dev/nvidia[0-{gpus}]\n".format(
                    nodename=event.host.hostname, gpus=event.host.gpus - 1
                )

                if new_gres_node not in gres_node_list:
                    gres_node_list.append(new_gres_node)

    return gres_node_list


def _update_node_lists(update_events):
    """
    Process events and add new nodes to existing node_list.

    Returns updated node_list and nodes_to_restart.
    Since events are the same, nodes_to_restart should be the same for gres_nodes and nodes.
    """
    # Get the current node list
    node_list = _read_node_list(PCLUSTER_NODES_CONFIG)
    nodes_to_restart = []
    for event in update_events:
        if event.action == "REMOVE":
            node_name = "NodeName={0}".format(event.host.hostname)
            node_list = [node for node in node_list if node.split()[0] != node_name]
        elif event.action == "ADD":
            # Only include GPU info if instance has GPU
            gpu_info = "Gres=gpu:tesla:{gpus} ".format(gpus=event.host.gpus) if event.host.gpus != 0 else ""
            new_node = (
                "NodeName={nodename} Sockets={sockets} CoresPerSocket={cores_per_socket} "
                "ThreadsPerCore={threads_per_core} RealMemory={memory} CPUs={cpus} {gpu_info}State=UNKNOWN\n"
            ).format(
                nodename=event.host.hostname,
                sockets=event.host.sockets,
                cores_per_socket=event.host.cores_per_socket,
                threads_per_core=event.host.threads_per_core,
                memory=event.host.memory,
                cpus=event.host.slots,
                gpu_info=gpu_info,
            )

            if new_node not in node_list:
                node_list.append(new_node)
            # Restarting also if already in config cause it might have failed at the previous iteration
            nodes_to_restart.append(event.host.hostname)

    return node_list, nodes_to_restart


def _add_dummy_to_node_list(node_list, max_cluster_size, instance_properties):
    dummy_nodes_count = max_cluster_size - len(node_list)
    if dummy_nodes_count > 0:
        # Include GPU information for dummy nodes if instance type has GPU
        if instance_properties["gpus"] == 0:
            gpu_info = ""
        else:
            gpu_info = "Gres=gpu:tesla:{0} ".format(instance_properties["gpus"])
        node_list.insert(
            0,
            "NodeName=dummy-compute[1-{0}] RealMemory={1} CPUs={2} {3}State=FUTURE\n".format(
                dummy_nodes_count, instance_properties["memory"], instance_properties["slots"], gpu_info
            ),
        )


def _add_dummy_to_gres_node_list(gres_node_list, max_cluster_size, instance_properties):
    dummy_nodes_count = max_cluster_size - len(gres_node_list)
    # Only add dummy node in gres.conf if instance has GPU
    if dummy_nodes_count > 0 and instance_properties["gpus"] != 0:
        gres_node_list.insert(
            0,
            "NodeName=dummy-compute[1-{0}] Name=gpu Type=tesla File=/dev/nvidia[0-{1}]\n".format(
                dummy_nodes_count, instance_properties["gpus"] - 1
            ),
        )


def update_cluster(max_cluster_size, cluster_user, update_events, instance_properties):
    """Update and write node lists( and gres_nodes if instance has GPU); restart relevant nodes."""
    node_list, nodes_to_restart = _update_node_lists(update_events)
    _add_dummy_to_node_list(node_list, max_cluster_size, instance_properties)
    # Add a dummy node if node_list is empty so that slurm can start when cluster is stopped
    if not node_list:
        node_list.append(
            'NodeName=dummy-compute-stop CPUs={0} State=DOWN Reason="Cluster is stopped or max size is 0"\n'.format(
                instance_properties["slots"]
            )
        )
    _write_node_list_to_file(node_list, PCLUSTER_NODES_CONFIG)
    # Always update gres.conf
    gres_node_list = _update_gres_node_lists(update_events)
    _add_dummy_to_gres_node_list(gres_node_list, max_cluster_size, instance_properties)
    _write_node_list_to_file(gres_node_list, PCLUSTER_GRES_CONFIG)

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


def init():
    pass
