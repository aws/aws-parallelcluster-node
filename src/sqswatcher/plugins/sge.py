# Copyright 2013-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
import logging

from common.schedulers.sge_commands import (
    QCONF_COMMANDS,
    add_host_slots,
    add_hosts_to_group,
    exec_qconf_command,
    install_sge_on_compute_nodes,
    remove_hosts_from_group,
    remove_hosts_from_queue,
)
from common.utils import EventType

log = logging.getLogger(__name__)


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

    succeeded_hosts = exec_qconf_command(hosts, QCONF_COMMANDS["ADD_ADMINISTRATIVE_HOST"])
    succeeded_hosts = exec_qconf_command(succeeded_hosts, QCONF_COMMANDS["ADD_SUBMIT_HOST"])
    succeeded_hosts = install_sge_on_compute_nodes(succeeded_hosts, cluster_user)
    succeeded_hosts = add_hosts_to_group(succeeded_hosts)
    succeeded_hosts = add_host_slots(succeeded_hosts)
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
    succeeded_hosts = succeeded_hosts.intersection(set(remove_hosts_from_queue(hosts)))
    succeeded_hosts = succeeded_hosts.intersection(set(remove_hosts_from_group(hosts)))
    succeeded_hosts = succeeded_hosts.intersection(
        set(exec_qconf_command(hosts, QCONF_COMMANDS["REMOVE_ADMINISTRATIVE_HOST"]))
    )
    succeeded_hosts = succeeded_hosts.intersection(set(exec_qconf_command(hosts, QCONF_COMMANDS["REMOVE_SUBMIT_HOST"])))
    succeeded_hosts = succeeded_hosts.intersection(
        set(exec_qconf_command(hosts, QCONF_COMMANDS["REMOVE_EXECUTION_HOST"]))
    )
    return [host.hostname for host in succeeded_hosts]


def update_cluster(max_cluster_size, cluster_user, update_events, instance_properties):
    if not update_events:
        return [], []

    hosts_to_add = []
    hosts_to_remove = []
    for event in update_events:
        if event.action == EventType.REMOVE:
            hosts_to_remove.append(event.host)
        elif event.action == EventType.ADD:
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


def init():
    pass
