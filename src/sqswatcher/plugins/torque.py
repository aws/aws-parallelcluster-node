# Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import threading
import time

from common.schedulers.torque_commands import (
    TORQUE_NODE_DISABLED_STATE,
    add_nodes,
    delete_nodes,
    get_compute_nodes_info,
    get_pending_jobs_info,
    lock_node,
    update_cluster_limits,
    wakeup_scheduler,
)
from common.utils import EventType

log = logging.getLogger(__name__)


def update_cluster(max_cluster_size, cluster_user, update_events, instance_properties):
    succeeded = []
    failed = []

    if update_events:
        hosts_to_add = []
        hosts_to_remove = []
        for event in update_events:
            if event.action == EventType.REMOVE:
                hosts_to_remove.append(event.host.hostname)
            elif event.action == EventType.ADD:
                hosts_to_add.append(event.host.hostname)

        added_hosts = add_nodes(hosts_to_add, instance_properties["slots"])
        removed_hosts = delete_nodes(hosts_to_remove)

        for event in update_events:
            if event.host.hostname in added_hosts or event.host.hostname in removed_hosts:
                succeeded.append(event)
            else:
                failed.append(event)

    update_cluster_limits(max_cluster_size, instance_properties["slots"])

    return failed, succeeded


def _is_node_locked(hostname):
    node = get_compute_nodes_info(hostname_filter=[hostname]).get(hostname)
    if TORQUE_NODE_DISABLED_STATE in node.state:
        return True
    return False


def perform_health_actions(health_events):
    """Update and write node lists( and gres_nodes if instance has GPU); restart relevant nodes."""
    failed = []
    succeeded = []
    for event in health_events:
        try:
            # to-do, ignore fail to lock message if node is not in scheduler
            if _is_node_locked(event.host.hostname):
                log.error(
                    "Instance %s/%s currently in disabled state 'offline'. "
                    "Risk of lock being released by nodewatcher if locking the node because of scheduled event now. "
                    "Marking event as failed to retry later.",
                    event.host.instance_id,
                    event.host.hostname,
                )
                failed.append(event)
                continue

            note = "Node requires replacement due to an EC2 scheduled maintenance event"
            lock_node(hostname=event.host.hostname, unlock=False, note=note)

            if _is_node_locked:
                succeeded.append(event)
                log.info("Successfully locked %s in response to scheduled maintainence event", event.host.hostname)
            else:
                failed.append(event)
                log.error("Failed to lock %s in response to scheduled maintainence event", event.host.hostname)

        except Exception as e:
            log.error(
                "Encountered exception when locking %s because of a scheduled maintainence event: %s",
                event.host.hostname,
                e,
            )
            failed.append(event)

    return failed, succeeded


def init():
    if hasattr(init, "wakeup_scheduler_worker_thread") and init.wakeup_scheduler_worker_thread.is_alive():
        return
    init.wakeup_scheduler_worker_thread = threading.Thread(target=_wakeup_scheduler_worker)
    init.wakeup_scheduler_worker_thread.start()


def _wakeup_scheduler_worker():
    logging.info("Started wakeup_scheduler_worker")
    while True:
        try:
            if len(get_pending_jobs_info()) > 0:
                wakeup_scheduler()
        except Exception as e:
            log.warning("Encountered failure in wakeup_scheduler_worker: %s", e)
        time.sleep(60)
