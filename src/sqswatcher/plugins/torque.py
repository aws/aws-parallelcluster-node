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

from common.schedulers.torque_commands import add_nodes, delete_nodes, update_cluster_limits, wakeup_scheduler

log = logging.getLogger(__name__)


def update_cluster(max_cluster_size, cluster_user, update_events, instance_properties):
    succeeded = []
    failed = []

    if update_events:
        hosts_to_add = []
        hosts_to_remove = []
        for event in update_events:
            if event.action == "REMOVE":
                hosts_to_remove.append(event.host.hostname)
            elif event.action == "ADD":
                hosts_to_add.append(event.host.hostname)

        added_hosts = add_nodes(hosts_to_add, instance_properties["slots"])
        removed_hosts = delete_nodes(hosts_to_remove)

        if added_hosts:
            wakeup_scheduler(added_hosts)

        for event in update_events:
            if event.host.hostname in added_hosts or event.host.hostname in removed_hosts:
                succeeded.append(event)
            else:
                failed.append(event)

    update_cluster_limits(max_cluster_size, instance_properties["slots"])

    return failed, succeeded
