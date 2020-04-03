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
import logging

from common.schedulers.torque_commands import get_compute_nodes_info, get_pending_jobs_info

from .utils import get_optimal_nodes

log = logging.getLogger(__name__)


# get nodes requested from pending jobs
def get_required_nodes(instance_properties, max_size):
    pending_jobs = get_pending_jobs_info(max_slots_filter=instance_properties.get("slots"))

    slots_requested = []
    nodes_requested = []
    for job in pending_jobs:
        if job.resources_list.nodes_resources:
            for nodes, ppn in job.resources_list.nodes_resources:
                nodes_requested.append(nodes)
                slots_requested.append({"slots": ppn * nodes})
        elif job.resources_list.ncpus:
            nodes_requested.append(1)
            slots_requested.append({"slots": job.resources_list.ncpus})
        elif job.resources_list.nodes_count:
            nodes_requested.append(job.resources_list.nodes_count)
            slots_requested.append({"slots": 1 * job.resources_list.nodes_count})

    return get_optimal_nodes(nodes_requested, slots_requested, instance_properties)


# get nodes reserved by running jobs
def get_busy_nodes():
    nodes = get_compute_nodes_info()
    logging.info("Found the following compute nodes:\n%s", nodes)
    busy_nodes = 0
    for node in nodes.values():
        # when a node is added it transitions from down,offline,MOM-list-not-sent -> down -> free
        if node.jobs or (
            any(state in ["state-unknown"] for state in node.state) and "MOM-list-not-sent" not in node.state
        ):
            busy_nodes += 1

    return busy_nodes
