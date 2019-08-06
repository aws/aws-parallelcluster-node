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

log = logging.getLogger(__name__)


def get_optimal_nodes(nodes_requested, slots_requested, instance_properties):
    """
    Get the optimal number of nodes required to satisfy the number of nodes and slots requested.

    :param nodes_requested: Array containing the number of nodes requested by the ith job
    :param slots_requested: Array containing the number of slots requested by the ith job
    :param instance_properties: instance properties, i.e. number of slots available per node
    :return: The optimal number of nodes required to satisfy the input queue.
    """
    vcpus = instance_properties.get("slots")
    slots_remaining_per_node = []

    for slots, num_of_nodes in zip(slots_requested, nodes_requested):
        log.info("Requested %s nodes and %s slots" % (num_of_nodes, slots))
        # For simplicity, uniformly distribute the numbers of cpus requested across all the requested nodes
        slots_required_per_node = -(-slots // num_of_nodes)

        if slots_required_per_node > vcpus:
            log.warning(
                "Slots required per node (%d) is greater than vcpus available on single node (%d), skipping job...",
                slots_required_per_node,
                vcpus,
            )
            continue

        # Verify if there are enough available slots in the nodes allocated in the previous rounds
        for slot_idx, slots_available in enumerate(slots_remaining_per_node):
            if num_of_nodes > 0 and slots_available >= slots_required_per_node:
                log.info("Slot available in existing node")
                # The node represented by slot_idx can be used to run this job
                slots_remaining_per_node[slot_idx] -= slots_required_per_node
                num_of_nodes -= 1

        log.info("After looking at already allocated nodes, %s more nodes are needed" % num_of_nodes)

        # Since the number of available slots were unable to run this job entirely, only add the necessary nodes.
        for _ in range(num_of_nodes):
            log.info("Adding node. Using %s slots" % slots_required_per_node)
            slots_remaining_per_node.append(vcpus - slots_required_per_node)

    # return the number of nodes added
    return len(slots_remaining_per_node)
