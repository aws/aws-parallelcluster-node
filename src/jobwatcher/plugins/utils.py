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
import copy
import logging

log = logging.getLogger(__name__)


def get_optimal_nodes(nodes_requested, resources_requested, instance_properties):
    """
    Get the optimal number of nodes required to satisfy the number of nodes and slots requested.

    :param nodes_requested: Array containing the number of nodes requested by the ith job
    :param resources_requested: Array containing dict of all resources requested by the ith job,
        i.e. {"slots": 4, "gpus": 4}
    :param instance_properties: instance properties, i.e. slots/gpu/memory available per node
    :return: The optimal number of nodes required to satisfy the input queue.
    """
    resources_remaining_per_node = []

    for job_resources, num_of_nodes in zip(resources_requested, nodes_requested):
        log.info(
            "Processing job that requested {0} nodes and the following resources: {1}".format(
                num_of_nodes, job_resources
            )
        )

        # For simplicity, uniformly distribute the resource requested across all the requested nodes
        job_resources_per_node = {}
        for resource_type in job_resources:
            job_resources_per_node[resource_type] = -(-job_resources[resource_type] // num_of_nodes)

        # Verify if there are enough available slots in the nodes allocated in the previous rounds
        for slot_idx, resources_available in enumerate(resources_remaining_per_node):
            if num_of_nodes == 0:
                break
            # Check if node represented by slot_idx can be used to run this job
            job_runnable_on_node = job_runnable_on_given_node(
                job_resources_per_node, resources_available, existing_node=True
            )
            if job_runnable_on_node:
                for resource_type in job_resources:
                    resources_remaining_per_node[slot_idx][resource_type] -= job_resources_per_node[resource_type]
                num_of_nodes -= 1

        # Since the number of available slots were unable to run this job entirely, only add the necessary nodes.
        for _ in range(num_of_nodes):
            new_node = copy.deepcopy(instance_properties)
            for resource_type in job_resources:
                new_node[resource_type] -= job_resources_per_node[resource_type]
            resources_remaining_per_node.append(new_node)

    # return the number of nodes added
    log.info("Computed following allocation for required nodes %s", resources_remaining_per_node)
    return len(resources_remaining_per_node)


def job_runnable_on_given_node(job_resources_per_node, resources_available, existing_node=False):
    """Check to see if job can be run on a given node."""
    for resource_type in job_resources_per_node:
        try:
            if resources_available[resource_type] < job_resources_per_node[resource_type]:
                # Don't log an insufficient node resources message if the node exists.
                # Doing so pollutes the logs.
                if not existing_node:
                    logging.warning(
                        (
                            "Resource:{0} required per node ({1}) is greater than resources "
                            "available on single node ({2}), skipping job..."
                        ).format(
                            resource_type, job_resources_per_node[resource_type], resources_available[resource_type]
                        )
                    )
                return False
        except KeyError as e:
            logging.warning(e)
            return False

    return True
