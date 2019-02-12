import shlex
import subprocess as sub
import os
import logging

log = logging.getLogger(__name__)


def run_command(command, env):
    _command = shlex.split(command)
    try:
        DEV_NULL = open(os.devnull, "rb")
        env.update(os.environ.copy())
        process = sub.Popen(_command, env=env, stdout=sub.PIPE, stderr=sub.STDOUT, stdin=DEV_NULL)
        _output = process.communicate()[0]
        return _output
    except sub.CalledProcessError:
        log.error("Failed to run %s\n" % _command)
        exit(1)
    finally:
        DEV_NULL.close()


def get_optimal_nodes(nodes_requested, slots_requested, instance_properties):
    """
    Get the optimal number of nodes required to satisfy the number of nodes and slots requested.

    :param nodes_requested: Array containing the number of nodes requested by the ith job
    :param slots_requested: Array containing the number of slots requested by the ith job
    :param instance_properties: instance properties, i.e. number of slots available per node
    :return: The optimal number of nodes required to satisfy the input queue.
    """
    vcpus = instance_properties.get('slots')
    slots_remaining_per_node = []

    for node_idx, num_of_nodes in enumerate(nodes_requested):
        log.info("Requested %s nodes and %s slots" % (num_of_nodes, slots_requested[node_idx]))
        # For simplicity, uniformly distribute the numbers of cpus requested across all the requested nodes
        slots_required_per_node = -(-slots_requested[node_idx] // num_of_nodes)

        if slots_required_per_node > vcpus:
            # If slots required per node is greater than vcpus, add additional nodes
            # and recalculate slots_required_per_node
            log.info("Slots required per node is greater than vcpus, recalculating")
            num_of_nodes = -(-slots_requested[node_idx] // vcpus)
            slots_required_per_node = -(-slots_requested[node_idx] // num_of_nodes)
            log.info("Recalculated: %s nodes and %s slots required per node" % (num_of_nodes, slots_required_per_node))

        # Verify if there are enough available slots in the nodes allocated in the previous rounds
        for slot_idx, slots_available in enumerate(slots_remaining_per_node):
            if num_of_nodes > 0 and slots_available >= slots_required_per_node:
                log.info("Slot available in existing node")
                # The node represented by slot_idx can be used to run this job
                slots_remaining_per_node[slot_idx] -= slots_required_per_node
                num_of_nodes -= 1

        log.info("After looking at already allocated nodes, %s more nodes are needed" % num_of_nodes)

        # Since the number of available slots were unable to run this job entirely, only add the necessary nodes.
        for i in range(num_of_nodes):
            log.info("Adding node. Using %s slots" % slots_required_per_node)
            slots_remaining_per_node.append(vcpus - slots_required_per_node)

    # return the number of nodes added
    return len(slots_remaining_per_node)
