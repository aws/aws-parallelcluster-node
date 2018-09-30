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
    vcpus = instance_properties.get('slots')
    slots_remaining_per_node = []

    for node_idx, node in enumerate(nodes_requested):
        log.info("Requested node %s with slots %s" % (node, slots_requested[node_idx]))
        # for simplicity, uniformly distribute the numbers of cpus requested across all the requested nodes
        slots_required_per_node = -(-slots_requested[node_idx] // node)

        if slots_required_per_node > vcpus:
            # if slots required per node is greater than vcpus, add additional nodes
            # and recalculate slots_required_per_node
            log.info("Slots required per node is greater than vcpus, recalculating")
            node = -(-slots_requested[node_idx] // vcpus)
            slots_required_per_node = -(-slots_requested[node_idx] // node)
            log.info("Recalculated: node %s and slots_required_per_node %s" % (node, slots_required_per_node))

        for slot_idx, slots_available in enumerate(slots_remaining_per_node):
            if node > 0 and slots_available >= slots_required_per_node:
                log.info("Slot available in existing node")
                # The node represented by slot_idx can be used to run this job
                slots_remaining_per_node[slot_idx] -= slots_required_per_node
                node -= 1

        log.info("After looking at already allocated nodes, %s more nodes are needed" % node)

        # Since the number of available slots were unable to run this job entirely, only add the necessary nodes.
        for i in range(node):
            log.info("Adding node. Using %s slots" % slots_required_per_node)
            slots_remaining_per_node.append(vcpus - slots_required_per_node)

    # return the number of nodes added
    return len(slots_remaining_per_node)
