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
            # FIXME: Replace this code with the commented section below after testing with Torque
            # If slots required per node is greater than vcpus, add additional nodes
            # and recalculate slots_required_per_node
            log.info("Slots required per node is greater than vcpus, recalculating")
            num_of_nodes = -(-slots // vcpus)
            slots_required_per_node = -(-slots // num_of_nodes)
            log.info("Recalculated: %s nodes and %s slots required per node" % (num_of_nodes, slots_required_per_node))
            # log.warning(
            #     "Slots required per node (%d) is greater than vcpus available on single node (%d), skipping job...",
            #     slots_required_per_node,
            #     vcpus,
            # )
            # continue

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
