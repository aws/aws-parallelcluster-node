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

# In Python 2, division of two ints produces an int. In Python 3, it produces a float. Uniforming the behaviour.
from __future__ import division

import logging
import math

from common.utils import check_command_output

PENDING_RESOURCES_REASONS = [
    "Resources",
    "Nodes required for job are DOWN, DRAINED or reserved for jobs in higher priority partitions",
    "BeginTime",
    "NodeDown",
    "Priority",
    "ReqNodeNotAvail, May be reserved for other job",
]


def get_jobs_info(job_state_filter=None):
    """
    Retrieve the list of submitted jobs.

    :param job_state_filter: filter jobs by the given state
    :return: a list of SlurmJob objects representing the submitted jobs.
    """
    command = "/opt/slurm/bin/squeue -r -o '%i|%t|%D|%C|%c|%r'"
    if job_state_filter:
        command += " --states {0}".format(job_state_filter)

    output = check_command_output(command)
    return SlurmJob.from_table(output)


def get_pending_jobs_info(max_slots_filter=None, max_nodes_filter=None, filter_by_pending_reasons=None):
    """
    Retrieve the list of pending jobs from the Slurm scheduler.

    The list is filtered based on the args passed to the function.
    Moreover the number of required nodes is recomputed by taking into account the number of
    CPUs required by each task and the number of nodes requested by the user. See _recompute_required_nodes_per_job
    for additional details.

    :param max_slots_filter: max number of slots in a compute node.
    :param max_nodes_filter: max number of nodes in the cluster.
    :param filter_by_pending_reasons: retrieve only jobs with the following pending reasons.
    :return: array of filtered SlurmJobs
    """
    pending_jobs = get_jobs_info(job_state_filter="PD")
    if max_slots_filter:
        _recompute_required_nodes_per_job(pending_jobs, max_slots_filter)
    if max_slots_filter or filter_by_pending_reasons or max_nodes_filter:
        filtered_jobs = []
        for job in pending_jobs:
            if max_slots_filter and job.cpus_min_per_node > max_slots_filter:
                logging.info(
                    "Skipping job %s since required slots per node (%d) exceed max slots (%d)",
                    job.id,
                    job.cpus_min_per_node,
                    max_slots_filter,
                )
            elif max_nodes_filter and job.nodes > max_nodes_filter:
                logging.info(
                    "Skipping job %s since required nodes (%d) exceed max nodes in cluster (%d)",
                    job.id,
                    job.nodes,
                    max_nodes_filter,
                )
            elif filter_by_pending_reasons and job.pending_reason not in filter_by_pending_reasons:
                logging.info("Skipping pending job %s due to pending reason: %s", job.id, job.pending_reason)
            else:
                filtered_jobs.append(job)

        return filtered_jobs
    else:
        return pending_jobs


def _recompute_required_nodes_per_job(pending_jobs, node_slots):
    """
    Adjust the number of required nodes if necessary.

    According to squeue manual page:
    %D    Number of nodes allocated to the job or the minimum number of nodes required by a pending job.
    The actual number of nodes allocated to a pending job may exceed this number if the job specified a node range count
    (e.g.  minimum and maximum node counts) or the job specifies a processor count instead of a node count and the
    cluster contains nodes with varying processor counts.

    For example, when submitting a job with: sbatch -c 3 -n 5 in a cluster with 4-slot nodes, the output of the squeue
    command is:
        /opt/slurm/bin/squeue -r -o '%i|%t|%D|%C|%c|%r'
        JOBID|ST|NODES|CPUS|MIN_CPUS|REASON
        91|PD|4|15|3|Nodes required for job are DOWN, DRAINED or reserved for jobs in higher priority partitions

    The following function, based on the number of slots required per task, computes the necessary amount of nodes
    to run the job by considering the number of tasks that can fit on a single node.

    :param pending_jobs: array of SlurmJob
    :param node_slots: max slots per compute node
    """
    for job in pending_jobs:
        if node_slots >= job.cpus_min_per_node:
            # check the max number of slots I can fill with tasks of size cpus_min_per_node
            usable_slots_per_node = node_slots - (node_slots % job.cpus_min_per_node)
            # compute number of nodes by considering nodes of size usable_slots_per_node
            required_nodes = int(math.ceil(job.cpus_total / usable_slots_per_node))
            # setting nodes to the max between the computed required nodes and the number of nodes given by
            # the squeue output. This is done to handle job submissions with the -N option where the user can
            # specify a number of nodes that is bigger than the min required.
            # e.g. sbatch -c 1 -N 2 -n 2 => (cpus_min_per_node=1, cpus_total=2, nodes=2)
            job.nodes = max(required_nodes, job.nodes)


class SlurmObject:
    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        attrs = ", ".join(["{key}={value}".format(key=key, value=repr(value)) for key, value in self.__dict__.items()])
        return "{class_name}({attrs})".format(class_name=self.__class__.__name__, attrs=attrs)


class SlurmJob(SlurmObject):
    # JOBID|ST|NODES|CPUS|MIN_CPUS|REASON
    # 72|PD|2|5|1|Nodes required for job are DOWN, DRAINED or reserved for jobs in higher priority partitions
    # 86|PD|10|40|4|PartitionConfig
    # 87|PD|10|10|1|PartitionNodeLimit
    MAPPINGS = {
        "JOBID": {"field": "id"},
        "ST": {"field": "state"},
        "NODES": {"field": "nodes", "transformation": int},
        "CPUS": {"field": "cpus_total", "transformation": int},
        "MIN_CPUS": {"field": "cpus_min_per_node", "transformation": int},
        "REASON": {"field": "pending_reason"},
    }

    def __init__(self, id=None, state="", nodes=0, cpus_total=0, cpus_min_per_node=0, pending_reason=""):
        self.id = id
        self.state = state
        self.nodes = nodes
        self.cpus_total = cpus_total
        self.cpus_min_per_node = cpus_min_per_node
        self.pending_reason = pending_reason

    @staticmethod
    def from_table(table):
        return _from_table_to_obj_list(table, SlurmJob)


def _from_table_to_obj_list(table, obj_type, separator="|"):
    """
    Maps a given tabular output into a python object.

    The python object you want to map the table into needs to define a MAPPINGS dictionary which declare how
    to map each row element into the object itself.
    Each entry of the MAPPINGS dictionary is composed as follow:
    - key: name of the table column (specified in the header)
    - value: a dict containing:
        - field: name of the object attribute you want to map the value to
        - transformation: a function that will be called on the value before assigning this to the object attribute.
    Default values can be defined in the class __init__ definition.

    :param table: string containing the table to parse
    :param obj_type: type of the object you want to map the table into
    :return: a list obj_type instances containing the parsed data
    """
    lines = table.splitlines()
    results = []
    if len(lines) > 1:
        mappings = obj_type.MAPPINGS
        columns = lines[0].split(separator)
        rows = lines[1:]
        for row in rows:
            obj = obj_type()
            for item, column in zip(row.split(separator), columns):
                mapping = mappings.get(column)
                if mapping:
                    transformation_func = mapping.get("transformation")
                    value = item if transformation_func is None else transformation_func(item)
                    setattr(obj, mapping["field"], value)
            results.append(obj)

    return results
