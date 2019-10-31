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

from common.schedulers.slurm_commands import (
    PENDING_RESOURCES_REASONS,
    get_pending_jobs_info,
    process_gpus_total_for_job,
)
from common.utils import check_command_output
from jobwatcher.plugins.utils import get_optimal_nodes

log = logging.getLogger(__name__)


# get nodes requested from pending jobs
def get_required_nodes(instance_properties, max_size):
    log.info("Computing number of required nodes for submitted jobs")
    pending_jobs = get_pending_jobs_info(
        instance_properties=instance_properties,
        max_nodes_filter=max_size,
        filter_by_pending_reasons=PENDING_RESOURCES_REASONS,
    )
    logging.info("Found the following pending jobs:\n%s", pending_jobs)

    resources_requested = []
    nodes_requested = []
    for job in pending_jobs:
        resources_for_job = {}
        resources_for_job["gpus"] = process_gpus_total_for_job(job)
        resources_for_job["slots"] = job.cpus_total
        resources_requested.append(resources_for_job)
        nodes_requested.append(job.nodes)

    return get_optimal_nodes(nodes_requested, resources_requested, instance_properties)


# get nodes reserved by running jobs
def get_busy_nodes():
    command = "/opt/slurm/bin/sinfo -h -o '%D %t'"
    # Sample output:
    # 2 mix
    # 4 alloc
    # 10 idle
    # 1 down*
    output = check_command_output(command)
    logging.info("Found the following compute nodes:\n%s", output.rstrip())
    nodes = 0
    output = output.split("\n")
    for line in output:
        line_arr = line.split()
        if len(line_arr) == 2 and (line_arr[1] in ["mix", "alloc", "down", "down*"]):
            nodes += int(line_arr[0])
    return nodes
