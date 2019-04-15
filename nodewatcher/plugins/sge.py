# Copyright 2013-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import subprocess

from common.sge import check_sge_command_output, run_sge_command

log = logging.getLogger(__name__)


def hasJobs(hostname):
    # Checking for running jobs on the node, with parallel job view expanded (-g t)
    command = "qstat -g t -l hostname={0} -u '*'".format(hostname)

    # Command output
    # job-ID  prior   name       user         state submit/start at     queue                          master ja-task-ID
    # ------------------------------------------------------------------------------------------------------------------
    # 16 0.6 0500 job.sh     ec2-user     r     02/06/2019 11:06:30 all.q@ip-172-31-68-26.ec2.inte SLAVE
    #                                                               all.q@ip-172-31-68-26.ec2.inte SLAVE
    #                                                               all.q@ip-172-31-68-26.ec2.inte SLAVE
    #                                                               all.q@ip-172-31-68-26.ec2.inte SLAVE
    # 17 0.50500 STDIN      ec2-user     r     02/06/2019 11:06:30 all.q@ip-172-31-68-26.ec2.inte MASTER 1
    # 17 0.50500 STDIN      ec2-user     r     02/06/2019 11:06:30 all.q@ip-172-31-68-26.ec2.inte MASTER 2

    try:
        output = check_sge_command_output(command)
        has_jobs = output != ""
    except subprocess.CalledProcessError:
        has_jobs = False

    return has_jobs


def hasPendingJobs():
    command = "qstat -g d -s p -u '*'"

    # Command outputs the pending jobs in the queue in the following format
    # job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID
    # -----------------------------------------------------------------------------------------------------------------
    #      70 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:24                                    1
    #      71 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:24                                    1
    #      72 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:25                                    1
    #      73 0.55500 job.sh     ec2-user     qw    08/08/2018 22:37:25                                    1

    try:
        output = check_sge_command_output(command)
        lines = filter(None, output.split("\n"))
        has_pending = True if len(lines) > 1 else False
        error = False
    except subprocess.CalledProcessError:
        error = True
        has_pending = False

    return has_pending, error


def lockHost(hostname, unlock=False):
    mod = unlock and "-e" or "-d"
    command = ["qmod", mod, "all.q@%s" % hostname]

    try:
        run_sge_command(command)
    except subprocess.CalledProcessError:
        log.error("Error %s host %s", "unlocking" if unlock else "locking", hostname)
