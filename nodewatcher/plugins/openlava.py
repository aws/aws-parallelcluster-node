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

__author__ = 'dougalb'

import subprocess
import logging

log = logging.getLogger(__name__)

def getJobs(hostname):
    # Checking for running jobs on the node
    command = ['/opt/openlava/bin/bjobs', '-m', hostname, '-u', 'all']
    try:
        output = subprocess.Popen(command, stdout=subprocess.PIPE).communicate()[0]
    except subprocess.CalledProcessError:
        log.error("Failed to run %s\n" % command)
        output = ""

    if output == "":
        _jobs = False
    else:
        _jobs = True

    return _jobs


def lockHost(hostname, unlock=False):
    # http://wp.auburn.edu/morgaia/?p=103
    _mod = unlock and 'hopen' or 'hclose'
    command = ['/opt/openlava/bin/badmin', _mod, hostname]
    try:
        subprocess.check_call(command)
    except subprocess.CalledProcessError:
        log.error("Failed to run %s\n" % command)

