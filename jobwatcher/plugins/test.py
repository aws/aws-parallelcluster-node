# Copyright 2013-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging

log = logging.getLogger(__name__)

# get nodes requested from pending jobs
def get_required_nodes(instance_properties):
    # Test function. Change as needed.
    slots = 4
    vcpus = instance_properties.get('slots')
    return -(-slots // vcpus)

# get nodes reserved by running jobs
def get_busy_nodes(instance_properties):
    # Test function. Change as needed.
    slots = 13
    vcpus = instance_properties.get('slots')
    return -(-slots // vcpus)

def nodes(slots, instance_properties):
    if slots <= 0:
        return 0
    with open('/opt/cfncluster/instances.json') as f:
        instances = json.load(f)
        vcpus = int(instances[instance_type]["vcpus"])
        log.info("Instance %s has %s slots." % (instance_type, vcpus))
        return
