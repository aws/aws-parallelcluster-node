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

__author__ = "seaam"

import boto3
import logging
import sys
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

def get_asg_name(stack_name, region, proxy_config):
    cfn = boto3.client('cloudformation', region_name=region, config=proxy_config)
    asg_name = ""

    try:
        r = cfn.describe_stack_resource(StackName=stack_name, LogicalResourceId='ComputeFleet')
        asg_name = r.get('StackResourceDetail').get('PhysicalResourceId')
        log.info("asg=%s" % asg_name)
    except ClientError as e:
        log.error("No asg found for cluster %s" % stack_name)
        sys.exit(1)

    return asg_name

def load_scheduler_module(module, scheduler):
    scheduler = '%s.plugins.%s' % (module, scheduler)
    _scheduler = __import__(scheduler)
    _scheduler = sys.modules[scheduler]

    log.debug("scheduler=%s" % repr(_scheduler))

    return _scheduler
