#!/usr/bin/env python2.6

# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied.
# See the License for the specific language governing permissions and limitations under the License.

import sys
import time

import boto3


class ASGNotFoundError(Exception):
    pass


class CriticalError(Exception):
    """Critical error for the daemon."""
    pass


def load_module(module):
    """
    Load python module.

    :param module: module path, relative to the caller one.
    :return: the loaded scheduler module
    """
    # import module
    __import__(module)
    # get module from the loaded maps
    scheduler_module = sys.modules[module]
    return scheduler_module


def get_asg_name(stack_name, region, proxy_config, log, attempts=4, delay=30):
    """
    Get autoscaling group name associated to the given stack.

    :param stack_name: stack name to search for
    :param region: AWS region
    :param proxy_config: Proxy configuration
    :param log: logger
    :param attempts: the number of times to try before giving up if the ASG is not yet ready
    :param delay: delay between retries in seconds
    :raise ASGNotFoundError if the ASG is not found (after the timeout) or if an unexpected error occurs
    :return: the ASG name
    """
    asg_client = boto3.client("autoscaling", region_name=region, config=proxy_config)

    count = 0
    while True:
        try:
            response = asg_client.describe_tags(Filters=[{"Name": "Value", "Values": [stack_name]}])
            asg_name = response.get("Tags")[0].get("ResourceId")
            log.info("ASG %s found for the stack %s", asg_name, stack_name)
            return asg_name
        except IndexError:
            if count < attempts:
                log.warning("No ASG found for stack %s, waiting %s seconds...", stack_name, delay)
                time.sleep(delay)
                count += 1
            else:
                raise ASGNotFoundError("Unable to get ASG for stack %s" % stack_name)
        except Exception as e:
            raise ASGNotFoundError("Unable to get ASG for stack %s. Failed with exception: %s" % (stack_name, e))


def get_asg_settings(region, proxy_config, asg_name, log):
    try:
        asg_client = boto3.client("autoscaling", region_name=region, config=proxy_config)
        asg = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name]).get('AutoScalingGroups')[0]
        min_size = asg.get('MinSize')
        desired_capacity = asg.get('DesiredCapacity')
        max_size = asg.get('MaxSize')

        log.info("min/desired/max %d/%d/%d" % (min_size, desired_capacity, max_size))
        return min_size, desired_capacity, max_size
    except Exception as e:
        log.error("Failed when retrieving data for ASG %s with exception %s", asg_name, e)
        raise
