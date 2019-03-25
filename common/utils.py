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

import os
import shlex
import subprocess
import sys

import boto3
from future.moves.subprocess import check_output
from retrying import retry


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


@retry(
    stop_max_attempt_number=5,
    wait_exponential_multiplier=10000,
    retry_on_exception=lambda exception: isinstance(exception, IndexError)
)
def get_asg_name(stack_name, region, proxy_config, log):
    """
    Get autoscaling group name associated to the given stack.

    :param stack_name: stack name to search for
    :param region: AWS region
    :param proxy_config: Proxy configuration
    :param log: logger
    :raise ASGNotFoundError if the ASG is not found (after the timeout) or if an unexpected error occurs
    :return: the ASG name
    """
    asg_client = boto3.client("autoscaling", region_name=region, config=proxy_config)
    try:
        response = asg_client.describe_tags(Filters=[{"Name": "Value", "Values": [stack_name]}])
        asg_name = response.get("Tags")[0].get("ResourceId")
        log.info("ASG %s found for the stack %s", asg_name, stack_name)
        return asg_name
    except IndexError:
        log.warning("Unable to get ASG for stack %s", stack_name)
        raise
    except Exception as e:
        raise CriticalError("Unable to get ASG for stack {0}. Failed with exception: {1}".format(stack_name, e))


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


def check_command_output(command, log, env=None):
    """
    Execute shell command and retrieve command output.

    :param command: command to execute
    :param env: a dictionary containing environment variables
    :param log: logger
    :return: the command output
    :raise: subprocess.CalledProcessError if the command fails
    """
    try:
        if isinstance(command, str) or isinstance(command, unicode):
            command = shlex.split(command.encode("ascii"))
        if env is None:
            env = {}

        env.update(os.environ.copy())
        log.debug("Executing command: %s" % command)
        return check_output(command, env=env, stderr=subprocess.STDOUT, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        # CalledProcessError.__str__ already produces a significant error message
        log.error(e)
        raise
    except OSError as e:
        log.error("Unable to execute the command %s. Failed with exception: %s", command, e)
        raise


def run_command(command, log, env=None):
    """
    Execute shell command.

    :param command: command to execute
    :param env: a dictionary containing environment variables
    :param log: logger
    :raise: subprocess.CalledProcessError if the command fails
    """
    try:
        if isinstance(command, str) or isinstance(command, unicode):
            command = shlex.split(command.encode("ascii"))
        if env is None:
            env = {}

        env.update(os.environ.copy())
        log.debug("Executing command: %s" % command)
        subprocess.check_call(command, env=env)
    except subprocess.CalledProcessError as e:
        # CalledProcessError.__str__ already produces a significant error message
        log.error(e)
        raise
    except OSError as e:
        log.error("Unable to execute the command %s. Failed with exception: %s", command, e)
        raise
