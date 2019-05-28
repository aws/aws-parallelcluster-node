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
from future.moves.subprocess import check_output

import json
import logging
import os
import shlex
import subprocess
import sys

import boto3
from retrying import retry

log = logging.getLogger(__name__)


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
    wait_exponential_max=80000,
    retry_on_exception=lambda exception: isinstance(exception, IndexError),
)
def get_asg_name(stack_name, region, proxy_config):
    """
    Get autoscaling group name associated to the given stack.

    :param stack_name: stack name to search for
    :param region: AWS region
    :param proxy_config: Proxy configuration
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


def get_asg_settings(region, proxy_config, asg_name):
    try:
        asg_client = boto3.client("autoscaling", region_name=region, config=proxy_config)
        asg = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name]).get("AutoScalingGroups")[0]
        min_size = asg.get("MinSize")
        desired_capacity = asg.get("DesiredCapacity")
        max_size = asg.get("MaxSize")

        log.info("min/desired/max %d/%d/%d" % (min_size, desired_capacity, max_size))
        return min_size, desired_capacity, max_size
    except Exception as e:
        log.error("Failed when retrieving data for ASG %s with exception %s", asg_name, e)
        raise


def check_command_output(command, env=None, raise_on_error=True):
    """
    Execute shell command and retrieve command output.

    :param command: command to execute
    :param env: a dictionary containing environment variables
    :param raise_on_error: True to raise subprocess.CalledProcessError on errors
    :return: the command output
    :raise: subprocess.CalledProcessError if the command fails
    """
    return _run_command(
        lambda _command, _env: check_output(_command, env=_env, stderr=subprocess.STDOUT, universal_newlines=True),
        command,
        env,
        raise_on_error,
    )


def run_command(command, env=None, raise_on_error=True):
    """
    Execute shell command.

    :param command: command to execute
    :param env: a dictionary containing environment variables
    :param raise_on_error: True to raise subprocess.CalledProcessError on errors
    :raise: subprocess.CalledProcessError if the command fails
    """
    _run_command(lambda _command, _env: subprocess.check_call(_command, env=_env), command, env, raise_on_error)


def _run_command(command_function, command, env=None, raise_on_error=True):
    try:
        if isinstance(command, str) or isinstance(command, unicode):
            command = shlex.split(command.encode("ascii"))
        if env is None:
            env = {}

        env.update(os.environ.copy())
        log.debug("Executing command: %s" % command)
        return command_function(command, env)
    except subprocess.CalledProcessError as e:
        # CalledProcessError.__str__ already produces a significant error message
        if raise_on_error:
            log.error(e)
            raise
        else:
            log.warning(e)
            return e.output if hasattr(e, "output") else ""
    except OSError as e:
        log.error("Unable to execute the command %s. Failed with exception: %s", command, e)
        raise


def get_cloudformation_stack_parameters(region, proxy_config, stack_name):
    try:
        cfn_client = boto3.client("cloudformation", region_name=region, config=proxy_config)
        response = cfn_client.describe_stacks(StackName=stack_name)
        parameters = {}
        for parameter in response["Stacks"][0]["Parameters"]:
            parameters[parameter["ParameterKey"]] = parameter["ParameterValue"]

        return parameters
    except Exception as e:
        log.error("Failed when retrieving stack parameters for stack %s with exception %s", stack_name, e)
        raise


def _read_cfnconfig():
    """
    Read configuration file.

    :return: a dictionary containing the configuration parameters
    """
    cfnconfig_params = {}
    cfnconfig_file = "/opt/parallelcluster/cfnconfig"
    log.info("Reading %s", cfnconfig_file)
    with open(cfnconfig_file) as f:
        for kvp in f:
            key, value = kvp.partition("=")[::2]
            cfnconfig_params[key.strip()] = value.strip()
    return cfnconfig_params


def _get_vcpus_from_pricing_file(region, proxy_config, instance_type):
    """
    Read pricing file and get number of vcpus for the given instance type.

    :return: the number of vcpus or -1 if the instance type cannot be found
    """
    instances = _fetch_pricing_file(region, proxy_config)

    return _get_vcpus_by_instance_type(instances, instance_type)


def get_instance_properties(region, proxy_config, instance_type):
    """
    Get instance properties for the given instance type, according to the cfn_scheduler_slots configuration parameter.

    :return: a dictionary containing the instance properties. E.g. {'slots': <slots>}
    """
    # get vcpus from the pricing file
    vcpus = _get_vcpus_from_pricing_file(region, proxy_config, instance_type)

    try:
        cfnconfig_params = _read_cfnconfig()
        cfn_scheduler_slots = cfnconfig_params["cfn_scheduler_slots"]
    except KeyError:
        log.error("Required config parameter 'cfn_scheduler_slots' not found in cfnconfig file. Assuming 'vcpus'")
        cfn_scheduler_slots = "vcpus"

    if cfn_scheduler_slots == "cores":
        log.info("Instance %s will use number of cores as slots based on configuration." % instance_type)
        slots = -(-vcpus // 2)

    elif cfn_scheduler_slots == "vcpus":
        log.info("Instance %s will use number of vcpus as slots based on configuration." % instance_type)
        slots = vcpus

    elif cfn_scheduler_slots.isdigit():
        slots = int(cfn_scheduler_slots)
        log.info("Instance %s will use %s slots based on configuration." % (instance_type, slots))

        if slots <= 0:
            log.error(
                "cfn_scheduler_slots config parameter '{0}' must be greater than 0. Assuming 'vcpus'".format(
                    cfn_scheduler_slots
                )
            )
            slots = vcpus
    else:
        log.error("cfn_scheduler_slots config parameter '%s' is invalid. Assuming 'vcpus'" % cfn_scheduler_slots)
        slots = vcpus

    log.info("Number of slots computed for instance %s: %d", instance_type, slots)
    return {"slots": slots}


@retry(stop_max_attempt_number=3, wait_fixed=5000)
def _fetch_pricing_file(region, proxy_config):
    """
    Download pricing file.

    :param region: AWS Region
    :param proxy_config: Proxy Configuration
    :raise Exception if unable to download the pricing file.
    """
    bucket_name = "%s-aws-parallelcluster" % region
    try:
        s3 = boto3.resource("s3", region_name=region, config=proxy_config)
        instances_file_content = s3.Object(bucket_name, "instances/instances.json").get()["Body"].read()
        return json.loads(instances_file_content)
    except Exception as e:
        log.critical(
            "Could not load instance mapping file from S3 bucket {0}. Failed with exception: {1}".format(bucket_name, e)
        )
        raise


def _get_vcpus_by_instance_type(instances, instance_type):
    """
    Get vcpus for the given instance type from the pricing file.

    :param instances: dictionary conatining the content of the instances file
    :param instance_type: The instance type to search for
    :return: the number of vcpus for the given instance type
    :raise CriticalError if unable to find the given instance or whatever error.
    """
    try:
        vcpus = int(instances[instance_type]["vcpus"])
        log.info("Instance %s has %s vcpus." % (instance_type, vcpus))
        return vcpus
    except KeyError:
        error_msg = "Unable to get vcpus from instances file. Instance type {0} not found.".format(instance_type)
        log.critical(error_msg)
        raise CriticalError(error_msg)
    except Exception as e:
        error_msg = "Unable to get vcpus for the instance type {0} from file. Failed with exception {1}".format(
            instance_type, e
        )
        log.critical(error_msg)
        raise CriticalError(error_msg)


@retry(stop_max_attempt_number=3, wait_fixed=5000)
def get_compute_instance_type(region, proxy_config, stack_name, fallback):
    try:
        parameters = get_cloudformation_stack_parameters(region, proxy_config, stack_name)
        return parameters["ComputeInstanceType"]
    except Exception:
        if fallback:
            return fallback
        raise
