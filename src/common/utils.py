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
import collections
import itertools
import json
import logging
import os
import pwd
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from enum import Enum

import boto3
import requests
from botocore.exceptions import ClientError
from common.time_utils import seconds
from retrying import retry

log = logging.getLogger(__name__)


class CriticalError(Exception):
    """Critical error for the daemon."""

    pass


class EventType(Enum):
    ADD = "ADD"
    REMOVE = "REMOVE"


Host = collections.namedtuple("Host", ["instance_id", "hostname", "slots", "gpus"])
UpdateEvent = collections.namedtuple("UpdateEvent", ["action", "message", "host"])


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


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=seconds(0.5), wait_exponential_max=seconds(10))
def get_asg_settings(region, proxy_config, asg_name):
    try:
        asg_client = boto3.client("autoscaling", region_name=region, config=proxy_config)
        asg = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name]).get("AutoScalingGroups")[0]
        min_size = asg.get("MinSize")
        desired_capacity = asg.get("DesiredCapacity")
        max_size = asg.get("MaxSize")

        log.info("ASG min/desired/max: %d/%d/%d" % (min_size, desired_capacity, max_size))
        return min_size, desired_capacity, max_size
    except Exception as e:
        log.error("Failed when retrieving data for ASG %s with exception %s", asg_name, e)
        raise


def check_command_output(
    command, env=None, raise_on_error=True, execute_as_user=None, log_error=True, timeout=60, shell=False
):
    """
    Execute shell command and retrieve command output.

    :param command: command to execute
    :param env: a dictionary containing environment variables
    :param raise_on_error: True to raise subprocess.CalledProcessError on errors
    :param execute_as_user: the user executing the command
    :param log_error: control whether to log or not an error
    :return: the command output
    :raise: subprocess.CalledProcessError if the command fails
    """
    if isinstance(command, str) and not shell:
        command = shlex.split(command)
    # A nosec comment is appended to the following line in order to disable the B602 check.
    # This check is disabled for the following reasons:
    # - Some callers (e.g., common slurm commands) require the use of `shell=True`.
    # - All values passed as the command arg are constructed from known inputs.
    result = _run_command(  # nosec
        lambda _command, _env, _preexec_fn: subprocess.run(
            _command,
            env=_env,
            preexec_fn=_preexec_fn,
            timeout=timeout,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            shell=shell,
        ),
        command,
        env,
        raise_on_error,
        execute_as_user,
        log_error,
    )

    return result.stdout if hasattr(result, "stdout") else ""


def run_command(command, env=None, raise_on_error=True, execute_as_user=None, log_error=True, timeout=60, shell=False):
    """
    Execute shell command.

    :param command: command to execute
    :param env: a dictionary containing environment variables
    :param raise_on_error: True to raise subprocess.CalledProcessError on errors
    :param log_error: control whether to log or not an error
    :raise: subprocess.CalledProcessError if the command fails
    """
    if isinstance(command, str) and not shell:
        command = shlex.split(command)
    # A nosec comment is appended to the following line in order to disable the B602 check.
    # This check is disabled for the following reasons:
    # - Some callers (e.g., common slurm commands) require the use of `shell=True`.
    # - All values passed as the command arg are constructed from known inputs.
    _run_command(  # nosec
        lambda _command, _env, _preexec_fn: subprocess.run(
            _command,
            env=_env,
            preexec_fn=_preexec_fn,
            timeout=timeout,
            check=True,
            encoding="utf-8",
            shell=shell,
        ),
        command,
        env,
        raise_on_error,
        execute_as_user,
        log_error,
    )


def _demote(user_uid, user_gid):
    def set_ids():
        os.setgid(user_gid)
        os.setuid(user_uid)

    return set_ids


def _run_command(command_function, command, env=None, raise_on_error=True, execute_as_user=None, log_error=True):
    try:
        if env is None:
            env = {}

        env.update(os.environ.copy())
        if execute_as_user:
            log.debug("Executing command as user '{0}': {1}".format(execute_as_user, command))
            pw_record = pwd.getpwnam(execute_as_user)
            user_uid = pw_record.pw_uid
            user_gid = pw_record.pw_gid
            preexec_fn = _demote(user_uid, user_gid)
            return command_function(command, env, preexec_fn)
        else:
            log.debug("Executing command: %s" % command)
            return command_function(command, env, None)
    except subprocess.CalledProcessError as e:
        # CalledProcessError.__str__ already produces a significant error message
        if raise_on_error:
            if log_error:
                log.error(e)
            raise
        else:
            if log_error:
                log.warning(e)
            return e
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


@retry(
    stop_max_attempt_number=5,
    wait_exponential_multiplier=5000,
    retry_on_exception=lambda exception: isinstance(exception, ClientError)
    and exception.response.get("Error").get("Code") == "RequestLimitExceeded",
)
def _fetch_instance_info(region, proxy_config, instance_type):
    """
    Fetch instance type information from EC2's DescribeInstanceTypes API.

    :param region: AWS region
    :param proxy_config: proxy configuration
    :param instance_type: instance type to get info for
    :return: dict with the same format as those returned in the "InstanceTypes" array of the
             object returned by DescribeInstanceTypes
    """
    emsg_format = "Error when calling DescribeInstanceTypes for instance type {instance_type}: {exception_message}"
    ec2_client = boto3.client("ec2", region_name=region, config=proxy_config)
    try:
        return ec2_client.describe_instance_types(InstanceTypes=[instance_type]).get("InstanceTypes")[0]
    except ClientError as client_error:
        log.critical(
            emsg_format.format(
                instance_type=instance_type, exception_message=client_error.response.get("Error").get("Message")
            )
        )
        raise  # NOTE: raising ClientError is necessary to trigger retries
    except Exception as exception:
        emsg = emsg_format.format(instance_type=instance_type, exception_message=exception)
        log.critical(emsg)
        raise CriticalError(emsg)


def _get_instance_info(region, proxy_config, instance_type, additional_instance_types_data=None):
    """
    Call the DescribeInstanceTypes to get number of vcpus and gpus for the given instance type.

    :return: (the number of vcpus or -1 if the instance type cannot be found,
                number of gpus or None if the instance does not have gpu)
    """
    instance_info = None

    # First attempt to describe the instance is from configuration data, if present
    if additional_instance_types_data:
        instance_info = additional_instance_types_data.get(instance_type, None)

    # If no data is provided from configuration we retrieve it from ec2
    if not instance_info:
        log.debug("Fetching info for instance_type {0}".format(instance_type))
        instance_info = _fetch_instance_info(region, proxy_config, instance_type)
        log.debug("Received the following information for instance type {0}: {1}".format(instance_type, instance_info))

    return _get_vcpus_from_instance_info(instance_info), _get_gpus_from_instance_info(instance_info)


def get_instance_properties(region, proxy_config, instance_type, additional_instance_types_data=None):
    """
    Get instance properties for the given instance type, according to the cfn_scheduler_slots configuration parameter.

    :return: a dictionary containing the instance properties. E.g. {'slots': slots, 'gpus': gpus}
    """
    # Caching mechanism to avoid repetitively retrieving info from pricing file
    if not hasattr(get_instance_properties, "cache"):
        get_instance_properties.cache = {}

    if instance_type not in get_instance_properties.cache:
        # get vcpus and gpus from the pricing file, gpus = 0 if instance does not have GPU
        vcpus, gpus = _get_instance_info(region, proxy_config, instance_type, additional_instance_types_data)

        try:
            cfnconfig_params = _read_cfnconfig()
            # cfn_scheduler_slots could be vcpus/cores based on disable_hyperthreading = false/true
            # cfn_scheduler_slots could be vcpus/cores/integer based on extra json
            # {'cfn_scheduler_slots' = 'vcpus'/'cores'/integer}
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

        log.info("Added instance type: {0} to get_instance_properties cache".format(instance_type))
        get_instance_properties.cache[instance_type] = {"slots": slots, "gpus": int(gpus)}

    log.info("Retrieved instance properties: {0}".format(get_instance_properties.cache[instance_type]))
    return get_instance_properties.cache[instance_type]


def _get_vcpus_from_instance_info(instance_info):
    """
    Return the number of vCPUs the instance described by instance_info has.

    :param instance_info: dict as returned _fetch_instance_info
    :return: number of vCPUs for the instance type
    """
    try:
        return int(instance_info.get("VCpuInfo", {}).get("DefaultVCpus"))
    except Exception as exception:
        error_msg = "Unable to get vcpus for the instance type {0}. Failed with exception {1}".format(
            instance_info.get("InstanceType") if hasattr(instance_info, "get") else None, exception
        )
        log.critical(error_msg)
        raise CriticalError(error_msg)


def _get_gpus_from_instance_info(instance_info):
    """
    Return the number of GPUs the instance described by instance_info has.

    :param instance_info: dict as returned _fetch_instance_info
    :return: number of GPUs for the instance type
    """
    try:
        return sum([gpu_entry.get("Count", 0) for gpu_entry in instance_info.get("GpuInfo", {}).get("Gpus", [])])
    except Exception as exception:
        error_msg = "Unable to get gpus for the instance type {0}. Failed with exception {1}".format(
            instance_info.get("InstanceType") if hasattr(instance_info, "get") else None, exception
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


def sleep_remaining_loop_time(total_loop_time, loop_start_time=None):
    end_time = datetime.now(tz=timezone.utc)
    if not loop_start_time:
        loop_start_time = end_time
    # Always convert the received loop_start_time to utc timezone. This is so that we never rely on the system local
    # time and risk to compare naive datatime instances with localized ones
    loop_start_time = loop_start_time.astimezone(tz=timezone.utc)
    time_delta = (end_time - loop_start_time).total_seconds()
    if 0 <= time_delta < total_loop_time:
        time.sleep(total_loop_time - time_delta)


def retrieve_max_cluster_size(region, proxy_config, asg_name, fallback):
    try:
        _, _, max_size = get_asg_settings(region, proxy_config, asg_name)
        return max_size
    except Exception as e:
        if fallback:
            logging.warning("Failed when retrieving max cluster size with error %s. Returning fallback value", e)
            return fallback
        error_msg = "Unable to retrieve max size from ASG. Failed with error {0}. No fallback value available.".format(
            e
        )
        log.critical(error_msg)
        raise CriticalError(error_msg)


def grouper(iterable, n):
    """Slice iterable into chunks of size n."""
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def load_additional_instance_types_data(config, section):
    """Load instance types data from configuration, if set; an empty dict is returned otherwise."""
    instance_types_data = {}
    if config.has_option(section, "instance_types_data"):
        instance_types_data_str = config.get(section, "instance_types_data")
        if instance_types_data_str:
            try:
                instance_types_data_str = str(instance_types_data_str).strip()

                # Load json value if not empty
                if instance_types_data_str:
                    instance_types_data = json.loads(instance_types_data_str)

                # Fallback to empty dict if value is None
                if not instance_types_data:
                    instance_types_data = {}

                log.info(
                    "Additional instance types data loaded for instance types '{0}': {1}".format(
                        instance_types_data.keys(), instance_types_data
                    )
                )
            except Exception as e:
                raise CriticalError("Error loading instance types data from configuration: {0}".format(e))
    return instance_types_data


def get_metadata(metadata_path):
    """
    Get EC2 instance metadata.

    :param metadata_path: the metadata relative path
    :return the metadata value.
    """
    try:
        token = requests.put(
            "http://169.254.169.254/latest/api/token", headers={"X-aws-ec2-metadata-token-ttl-seconds": "300"}
        )
        headers = {}
        if token.status_code == requests.codes.ok:
            headers["X-aws-ec2-metadata-token"] = token.content
        metadata_url = "http://169.254.169.254/latest/meta-data/{0}".format(metadata_path)
        metadata_value = requests.get(metadata_url, headers=headers).text
    except Exception as e:
        error_msg = "Unable to get {0} metadata. Failed with exception: {1}".format(metadata_path, e)
        log.critical(error_msg)
        raise CriticalError(error_msg)

    log.debug("%s=%s", metadata_path, metadata_value)
    return metadata_value
