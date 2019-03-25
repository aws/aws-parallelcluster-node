#!/usr/bin/env python

# Copyright 2013-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import ConfigParser
import collections
import json
import logging
import os
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from retrying import retry

from common.utils import CriticalError, get_asg_name, load_module, get_asg_settings

log = logging.getLogger(__name__)


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
            key, value = kvp.partition('=')[::2]
            cfnconfig_params[key.strip()] = value.strip()
    return cfnconfig_params


@retry(stop_max_attempt_number=3, wait_fixed=5000)
def _get_vcpus_from_pricing_file(config):
    """
    Read pricing file and get number of vcpus for the given instance type.

    :param config: JobwatcherConfiguration object
    :return: the number of vcpus or -1 if the instance type cannot be found
    """
    _create_data_dir(config.pcluster_dir)

    folder = config.pcluster_dir
    if not folder.endswith("/"):
        folder += "/"
    pricing_file = folder + "instances.json"
    _fetch_pricing_file(pricing_file, config.region, config.proxy_config)

    return _get_vcpus_by_instance_type(pricing_file, config.instance_type)


def _get_instance_properties(config):
    """
    Get instance properties for the given instance type, according to the cfn_scheduler_slots configuration parameter.

    :param config: JobwatcherConfiguration object
    :return: a dictionary containing the instance properties. E.g. {'slots': <slots>}
    """
    # get vcpus from the pricing file
    vcpus = _get_vcpus_from_pricing_file(config)

    try:
        cfnconfig_params = _read_cfnconfig()
        cfn_scheduler_slots = cfnconfig_params["cfn_scheduler_slots"]
    except KeyError:
        log.error("Required config parameter 'cfn_scheduler_slots' not found in cfnconfig file. Assuming 'vcpus'")
        cfn_scheduler_slots = "vcpus"

    if cfn_scheduler_slots == "cores":
        log.info("Instance %s will use number of cores as slots based on configuration." % config.instance_type)
        slots = -(-vcpus//2)

    elif cfn_scheduler_slots == "vcpus":
        log.info("Instance %s will use number of vcpus as slots based on configuration." % config.instance_type)
        slots = vcpus

    elif cfn_scheduler_slots.isdigit():
        slots = int(cfn_scheduler_slots)
        log.info("Instance %s will use %s slots based on configuration." % (config.instance_type, slots))

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

    return {'slots': slots}


def _create_data_dir(pcluster_dir):
    """
    Create jobwatcher data dir.

    :param pcluster_dir: the folder to create.
    :raise CriticalError if unable to create the folder.
    """
    try:
        if not os.path.exists(pcluster_dir):
            os.makedirs(pcluster_dir)
    except OSError as e:
        log.critical("Could not create directory {0}. Failed with exception: {1}".format(pcluster_dir, e))
        raise


def _fetch_pricing_file(pricing_file, region, proxy_config):
    """
    Download pricing file.

    :param pricing_file: pricing file path
    :param region: AWS Region
    :param proxy_config: Proxy Configuration
    :raise ClientError if unable to download the pricing file.
    """
    bucket_name = '%s-aws-parallelcluster' % region
    try:
        s3 = boto3.resource('s3', region_name=region, config=proxy_config)
        s3.Bucket(bucket_name).download_file('instances/instances.json', pricing_file)
    except ClientError as e:
        log.critical("Could not save instance mapping file {0} from S3 bucket {1}. Failed with exception: {2}".format(
            pricing_file, bucket_name, e)
        )
        raise


def _get_vcpus_by_instance_type(pricing_file, instance_type):
    """
    Get vcpus for the given instance type from the pricing file.

    :param pricing_file: pricing file path
    :param instance_type: The instance type to search for
    :return: the number of vcpus for the given instance type
    :raise CriticalError if unable to find the given instance or whatever error.
    """
    try:
        # read vcpus value from file
        with open(pricing_file) as f:
            instances = json.load(f)
            vcpus = int(instances[instance_type]["vcpus"])
            log.info("Instance %s has %s vcpus." % (instance_type, vcpus))
            return vcpus
    except KeyError:
        error_msg = "Unable to get vcpus from file {0}. Instance type {1} not found.".format(
            pricing_file, instance_type
        )
        log.critical(error_msg)
        raise CriticalError(error_msg)
    except Exception:
        error_msg = "Unable to get vcpus for the instance type {0} from file {1}".format(instance_type, pricing_file)
        log.critical(error_msg)
        raise CriticalError(error_msg)


JobwatcherConfig = collections.namedtuple(
    "JobwatcherConfig", ["region", "scheduler", "stack_name", "instance_type", "pcluster_dir", "proxy_config"]
)


def _get_config():
    """
    Get configuration from config file.

    :return: configuration parameters
    """
    config_file = "/etc/jobwatcher.cfg"
    log.info("Reading %s", config_file)

    config = ConfigParser.RawConfigParser()
    config.read(config_file)
    if config.has_option("jobwatcher", "loglevel"):
        lvl = logging._levelNames[config.get("jobwatcher", "loglevel")]
        logging.getLogger().setLevel(lvl)

    region = config.get("jobwatcher", "region")
    scheduler = config.get("jobwatcher", "scheduler")
    stack_name = config.get("jobwatcher", "stack_name")
    instance_type = config.get("jobwatcher", "compute_instance_type")
    pcluster_dir = config.get("jobwatcher", "cfncluster_dir")

    _proxy = config.get("jobwatcher", "proxy")
    proxy_config = Config()
    if _proxy != "NONE":
        proxy_config = Config(proxies={"https": _proxy})

    log.info(
        "Configured parameters: region=%s scheduler=%s stack_name=%s instance_type=%s pcluster_dir=%s proxy=%s",
        region, scheduler, stack_name, instance_type, pcluster_dir, _proxy
    )
    return JobwatcherConfig(region, scheduler, stack_name, instance_type, pcluster_dir, proxy_config)


def _poll_scheduler_status(config, asg_name, scheduler_module, instance_properties):
    """
    Verify scheduler status and ask the ASG new nodes, if required.

    :param config: JobwatcherConfig object
    :param asg_name: ASG name
    :param scheduler_module: scheduler module
    :param instance_properties: instance properties
    """
    while True:
        # Get number of nodes requested
        pending = scheduler_module.get_required_nodes(instance_properties)

        if pending < 0:
            log.critical("Error detecting number of required nodes. The cluster will not scale up.")

        elif pending == 0:
            log.info("There are no pending jobs. Noop.")

        else:
            # Get current number of nodes
            running = scheduler_module.get_busy_nodes(instance_properties)
            log.info("%d nodes requested, %d nodes running", pending, running)

            # get current limits
            _, current_desired, max_size = get_asg_settings(config.region, config.proxy_config, asg_name, log)

            # Check to make sure requested number of instances is within ASG limits
            required = running + pending
            if required <= current_desired:
                log.info("%d nodes required, %d nodes in asg. Noop" % (required, current_desired))
            else:
                if required > max_size:
                    log.info(
                        "The number of required nodes %d is greater than max %d. Requesting max %d."
                        % (required, max_size, max_size)
                    )
                else:
                    log.info(
                        "Setting desired to %d nodes, requesting %d more nodes from asg."
                        % (required, required - current_desired)
                    )
                requested = min(required, max_size)

                # update ASG
                asg_client = boto3.client('autoscaling', region_name=config.region, config=config.proxy_config)
                asg_client.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=requested)

        time.sleep(60)


@retry(wait_fixed=60000)
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s")
    log.info("jobwatcher startup")
    try:
        config = _get_config()
        asg_name = get_asg_name(config.stack_name, config.region, config.proxy_config, log)
        instance_properties = _get_instance_properties(config)

        scheduler_module = load_module("jobwatcher.plugins." + config.scheduler)

        _poll_scheduler_status(config, asg_name, scheduler_module, instance_properties)
    except Exception as e:
        log.critical("An unexpected error occurred: %s", e)
        raise


if __name__ == '__main__':
    main()
