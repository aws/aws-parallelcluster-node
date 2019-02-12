#!/usr/bin/env python2.6

# Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

__author__ = 'seaam'

import ConfigParser
import boto3
import os
import sys
import time
import logging
import json
from botocore.exceptions import ClientError
from botocore.config import Config

log = logging.getLogger(__name__)
pricing_file = '/opt/parallelcluster/instances.json'
cfnconfig_file = '/opt/parallelcluster/cfnconfig'


def _load_scheduler_module(scheduler):
    """
    Load scheduler module, containing scheduler specific functions.

    :param scheduler: scheduler name, it must corresponds to the <scheduler>.py file in the current folder.
    :return: the scheduler module
    """
    scheduler = 'jobwatcher.plugins.' + scheduler
    _scheduler = __import__(scheduler)
    _scheduler = sys.modules[scheduler]

    log.debug("scheduler=%s" % repr(_scheduler))

    return _scheduler


def _get_asg_name(stack_name, region, proxy_config):
    """
    Get autoscaling group name.

    :param stack_name: stack name to search for
    :param region: AWS region
    :param proxy_config: Proxy configuration
    :return: the ASG name
    """
    asg_conn = boto3.client('autoscaling', region_name=region, config=proxy_config)
    asg_name = ""
    no_asg = True

    while no_asg:
        try:
            r = asg_conn.describe_tags(Filters=[{'Name': 'value', 'Values': [stack_name]}])
            asg_name = r.get('Tags')[0].get('ResourceId')
            no_asg = False
        except IndexError:
            log.error("No asg found for cluster %s" % stack_name)
            time.sleep(30)

    return asg_name


def _read_cfnconfig():
    """
    Read configuration file.

    :return: a dictionary containing the configuration parameters
    """
    cfnconfig_params = {}
    with open(cfnconfig_file) as f:
        for kvp in f:
            key, value = kvp.partition('=')[::2]
            cfnconfig_params[key.strip()] = value.strip()
    return cfnconfig_params


def _get_vcpus_from_pricing_file(instance_type):
    """
    Read pricing file and get number of vcpus for the given instance type.

    :param instance_type: the instance type to search for.
    :return: the number of vcpus or -1 if the instance type cannot be found
    """
    with open(pricing_file) as f:
        instances = json.load(f)
        try:
            vcpus = int(instances[instance_type]["vcpus"])
            log.info("Instance %s has %s vcpus." % (instance_type, vcpus))
        except KeyError:
            log.error("Unable to get vcpus from file %s. Instance type %s not found." % (pricing_file, instance_type))
            vcpus = -1

        return vcpus


def _get_instance_properties(instance_type):
    """
    Get instance properties for the given instance type, according to the cfn_scheduler_slots configuration parameter.

    :param instance_type: instance type to search for
    :return: a dictionary containing the instance properties. E.g. {'slots': <slots>}
    """
    try:
        cfnconfig_params = _read_cfnconfig()
        cfn_scheduler_slots = cfnconfig_params["cfn_scheduler_slots"]
    except KeyError:
        log.error(
            "Required config parameter 'cfn_scheduler_slots' not found in file %s. Assuming 'vcpus'" % cfnconfig_file
        )
        cfn_scheduler_slots = "vcpus"

    vcpus = _get_vcpus_from_pricing_file(instance_type)

    if cfn_scheduler_slots == "cores":
        log.info("Instance %s will use number of cores as slots based on configuration." % instance_type)
        slots = -(-vcpus//2)

    elif cfn_scheduler_slots == "vcpus":
        log.info("Instance %s will use number of vcpus as slots based on configuration." % instance_type)
        slots = vcpus

    elif cfn_scheduler_slots.isdigit():
        slots = int(cfn_scheduler_slots)
        log.info("Instance %s will use %s slots based on configuration." % (instance_type, slots))

        if slots <= 0:
            log.error(
                "cfn_scheduler_slots config parameter '%s' must be greater than 0. "
                "Assuming 'vcpus'" % cfn_scheduler_slots
            )
            slots = vcpus
    else:
        log.error("cfn_scheduler_slots config parameter '%s' is invalid. Assuming 'vcpus'" % cfn_scheduler_slots)
        slots = vcpus

    if slots <= 0:
        log.critical("slots value is invalid. Setting it to 0.")
        slots = 0

    return {'slots': slots}


def _fetch_pricing_file(pcluster_dir, region, proxy_config):
    """
    Download pricing file.

    :param proxy_config: Proxy Configuration
    :param pcluster_dir: Parallelcluster configuration folder
    :param region: AWS Region
    """
    s3 = boto3.resource('s3', region_name=region, config=proxy_config)
    try:
        if not os.path.exists(pcluster_dir):
            os.makedirs(pcluster_dir)
    except OSError as ex:
        log.critical('Could not create directory %s. Failed with exception: %s' % (pcluster_dir, ex))
        raise
    bucket_name = '%s-aws-parallelcluster' % region
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.download_file('instances/instances.json', '%s/instances.json' % pcluster_dir)
    except ClientError as e:
        log.critical("Could not save instance mapping file %s/instances.json from S3 bucket %s. "
                     "Failed with exception: %s" % (pcluster_dir, bucket_name, e))
        raise


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s'
    )

    _configfilename = "/etc/jobwatcher.cfg"
    log.info("Reading configuration file %s" % _configfilename)
    config = ConfigParser.RawConfigParser()
    config.read(_configfilename)
    if config.has_option('jobwatcher', 'loglevel'):
        lvl = logging._levelNames[config.get('jobwatcher', 'loglevel')]
        logging.getLogger().setLevel(lvl)
    region = config.get('jobwatcher', 'region')
    scheduler = config.get('jobwatcher', 'scheduler')
    stack_name = config.get('jobwatcher', 'stack_name')
    instance_type = config.get('jobwatcher', 'compute_instance_type')
    pcluster_dir = config.get('jobwatcher', 'cfncluster_dir')
    _proxy = config.get('jobwatcher', 'proxy')
    proxy_config = Config()

    if not _proxy == "NONE":
        proxy_config = Config(proxies={'https': _proxy})
        log.info("Configured proxy is: %s" % _proxy)

    try:
        asg_name = config.get('jobwatcher', 'asg_name')
    except ConfigParser.NoOptionError:
        asg_name = _get_asg_name(stack_name, region, proxy_config)
        config.set('jobwatcher', 'asg_name', asg_name)
        log.info("Saving asg_name %s in the config file %s" % (asg_name, _configfilename))
        with open(_configfilename, 'w') as configfile:
            config.write(configfile)

    # fetch the pricing file on startup
    _fetch_pricing_file(pcluster_dir, region, proxy_config)

    # load scheduler
    s = _load_scheduler_module(scheduler)

    while True:
        # get the number of vcpu's per compute instance
        instance_properties = _get_instance_properties(instance_type)
        if instance_properties.get('slots') <= 0:
            log.critical("Error detecting number of slots per instance. The cluster will not scale up.")

        else:
            # Get number of nodes requested
            pending = s.get_required_nodes(instance_properties)

            if pending < 0:
                log.critical("Error detecting number of required nodes. The cluster will not scale up.")

            elif pending == 0:
                log.debug("There are no pending jobs. Noop.")

            else:
                # Get current number of nodes
                running = s.get_busy_nodes(instance_properties)
                log.info("%s jobs pending; %s jobs running" % (pending, running))

                # connect to asg
                asg_client = boto3.client('autoscaling', region_name=region, config=proxy_config)

                # get current limits
                asg = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name]).get('AutoScalingGroups')[0]

                min_size = asg.get('MinSize')
                current_desired = asg.get('DesiredCapacity')
                max_size = asg.get('MaxSize')
                log.info("min/desired/max %d/%d/%d" % (min_size, current_desired, max_size))
                log.info("%d nodes requested, %d nodes running" % (pending, running))

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
                    asg_client.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=requested)

        time.sleep(60)


if __name__ == '__main__':
    main()
