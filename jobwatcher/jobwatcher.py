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
pricing_file = '/opt/cfncluster/instances.json'
cfnconfig_file = '/opt/cfncluster/cfnconfig'


def load_scheduler_module(scheduler):
    scheduler = 'jobwatcher.plugins.' + scheduler
    _scheduler = __import__(scheduler)
    _scheduler = sys.modules[scheduler]

    log.debug("scheduler=%s" % repr(_scheduler))

    return _scheduler


def get_asg_name(stack_name, region, proxy_config):
    asg_conn = boto3.client('autoscaling', region_name=region, config=proxy_config)
    asg_name = ""
    no_asg = True

    while no_asg:
        try:
            r = asg_conn.describe_tags(Filters=[{'Name': 'value', 'Values': [stack_name]}])
            asg_name = r.get('Tags')[0].get('ResourceId')
            no_asg = False
        except IndexError as e:
            log.error("No asg found for cluster %s" % stack_name)
            time.sleep(30)

    return asg_name


def read_cfnconfig():
    cfnconfig_params = {}
    with open(cfnconfig_file) as f:
        for kvp in f:
            key, value = kvp.partition('=')[::2]
            cfnconfig_params[key.strip()] = value.strip()
    return cfnconfig_params


def get_vcpus_from_pricing_file(instance_type):
    with open(pricing_file) as f:
        instances = json.load(f)
        try:
            vcpus = int(instances[instance_type]["vcpus"])
            log.info("Instance %s has %s vcpus." % (instance_type, vcpus))
            return vcpus
        except KeyError as e:
            log.error("Instance %s not found in file %s." % (instance_type, pricing_file))
            exit(1)


def get_instance_properties(instance_type):
    cfnconfig_params = read_cfnconfig()
    try:
        cfn_scheduler_slots = cfnconfig_params["cfn_scheduler_slots"]
        slots = 0
        vcpus = get_vcpus_from_pricing_file(instance_type)

        if cfn_scheduler_slots == "cores":
            log.info("Instance %s will use number of cores as slots based on configuration." % instance_type)
            slots = -(-vcpus//2)
        elif cfn_scheduler_slots == "vcpus":
            log.info("Instance %s will use number of vcpus as slots based on configuration." % instance_type)
            slots = vcpus
        elif cfn_scheduler_slots.isdigit():
            slots = int(cfn_scheduler_slots)
            log.info("Instance %s will use %s slots based on configuration." % (instance_type, slots))

        if not slots > 0:
            log.critical("cfn_scheduler_slots config parameter '%s' was invalid" % cfn_scheduler_slots)
            exit(1)

        return {'slots': slots}

    except KeyError:
        log.error("Required config parameter 'cfn_scheduler_slots' not found in file %s." % cfnconfig_file)
        exit(1)


def fetch_pricing_file(proxy_config, cfncluster_dir, region):
    s3 = boto3.resource('s3', region_name=region, config=proxy_config)
    try:
        if not os.path.exists(cfncluster_dir):
            os.makedirs(cfncluster_dir)
    except OSError as ex:
        log.critical('Could not create directory %s. Failed with exception: %s' % (cfncluster_dir, ex))
        raise
    bucket_name = '%s-cfncluster' % region
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.download_file('instances/instances.json', '%s/instances.json' % cfncluster_dir)
    except ClientError as e:
        log.critical("Could not save instance mapping file %s/instances.json from S3 bucket %s. Failed with exception: %s" % (cfncluster_dir, bucket_name, e))
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
    cfncluster_dir = config.get('jobwatcher', 'cfncluster_dir')
    _proxy = config.get('jobwatcher', 'proxy')
    proxy_config = Config()

    if not _proxy == "NONE":
        proxy_config = Config(proxies={'https': _proxy})
        log.info("Configured proxy is: %s" % _proxy)

    try:
        asg_name = config.get('jobwatcher', 'asg_name')
    except ConfigParser.NoOptionError:
        asg_name = get_asg_name(stack_name, region, proxy_config)
        config.set('jobwatcher', 'asg_name', asg_name)
        log.info("Saving asg_name %s in the config file %s" % (asg_name, _configfilename))
        with open(_configfilename, 'w') as configfile:
            config.write(configfile)

    # fetch the pricing file on startup
    fetch_pricing_file(proxy_config, cfncluster_dir, region)

    # load scheduler
    s = load_scheduler_module(scheduler)

    while True:
        # get the number of vcpu's per compute instance
        instance_properties = get_instance_properties(instance_type)

        # Get number of nodes requested
        pending = s.get_required_nodes(instance_properties)

        # Get number of nodes currently
        running = s.get_busy_nodes(instance_properties)

        log.info("%s jobs pending; %s jobs running" % (pending, running))

        if pending > 0:
            # connect to asg
            asg_conn = boto3.client('autoscaling', region_name=region, config=proxy_config)

            # get current limits
            asg = asg_conn.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name]).get('AutoScalingGroups')[0]

            min = asg.get('MinSize')
            current_desired = asg.get('DesiredCapacity')
            max = asg.get('MaxSize')
            log.info("min/desired/max %d/%d/%d" % (min, current_desired, max))
            log.info("Nodes requested %d, Nodes running %d" % (pending, running))

            # check to make sure it's in limits
            desired = running + pending
            if desired > max:
                log.info("%d requested nodes is greater than max %d. Requesting max %d." % (desired, max, max))
                asg_conn.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=max)
            elif desired <= current_desired:
                log.info("%d nodes desired %d nodes in asg. Noop" % (desired, current_desired))
            else:
                log.info("Setting desired to %d nodes, requesting %d more nodes from asg." % (desired, desired - current_desired))
                asg_conn.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=desired)

        time.sleep(60)


if __name__ == '__main__':
    main()
