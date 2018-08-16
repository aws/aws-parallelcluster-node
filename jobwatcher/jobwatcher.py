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

def loadSchedulerModule(scheduler):
    scheduler = 'jobwatcher.plugins.' + scheduler
    _scheduler = __import__(scheduler)
    _scheduler = sys.modules[scheduler]

    log.debug("scheduler=%s" % repr(_scheduler))

    return _scheduler

def get_instance_properties(instance_type):
    with open(pricing_file) as f:
        instances = json.load(f)
        try:
            slots = int(instances[instance_type]["vcpus"])
            log.info("Instance %s has %s slots." % (instance_type, slots))
            return {'slots': slots}
        except KeyError as e:
            log.error("Instance %s not found in file %s." % (instance_type, pricing_file))
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
        log.critical("Could not save instance mapping file %s from S3 bucket %s. Failed with exception: %s" % (cfncluster_file, bucket_name, e))
        raise

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s'
    )

    config = ConfigParser.RawConfigParser()
    config.read('/etc/jobwatcher.cfg')
    if config.has_option('jobwatcher', 'loglevel'):
        lvl = logging._levelNames[config.get('jobwatcher', 'loglevel')]
        logging.getLogger().setLevel(lvl)
    region = config.get('jobwatcher', 'region')
    asg_name = config.get('jobwatcher', 'asg_name')
    scheduler = config.get('jobwatcher', 'scheduler')
    instance_type = config.get('jobwatcher', 'compute_instance_type')
    cfncluster_dir = config.get('jobwatcher', 'cfncluster_dir')
    _proxy = config.get('jobwatcher', 'proxy')
    proxy_config = Config()

    if not _proxy == "NONE":
        proxy_config = Config(proxies={'https': _proxy})

    # fetch the pricing file on startup
    fetch_pricing_file(proxy_config, cfncluster_dir, region)

    # load scheduler
    s = loadSchedulerModule(scheduler)

    while True:
        # get the number of vcpu's per compute instance
        instance_properties = get_instance_properties(instance_type)

        # Get number of nodes requested
        pending = s.get_required_nodes(instance_properties)

        # Get number of nodes currently
        running = s.get_busy_nodes(instance_properties)

        # connect to asg
        asg_conn = boto3.client('autoscaling', region_name=region)

        # get current limits
        asg = asg_conn.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])\
                .get('AutoScalingGroups')[0]

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
