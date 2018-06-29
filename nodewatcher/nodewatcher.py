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

__author__ = 'dougalb'

from datetime import datetime
import urllib2
import os
import time
import sys
import tempfile
import logging
import boto3
import ConfigParser
from botocore.config import Config

log = logging.getLogger(__name__)

def getConfig(instance_id):
    log.debug('reading /etc/nodewatcher.cfg')

    config = ConfigParser.RawConfigParser()
    config.read('/etc/nodewatcher.cfg')
    if config.has_option('nodewatcher', 'loglevel'):
        lvl = logging._levelNames[config.get('nodewatcher', 'loglevel')]
        logging.getLogger().setLevel(lvl)
    _region = config.get('nodewatcher', 'region')
    _scheduler = config.get('nodewatcher', 'scheduler')
    _proxy = config.get('nodewatcher', 'proxy')
    proxy_config = Config()

    if not _proxy == "NONE":
        proxy_config = Config(proxies={'https': _proxy})

    try:
        _asg = config.get('nodewatcher', 'asg')
    except ConfigParser.NoOptionError:
        ec2 = boto3.resource('ec2', region_name=_region, config=proxy_config)

        instances = ec2.instances.filter(InstanceIds=[instance_id])
        instance = next(iter(instances or []), None)
        _asg = filter(lambda tag: tag.get('Key') == 'aws:autoscaling:groupName', instance.tags)[0].get('Value')
        log.debug("discovered asg: %s" % _asg)
        config.set('nodewatcher', 'asg', _asg)

        tup = tempfile.mkstemp(dir=os.getcwd())
        fd = os.fdopen(tup[0], 'w')
        config.write(fd)
        fd.close

        os.rename(tup[1], 'nodewatcher.cfg')

    log.debug("region=%s asg=%s scheduler=%s prox_config=%s" % (_region, _asg, _scheduler, proxy_config))
    return _region, _asg, _scheduler, proxy_config

def getHourPercentile(instance_id, ec2):
    instances = ec2.instances.filter(InstanceIds=[instance_id])
    instance = next(iter(instances or []), None)
    _launch_time = instance.launch_time.replace(tzinfo=None)
    _current_time = datetime.utcnow()
    _delta = _current_time - _launch_time
    _delta_in_hours = _delta.seconds / 3600.0
    _hour_percentile = (_delta_in_hours % 1) * 100

    log.debug("launch=%s delta=%s percentile=%s" % (_launch_time, _delta,
                                                    _hour_percentile))

    return _hour_percentile

def getInstanceId():

    try:
        _instance_id = urllib2.urlopen("http://169.254.169.254/latest/meta-data/instance-id").read()
    except urllib2.URLError:
        log.critical('Unable to get instance-id from metadata')
        sys.exit(1)

    log.debug("instance_id=%s" % _instance_id)

    return _instance_id

def getHostname():

    try:
        _hostname = urllib2.urlopen("http://169.254.169.254/latest/meta-data/local-hostname").read()
    except urllib2.URLError:
        log.critical('Unable to get hostname from metadata')
        sys.exit(1)

    log.debug("hostname=%s" % _hostname)

    return _hostname

def loadSchedulerModule(scheduler):
    scheduler = 'nodewatcher.plugins.' + scheduler
    _scheduler = __import__(scheduler)
    _scheduler = sys.modules[scheduler]

    log.debug("scheduler=%s" % repr(_scheduler))

    return _scheduler

def getJobs(s,hostname):

    _jobs = s.getJobs(hostname)

    log.debug("jobs=%s" % _jobs)

    return _jobs

def lockHost(s,hostname,unlock=False):
    log.debug("%s %s" % (unlock and "unlocking" or "locking",
                         hostname))

    _r = s.lockHost(hostname, unlock)

    time.sleep(15) # allow for some settling

    return _r

def selfTerminate(asg_name, asg_conn, instance_id):
    if not maintainSize(asg_name, asg_conn):
        log.info("terminating %s" % instance_id)
        asg_conn.terminate_instance_in_auto_scaling_group(InstanceId=instance_id, ShouldDecrementDesiredCapacity=True)

def maintainSize(asg_name, asg_conn):
    asg = asg_conn.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name]) \
        .get('AutoScalingGroups')[0]
    _capacity = asg.get('DesiredCapacity')
    _min_size = asg.get('MinSize')
    log.info("capacity=%d min_size=%d" % (_capacity, _min_size))
    if _capacity > _min_size:
        log.debug('capacity greater then min size.')
        return False
    else:
        log.debug('capacity less then or equal to min size.')
        return True

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s'
    )
    log.info("nodewatcher startup")
    instance_id = getInstanceId()
    hostname = getHostname()
    region, asg_name, scheduler, proxy_config = getConfig(instance_id)

    s = loadSchedulerModule(scheduler)

    while True:
        time.sleep(60)
        ec2_conn = boto3.resource('ec2', region_name=region, config=proxy_config)
        asg_conn = boto3.client('autoscaling', region_name=region, config=proxy_config)
        hour_percentile = getHourPercentile(instance_id, ec2_conn)
        log.info('Percent of hour used: %d' % hour_percentile)

        if hour_percentile < 95:
            continue

        jobs = getJobs(s, hostname)
        if jobs == True:
            log.info('Instance has active jobs.')
        else:
            if maintainSize(asg_name, asg_conn):
                continue
            # avoid race condition by locking and verifying
            lockHost(s, hostname)
            jobs = getJobs(s, hostname)
            if jobs == True:
                log.info('Instance actually has active jobs.')
                lockHost(s, hostname, unlock=True)
                continue
            else:
                selfTerminate(asg_name, asg_conn, instance_id)

if __name__ == "__main__":
    main()
