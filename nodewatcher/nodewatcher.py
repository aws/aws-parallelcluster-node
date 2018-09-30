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
from botocore.exceptions import ClientError
from botocore.config import Config
import json
import atexit
import errno


log = logging.getLogger(__name__)
_DATA_DIR = "/var/run/nodewatcher/"
_IDLETIME_FILE = _DATA_DIR + "node_idletime.json"
_CURRENT_IDLETIME = 'current_idletime'

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

    _scaledown_idletime = int(config.get('nodewatcher', 'scaledown_idletime'))
    _stack_name = config.get('nodewatcher', 'stack_name')
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

    log.debug("region=%s asg=%s scheduler=%s prox_config=%s idle_time=%s" % (_region, _asg, _scheduler, proxy_config, _scaledown_idletime))
    return _region, _asg, _scheduler, proxy_config, _scaledown_idletime, _stack_name

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

def hasJobs(s,hostname):

    _jobs = s.hasJobs(hostname)

    log.debug("jobs=%s" % _jobs)

    return _jobs

def hasPendingJobs(s):
    _has_pending_jobs, _error = s.hasPendingJobs()
    log.debug("has_pending_jobs=%s, error=%s" % (_has_pending_jobs, _error))
    return _has_pending_jobs, _error

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


def stackCreationComplete(stack_name, region, proxy_config):
    log.info('Checking for status of the stack %s' % stack_name)
    cfn_client = boto3.client('cloudformation', region_name=region, config=proxy_config)
    stacks = cfn_client.describe_stacks(StackName=stack_name)
    return stacks['Stacks'][0]['StackStatus'] == 'CREATE_COMPLETE'


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s'
    )
    log.info("nodewatcher startup")
    instance_id = getInstanceId()
    hostname = getHostname()
    region, asg_name, scheduler, proxy_config, idle_time, stack_name = getConfig(instance_id)


    s = loadSchedulerModule(scheduler)

    try:
        if not os.path.exists(_DATA_DIR):
            os.makedirs(_DATA_DIR)
    except OSError as ex:
        log.critical('Creating directory %s to persist current idle time failed with exception: %s '
                     % (_DATA_DIR, ex))
        raise

    if os.path.isfile(_IDLETIME_FILE):
        with open(_IDLETIME_FILE) as f:
            data = json.loads(f.read())
    else:
        data = {_CURRENT_IDLETIME: 0}

    stack_creation_complete = False
    while True:
        time.sleep(60)
        if not stack_creation_complete:
            stack_creation_complete = stackCreationComplete(stack_name, region, proxy_config)
            log.info('%s creation complete: %s' % (stack_name, stack_creation_complete))
            continue
        asg_conn = boto3.client('autoscaling', region_name=region, config=proxy_config)

        has_jobs = hasJobs(s, hostname)
        if has_jobs:
            log.info('Instance has active jobs.')
            data[_CURRENT_IDLETIME] = 0
        else:
            if maintainSize(asg_name, asg_conn):
                continue
            else:
                data[_CURRENT_IDLETIME] += 1
                log.info('Instance %s has no job for the past %s minute(s)' % (instance_id, data[_CURRENT_IDLETIME]))
                with open(_IDLETIME_FILE, 'w') as outfile:
                    json.dump(data, outfile)

                if data[_CURRENT_IDLETIME] >= idle_time:
                    lockHost(s, hostname)
                    has_jobs = hasJobs(s, hostname)
                    if has_jobs:
                        log.info('Instance has active jobs.')
                        data[_CURRENT_IDLETIME] = 0
                        lockHost(s, hostname, unlock=True)
                    else:
                        has_pending_jobs, error = hasPendingJobs(s)
                        if not error and not has_pending_jobs:
                            os.remove(_IDLETIME_FILE)
                            try:
                                selfTerminate(asg_name, asg_conn, instance_id)
                            except ClientError as ex:
                                log.error('Failed to terminate instance: %s with exception %s' % (instance_id, ex))
                                lockHost(s, hostname, unlock=True)
                        else:
                            if has_pending_jobs:
                                log.info('Queue has pending jobs. Not terminating instance')
                            elif error:
                                log.info('Encountered an error while polling queue for pending jobs. Not terminating instance')
                            lockHost(s, hostname, unlock=True)


if __name__ == "__main__":
    main()
