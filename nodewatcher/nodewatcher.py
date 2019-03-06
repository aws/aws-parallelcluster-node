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

import ConfigParser
import json
import logging
import os
import sys
import tempfile
import time
import urllib2

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)
_DATA_DIR = "/var/run/nodewatcher/"
_IDLETIME_FILE = _DATA_DIR + "node_idletime.json"
_CURRENT_IDLETIME = 'current_idletime'


def _get_config(instance_id):
    """
    Get configuration from config file.

    :param instance_id: instance id used to retrieve ASG group name
    :return: configuration parameters
    """
    log.debug('Reading /etc/nodewatcher.cfg')

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
        log.debug("Discovered asg: %s" % _asg)
        config.set('nodewatcher', 'asg', _asg)

        tup = tempfile.mkstemp(dir=os.getcwd())
        fd = os.fdopen(tup[0], 'w')
        config.write(fd)
        fd.close

        os.rename(tup[1], 'nodewatcher.cfg')

    log.debug(
        "region=%s asg=%s scheduler=%s prox_config=%s idle_time=%s" % (
            _region, _asg, _scheduler, proxy_config, _scaledown_idletime
        )
    )
    return _region, _asg, _scheduler, proxy_config, _scaledown_idletime, _stack_name


def _get_metadata(metadata_path):
    """
    Get EC2 instance metadata.

    :param metadata_path: the metadata relative path
    :return the metadata value.
    """
    try:
        _instance_id = urllib2.urlopen("http://169.254.169.254/latest/meta-data/{0}".format(metadata_path)).read()
    except urllib2.URLError:
        log.critical("Unable to get {0} metadata".format(metadata_path))
        sys.exit(1)

    log.debug("instance_id=%s" % _instance_id)

    return _instance_id


def _load_scheduler_module(scheduler):
    """
    Load scheduler module, containing scheduler specific functions.

    :param scheduler: scheduler name, it must corresponds to the <scheduler>.py file in the current folder.
    :return: the scheduler module
    """
    scheduler = 'nodewatcher.plugins.' + scheduler
    _scheduler = __import__(scheduler)
    _scheduler = sys.modules[scheduler]

    log.debug("scheduler=%s" % repr(_scheduler))
    return _scheduler


def _has_jobs(scheduler_module, hostname):
    """
    Verify if there are running jobs in the given host.

    :param scheduler_module: scheduler specific module to use
    :param hostname: host to search for
    :return: true if the given host has running jobs
    """
    _jobs = scheduler_module.hasJobs(hostname)
    log.debug("jobs=%s" % _jobs)
    return _jobs


def _has_pending_jobs(scheduler_module):
    """
    Verify if there are penging jobs in the cluster.

    :param scheduler_module: scheduler specific module to use
    :return: true if there are pending jobs and the error code
    """
    _has_pending_jobs, _error = scheduler_module.hasPendingJobs()
    log.debug("has_pending_jobs=%s, error=%s" % (_has_pending_jobs, _error))
    return _has_pending_jobs, _error


def _lock_host(scheduler_module, hostname, unlock=False):
    """
    Lock/Unlock the given host (e.g. before termination).

    :param scheduler_module: scheduler specific module to use
    :param hostname: host to lock
    :param unlock: False to lock the host, True to unlock
    """
    log.debug("%s %s" % (unlock and "unlocking" or "locking", hostname))
    scheduler_module.lockHost(hostname, unlock)
    time.sleep(15)  # allow for some settling


def _self_terminate(asg_name, asg_client, instance_id):
    """
    Terminate the given instance and decrease ASG desired capacity.

    :param asg_name: ASG name
    :param asg_client: ASG boto3 client
    :param instance_id: the instnace to terminate
    """
    if not _maintain_size(asg_name, asg_client):
        log.info("Self terminating %s" % instance_id)
        asg_client.terminate_instance_in_auto_scaling_group(InstanceId=instance_id, ShouldDecrementDesiredCapacity=True)


def _maintain_size(asg_name, asg_client):
    """
    Verify if the desired capacity is lower than the configured min size.
    
    :param asg_name: the ASG to query for
    :param asg_client: ASG boto3 client
    :return: True if the desired capacity is lower than the configured min size.
    """
    asg = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name]).get('AutoScalingGroups')[0]
    _capacity = asg.get('DesiredCapacity')
    _min_size = asg.get('MinSize')
    log.info("DesiredCapacity is %d, MinSize is %d" % (_capacity, _min_size))
    if _capacity > _min_size:
        log.debug('Capacity greater than min size.')
        return False
    else:
        log.debug('Capacity less than or equal to min size.')
        return True


def _is_stack_ready(stack_name, region, proxy_config):
    """
    Verify if the Stack is in one of the *_COMPLETE states.

    :param stack_name: Stack to query for
    :param region: AWS region
    :param proxy_config: Proxy configuration
    :return: true if the stack is in the *_COMPLETE status
    """
    log.info('Checking for status of the stack %s' % stack_name)
    cfn_client = boto3.client('cloudformation', region_name=region, config=proxy_config)
    stacks = cfn_client.describe_stacks(StackName=stack_name)
    return stacks['Stacks'][0]['StackStatus'] in ['CREATE_COMPLETE', 'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE']


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s'
    )
    log.info('nodewatcher startup')
    instance_id = _get_metadata("instance-id")
    hostname = _get_metadata("local-hostname")
    log.info('Instance id is %s, hostname is %s' % (instance_id, hostname))
    region, asg_name, scheduler, proxy_config, idle_time, stack_name = _get_config(instance_id)

    scheduler_module = _load_scheduler_module(scheduler)

    try:
        if not os.path.exists(_DATA_DIR):
            os.makedirs(_DATA_DIR)
    except OSError as ex:
        log.critical('Creating directory %s to persist current idle time failed with exception: %s ' % (_DATA_DIR, ex))
        raise

    if os.path.isfile(_IDLETIME_FILE):
        with open(_IDLETIME_FILE) as f:
            data = json.loads(f.read())
    else:
        data = {_CURRENT_IDLETIME: 0}

    stack_ready = False
    termination_in_progress = False
    while True:
        # if this node is terminating sleep for a long time and wait for termination
        if termination_in_progress:
            time.sleep(300)
            log.info('Instance is still terminating')
            continue
        time.sleep(60)
        if not stack_ready:
            stack_ready = _is_stack_ready(stack_name, region, proxy_config)
            log.info('Stack %s ready: %s' % (stack_name, stack_ready))
            continue
        asg_conn = boto3.client('autoscaling', region_name=region, config=proxy_config)

        has_jobs = _has_jobs(scheduler_module, hostname)
        if has_jobs:
            log.info('Instance has active jobs.')
            data[_CURRENT_IDLETIME] = 0
        else:
            if _maintain_size(asg_name, asg_conn):
                continue
            else:
                data[_CURRENT_IDLETIME] += 1
                log.info('Instance had no job for the past %s minute(s)' % data[_CURRENT_IDLETIME])
                with open(_IDLETIME_FILE, 'w') as outfile:
                    json.dump(data, outfile)

                if data[_CURRENT_IDLETIME] >= idle_time:
                    _lock_host(scheduler_module, hostname)
                    has_jobs = _has_jobs(scheduler_module, hostname)
                    if has_jobs:
                        log.info('Instance has active jobs.')
                        data[_CURRENT_IDLETIME] = 0
                        _lock_host(scheduler_module, hostname, unlock=True)
                    else:
                        has_pending_jobs, error = _has_pending_jobs(scheduler_module)
                        if not error and not has_pending_jobs:
                            os.remove(_IDLETIME_FILE)
                            try:
                                _self_terminate(asg_name, asg_conn, instance_id)
                                termination_in_progress = True
                            except ClientError as ex:
                                log.error('Failed to terminate instance with exception %s' % ex)
                                _lock_host(scheduler_module, hostname, unlock=True)
                        else:
                            if has_pending_jobs:
                                log.info('Queue has pending jobs. Not terminating instance')
                            elif error:
                                log.info('Encountered an error while polling queue for pending jobs. '
                                         'Not terminating instance')
                            _lock_host(scheduler_module, hostname, unlock=True)


if __name__ == "__main__":
    main()
