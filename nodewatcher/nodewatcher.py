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
import logging
import boto3
from boto3.dynamodb.conditions import Attr
import ConfigParser
from botocore.exceptions import ClientError
from botocore.config import Config
import json


log = logging.getLogger(__name__)
_DATA_DIR = "/var/run/nodewatcher/"
_IDLETIME_FILE = _DATA_DIR + "node_idletime.json"
_CURRENT_IDLETIME = 'current_idletime'


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


def get_config():
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
    _table_name = config.get('nodewatcher', 'table_name')
    _asg = get_asg_name(_stack_name, _region, proxy_config)

    log.debug("region=%s asg=%s scheduler=%s prox_config=%s idle_time=%s" % (_region, _asg, _scheduler, proxy_config, _scaledown_idletime))
    return _region, _asg, _scheduler, proxy_config, _scaledown_idletime, _stack_name, _table_name


def get_instance_id():
    try:
        _instance_id = urllib2.urlopen("http://169.254.169.254/latest/meta-data/instance-id").read()
    except urllib2.URLError:
        log.critical('Unable to get instance-id from metadata')
        sys.exit(1)

    log.debug("instance_id=%s" % _instance_id)

    return _instance_id


def load_scheduler_module(scheduler):
    scheduler = 'nodewatcher.plugins.' + scheduler
    _scheduler = __import__(scheduler)
    _scheduler = sys.modules[scheduler]

    log.debug("scheduler=%s" % repr(_scheduler))

    return _scheduler


def node_has_jobs(scheduler, hostname):

    _jobs = scheduler.has_jobs(hostname)

    log.info("jobs=%s" % _jobs)

    return _jobs


def get_idle_nodes(scheduler):
    idle_nodes = scheduler.get_idle_nodes()
    log.info("idle_node_count=%s" % len(idle_nodes))

    return idle_nodes


def queue_has_pending_jobs(scheduler):
    _has_pending_jobs, _error = scheduler.has_pending_jobs()
    log.info("has_pending_jobs=%s, error=%s" % (_has_pending_jobs, _error))
    return _has_pending_jobs, _error


def get_current_cluster_size(scheduler):
    current_cluster_size = scheduler.get_current_cluster_size()
    log.info("current_cluster_size=%s" % current_cluster_size)
    return current_cluster_size


def lock_host(scheduler, hostname):
    log.info("locking %s" % hostname)

    _r = scheduler.lock_host(hostname, False)

    time.sleep(15) # allow for some settling

    return _r


def unlock_host(scheduler,hostname):
    log.info("unlocking %s" % hostname)

    _r = scheduler.lock_host(hostname, True)

    time.sleep(15) # allow for some settling

    return _r


def self_terminate(dynamodb_table, asg_conn, instance_id):
    log.info("terminating %s" % instance_id)
    try:
        dynamodb_table.update_item(
            Key={'instanceId': instance_id},
            UpdateExpression='SET #terminated_by_cluster = :terminated',
            ExpressionAttributeNames={'#terminated_by_cluster': 'terminated_by_cluster'},
            ExpressionAttributeValues={':terminated': True}
        )
        asg_conn.terminate_instance_in_auto_scaling_group(InstanceId=instance_id, ShouldDecrementDesiredCapacity=True)
        return True
    except ClientError as ex:
        log.error("terminate_instance_in_auto_scaling_group failed with exception %s" % ex)
    return False


def get_stack_creation_status(stack_name, region, proxy_config):
    log.info('Checking for status of the stack %s' % stack_name)
    cfn_client = boto3.client('cloudformation', region_name=region, config=proxy_config)
    stacks = cfn_client.describe_stacks(StackName=stack_name)
    return stacks['Stacks'][0]['StackStatus'] == 'CREATE_COMPLETE' or \
           stacks['Stacks'][0]['StackStatus'] == 'UPDATE_COMPLETE'


def get_hostname():
    try:
        _hostname = urllib2.urlopen("http://169.254.169.254/latest/meta-data/local-hostname").read()
    except urllib2.URLError:
        log.critical('Unable to get hostname from metadata')
        sys.exit(1)

    log.debug("hostname=%s" % _hostname)

    return _hostname


def check_if_terminable(dynamodb_table, min_size):
    log.info("Checking if this node is terminable")
    try:
        dynamodb_table.update_item(
            Key={'instanceId': 'current_cluster_size'},
            UpdateExpression='SET #count = #count - :count',
            ConditionExpression=Attr('count').gt(min_size),
            ExpressionAttributeNames={'#count': 'count'},
            ExpressionAttributeValues={':count': 1}
        )
        return True
    except ClientError as ex:
        log.info("Node is not terminable. Failed with ex " % ex)
        item = dynamodb_table.get_item(ConsistentRead=True, Key={"instanceId": "current_cluster_size"})
        if item.get('Item') is not None:
            count = item.get('Item').get('count')
            log.info("Current count is %s; Minimum size is %s" % (count, min_size))
        return False


def update_table_when_termination_fails(dynamodb_table, instance_id):
    log.info("Updating table since termination request on this node failed")
    try:
        dynamodb_table.update_item(
            Key={'instanceId': instance_id},
            UpdateExpression='SET #terminated_by_cluster = :terminated',
            ExpressionAttributeNames={'#terminated_by_cluster': 'terminated_by_cluster'},
            ExpressionAttributeValues={':terminated': False}
        )
        dynamodb_table.update_item(
            Key={'instanceId': 'current_cluster_size'},
            UpdateExpression='SET #count = #count + :count',
            ExpressionAttributeNames={'#count': 'count'},
            ExpressionAttributeValues={':count': 1}
        )
    except ClientError as ex:
        log.error("Update table failed with  exception " % ex)


def get_desired_sizes(scheduler, asg_name, asg_conn):
    asg = asg_conn.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name]) \
        .get('AutoScalingGroups')[0]
    asg_desired_capacity = asg.get('DesiredCapacity')
    log.info("asg_desired=%d" % asg_desired_capacity)
    total_compute_nodes = get_current_cluster_size(scheduler)
    log.info("total_compute_nodes=%d" % total_compute_nodes)
    capacity = min(asg_desired_capacity, total_compute_nodes)
    min_size = asg.get('MinSize')

    log.info("capacity=%d min_size=%d" % (capacity, min_size))
    return min_size, capacity


def main():
    # This daemon runs on the compute nodes.
    # The purpose of this daemon is to monitor the node and if idle for long enough,
    # send termination requests to the ASG
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s'
    )
    log.info("nodewatcher startup")
    region, asg_name, scheduler, proxy_config, idle_time, stack_name, table_name = get_config()

    scheduler = load_scheduler_module(scheduler)

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

    instance_id = get_instance_id()
    hostname = get_hostname()

    dynamodb_resource = boto3.resource('dynamodb', region_name=region, config=proxy_config)
    try:
        dynamodb_table = dynamodb_resource.Table(table_name)
    except ClientError as e:
        log.critical('Creating a table client failed with exception %s. Exiting' % e)
        raise

    stack_creation_complete = False
    termination_in_progress = False
    while True:
        # If termination is in progress, keep the daemon running as a no-op
        if termination_in_progress:
            log.info("terminating %s" % instance_id)
            time.sleep(300)
            log.info('%s is still terminating' % hostname)
            continue
        time.sleep(60)
        if not stack_creation_complete:
            stack_creation_complete = get_stack_creation_status(stack_name, region, proxy_config)
            log.info('%s creation complete: %s' % (stack_name, stack_creation_complete))
            continue
        asg_conn = boto3.client('autoscaling', region_name=region, config=proxy_config)

        has_jobs = node_has_jobs(scheduler, hostname)
        if has_jobs:
            log.info('Instance has active jobs.')
            data[_CURRENT_IDLETIME] = 0
        else:
            min_size, current_size = get_desired_sizes(scheduler, asg_name, asg_conn)
            if current_size <= min_size:
                log.info('capacity less then or equal to min size. Node not eligible for termination')
                continue
            else:
                log.info('local capacity greater then min size')
                data[_CURRENT_IDLETIME] += 1
                log.info('Instance %s has no job for the past %s minute(s)' % (instance_id, data[_CURRENT_IDLETIME]))
                with open(_IDLETIME_FILE, 'w') as outfile:
                    json.dump(data, outfile)

                if data[_CURRENT_IDLETIME] >= idle_time:
                    lock_host(scheduler, hostname)
                    has_jobs = node_has_jobs(scheduler, hostname)
                    if has_jobs:
                        log.info('Instance has active jobs.')
                        data[_CURRENT_IDLETIME] = 0
                        unlock_host(scheduler, hostname)
                    else:
                        has_pending_jobs, error = queue_has_pending_jobs(scheduler)
                        if not error and not has_pending_jobs:
                            log.info("Node eligible for termination")
                            try:
                                # atomic decrement on the dynamoDB table if the decrement fails,
                                # it is because the cluster size (as reported by all compute nodes)
                                # is no longer greater than or equal to min_queue_size

                                node_terminable = check_if_terminable(dynamodb_table, min_size)

                                # if node can be terminated, go ahead and terminate it.
                                # Dynamo table has been updated before termination

                                if node_terminable:
                                    termination_in_progress = self_terminate(dynamodb_table, asg_conn, instance_id)

                                    # For some reason if the termination fails
                                    # (can only fail if asg fails, which is rare)
                                    # unlock host and increment the count in the table
                                    if not termination_in_progress:
                                        log.info("Termination was unsuccessful, unlocking host")
                                        unlock_host(scheduler, hostname)
                                        update_table_when_termination_fails(dynamodb_table, instance_id)
                                    else:
                                        os.remove(_IDLETIME_FILE)
                                else:
                                    unlock_host(scheduler, hostname)

                            except ClientError as ex:
                                log.error('Failed to terminate instance: %s with exception %s' % (instance_id, ex))
                                unlock_host(scheduler, hostname)
                        else:
                            if has_pending_jobs:
                                log.info('Queue has pending jobs. Not terminating instance')
                            elif error:
                                log.info('Encountered an error while polling queue for pending jobs.'
                                         'Not terminating instance')
                            unlock_host(scheduler, hostname)


if __name__ == "__main__":
    main()
