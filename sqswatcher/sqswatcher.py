#!/usr/bin/env python
# Copyright 2013-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import json
import time
import sys
import ConfigParser
import logging

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

def getConfig():
    log.debug('reading /etc/sqswatcher.cfg')

    config = ConfigParser.RawConfigParser()
    config.read('/etc/sqswatcher.cfg')
    if config.has_option('sqswatcher', 'loglevel'):
        lvl = logging._levelNames[config.get('sqswatcher', 'loglevel')]
        logging.getLogger().setLevel(lvl)
    _region = config.get('sqswatcher', 'region')
    _sqsqueue = config.get('sqswatcher', 'sqsqueue')
    _table_name = config.get('sqswatcher', 'table_name')
    _scheduler = config.get('sqswatcher', 'scheduler')
    _cluster_user = config.get('sqswatcher', 'cluster_user')
    _proxy = config.get('sqswatcher', 'proxy')
    proxy_config = Config()

    if not _proxy == "NONE":
        proxy_config = Config(proxies={'https': _proxy})

    log.debug(" ".join("%s=%s" % i
                       for i in [('_region', _region),
                                 ('_sqsqueue', _sqsqueue),
                                 ('_table_name', _table_name),
                                 ('_scheduler', _scheduler),
                                 ('_cluster_user', _cluster_user),
                                 ('_proxy', _proxy)]))

    return _region, _sqsqueue, _table_name, _scheduler, _cluster_user, proxy_config


def setupQueue(region, sqsqueue, proxy_config):
    log.debug('running setupQueue')

    sqs = boto3.resource('sqs', region_name=region, config=proxy_config)

    _q = sqs.get_queue_by_name(QueueName=sqsqueue)
    return _q


def setupDDBTable(region, table_name, proxy_config):
    log.debug('running setupDDBTable')

    dynamodb = boto3.client('dynamodb', region_name=region, config=proxy_config)
    tables = dynamodb.list_tables().get('TableNames')

    dynamodb2 = boto3.resource('dynamodb',  region_name=region, config=proxy_config)
    if table_name in tables:
        _table = dynamodb2.Table(table_name)
    else:
        _table = dynamodb2.create_table(TableName=table_name,
                              KeySchema=[
                                  {
                                    'AttributeName': 'instanceId',
                                    'KeyType': 'HASH'
                                  }
                              ],
                              AttributeDefinitions=[
                                {
                                    'AttributeName': 'instanceId',
                                    'AttributeType': 'S'
                                }
                              ],
                              ProvisionedThroughput={
                                'ReadCapacityUnits': 5,
                                'WriteCapacityUnits': 5
                              })
        _table.meta.client.get_waiter('table_exists').wait(TableName=table_name)


    return _table


def loadSchedulerModule(scheduler):
    scheduler = 'sqswatcher.plugins.' + scheduler
    _scheduler = __import__(scheduler)
    _scheduler = sys.modules[scheduler]

    log.debug("scheduler=%s" % repr(_scheduler))

    return _scheduler


def pollQueue(scheduler, q, t, proxy_config):
    log.debug("startup")
    s = loadSchedulerModule(scheduler)

    while True:

        results = q.receive_messages(MaxNumberOfMessages=10)

        while len(results) > 0:

            for message in results:
                message_text = json.loads(message.body)
                message_attrs = json.loads(message_text.get('Message'))
                log.debug("SQS Message %s" % message_attrs)

                try:
                    eventType = message_attrs.get('Event')
                except:
                    try:
                        eventType = message_attrs.get('detail-type')
                    except KeyError:
                        log.warn("Unable to read message. Deleting.")
                        message.delete()
                        break

                log.info("eventType=%s" % eventType)
                if eventType == 'autoscaling:TEST_NOTIFICATION':
                    message.delete()
                elif eventType == 'cfncluster:COMPUTE_READY':
                    instanceId = message_attrs.get('EC2InstanceId')
                    slots = message_attrs.get('Slots')
                    log.info("instanceId=%s" % instanceId)
                    ec2 = boto3.resource('ec2', region_name=region, config=proxy_config)

                    retry = 0
                    wait = 15
                    while retry < 3:
                        try:
                            instances = ec2.instances.filter(InstanceIds=[instanceId])
                            instance = next(iter(instances or []), None)

                            if not instance:
                                log.warning("Unable to find running instance %s." % instanceId)
                            else:
                                hostname = instance.private_dns_name.split('.')[:1][0]
                                log.info("Adding Hostname: %s" % hostname)
                                s.addHost(hostname=hostname, cluster_user=cluster_user, slots=slots)

                                t.put_item(Item={
                                    'instanceId': instanceId,
                                    'hostname': hostname
                                })

                            message.delete()
                            break
                        except ClientError as e:
                            if e.response.get('Error').get('Code') == 'RequestLimitExceeded':
                                time.sleep(wait)
                                retry += 1
                                wait = (wait*2+retry)
                            else:
                                raise e
                        except:
                            log.critical("Unexpected error: %s" % sys.exc_info()[0])
                            raise

                elif (eventType == 'autoscaling:EC2_INSTANCE_TERMINATE') or (eventType == 'EC2 Instance State-change Notification'):
                    if eventType == 'autoscaling:EC2_INSTANCE_TERMINATE':
                        instanceId = message_attrs.get('EC2InstanceId')
                    elif eventType == 'EC2 Instance State-change Notification':
                        if message_attrs.get('detail').get('state') == 'terminated':
                            log.info('Terminated instance state from CloudWatch')
                            instanceId = message_attrs.get('detail').get('instance-id')
                        else:
                            log.info('Not Terminated, ignoring')
                            message.delete()
                            break

                    log.info("instanceId=%s" % instanceId)
                    try:
                        item = t.get_item(ConsistentRead=True, Key={"instanceId": instanceId})
                        if item.get('Item') is not None:
                            hostname = item.get('Item').get('hostname')

                            if hostname:
                                s.removeHost(hostname, cluster_user)

                            t.delete_item(Key={"instanceId": instanceId})
                        else:
                            log.error("Did not find %s in the metadb\n" % instanceId)
                    except:
                        log.critical("Unexpected error: %s" % sys.exc_info()[0])
                        raise

                    message.delete()

            results = q.receive_messages(MaxNumberOfMessages=10)

        time.sleep(30)

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s'
    )
    log.info("sqswatcher startup")
    global region, cluster_user
    region, sqsqueue, table_name, scheduler, cluster_user, proxy_config = getConfig()
    q = setupQueue(region, sqsqueue, proxy_config)
    t = setupDDBTable(region, table_name, proxy_config)
    pollQueue(scheduler, q, t, proxy_config)

if __name__ == "__main__":
    main()
