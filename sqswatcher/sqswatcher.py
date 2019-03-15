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

import collections
import ConfigParser
import json
import logging
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from common.utils import load_module


class HostRemovalError(Exception):
    pass


class QueryConfigError(Exception):
    pass


log = logging.getLogger(__name__)


SQSWatcherConfig = collections.namedtuple(
    "SQSWatcherConfig",
    ["region", "scheduler", "sqsqueue", "table_name", "cluster_user", "proxy_config", "max_queue_size"],
)


def _get_config():
    """
    Get sqswatcher configuration.

    :return: the configuration parameters
    """
    config_file = "/etc/sqswatcher.cfg"
    log.info("Reading %s", config_file)

    config = ConfigParser.RawConfigParser()
    config.read(config_file)
    if config.has_option("sqswatcher", "loglevel"):
        lvl = logging._levelNames[config.get("sqswatcher", "loglevel")]
        logging.getLogger().setLevel(lvl)

    region = config.get("sqswatcher", "region")
    scheduler = config.get("sqswatcher", "scheduler")
    sqsqueue = config.get("sqswatcher", "sqsqueue")
    table_name = config.get("sqswatcher", "table_name")
    cluster_user = config.get("sqswatcher", "cluster_user")
    max_queue_size = int(config.get("sqswatcher", "max_queue_size"))

    _proxy = config.get("sqswatcher", "proxy")
    proxy_config = Config()
    if _proxy != "NONE":
        proxy_config = Config(proxies={"https": _proxy})

    log.info(
        "Configured parameters: region=%s scheduler=%s sqsqueue=%s table_name=%s cluster_user=%s "
        "proxy=%s max_queue_size=%d",
        region,
        scheduler,
        sqsqueue,
        table_name,
        cluster_user,
        _proxy,
        max_queue_size,
    )
    return SQSWatcherConfig(region, scheduler, sqsqueue, table_name, cluster_user, proxy_config, max_queue_size)


def _setup_queue(region, queue_name, proxy_config):
    """
    Get SQS Queue by queue name.

    :param region: AWS region
    :param queue_name: Queue name to search for
    :param proxy_config: proxy configuration
    :return: the Queue object
    """
    log.debug("running _setup_queue")

    sqs = boto3.resource("sqs", region_name=region, config=proxy_config)

    _queue = sqs.get_queue_by_name(QueueName=queue_name)
    return _queue


def _setup_ddb_table(region, table_name, proxy_config):
    """
    Get DynamoDB table by name.

    :param region: AWS region
    :param table_name: Table name to search for
    :param proxy_config: proxy configuration
    :return: the Table object
    """
    log.debug("running _setup_ddb_table")

    dynamodb = boto3.client("dynamodb", region_name=region, config=proxy_config)
    tables = dynamodb.list_tables().get("TableNames")

    dynamodb2 = boto3.resource("dynamodb", region_name=region, config=proxy_config)
    if table_name in tables:
        _table = dynamodb2.Table(table_name)
    else:
        _table = dynamodb2.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "instanceId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "instanceId", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        _table.meta.client.get_waiter("table_exists").wait(TableName=table_name)

    return _table


def _exponential_retry(func, attempts=3, delay=15, multiplier=2):
    """
    Execute the given boto3 function multiple times with an exponential delay in case of RequestLimitExceeded.

    :param func: the boto3 function to execute
    :param attempts: the number of times to try before giving up
    :param delay: initial delay between retries in seconds
    :param multiplier: multiplier factor for the delay
    :return the value returned from the function, if any
    """
    count = 0
    wait = delay
    while count < attempts:
        try:
            return func()
        except ClientError as e:
            if e.response.get("Error").get("Code") == "RequestLimitExceeded":
                count += 1
                if count < attempts:
                    log.debug("Request limit exceeded, waiting other %s seconds..." % wait)
                    time.sleep(wait)
                    wait *= multiplier
            else:
                log.critical(e.response.get("Error").get("Message"))
                break
        except:
            log.critical("Unexpected error: %s" % sys.exc_info()[0])
            break


def _add_host(scheduler_module, region, cluster_user, table, instance_id, slots, proxy_config, max_cluster_size):
    """
    Add the given instance_id to the scheduler cluster and to the instances table.

    :param scheduler_module: scheduler specific module to use
    :param region: AWS region
    :param cluster_user: Cluster user
    :param table: dynamodb table in which the instance must be added
    :param instance_id: the id of the instance to add
    :param slots: the number of slots associated to the instance
    :param proxy_config: proxy configuration to use
    """
    ec2 = boto3.resource("ec2", region_name=region, config=proxy_config)
    instances = _exponential_retry(lambda: ec2.instances.filter(InstanceIds=[instance_id]))
    instance = next(iter(instances or []), None)

    if not instance:
        log.error("Unable to find running instance %s." % instance_id)
    else:
        hostname = instance.private_dns_name.split(".")[:1][0]
        if hostname:
            log.info("Adding hostname: %s" % hostname)
            scheduler_module.addHost(
                hostname=hostname, cluster_user=cluster_user, slots=slots, max_cluster_size=max_cluster_size
            )
            log.info("Host %s successfully added to the cluster" % hostname)

            table.put_item(Item={"instanceId": instance_id, "hostname": hostname})
            log.info("Instance %s successfully added to the database" % instance_id)
        else:
            log.error("Unable to get the hostname for the instance %s" % instance_id)


def _remove_host(scheduler_module, cluster_user, table, instance_id, max_cluster_size):
    """
    Remove the given instance_id from the scheduler cluster and from the instances table.

    :param scheduler_module: scheduler specific module to use
    :param cluster_user: cluster user
    :param table: dynamodb table from which the instance item must be removed
    :param instance_id: the id of the instance to remove
    """
    item = _exponential_retry(lambda: table.get_item(ConsistentRead=True, Key={"instanceId": instance_id}))
    if item.get("Item") is not None:
        hostname = item.get("Item").get("hostname")
        if hostname:
            log.info("Removing hostname: %s" % hostname)
            scheduler_module.removeHost(hostname, cluster_user, max_cluster_size=max_cluster_size)
            log.info("Host %s successfully removed from the cluster" % hostname)
        else:
            log.warning("Hostname is empty for the instance %s." % instance_id)

        _exponential_retry(lambda: table.delete_item(Key={"instanceId": instance_id}))
        log.info("Instance %s successfully removed from the database" % instance_id)
    else:
        log.error("Instance %s not found in the database" % instance_id)


def _requeue_message(queue, message):
    """
    Requeue the given message into the specified queue

    :param queue: the queue where to send the message
    :param message: the message to requeue
    """
    queue.send_message(MessageBody=message.body, DelaySeconds=60)


def _poll_queue(scheduler, region, cluster_user, queue, table, proxy_config, max_cluster_size):
    """
    Poll SQS queue.

    :param scheduler: scheduler
    :param region: AWS region
    :param cluster_user: cluster user
    :param queue: SQS queue name to poll
    :param table: DB table name
    :param proxy_config: Proxy configuration
    """
    log.debug("startup")
    scheduler_module = load_module("sqswatcher.plugins." + scheduler)

    while True:

        results = queue.receive_messages(MaxNumberOfMessages=10)
        while len(results) > 0:

            for message in results:
                message_text = json.loads(message.body)
                message_attrs = json.loads(message_text.get("Message"))
                log.debug("SQS Message %s" % message_attrs)

                event_type = message_attrs.get("Event")
                if not event_type:
                    log.warning("Unable to read message event type. Deleting message %s.", message_text)
                    message.delete()
                    continue

                log.info("event_type=%s" % event_type)

                if event_type == "parallelcluster:COMPUTE_READY":
                    instance_id = message_attrs.get("EC2InstanceId")
                    slots = message_attrs.get("Slots")
                    log.info("instance_id=%s" % instance_id)
                    _add_host(
                        scheduler_module,
                        region,
                        cluster_user,
                        table,
                        instance_id,
                        slots,
                        proxy_config,
                        max_cluster_size,
                    )
                    message.delete()

                elif event_type == "autoscaling:EC2_INSTANCE_TERMINATE":
                    instance_id = message_attrs.get("EC2InstanceId")

                    log.info("instance_id=%s" % instance_id)
                    try:
                        _remove_host(scheduler_module, cluster_user, table, instance_id, max_cluster_size)
                    except HostRemovalError:
                        log.info("Unable to remove host, requeuing %s message" % event_type)
                        _requeue_message(queue, message)
                    except QueryConfigError:
                        log.info("Unable to query scheduler configuration, discarding %s message" % event_type)

                    message.delete()
                else:
                    log.info("Unsupported event type %s. Discarding message." % event_type)
                    message.delete()

        time.sleep(30)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s")
    log.info("sqswatcher startup")

    config = _get_config()
    queue = _setup_queue(config.region, config.sqsqueue, config.proxy_config)
    table = _setup_ddb_table(config.region, config.table_name, config.proxy_config)

    _poll_queue(
        config.scheduler, config.region, config.cluster_user, queue, table, config.proxy_config, config.max_queue_size
    )


if __name__ == "__main__":
    main()
