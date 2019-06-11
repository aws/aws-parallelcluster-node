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

from future.moves.collections import OrderedDict

import collections
import itertools
import json
import logging
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from configparser import ConfigParser
from retrying import retry

from common.time_utils import seconds
from common.utils import (
    CriticalError,
    get_asg_name,
    get_asg_settings,
    get_compute_instance_type,
    get_instance_properties,
    load_module,
)

LOOP_TIME = 30
CLUSTER_PROPERTIES_REFRESH_INTERVAL = 180


class QueryConfigError(Exception):
    pass


log = logging.getLogger(__name__)


SQSWatcherConfig = collections.namedtuple(
    "SQSWatcherConfig", ["region", "scheduler", "sqsqueue", "table_name", "cluster_user", "proxy_config", "stack_name"]
)

Host = collections.namedtuple("Host", ["instance_id", "hostname", "slots"])

UpdateEvent = collections.namedtuple("UpdateEvent", ["action", "message", "host"])


def _get_config():
    """
    Get sqswatcher configuration.

    :return: the configuration parameters
    """
    config_file = "/etc/sqswatcher.cfg"
    log.info("Reading %s", config_file)

    config = ConfigParser()
    config.read(config_file)
    if config.has_option("sqswatcher", "loglevel"):
        lvl = logging._levelNames[config.get("sqswatcher", "loglevel")]
        logging.getLogger().setLevel(lvl)

    region = config.get("sqswatcher", "region")
    scheduler = config.get("sqswatcher", "scheduler")
    sqsqueue = config.get("sqswatcher", "sqsqueue")
    table_name = config.get("sqswatcher", "table_name")
    cluster_user = config.get("sqswatcher", "cluster_user")
    stack_name = config.get("sqswatcher", "stack_name")

    _proxy = config.get("sqswatcher", "proxy")
    proxy_config = Config()
    if _proxy != "NONE":
        proxy_config = Config(proxies={"https": _proxy})

    log.info(
        "Configured parameters: region=%s scheduler=%s sqsqueue=%s table_name=%s cluster_user=%s "
        "proxy=%s stack_name=%s",
        region,
        scheduler,
        sqsqueue,
        table_name,
        cluster_user,
        _proxy,
        stack_name,
    )
    return SQSWatcherConfig(region, scheduler, sqsqueue, table_name, cluster_user, proxy_config, stack_name)


@retry(stop_max_attempt_number=3, wait_fixed=5000)
def _get_sqs_queue(region, queue_name, proxy_config):
    """
    Get SQS Queue by queue name.

    :param region: AWS region
    :param queue_name: Queue name to search for
    :param proxy_config: proxy configuration
    :return: the Queue object
    """
    log.debug("Getting SQS queue '%s'", queue_name)
    sqs = boto3.resource("sqs", region_name=region, config=proxy_config)
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        log.debug("SQS queue is %s", queue)
    except ClientError as e:
        log.critical("Unable to get the SQS queue '%s'. Failed with exception: %s", queue_name, e)
        raise

    return queue


@retry(
    stop_max_attempt_number=3,
    wait_fixed=5000,
    retry_on_exception=lambda exception: not isinstance(exception, CriticalError),
)
def _get_ddb_table(region, table_name, proxy_config):
    """
    Get DynamoDB table by name.

    :param region: AWS region
    :param table_name: Table name to search for
    :param proxy_config: proxy configuration
    :return: the Table object
    """
    log.debug("Getting DynamoDB table '%s'", table_name)
    ddb_client = boto3.client("dynamodb", region_name=region, config=proxy_config)
    try:
        tables = ddb_client.list_tables().get("TableNames")
        if table_name not in tables:
            error_msg = "Unable to find the DynamoDB table '{0}'".format(table_name)
            log.critical(error_msg)
            raise CriticalError(error_msg)

        ddb_resource = boto3.resource("dynamodb", region_name=region, config=proxy_config)
        table = ddb_resource.Table(table_name)
        log.debug("DynamoDB table found correctly.")
    except ClientError as e:
        log.critical("Unable to get the DynamoDB table '%s'. Failed with exception: %s", table_name, e)
        raise

    return table


def _retry_on_request_limit_exceeded(func):
    @retry(
        stop_max_attempt_number=5,
        wait_exponential_multiplier=5000,
        retry_on_exception=lambda exception: isinstance(exception, ClientError)
        and exception.response.get("Error").get("Code") == "RequestLimitExceeded",
    )
    def _retry():
        return func()

    return _retry()


def _requeue_message(queue, message):
    """
    Requeue the given message into the specified queue

    :param queue: the queue where to send the message
    :param message: the message to requeue
    """
    max_retries = 2
    message_body = json.loads(message.body)
    if "TTL" not in message_body:
        message_body["TTL"] = max_retries
    else:
        message_body["TTL"] = message_body["TTL"] - 1
        if message_body["TTL"] < 1:
            log.warning(
                "Discarding the following message since exceeded the number of max retries (%d): %s",
                max_retries,
                message_body,
            )
            return
    message_body_string = json.dumps(message_body)
    log.warning("Re-queuing failed event %s", message_body_string)
    queue.send_message(MessageBody=message_body_string, DelaySeconds=60)


def _retrieve_all_sqs_messages(queue):
    log.info("Retrieving messages from SQS queue")
    max_messages_per_call = 10
    max_messages = 50
    messages = []
    while len(messages) < max_messages:
        # setting WaitTimeSeconds in order to use Amazon SQS Long Polling.
        # when not using Long Polling with a small queue you might not receive any message
        # since only a subset of random machines is queried.
        retrieved_messages = queue.receive_messages(MaxNumberOfMessages=max_messages_per_call, WaitTimeSeconds=2)
        if len(retrieved_messages) > 0:
            messages.extend(retrieved_messages)
        else:
            # the queue is not always returning max_messages_per_call even when available
            # looping until receive_messages returns at least 1 message
            break

    log.info("Retrieved %d messages from SQS queue", len(messages))

    return messages


def _parse_sqs_messages(messages, table):
    update_events = OrderedDict()
    for message in messages:
        message_text = json.loads(message.body)
        message_attrs = json.loads(message_text.get("Message"))

        event_type = message_attrs.get("Event")
        if not event_type:
            log.warning("Unable to read message. Deleting.")
            message.delete()
            continue

        instance_id = message_attrs.get("EC2InstanceId")
        if event_type == "parallelcluster:COMPUTE_READY":
            log.info("Processing COMPUTE_READY event for instance %s", instance_id)
            update_event = _process_compute_ready_event(message_attrs, message)
        elif event_type == "autoscaling:EC2_INSTANCE_TERMINATE":
            log.info("Processing EC2_INSTANCE_TERMINATE event for instance %s", instance_id)
            update_event = _process_instance_terminate_event(message_attrs, message, table)
        else:
            log.info("Unsupported event type %s. Discarding message." % event_type)
            update_event = None

        if update_event:
            hostname = update_event.host.hostname
            if hostname in update_events:
                # delete first to preserve messages order in dict
                del update_events[hostname]
            update_events[hostname] = update_event
        else:
            # discarding message
            log.warning("Discarding message %s", message)
            message.delete()

    return update_events.values()


def _process_compute_ready_event(message_attrs, message):
    instance_id = message_attrs.get("EC2InstanceId")
    slots = message_attrs.get("Slots")
    hostname = message_attrs.get("LocalHostname").split(".")[0]
    return UpdateEvent("ADD", message, Host(instance_id, hostname, slots))


def _process_instance_terminate_event(message_attrs, message, table):
    instance_id = message_attrs.get("EC2InstanceId")
    try:
        item = _retry_on_request_limit_exceeded(
            lambda: table.get_item(ConsistentRead=True, Key={"instanceId": instance_id})
        )
    except Exception as e:
        log.error("Failed when retrieving instance data for instance %s from db with exception %s", instance_id, e)
        return None

    if item.get("Item") is not None:
        hostname = item.get("Item").get("hostname")
        return UpdateEvent("REMOVE", message, Host(instance_id, hostname, None))
    else:
        log.error("Instance %s not found in the database.", instance_id)
        return None


def _process_sqs_messages(
    update_events,
    scheduler_module,
    sqs_config,
    table,
    queue,
    max_cluster_size,
    instance_properties,
    force_cluster_update,
):
    # Update the scheduler only when there are messages from the queue or
    # tha ASG max size got updated.
    if not update_events and not force_cluster_update:
        return

    failed_events, succeeded_events = scheduler_module.update_cluster(
        max_cluster_size, sqs_config.cluster_user, update_events, instance_properties
    )

    for event in update_events:
        try:
            # Add an item to the table also in case of failures to handle host removal correctly
            if event.action == "ADD":
                _retry_on_request_limit_exceeded(
                    lambda: table.put_item(Item={"instanceId": event.host.instance_id, "hostname": event.host.hostname})
                )
            # Remove item from table only in case of success
            elif event.action == "REMOVE" and event in succeeded_events:
                _retry_on_request_limit_exceeded(lambda: table.delete_item(Key={"instanceId": event.host.instance_id}))
            log.debug("Successfully processed event %s", event)
        except Exception as e:
            log.error(
                "Failed when updating dynamo db table for instance %s with exception %s", event.host.instance_id, e
            )
            if event not in failed_events:
                failed_events.append(event)
                succeeded_events.remove(event)

    for event in failed_events:
        _requeue_message(queue, event.message)

    for event in itertools.chain(failed_events, succeeded_events):
        log.debug("Removing event from queue: %s", event)
        event.message.delete()


def _retrieve_max_cluster_size(sqs_config, asg_name, fallback):
    try:
        _, _, max_size = get_asg_settings(sqs_config.region, sqs_config.proxy_config, asg_name)
        return max_size
    except Exception:
        return fallback


def _poll_queue(sqs_config, queue, table, asg_name):
    """
    Poll SQS queue.

    :param sqs_config: SQS daemon configuration
    :param queue: SQS Queue object connected to the cluster queue
    :param table: DB table resource object
    """
    scheduler_module = load_module("sqswatcher.plugins." + sqs_config.scheduler)

    max_cluster_size = None
    instance_type = None
    cluster_properties_refresh_timer = 0
    while True:
        force_cluster_update = False
        # dynamically retrieve max_cluster_size and compute_instance_type
        if (
            not max_cluster_size
            or not instance_type
            or cluster_properties_refresh_timer >= CLUSTER_PROPERTIES_REFRESH_INTERVAL
        ):
            cluster_properties_refresh_timer = 0
            logging.info("Refreshing cluster properties")
            new_max_cluster_size = _retrieve_max_cluster_size(sqs_config, asg_name, fallback=max_cluster_size)
            new_instance_type = get_compute_instance_type(
                sqs_config.region, sqs_config.proxy_config, sqs_config.stack_name, fallback=instance_type
            )
            force_cluster_update = new_max_cluster_size != max_cluster_size or new_instance_type != instance_type
            if new_instance_type != instance_type:
                instance_type = new_instance_type
                instance_properties = get_instance_properties(sqs_config.region, sqs_config.proxy_config, instance_type)
            max_cluster_size = new_max_cluster_size
        cluster_properties_refresh_timer += LOOP_TIME

        messages = _retrieve_all_sqs_messages(queue)
        update_events = _parse_sqs_messages(messages, table)
        _process_sqs_messages(
            update_events,
            scheduler_module,
            sqs_config,
            table,
            queue,
            max_cluster_size,
            instance_properties,
            force_cluster_update,
        )
        time.sleep(LOOP_TIME)


@retry(wait_fixed=seconds(LOOP_TIME))
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s")
    log.info("sqswatcher startup")

    try:
        config = _get_config()
        queue = _get_sqs_queue(config.region, config.sqsqueue, config.proxy_config)
        table = _get_ddb_table(config.region, config.table_name, config.proxy_config)
        asg_name = get_asg_name(config.stack_name, config.region, config.proxy_config)

        _poll_queue(config, queue, table, asg_name)
    except Exception as e:
        log.critical("An unexpected error occurred: %s", e)
        raise


if __name__ == "__main__":
    main()
