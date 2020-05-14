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
import itertools
import json
import logging
import platform
from collections import OrderedDict
from datetime import datetime

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from configparser import ConfigParser
from retrying import retry

from common.ssh_keyscan import update_ssh_known_hosts
from common.time_utils import seconds
from common.utils import (
    SUPPORTED_EVENTTYPE_FOR_QUEUETYPE,
    CriticalError,
    EventType,
    Host,
    QueueType,
    UpdateEvent,
    get_asg_name,
    get_cluster_instance_info,
    get_compute_instance_type,
    get_instance_properties,
    load_module,
    retrieve_max_cluster_size,
    sleep_remaining_loop_time,
)

LOOP_TIME = 30
CLUSTER_PROPERTIES_REFRESH_INTERVAL = 180
DEFAULT_MAX_PROCESSED_MESSAGES = 200


class QueryConfigError(Exception):
    pass


log = logging.getLogger(__name__)


SQSWatcherConfig = collections.namedtuple(
    "SQSWatcherConfig",
    [
        "region",
        "scheduler",
        "sqsqueue",
        "health_sqsqueue",
        "table_name",
        "cluster_user",
        "proxy_config",
        "stack_name",
        "max_processed_messages",
        "disable_health_check",
    ],
)


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
    health_sqsqueue = config.get("sqswatcher", "health_sqsqueue")
    disable_health_check = config.getboolean("sqswatcher", "disable_health_check", fallback=False)
    table_name = config.get("sqswatcher", "table_name")
    cluster_user = config.get("sqswatcher", "cluster_user")
    stack_name = config.get("sqswatcher", "stack_name")
    max_processed_messages = int(
        config.get("sqswatcher", "max_processed_messages", fallback=DEFAULT_MAX_PROCESSED_MESSAGES)
    )

    _proxy = config.get("sqswatcher", "proxy")
    proxy_config = Config()
    if _proxy != "NONE":
        proxy_config = Config(proxies={"https": _proxy})

    log.info(
        (
            "Configured parameters: region=%s scheduler=%s sqsqueue=%s health_sqsqueue=%s table_name=%s "
            "cluster_user=%s proxy=%s stack_name=%s max_processed_messages=%s disable_health_check=%s",
            region,
            scheduler,
            sqsqueue,
            health_sqsqueue,
            table_name,
            cluster_user,
            _proxy,
            stack_name,
            max_processed_messages,
            disable_health_check,
        )
    )
    return SQSWatcherConfig(
        region,
        scheduler,
        sqsqueue,
        health_sqsqueue,
        table_name,
        cluster_user,
        proxy_config,
        stack_name,
        max_processed_messages,
        disable_health_check,
    )


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
    endpoint_url = "https://sqs.{0}.{1}".format(
        region, "amazonaws.com.cn" if region.startswith("cn-") else "amazonaws.com"
    )
    sqs = boto3.resource("sqs", region_name=region, config=proxy_config, endpoint_url=endpoint_url)
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        log.debug("SQS queue is %s", queue)
        # Set queue_name attribute
        queue.queue_name = queue.url.split("/")[-1]
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
        # Check that table exists
        ddb_client.describe_table(TableName=table_name)

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
    Requeue the given message into the specified queue.

    :param queue: the queue where to send the message
    :param message: the message to requeue
    """
    max_retries = 1
    # CW event rule can only send messages to SQS with "" enclosing the message.
    # However, when message is re-queued it is sent without ""
    message_body = (
        json.loads(json.loads(message.body)) if str(message.body).startswith('"') else json.loads(message.body)
    )
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


def _retrieve_all_sqs_messages(queue, max_processed_messages):
    log.info("Retrieving messages from SQS queue %s", queue.queue_name)
    max_messages_per_call = 10
    messages = []
    try:
        while len(messages) < max_processed_messages:
            # setting WaitTimeSeconds in order to use Amazon SQS Long Polling.
            # when not using Long Polling with a small queue you might not receive any message
            # since only a subset of random machines is queried.
            retrieved_messages = queue.receive_messages(
                MaxNumberOfMessages=min(max_processed_messages - len(messages), max_messages_per_call),
                WaitTimeSeconds=2,
            )
            if len(retrieved_messages) > 0:
                messages.extend(retrieved_messages)
            else:
                # the queue is not always returning max_messages_per_call even when available
                # looping until receive_messages returns at least 1 message
                break

        log.info("Retrieved %s messages from SQS queue %s", len(messages), queue.queue_name)
    except Exception as e:
        logging.error("Failed to retrieve messages from queue %s with exception: %s", queue.queue_name, e)

    return messages


def _resolve_events_hostname_collision(event_pool):
    update_events = OrderedDict()
    for event in event_pool:
        log.info("Processing %s event for instance %s", event.action, event.host)
        hostname = event.host.hostname
        if hostname in update_events:
            # events are looped in the order they are fetched and by iterating over REMOVE events after ADD events.
            # in case of collisions let's always remove the item that comes first in order to always favor REMOVE ops.
            log.info(
                "Hostname collision. Discarding event %s in favour of event %s", update_events[hostname], event,
            )
            update_events[hostname].message.delete()
            del update_events[hostname]
        update_events[hostname] = event

    return update_events.values()


def _parse_sqs_messages(sqs_config, messages, table, queue):
    try:
        parsed_events = _parse_messages_helper(
            messages, sqs_config, queue_type=QueueType.instance, table=table, queue=queue
        )
        return _resolve_events_hostname_collision(
            event_pool=itertools.chain(parsed_events[EventType.ADD], parsed_events[EventType.REMOVE])
        )
    except Exception as e:
        logging.error("Failed to parse messages from %s with exception: %s", queue.queue_name, e)
    return []


def _parse_health_messages(sqs_config, messages, table, health_queue):
    try:
        parsed_events = _parse_messages_helper(
            messages, sqs_config, queue_type=QueueType.health, table=table, queue=health_queue,
        )
        return _resolve_events_hostname_collision(event_pool=parsed_events[EventType.HEALTH])
    except Exception as e:
        logging.error("Failed to parse messages from %s with exception: %s", health_queue.queue_name, e)
    return []


def _parse_messages_helper(messages, sqs_config, queue_type, table=None, queue=None):
    parsed_events = {
        EventType.ADD: [],
        EventType.REMOVE: [],
        EventType.HEALTH: [],
    }
    event_name_to_processing_function = {
        EventType.HEALTH: lambda: _process_scheduled_maintenance_event(
            sqs_config.stack_name, sqs_config.region, sqs_config.proxy_config, message_attrs, message, table, queue
        ),
        EventType.ADD: lambda: _process_compute_ready_event(
            sqs_config.region, sqs_config.proxy_config, message_attrs, message, table
        ),
        EventType.REMOVE: lambda: _process_instance_terminate_event(message_attrs, message, table, queue),
    }
    event_name_to_event_type = {
        "parallelcluster:EC2_SCHEDULED_EVENT": EventType.HEALTH,
        "parallelcluster:COMPUTE_READY": EventType.ADD,
        "autoscaling:EC2_INSTANCE_TERMINATE": EventType.REMOVE,
    }
    for message in messages:
        # CW event rule can only send messages to SQS with "" enclosing the message.
        # However, when message is re-queued it is sent without ""
        message_text = (
            json.loads(json.loads(message.body)) if str(message.body).startswith('"') else json.loads(message.body)
        )
        message_attrs = json.loads(message_text.get("Message"))
        event_name = message_attrs.get("Event")
        if not event_name:
            log.warning("Unable to read message. Deleting.")
            message.delete()
            continue
        event_type = event_name_to_event_type.get(event_name, None)

        if event_type in SUPPORTED_EVENTTYPE_FOR_QUEUETYPE[queue_type]:
            event = event_name_to_processing_function.get(event_type)()
            if event:
                parsed_events[event_type].append(event)
        else:
            log.info("Unsupported event type %s for %s queue. Discarding message.", event_type, queue_type)
            message.delete()

    return parsed_events


def _process_scheduled_maintenance_event(stack_name, region, proxy_config, message_attrs, message, table, queue):
    try:
        instance_id = message_attrs.get("EC2InstanceId")
        instances_in_cluster = get_cluster_instance_info(
            stack_name, region, proxy_config, instance_ids=[instance_id], include_master=False
        )
        if instance_id in instances_in_cluster:
            hostname = _retrieve_hostname_from_ddb(instance_id, table)
            log.info("Relevant EC2 scheduled event for instance:%s in ASG.", instance_id)
            if hostname:
                return UpdateEvent(EventType.HEALTH, message, Host(instance_id, hostname, None, None))
            else:
                log.error("Instance %s not found in the database.", instance_id)
                _requeue_message(queue, message)
                message.delete()
        else:
            log.info("Irrelevant EC2 scheduled event for instance:%s. Discarding message.", instance_id)
            message.delete()
    except Exception as e:
        log.error("Failed when processing scheduled event message for instance %s with exception %s", instance_id, e)
        _requeue_message(queue, message)
        message.delete()

    return None


def _process_compute_ready_event(sqs_config_region, sqs_config_proxy, message_attrs, message, table):
    instance_id = message_attrs.get("EC2InstanceId")
    instance_type = message_attrs.get("EC2InstanceType")
    # Get instances properties for each event because instance types
    # from instance and CloudFormation could be out-of-sync
    instance_properties = get_instance_properties(sqs_config_region, sqs_config_proxy, instance_type)
    gpus = instance_properties["gpus"]
    slots = message_attrs.get("Slots")
    hostname = message_attrs.get("LocalHostname").split(".")[0]
    _retry_on_request_limit_exceeded(lambda: table.put_item(Item={"instanceId": instance_id, "hostname": hostname}))
    return UpdateEvent(EventType.ADD, message, Host(instance_id, hostname, slots, gpus))


def _process_instance_terminate_event(message_attrs, message, table, queue):
    instance_id = message_attrs.get("EC2InstanceId")
    try:
        hostname = _retrieve_hostname_from_ddb(instance_id, table)
    except Exception as e:
        log.error("Failed when retrieving instance data for instance %s from db with exception %s", instance_id, e)
        _requeue_message(queue, message)
        message.delete()
        return None

    if hostname:
        return UpdateEvent(EventType.REMOVE, message, Host(instance_id, hostname, None, None))
    else:
        log.error("Instance %s not found in the database.", instance_id)
        _requeue_message(queue, message)
        message.delete()
        return None


def _retrieve_hostname_from_ddb(instance_id, table):
    item = _retry_on_request_limit_exceeded(
        lambda: table.get_item(ConsistentRead=True, Key={"instanceId": instance_id})
    )
    if item.get("Item") is not None:
        hostname = item.get("Item").get("hostname")
        return hostname
    else:
        return None


def _process_health_messages(
    health_events, scheduler_module, sqs_config, health_queue,
):
    # Update the scheduler only when there is health event
    if not health_events:
        return

    failed_events, succeeded_events = perform_health_actions(scheduler_module, health_events)

    for event in succeeded_events:
        log.info("Successfully processed event %s for instance %s", event.action, event.host)
        event.message.delete()

    for event in failed_events:
        log.error("Failed when processing event %s for instance %s", event.action, event.host)
        _requeue_message(health_queue, event.message)
        event.message.delete()


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

    failed_events, succeeded_events = update_cluster(
        instance_properties, max_cluster_size, scheduler_module, sqs_config, update_events
    )

    for event in update_events:
        try:
            if event.action == EventType.REMOVE and event in succeeded_events:
                _retry_on_request_limit_exceeded(lambda: table.delete_item(Key={"instanceId": event.host.instance_id}))
        except Exception as e:
            log.error(
                "Failed when updating dynamo db table for instance %s with exception %s", event.host.instance_id, e
            )
            if event not in failed_events:
                failed_events.append(event)
                succeeded_events.remove(event)

    for event in succeeded_events:
        log.info("Successfully processed event %s for instance %s", event.action, event.host)
        event.message.delete()

    for event in failed_events:
        log.error("Failed when processing event %s for instance %s", event.action, event.host)
        if event.action == EventType.REMOVE:
            _requeue_message(queue, event.message)
        else:
            log.error("Deleting failed event %s from queue: %s", event.action, event.message.body)
        event.message.delete()


def update_cluster(instance_properties, max_cluster_size, scheduler_module, sqs_config, update_events):
    # Centos6 - Managing SSH host keys for the nodes joining and leaving the cluster
    # All other OSs support disabling StrictHostKeyChecking conditionally through ssh_config Match directive
    if ".el6." in platform.platform():
        update_ssh_known_hosts(update_events, sqs_config.cluster_user)

    try:
        failed_events, succeeded_events = scheduler_module.update_cluster(
            max_cluster_size, sqs_config.cluster_user, update_events, instance_properties
        )
    except Exception as e:
        log.error("Encountered error when processing events: %s", e)
        failed_events = update_events
        succeeded_events = []
    return failed_events, succeeded_events


def perform_health_actions(scheduler_module, health_events):
    try:
        failed_events, succeeded_events = scheduler_module.perform_health_actions(health_events)
    except Exception as e:
        log.error("Encountered error when processing events: %s", e)
        failed_events = health_events
        succeeded_events = []
    return failed_events, succeeded_events


def _process_instance_queue(
    sqs_config, instance_queue, scheduler_module, table, max_cluster_size, instance_properties, force_cluster_update
):
    messages = _retrieve_all_sqs_messages(instance_queue, sqs_config.max_processed_messages)
    update_events = _parse_sqs_messages(sqs_config, messages, table, instance_queue)
    _process_sqs_messages(
        update_events,
        scheduler_module,
        sqs_config,
        table,
        instance_queue,
        max_cluster_size,
        instance_properties,
        force_cluster_update,
    )


def _process_health_queue(sqs_config, health_queue, scheduler_module, table):
    messages = _retrieve_all_sqs_messages(health_queue, sqs_config.max_processed_messages)
    health_events = _parse_health_messages(sqs_config, messages, table, health_queue)
    _process_health_messages(
        health_events, scheduler_module, sqs_config, health_queue,
    )


def _poll_queue(sqs_config, instance_queue, health_queue, table, asg_name):
    """
    Poll SQS queue.

    :param sqs_config: SQS daemon configuration
    :param queue: SQS Queue object connected to the cluster queue
    :param table: DB table resource object
    """
    scheduler_module = load_module("sqswatcher.plugins." + sqs_config.scheduler)
    scheduler_module.init()

    max_cluster_size = None
    instance_type = None
    cluster_properties_refresh_timer = 0
    while True:
        start_time = datetime.now()

        force_cluster_update = False
        # dynamically retrieve max_cluster_size and compute_instance_type
        if (
            not max_cluster_size
            or not instance_type
            or cluster_properties_refresh_timer >= CLUSTER_PROPERTIES_REFRESH_INTERVAL
        ):
            cluster_properties_refresh_timer = 0
            logging.info("Refreshing cluster properties")
            new_max_cluster_size = retrieve_max_cluster_size(
                sqs_config.region, sqs_config.proxy_config, asg_name, fallback=max_cluster_size
            )
            new_instance_type = get_compute_instance_type(
                sqs_config.region, sqs_config.proxy_config, sqs_config.stack_name, fallback=instance_type
            )
            force_cluster_update = new_max_cluster_size != max_cluster_size or new_instance_type != instance_type
            if new_instance_type != instance_type:
                instance_type = new_instance_type
                instance_properties = get_instance_properties(sqs_config.region, sqs_config.proxy_config, instance_type)
            max_cluster_size = new_max_cluster_size
        cluster_properties_refresh_timer += LOOP_TIME

        _process_instance_queue(
            sqs_config,
            instance_queue,
            scheduler_module,
            table,
            max_cluster_size,
            instance_properties,
            force_cluster_update,
        )
        if not sqs_config.disable_health_check:
            _process_health_queue(sqs_config, health_queue, scheduler_module, table)

        sleep_remaining_loop_time(LOOP_TIME, start_time)


@retry(wait_fixed=seconds(LOOP_TIME))
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s")
    log.info("sqswatcher startup")

    try:
        config = _get_config()
        instance_queue = _get_sqs_queue(config.region, config.sqsqueue, config.proxy_config)
        health_queue = _get_sqs_queue(config.region, config.health_sqsqueue, config.proxy_config)
        table = _get_ddb_table(config.region, config.table_name, config.proxy_config)
        asg_name = get_asg_name(config.stack_name, config.region, config.proxy_config)

        _poll_queue(config, instance_queue, health_queue, table, asg_name)
    except Exception as e:
        log.exception("An unexpected error occurred: %s", e)
        raise


if __name__ == "__main__":
    main()
