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
    CriticalError,
    EventType,
    Host,
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
        "healthsqsqueue",
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
    healthsqsqueue = config.get("sqswatcher", "healthsqsqueue")
    disable_health_check = bool(config.get("sqswatcher", "disable_health_check", fallback=False))
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
            "Configured parameters: region={} scheduler={} sqsqueue={} healthqueue={} table_name={} cluster_user={} "
            "proxy={} stack_name={} max_processed_messages={} disable_health_check={}"
        ).format(
            region,
            scheduler,
            sqsqueue,
            healthsqsqueue,
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
        healthsqsqueue,
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


def _requeue_message(queue, message_body):
    """
    Requeue the given message into the specified queue.

    :param queue: the queue where to send the message
    :param message: the message to requeue
    """
    max_retries = 1
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
    max_messages_per_call = 10
    messages = []
    queue_name = queue.url.split("/")[-1]
    while len(messages) < max_processed_messages:
        # setting WaitTimeSeconds in order to use Amazon SQS Long Polling.
        # when not using Long Polling with a small queue you might not receive any message
        # since only a subset of random machines is queried.
        retrieved_messages = queue.receive_messages(
            MaxNumberOfMessages=min(max_processed_messages - len(messages), max_messages_per_call), WaitTimeSeconds=2
        )
        if len(retrieved_messages) > 0:
            messages.extend(retrieved_messages)
        else:
            # the queue is not always returning max_messages_per_call even when available
            # looping until receive_messages returns at least 1 message
            break

    log.info("Retrieved {} messages from SQS queue {}".format(len(messages), queue_name))

    return messages


def _parse_sqs_messages(sqs_config, messages, table, queue):
    update_events = OrderedDict()
    parsed_events = _parse_messages_helper(messages, False, sqs_config, table, queue)
    for event in itertools.chain(parsed_events["ADD"], parsed_events["REMOVE"]):
        log.info("Processing {} event for instance {}".format(event.action, event.host))
        hostname = event.host.hostname
        if hostname in update_events:
            # events are looped in the order they are fetched and by iterating over REMOVE events after ADD events.
            # in case of collisions let's always remove the item that comes first in order to always favor REMOVE ops.
            log.info(
                "Hostname collision. Discarding event {} in favour of event {}".format(update_events[hostname], event)
            )
            update_events[hostname].message.delete()
            del update_events[hostname]
        update_events[hostname] = event

    return update_events.values()


def _parse_health_messages(sqs_config, messages, health_queue):
    health_update_events = OrderedDict()
    instance_id_to_hostname = get_cluster_instance_info(sqs_config.stack_name, sqs_config.region, include_master=False)
    parsed_events = _parse_messages_helper(messages, True, sqs_config, instance_id_to_hostname=instance_id_to_hostname)
    for health_event in parsed_events["HEALTH"]:
        log.info("Processing {} event for instance {}".format(health_event.action, health_event.host))
        if health_event.host.instance_id in health_update_events:
            # delete first to preserve messages order in dict
            log.info(
                "InstanceID collision. Discarding event {} in favour of event {}".format(
                    health_update_events[health_event.host.instance_id], health_event,
                )
            )
            health_update_events[health_event.host.instance_id].message.delete()
            del health_update_events[health_event.host.instance_id]
        health_update_events[health_event.host.instance_id] = health_event

    return health_update_events.values()


def _parse_messages_helper(messages, is_health_queue, sqs_config, table=None, queue=None, instance_id_to_hostname=None):
    parsed_events = {
        "ADD": [],
        "REMOVE": [],
        "HEALTH": [],
    }
    for message in messages:
        # CW event rule can only send messages to SQS with "" enclosing the message.
        # However, when message is re-queued it is sent without ""
        message_text = (
            json.loads(json.loads(message.body)) if str(message.body).startswith('"') else json.loads(message.body)
        )
        message_attrs = json.loads(message_text.get("Message"))
        event_type = message_attrs.get("Event")

        if not event_type:
            log.warning("Unable to read message. Deleting.")
            message.delete()
            continue
        if is_health_queue:
            if event_type == "parallelcluster:EC2_SCHEDULED_EVENT":
                # filter events for instances currently in ASG
                instances_in_cluster = list(instance_id_to_hostname.keys())
                instance_id = message_attrs.get("EC2InstanceId")
                if instance_id in instances_in_cluster:
                    hostname = instance_id_to_hostname[instance_id]
                    parsed_events["HEALTH"].append(
                        UpdateEvent("SCHEDULED_EVENT", message, Host(instance_id, hostname, None, None))
                    )
                    log.info("Relevant EC2 scheduled event for instance:{} in ASG.".format(instance_id))
                else:
                    log.info("Irrelevant EC2 scheduled event for instance:{}. Discarding message.".format(instance_id))
                    message.delete()
            else:
                log.info("Unsupported event type {} for health queue. Discarding message.".format(event_type))
                message.delete()
        else:
            if event_type == "parallelcluster:COMPUTE_READY":
                add_event = _process_compute_ready_event(
                    sqs_config.region, sqs_config.proxy_config, message_attrs, message, table
                )
                if add_event:
                    parsed_events["ADD"].append(add_event)
            elif event_type == "autoscaling:EC2_INSTANCE_TERMINATE":
                remove_event = _process_instance_terminate_event(message_attrs, message, table, queue)
                if remove_event:
                    parsed_events["REMOVE"].append(remove_event)
            else:
                log.info("Unsupported event type {} for instance queue. Discarding message.".format(event_type))
                message.delete()

    return parsed_events


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
        item = _retry_on_request_limit_exceeded(
            lambda: table.get_item(ConsistentRead=True, Key={"instanceId": instance_id})
        )
    except Exception as e:
        log.error("Failed when retrieving instance data for instance %s from db with exception %s", instance_id, e)
        _requeue_message(queue, message)
        message.delete()
        return None

    if item.get("Item") is not None:
        hostname = item.get("Item").get("hostname")
        return UpdateEvent(EventType.REMOVE, message, Host(instance_id, hostname, None, None))
    else:
        log.error("Instance %s not found in the database.", instance_id)
        _requeue_message(queue, json.loads(message.body))
        message.delete()
        return None


def _process_health_messages(
    health_events, scheduler_module, sqs_config, health_queue, force_cluster_update,
):
    # Update the scheduler only when there are messages from the queue or
    # tha ASG max size got updated.
    if not health_events and not force_cluster_update:
        return

    failed_events, succeeded_events = perform_health_actions(scheduler_module, health_events)

    for event in succeeded_events:
        log.info("Successfully processed event {} for instance {}".format(event.action, event.host))
        event.message.delete()

    for event in failed_events:
        log.error("Failed when processing event {} for instance {}".format(event.action, event.host))
        message_text = (
            json.loads(json.loads(event.message.body))
            if str(event.message.body).startswith('"')
            else json.loads(event.message.body)
        )
        _requeue_message(health_queue, message_text)
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


def _process_health_queue(sqs_config, health_queue, scheduler_module, force_cluster_update):
    messages = _retrieve_all_sqs_messages(health_queue, sqs_config.max_processed_messages)
    health_events = _parse_health_messages(sqs_config, messages, health_queue)
    _process_health_messages(
        health_events, scheduler_module, sqs_config, health_queue, force_cluster_update,
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
            _process_health_queue(sqs_config, health_queue, scheduler_module, force_cluster_update)

        sleep_remaining_loop_time(LOOP_TIME, start_time)


@retry(wait_fixed=seconds(LOOP_TIME))
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s")
    log.info("sqswatcher startup")

    try:
        config = _get_config()
        instance_queue = _get_sqs_queue(config.region, config.sqsqueue, config.proxy_config)
        health_queue = _get_sqs_queue(config.region, config.healthsqsqueue, config.proxy_config)
        table = _get_ddb_table(config.region, config.table_name, config.proxy_config)
        asg_name = get_asg_name(config.stack_name, config.region, config.proxy_config)

        _poll_queue(config, instance_queue, health_queue, table, asg_name)
    except Exception as e:
        log.exception("An unexpected error occurred: %s", e)
        raise


if __name__ == "__main__":
    main()
