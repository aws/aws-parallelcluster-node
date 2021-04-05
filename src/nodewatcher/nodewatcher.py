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
import json
import logging
import os
import sys
import time
from configparser import ConfigParser
from datetime import datetime

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from common.time_utils import minutes, seconds
from common.utils import (
    get_asg_name,
    get_asg_settings,
    get_instance_properties,
    get_metadata,
    load_additional_instance_types_data,
    load_module,
    retrieve_max_cluster_size,
    sleep_remaining_loop_time,
)
from retrying import RetryError, retry

log = logging.getLogger(__name__)

DATA_DIR = "/var/run/nodewatcher/"
IDLETIME_FILE = DATA_DIR + "node_idletime.json"
# Timeout used when nodewatcher starts. The node might not be attached to scheduler yet.
INITIAL_TERMINATE_TIMEOUT = minutes(5)
# Timeout used at every iteration of nodewatcher loop.
TERMINATE_TIMEOUT = minutes(5)
LOOP_TIME = 60
CLUSTER_PROPERTIES_REFRESH_INTERVAL = 300

NodewatcherConfig = collections.namedtuple(
    "NodewatcherConfig",
    ["region", "scheduler", "stack_name", "scaledown_idletime", "proxy_config", "instance_types_data"],
)


def _get_config():
    """
    Get configuration from config file.

    :return: configuration parameters
    """
    config_file = "/etc/nodewatcher.cfg"
    log.info("Reading %s", config_file)

    config = ConfigParser()
    config.read(config_file)
    if config.has_option("nodewatcher", "loglevel"):
        lvl = logging._levelNames[config.get("nodewatcher", "loglevel")]
        logging.getLogger().setLevel(lvl)

    region = config.get("nodewatcher", "region")
    scheduler = config.get("nodewatcher", "scheduler")
    stack_name = config.get("nodewatcher", "stack_name")
    scaledown_idletime = int(config.get("nodewatcher", "scaledown_idletime"))
    instance_types_data = load_additional_instance_types_data(config, "nodewatcher")

    _proxy = config.get("nodewatcher", "proxy")
    proxy_config = Config()
    if _proxy != "NONE":
        proxy_config = Config(proxies={"https": _proxy})

    log.info(
        "Configured parameters: region=%s scheduler=%s stack_name=%s scaledown_idletime=%s proxy=%s "
        "instance_types_data=%s",
        region,
        scheduler,
        stack_name,
        scaledown_idletime,
        _proxy,
        instance_types_data,
    )
    return NodewatcherConfig(region, scheduler, stack_name, scaledown_idletime, proxy_config, instance_types_data)


def _has_jobs(scheduler_module, hostname):
    """
    Verify if there are running jobs in the given host.

    :param scheduler_module: scheduler specific module to use
    :param hostname: host to search for
    :return: true if the given host has running jobs
    """
    _jobs = scheduler_module.has_jobs(hostname)
    log.debug("jobs=%s" % _jobs)
    return _jobs


def _lock_host(scheduler_module, hostname, unlock=False):
    """
    Lock/Unlock the given host (e.g. before termination).

    :param scheduler_module: scheduler specific module to use
    :param hostname: host to lock
    :param unlock: False to lock the host, True to unlock
    """
    log.debug("%s %s" % (unlock and "unlocking" or "locking", hostname))
    scheduler_module.lock_host(hostname, unlock)
    time.sleep(15)  # allow for some settling


def _self_terminate(asg_client, instance_id, decrement_desired=True):
    """
    Terminate the given instance and decrease ASG desired capacity.

    :param asg_client: ASG boto3 client
    :param instance_id: the instance to terminate
    :param decrement_desired: if True decrements ASG desired by 1
    """
    try:
        log.info("Self terminating %s" % instance_id)
        asg_client.terminate_instance_in_auto_scaling_group(
            InstanceId=instance_id, ShouldDecrementDesiredCapacity=decrement_desired
        )
        sys.exit(0)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ValidationError":
            log.info("Min ASG size reached. Not terminating.")
        else:
            log.error("Failed when self terminating instance with error %s.", e.response)
    except Exception as e:
        log.error("Failed when self terminating instance with exception %s.", e)


def _maintain_size(asg_name, asg_client):
    """
    Verify if the desired capacity is lower than the configured min size.

    :param asg_name: the ASG to query for
    :param asg_client: ASG boto3 client
    :return: True if the desired capacity is lower than the configured min size.
    """
    try:
        asg = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name]).get("AutoScalingGroups")[0]
        _capacity = asg.get("DesiredCapacity")
        _min_size = asg.get("MinSize")
        log.info("DesiredCapacity is %d, MinSize is %d" % (_capacity, _min_size))
        if _capacity > _min_size:
            log.debug("Capacity greater than min size.")
            return False
        else:
            log.debug("Capacity less than or equal to min size.")
            return True
    except Exception as e:
        log.error(
            "Failed when checking min cluster size with exception %s. Assuming capacity is greater than min size.", e
        )
        return False


def _terminate_if_down(scheduler_module, config, asg_name, instance_id, max_wait):
    """Check that node is correctly attached to scheduler otherwise terminate the instance."""
    asg_client = boto3.client("autoscaling", region_name=config.region, config=config.proxy_config)

    @retry(wait_fixed=seconds(10), retry_on_result=lambda result: result is True, stop_max_delay=max_wait)
    def _poll_wait_for_node_ready():
        is_down = scheduler_module.is_node_down()
        if is_down:
            log.warning("Node reported as down")
        return is_down

    try:
        _poll_wait_for_node_ready()
    except RetryError:
        log.error("Node is marked as down by scheduler or not attached correctly. Terminating...")
        # jobwatcher already has the logic to request a new host in case of down nodes,
        # which is done in order to speed up cluster recovery.
        _self_terminate(asg_client, instance_id, decrement_desired=not _maintain_size(asg_name, asg_client))


@retry(
    wait_exponential_multiplier=seconds(1),
    wait_exponential_max=seconds(10),
    retry_on_result=lambda result: result is False,
    stop_max_delay=minutes(10),
)
def _wait_for_stack_ready(stack_name, region, proxy_config):
    """
    Verify if the Stack is in one of the *_COMPLETE states.

    :param stack_name: Stack to query for
    :param region: AWS region
    :param proxy_config: Proxy configuration
    :return: true if the stack is in the *_COMPLETE status
    """
    log.info("Waiting for stack %s to be ready", stack_name)
    cfn_client = boto3.client("cloudformation", region_name=region, config=proxy_config)
    stacks = cfn_client.describe_stacks(StackName=stack_name)
    stack_status = stacks["Stacks"][0]["StackStatus"]
    log.info("Stack %s is in status: %s", stack_name, stack_status)
    return stack_status in [
        "CREATE_COMPLETE",
        "UPDATE_COMPLETE",
        "UPDATE_ROLLBACK_COMPLETE",
        "CREATE_FAILED",
        "UPDATE_FAILED",
    ]


def _init_data_dir():
    """Create folder to store nodewatcher data."""
    try:
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
    except Exception as e:
        log.warning(
            "Unable to create the folder '%s' to persist current idle time. Failed with exception: %s", DATA_DIR, e
        )


def _store_idletime(idletime):
    """
    Save idletime to file, in json format.

    :param idletime: the idletime value to store
    """
    data = {"current_idletime": idletime}
    try:
        with open(IDLETIME_FILE, "w") as outfile:
            json.dump(data, outfile)
    except Exception as e:
        log.warning(
            "Unable to store idletime '%s' in the file '%s'. Failed with exception: %s", idletime, IDLETIME_FILE, e
        )


def _init_idletime():
    """
    Initialize idletime value (from file if there).

    :return: the current idletime value (0 if the file doesn't exist)
    """
    idletime = 0
    _init_data_dir()
    if os.path.isfile(IDLETIME_FILE):
        try:
            with open(IDLETIME_FILE) as f:
                data = json.loads(f.read())
                idletime = data.get("current_idletime", 0)
        except Exception as e:
            log.warning("Unable to get idletime from the file '%s'. Failed with exception: %s", IDLETIME_FILE, e)

    return idletime


def _lock_and_terminate(region, proxy_config, scheduler_module, hostname, instance_id):
    _lock_host(scheduler_module, hostname)
    if _has_jobs(scheduler_module, hostname):
        log.info("Instance has active jobs.")
        _lock_host(scheduler_module, hostname, unlock=True)
        return

    asg_client = boto3.client("autoscaling", region_name=region, config=proxy_config)
    _self_terminate(asg_client, instance_id)
    # _self_terminate exits on success
    _lock_host(scheduler_module, hostname, unlock=True)


def _refresh_cluster_properties(region, proxy_config, asg_name):
    """
    Return dynamic cluster properties (at the moment only max cluster size).

    The properties are fetched every CLUSTER_PROPERTIES_REFRESH_INTERVAL otherwise a cached value is returned.
    """
    if not hasattr(_refresh_cluster_properties, "cluster_properties_refresh_timer"):
        _refresh_cluster_properties.cluster_properties_refresh_timer = 0
        _refresh_cluster_properties.cached_max_cluster_size = None

    _refresh_cluster_properties.cluster_properties_refresh_timer += LOOP_TIME
    if (
        not _refresh_cluster_properties.cached_max_cluster_size
        or _refresh_cluster_properties.cluster_properties_refresh_timer >= CLUSTER_PROPERTIES_REFRESH_INTERVAL
    ):
        _refresh_cluster_properties.cluster_properties_refresh_timer = 0
        logging.info("Refreshing cluster properties")
        _refresh_cluster_properties.cached_max_cluster_size = retrieve_max_cluster_size(
            region, proxy_config, asg_name, fallback=_refresh_cluster_properties.cached_max_cluster_size
        )
    return _refresh_cluster_properties.cached_max_cluster_size


def _poll_instance_status(config, scheduler_module, asg_name, hostname, instance_id, instance_type):
    """
    Verify instance/scheduler status and self-terminate the instance.

    The instance will be terminate if not required and exceeded the configured scaledown_idletime.
    :param config: NodewatcherConfig object
    :param scheduler_module: scheduler module
    :param asg_name: ASG name
    :param hostname: current hostname
    :param instance_id: current instance id
    :param instance_type: current instance type
    """
    _wait_for_stack_ready(config.stack_name, config.region, config.proxy_config)
    _terminate_if_down(scheduler_module, config, asg_name, instance_id, INITIAL_TERMINATE_TIMEOUT)

    idletime = _init_idletime()
    instance_properties = get_instance_properties(
        config.region, config.proxy_config, instance_type, config.instance_types_data
    )
    start_time = None
    while True:
        sleep_remaining_loop_time(LOOP_TIME, start_time)
        start_time = datetime.now()

        max_cluster_size = _refresh_cluster_properties(config.region, config.proxy_config, asg_name)

        _store_idletime(idletime)
        _terminate_if_down(scheduler_module, config, asg_name, instance_id, TERMINATE_TIMEOUT)

        has_jobs = _has_jobs(scheduler_module, hostname)
        if has_jobs:
            log.info("Instance has active jobs.")
            idletime = 0
        else:
            has_pending_jobs, error = scheduler_module.has_pending_jobs(instance_properties, max_cluster_size)
            if error:
                # In case of failure _terminate_if_down will take care of removing the node
                log.warning("Encountered an error while polling queue for pending jobs. Considering node as busy")
                continue
            elif has_pending_jobs:
                log.info("Queue has pending jobs. Not terminating instance")
                idletime = 0
                continue

            try:
                min_size, desired_capacity, max_size = get_asg_settings(config.region, config.proxy_config, asg_name)
            except Exception as e:
                logging.error("Failed when retrieving ASG settings with exception %s", e)
                continue

            if desired_capacity <= min_size:
                log.info("Not terminating due to min cluster size reached")
                idletime = 0
            else:
                idletime += 1
                log.info("Instance had no job for the past %s minute(s)", idletime)

                if idletime >= config.scaledown_idletime:
                    _lock_and_terminate(config.region, config.proxy_config, scheduler_module, hostname, instance_id)
                    # _lock_and_terminate exits if termination is successful
                    # set idletime to 0 if termination is aborted
                    idletime = 0


@retry(wait_fixed=seconds(LOOP_TIME), retry_on_exception=lambda exception: not isinstance(exception, SystemExit))
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s")
    log.info("nodewatcher startup")
    try:
        config = _get_config()

        scheduler_module = load_module("nodewatcher.plugins." + config.scheduler)

        instance_id = get_metadata("instance-id")
        hostname = get_metadata("local-hostname")
        instance_type = get_metadata("instance-type")
        log.info("Instance id is %s, hostname is %s, instance type is %s", instance_id, hostname, instance_type)
        asg_name = get_asg_name(config.stack_name, config.region, config.proxy_config)

        _poll_instance_status(config, scheduler_module, asg_name, hostname, instance_id, instance_type)
    except Exception as e:
        log.exception("An unexpected error occurred: %s", e)
        raise


if __name__ == "__main__":
    main()
