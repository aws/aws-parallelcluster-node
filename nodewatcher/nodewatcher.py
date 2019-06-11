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
import errno
import json
import logging
import os
import sys
import tarfile
import time
import urllib2
from contextlib import closing

import boto3
from botocore.config import Config
from configparser import ConfigParser
from retrying import RetryError, retry

from common.time_utils import minutes, seconds
from common.utils import CriticalError, get_asg_name, get_asg_settings, get_instance_properties, load_module

log = logging.getLogger(__name__)

DATA_DIR = "/var/run/nodewatcher/"
IDLETIME_FILE = DATA_DIR + "node_idletime.json"
# Timeout used when nodewatcher starts. The node might not be attached to scheduler yet.
INITIAL_TERMINATE_TIMEOUT = minutes(3)
# Timeout used at every iteration of nodewatcher loop.
TERMINATE_TIMEOUT = minutes(1)


NodewatcherConfig = collections.namedtuple(
    "NodewatcherConfig", ["region", "scheduler", "stack_name", "scaledown_idletime", "proxy_config"]
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

    _proxy = config.get("nodewatcher", "proxy")
    proxy_config = Config()
    if _proxy != "NONE":
        proxy_config = Config(proxies={"https": _proxy})

    log.info(
        "Configured parameters: region=%s scheduler=%s stack_name=%s scaledown_idletime=%s proxy=%s",
        region,
        scheduler,
        stack_name,
        scaledown_idletime,
        _proxy,
    )
    return NodewatcherConfig(region, scheduler, stack_name, scaledown_idletime, proxy_config)


def _get_metadata(metadata_path):
    """
    Get EC2 instance metadata.

    :param metadata_path: the metadata relative path
    :return the metadata value.
    """
    try:
        metadata_value = urllib2.urlopen("http://169.254.169.254/latest/meta-data/{0}".format(metadata_path)).read()
    except urllib2.URLError as e:
        error_msg = "Unable to get {0} metadata. Failed with exception: {1}".format(metadata_path, e)
        log.critical(error_msg)
        raise CriticalError(error_msg)

    log.debug("%s=%s", metadata_path, metadata_value)
    return metadata_value


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
    except Exception as e:
        log.error("Failed when self terminating instance with exception %s.", e)
        return False


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


def _dump_logs(instance_id):
    """Dump gzipped /var/log dir to /home/logs/compute/$instance_id.tar.gz"""
    logs_dir = "/home/logs/compute"
    filename = "{0}/{1}.tar.gz".format(logs_dir, instance_id)
    try:
        try:
            os.makedirs(logs_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        log.info("Dumping logs to %s", filename)
        with closing(tarfile.open(filename, "w|gz")) as archive:
            archive.add("/var/log", recursive=True)
    except Exception as e:
        log.warning("Failed while dumping logs to %s with exception %s.", filename, e)


def _terminate_if_down(scheduler_module, config, asg_name, instance_id, max_wait):
    """Check that node is correctly attached to scheduler otherwise terminate the instance"""
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
        _dump_logs(instance_id)
        # decrement asg desired only if not reached the min.
        # jobwatcher already has the logic to request a new host in case of down nodes,
        # which is done in order to speed up cluster recovery.
        _self_terminate(asg_client, instance_id, decrement_desired=not _maintain_size(asg_name, asg_client))


@retry(wait_fixed=seconds(10), retry_on_result=lambda result: result is False, stop_max_delay=minutes(10))
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
    return stack_status in ["CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"]


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
    instance_properties = get_instance_properties(config.region, config.proxy_config, instance_type)
    while True:
        time.sleep(60)
        _store_idletime(idletime)
        _terminate_if_down(scheduler_module, config, asg_name, instance_id, TERMINATE_TIMEOUT)

        has_jobs = _has_jobs(scheduler_module, hostname)
        if has_jobs:
            log.info("Instance has active jobs.")
            idletime = 0
        else:
            asg_conn = boto3.client("autoscaling", region_name=config.region, config=config.proxy_config)
            if _maintain_size(asg_name, asg_conn):
                log.info("Not terminating due to min cluster size reached")
                idletime = 0
            else:
                _, _, max_size = get_asg_settings(config.region, config.proxy_config, asg_name)
                has_pending_jobs, error = scheduler_module.hasPendingJobs(instance_properties, max_size)
                if error:
                    log.warning(
                        "Encountered an error while polling queue for pending jobs. Skipping pending jobs check"
                    )
                elif has_pending_jobs:
                    log.info("Queue has pending jobs. Not terminating instance")
                    idletime = 0
                    continue

                idletime += 1
                log.info("Instance had no job for the past %s minute(s)", idletime)

                if idletime >= config.scaledown_idletime:
                    _lock_host(scheduler_module, hostname)
                    has_jobs = _has_jobs(scheduler_module, hostname)
                    if has_jobs:
                        log.info("Instance has active jobs.")
                        idletime = 0
                        _lock_host(scheduler_module, hostname, unlock=True)
                        continue

                    if _maintain_size(asg_name, asg_conn):
                        log.info("Not terminating due to min cluster size reached")
                        idletime = 0
                    else:
                        _self_terminate(asg_conn, instance_id)

                    _lock_host(scheduler_module, hostname, unlock=True)


@retry(wait_fixed=60000, retry_on_exception=lambda exception: not isinstance(exception, SystemExit))
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s")
    log.info("nodewatcher startup")
    try:
        config = _get_config()

        scheduler_module = load_module("nodewatcher.plugins." + config.scheduler)

        instance_id = _get_metadata("instance-id")
        hostname = _get_metadata("local-hostname")
        instance_type = _get_metadata("instance-type")
        log.info("Instance id is %s, hostname is %s, instance type is %s", instance_id, hostname, instance_type)
        asg_name = get_asg_name(config.stack_name, config.region, config.proxy_config)

        _poll_instance_status(config, scheduler_module, asg_name, hostname, instance_id, instance_type)
    except Exception as e:
        log.critical("An unexpected error occurred: %s", e)
        raise


if __name__ == "__main__":
    main()
