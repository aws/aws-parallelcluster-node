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
import logging
from configparser import ConfigParser
from datetime import datetime

import boto3
from botocore.config import Config
from common.time_utils import seconds
from common.utils import (
    get_asg_name,
    get_asg_settings,
    get_compute_instance_type,
    get_instance_properties,
    load_additional_instance_types_data,
    load_module,
    sleep_remaining_loop_time,
)
from retrying import retry

LOOP_TIME = 60
UPDATE_INSTANCE_PROPERTIES_INTERVAL = 180

log = logging.getLogger(__name__)


JobwatcherConfig = collections.namedtuple(
    "JobwatcherConfig", ["region", "scheduler", "stack_name", "pcluster_dir", "proxy_config", "instance_types_data"]
)


def _get_config():
    """
    Get configuration from config file.

    :return: configuration parameters
    """
    config_file = "/etc/jobwatcher.cfg"
    log.info("Reading %s", config_file)

    config = ConfigParser()
    config.read(config_file)
    if config.has_option("jobwatcher", "loglevel"):
        lvl = logging._levelNames[config.get("jobwatcher", "loglevel")]
        logging.getLogger().setLevel(lvl)

    region = config.get("jobwatcher", "region")
    scheduler = config.get("jobwatcher", "scheduler")
    stack_name = config.get("jobwatcher", "stack_name")
    pcluster_dir = config.get("jobwatcher", "cfncluster_dir")
    instance_types_data = load_additional_instance_types_data(config, "jobwatcher")

    _proxy = config.get("jobwatcher", "proxy")
    proxy_config = Config()
    if _proxy != "NONE":
        proxy_config = Config(proxies={"https": _proxy})

    log.info(
        "Configured parameters: region=%s scheduler=%s stack_name=%s pcluster_dir=%s proxy=%s instance_types_data=%s",
        region,
        scheduler,
        stack_name,
        pcluster_dir,
        _proxy,
        instance_types_data,
    )
    return JobwatcherConfig(region, scheduler, stack_name, pcluster_dir, proxy_config, instance_types_data)


def _poll_scheduler_status(config, asg_name, scheduler_module):
    """
    Verify scheduler status and ask the ASG new nodes, if required.

    :param config: JobwatcherConfig object
    :param asg_name: ASG name
    :param scheduler_module: scheduler module
    """
    instance_type = None
    instance_properties = None
    update_instance_properties_timer = 0
    while True:
        start_time = datetime.now()

        # Get instance properties
        if not instance_properties or update_instance_properties_timer >= UPDATE_INSTANCE_PROPERTIES_INTERVAL:
            logging.info("Refreshing compute instance properties")
            update_instance_properties_timer = 0
            new_instance_type = get_compute_instance_type(
                config.region, config.proxy_config, config.stack_name, fallback=instance_type
            )
            if new_instance_type != instance_type:
                instance_type = new_instance_type
                instance_properties = get_instance_properties(
                    config.region, config.proxy_config, instance_type, config.instance_types_data
                )
        update_instance_properties_timer += LOOP_TIME

        # get current limits
        _, current_desired, max_size = get_asg_settings(config.region, config.proxy_config, asg_name)

        # Get current number of nodes
        running = scheduler_module.get_busy_nodes()

        # Get number of nodes requested
        pending = scheduler_module.get_required_nodes(instance_properties, max_size)

        log.info("%d nodes requested, %d nodes busy or unavailable", pending, running)

        if pending < 0:
            log.critical("Error detecting number of required nodes. The cluster will not scale up.")

        elif pending == 0:
            log.info("There are no pending jobs or the requirements on pending jobs cannot be satisfied. Noop.")

        else:
            # Check to make sure requested number of instances is within ASG limits
            required = running + pending
            if required <= current_desired:
                log.info("%d nodes required, %d nodes in asg. Noop" % (required, current_desired))
            else:
                if required > max_size:
                    log.info(
                        "The number of required nodes %d is greater than max %d. Requesting max %d."
                        % (required, max_size, max_size)
                    )
                else:
                    log.info(
                        "Setting desired to %d nodes, requesting %d more nodes from asg."
                        % (required, required - current_desired)
                    )
                requested = min(required, max_size)

                # update ASG
                asg_client = boto3.client("autoscaling", region_name=config.region, config=config.proxy_config)
                asg_client.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=requested)

        sleep_remaining_loop_time(LOOP_TIME, start_time)


@retry(wait_fixed=seconds(LOOP_TIME))
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(module)s:%(funcName)s] %(message)s")
    log.info("jobwatcher startup")
    try:
        config = _get_config()
        asg_name = get_asg_name(config.stack_name, config.region, config.proxy_config)

        scheduler_module = load_module("jobwatcher.plugins." + config.scheduler)

        _poll_scheduler_status(config, asg_name, scheduler_module)
    except Exception as e:
        log.exception("An unexpected error occurred: %s", e)
        raise


if __name__ == "__main__":
    main()
