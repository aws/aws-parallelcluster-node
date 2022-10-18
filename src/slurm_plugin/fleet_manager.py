# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
import copy
import logging
import time
from abc import ABC, abstractmethod

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class EC2Instance:
    def __init__(self, id, private_ip, hostname, launch_time):
        """Initialize slurm node with attributes."""
        self.id = id
        self.private_ip = private_ip
        self.hostname = hostname
        self.launch_time = launch_time
        self.slurm_node = None

    def __eq__(self, other):
        """Compare 2 SlurmNode objects."""
        if isinstance(other, EC2Instance):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        attrs = ", ".join(["{key}={value}".format(key=key, value=repr(value)) for key, value in self.__dict__.items()])
        return "{class_name}({attrs})".format(class_name=self.__class__.__name__, attrs=attrs)

    def __str__(self):
        return f"{self.id}"

    def __hash__(self):
        return hash(self.id)

    @staticmethod
    def from_describe_instance_data(instance_info):
        try:
            return EC2Instance(
                instance_info["InstanceId"],
                instance_info["PrivateIpAddress"],
                instance_info["PrivateDnsName"].split(".")[0],
                instance_info["LaunchTime"],
            )
        except KeyError as e:
            logger.error("Unable to retrieve EC2 instance info: %s", e)
            raise e


class FleetManagerException(Exception):
    """Represent an error during the execution of an action with the FleetManager or FleetManagerFactory."""

    def __init__(self, message: str):
        super().__init__(message)


class FleetManagerFactory:
    @staticmethod
    def get_manager(
        cluster_name,
        region,
        boto3_config,
        fleet_config,
        queue,
        compute_resource,
        all_or_nothing,
        run_instances_overrides,
        create_fleet_overrides,
    ):
        try:
            queue_config = fleet_config[queue]
            compute_resource_config = queue_config[compute_resource]
            api = compute_resource_config["Api"]
        except KeyError as e:
            message = "Unable to find"
            if e.args[0] == "Api":
                message += f" 'Api' key in the compute resource '{compute_resource}',"
            else:
                message += f" queue '{queue}' or compute resource '{compute_resource}'"
            message += f" in the fleet config: {fleet_config}"

            logger.error(message)
            raise FleetManagerException(message)

        if api == "create-fleet":
            return Ec2CreateFleetManager(
                cluster_name,
                region,
                boto3_config,
                queue,
                compute_resource,
                compute_resource_config,
                all_or_nothing,
                create_fleet_overrides.get(queue, {}).get(compute_resource, {}),
            )
        elif api == "run-instances":
            return Ec2RunInstancesManager(
                cluster_name,
                region,
                boto3_config,
                queue,
                compute_resource,
                compute_resource_config,
                all_or_nothing,
                run_instances_overrides.get(queue, {}).get(compute_resource, {}),
            )
        else:
            raise FleetManagerException(
                f"Unsupported Api '{api}' specified in queue '{queue}', compute resource '{compute_resource}'"
            )


class FleetManager(ABC):
    """Abstract Fleet Manager."""

    @abstractmethod
    def __init__(
        self,
        cluster_name,
        region,
        boto3_config,
        queue,
        compute_resource,
        compute_resource_config,
        all_or_nothing,
        launch_overrides,
    ):
        self._cluster_name = cluster_name
        self._region = region
        self._boto3_config = boto3_config
        self._queue = queue
        self._compute_resource = compute_resource
        self._compute_resource_config = compute_resource_config
        self._all_or_nothing = all_or_nothing
        self._launch_overrides = launch_overrides

    @abstractmethod
    def _evaluate_launch_params(self, count):
        pass

    @abstractmethod
    def _launch_instances(self, launch_params):
        pass

    def launch_ec2_instances(self, count):
        """
        Launch EC2 instances.

        :raises ClientError in case of failures with Boto3 calls (run_instances, create_fleet, describe_instances)
        :raises FleetManagerException in case of missing required instance type info (e.g. private-ip) after 3 retries.
        """
        launch_params = self._evaluate_launch_params(count)
        assigned_nodes = self._launch_instances(launch_params)
        logger.debug("Launched the following instances: %s", assigned_nodes.get("Instances"))
        return [EC2Instance.from_describe_instance_data(instance_info) for instance_info in assigned_nodes["Instances"]]


class Ec2RunInstancesManager(FleetManager):
    """Manager to create EC2 instances fleet using EC2 run_instances API."""

    def __init__(
        self,
        cluster_name,
        region,
        boto3_config,
        queue,
        compute_resource,
        compute_resource_config,
        all_or_nothing,
        launch_overrides,
    ):
        super().__init__(
            cluster_name,
            region,
            boto3_config,
            queue,
            compute_resource,
            compute_resource_config,
            all_or_nothing,
            launch_overrides,
        )

    def _evaluate_launch_params(self, count):
        """Evaluate parameters to be passed to run_instances call."""
        launch_params = {
            # Set MinCount to "count" to make the run_instances call fail if entire count cannot be satisfied
            "MinCount": 1 if not self._all_or_nothing else count,
            "MaxCount": count,
            # LaunchTemplate is different for every compute resources in every queue
            "LaunchTemplate": {
                "LaunchTemplateName": f"{self._cluster_name}-{self._queue}-{self._compute_resource}",
                "Version": "$Latest",
            },
        }

        launch_params.update(self._launch_overrides)
        if self._launch_overrides:
            logger.info("Found RunInstances parameters override. Launching instances with: %s", launch_params)
        return launch_params

    def _launch_instances(self, launch_params):
        """Launch a batch of ec2 instances."""
        try:
            return run_instances(self._region, self._boto3_config, launch_params)
        except ClientError as e:
            logger.error("Failed RunInstances request: %s", e.response.get("ResponseMetadata").get("RequestId"))
            raise e


class Ec2CreateFleetManager(FleetManager):
    """Manager to create EC2 instances fleet using create_fleet API."""

    def __init__(
        self,
        cluster_name,
        region,
        boto3_config,
        queue,
        compute_resource,
        compute_resource_config,
        all_or_nothing,
        launch_overrides,
    ):
        super().__init__(
            cluster_name,
            region,
            boto3_config,
            queue,
            compute_resource,
            compute_resource_config,
            all_or_nothing,
            launch_overrides,
        )

    def _evaluate_launch_params(self, count):
        """Evaluate parameters to be passed to create_fleet call."""
        template_overrides = []
        try:
            common_launch_options = {
                # AllocationStrategy can assume different values for SpotOptions and OnDemandOptions
                "AllocationStrategy": self._compute_resource_config["AllocationStrategy"],
                "SingleInstanceType": False,
                "SingleAvailabilityZone": True,  # Set to False for Multi-AZ support
                # If the minimum target capacity is not reached, the fleet launches no instances
                "MinTargetCapacity": 1 if not self._all_or_nothing else count,
            }

            queue_overrides = {}
            if self._compute_resource_config["CapacityType"] == "spot":
                if self._compute_resource_config.get("MaxPrice"):
                    queue_overrides = {"MaxPrice": str(self._compute_resource_config["MaxPrice"])}

                launch_options = {"SpotOptions": common_launch_options}
            else:
                launch_options = {
                    "OnDemandOptions": {
                        **common_launch_options,
                        "CapacityReservationOptions": {"UsageStrategy": "use-capacity-reservations-first"},
                    },
                }

            for instance_type in self._compute_resource_config["Instances"]:
                override = copy.deepcopy(queue_overrides)
                override["InstanceType"] = instance_type["InstanceType"]
                template_overrides.append(override)

            launch_params = {
                "LaunchTemplateConfigs": [
                    {
                        "LaunchTemplateSpecification": {
                            # LaunchTemplate is different for every compute resources in every queue
                            "LaunchTemplateName": f"{self._cluster_name}-{self._queue}-{self._compute_resource}",
                            "Version": "$Latest",
                        },
                        "Overrides": template_overrides,
                    }
                ],
                "TargetCapacitySpecification": {
                    "TotalTargetCapacity": count,
                    "DefaultTargetCapacityType": self._compute_resource_config["CapacityType"],
                },
                "Type": "instant",
                **launch_options,
                # TODO verify if we need to add user's tag in "TagSpecifications": []
            }
        except KeyError as e:
            message = (
                f"Unable to find key {e} in the configuration of queue: {self._queue}, "
                f"compute resource {self._compute_resource}"
            )
            logger.error(message)
            raise FleetManagerException(message)

        launch_params.update(self._launch_overrides)
        if self._launch_overrides:
            logger.info("Found CreateFleet parameters override. Launching instances with: %s", launch_params)
        return launch_params

    def _launch_instances(self, launch_params):
        """Launch a batch of ec2 instances."""
        try:
            response = create_fleet(self._region, self._boto3_config, launch_params)
            logger.debug("CreateFleet response: %s", response)

            instances = response.get("Instances", [])
            log_level = logging.WARNING if instances else logging.ERROR
            for err in response.get("Errors", []):
                logger.log(
                    log_level,
                    "Error in CreateFleet request (%s): %s - %s",
                    response.get("ResponseMetadata", {}).get("RequestId"),
                    err.get("ErrorCode"),
                    err.get("ErrorMessage"),
                )

            instance_ids = [inst_id for instance in instances for inst_id in instance["InstanceIds"]]
            instances, partial_instance_ids = self._get_instances_info(instance_ids)
            if partial_instance_ids:
                logger.error("Unable to retrieve instance info for instances: %s", partial_instance_ids)

            return {"Instances": instances}
        except ClientError as e:
            logger.error("Failed CreateFleet request: %s", e.response.get("ResponseMetadata", {}).get("RequestId"))
            raise e

    def _get_instances_info(self, instance_ids: list):
        """
        Describe instances to retrieve info not available from create-fleet response.

        :raises ClientError in case of boto3 failure
        :return list of instances with complete information and list of IDs for instances with incomplete information
        """
        instances = []
        partial_instance_ids = instance_ids

        retry = 3
        while retry > 0 and partial_instance_ids:
            # Wait for instances to be available in EC2
            time.sleep(5)
            complete_instances, partial_instance_ids = self._retrieve_instances_info_from_ec2(partial_instance_ids)
            instances.extend(complete_instances)
            retry = retry - 1

        return instances, partial_instance_ids

    def _retrieve_instances_info_from_ec2(self, instance_ids: list):
        """
        Retrieve instance info from EC2 by Instance Ids and verify to have required info.

        :raises ClientError in case of boto3 failure
        :return list of instances with complete information and list of IDs for instances with incomplete information
        """
        complete_instances = []
        partial_instance_ids = []

        if instance_ids:
            ec2_client = boto3.client("ec2", region_name=self._region, config=self._boto3_config)
            paginator = ec2_client.get_paginator("describe_instances")
            response_iterator = paginator.paginate(InstanceIds=instance_ids)
            filtered_iterator = response_iterator.search("Reservations[].Instances[]")

            for instance_info in filtered_iterator:
                try:
                    # Try to build EC2Instance objects using all the required fields
                    EC2Instance.from_describe_instance_data(instance_info)
                    complete_instances.append(instance_info)
                except KeyError:
                    partial_instance_ids.append(instance_info["InstanceId"])

        return complete_instances, partial_instance_ids


def run_instances(region, boto3_config, run_instances_kwargs):
    """
    Check whether to override ec2 run_instances.

    This function is defined here to be able to overwrite it when executing manual tests or in integration tests.
    """
    try:
        from slurm_plugin.overrides import run_instances

        return run_instances(region=region, boto3_config=boto3_config, **run_instances_kwargs)
    except ImportError:
        logger.info("Launching instances with run_instances API. Parameters: %s", run_instances_kwargs)
        ec2_client = boto3.client("ec2", region_name=region, config=boto3_config)
        return ec2_client.run_instances(**run_instances_kwargs)


def create_fleet(region, boto3_config, create_fleet_kwargs):
    """
    Check whether to override ec2 create_fleet.

    This function is defined here to be able to overwrite it when executing manual tests or in integration tests.
    """
    try:
        from slurm_plugin.overrides import create_fleet

        return create_fleet(region=region, boto3_config=boto3_config, **create_fleet_kwargs)
    except ImportError:
        logger.info("Launching instances with create_fleet API. Parameters: %s", create_fleet_kwargs)
        ec2_client = boto3.client("ec2", region_name=region, config=boto3_config)
        return ec2_client.create_fleet(**create_fleet_kwargs)
