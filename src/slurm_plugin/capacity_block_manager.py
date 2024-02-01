# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List

from common.schedulers.slurm_reservation_commands import (
    create_slurm_reservation,
    delete_slurm_reservation,
    get_slurm_reservations_info,
    is_slurm_reservation,
    update_slurm_reservation,
)
from common.time_utils import seconds_to_minutes
from common.utils import SlurmCommandError
from slurm_plugin.slurm_resources import SlurmNode

from aws.common import AWSClientError
from aws.ec2 import CapacityReservationInfo, Ec2Client

logger = logging.getLogger(__name__)

# Time in minutes to wait to retrieve Capacity Block Reservation information from EC2
CAPACITY_BLOCK_RESERVATION_UPDATE_PERIOD = 10
SLURM_RESERVATION_NAME_PREFIX = "pcluster-"


class CapacityBlockManagerError(Exception):
    """Represent an error during the execution of an action with the CapacityBlockManager."""

    def __init__(self, message: str):
        super().__init__(message)


class CapacityType(Enum):
    """Enum to identify the type compute supported by the queues."""

    CAPACITY_BLOCK = "capacity-block"
    ONDEMAND = "on-demand"
    SPOT = "spot"


class CapacityBlock:
    """
    Class to store Capacity Block info from EC2 and fleet config.

    Contains info like:
    - associated queue and compute resources from the config,
    - state from EC2,
    - name of the slurm reservation that will be created for this CB.
    """

    def __init__(self, capacity_block_id):
        self.capacity_block_id = capacity_block_id
        self.compute_resources_map: Dict[str, set] = {}
        self._capacity_block_reservation_info = None
        self._nodenames = []

    def update_capacity_block_reservation_info(self, capacity_block_reservation_info: CapacityReservationInfo):
        """Update info from CapacityReservationInfo."""
        self._capacity_block_reservation_info = capacity_block_reservation_info

    def slurm_reservation_name(self):
        """Retrieve slurm reservation associated."""
        return f"{SLURM_RESERVATION_NAME_PREFIX}{self.capacity_block_id}"

    def add_compute_resource(self, queue_name: str, compute_resource_name: str):
        """
        Add compute resource to the internal map.

        The same CB can be associated to multiple queues/compute-resources.
        """
        compute_resources_by_queue = self.compute_resources_map.get(queue_name, set())
        compute_resources_by_queue.add(compute_resource_name)
        self.compute_resources_map.update({queue_name: compute_resources_by_queue})

    def add_nodename(self, nodename: str):
        """Add node name to the list of nodenames associated to the capacity block."""
        self._nodenames.append(nodename)

    def nodenames(self):
        """Return list of nodenames associated with the capacity block."""
        return self._nodenames

    def state(self):
        """Return state of the CB: payment-pending, pending, active, expired, and payment-failed."""
        return self._capacity_block_reservation_info.state()

    def is_active(self):
        """Return true if CB is in active state."""
        return self.state() == "active"

    def does_node_belong_to(self, node):
        """Return true if the node belongs to the CB."""
        return node.compute_resource_name in self.compute_resources_map.get(node.queue_name, set())

    @staticmethod
    def capacity_block_id_from_slurm_reservation_name(slurm_reservation_name: str):
        """Parse slurm reservation name to retrieve related capacity block id."""
        return slurm_reservation_name[len(SLURM_RESERVATION_NAME_PREFIX) :]  # noqa E203

    @staticmethod
    def is_capacity_block_slurm_reservation(slurm_reservation_name: str):
        """Return true if slurm reservation name is related to a CB, if it matches a specific internal convention."""
        return slurm_reservation_name.startswith(SLURM_RESERVATION_NAME_PREFIX)

    def __eq__(self, other):
        return (
            self.capacity_block_id == other.capacity_block_id
            and self.compute_resources_map == other.compute_resources_map
        )


class CapacityBlockManager:
    """Capacity Block Reservation Manager."""

    def __init__(self, region, fleet_config, boto3_config):
        self._region = region
        self._fleet_config = fleet_config
        self._boto3_config = boto3_config
        self._ec2_client = None
        # internal variables to store Capacity Block info from fleet config and EC2
        self._capacity_blocks: Dict[str, CapacityBlock] = {}
        self._capacity_blocks_update_time = None
        self._reserved_nodenames: List[str] = []
        self._slurm_reservation_update_errors = 0

    @property
    def ec2_client(self):
        if not self._ec2_client:
            self._ec2_client = Ec2Client(config=self._boto3_config, region=self._region)
        return self._ec2_client

    def _is_initialized(self):
        """Return true if Capacity Block Manager has been already initialized."""
        return self._capacity_blocks_update_time is not None

    def get_reserved_nodenames(self, nodes: List[SlurmNode]):
        """Manage nodes part of capacity block reservation. Returns list of reserved nodes."""
        try:
            # evaluate if it's the moment to update info
            now = datetime.now(tz=timezone.utc)
            if self._is_time_to_update(now):
                reserved_nodenames = []
                self._slurm_reservation_update_errors = 0

                # find an updated list of capacity blocks from fleet config
                capacity_blocks = self._retrieve_capacity_blocks_from_fleet_config()
                if capacity_blocks:
                    # update capacity blocks details from ec2 (e.g. state)
                    self._update_capacity_blocks_info_from_ec2(capacity_blocks)
                    # associate nodenames to capacity blocks,
                    # according to queues and compute resources from fleet configuration
                    self._associate_nodenames_to_capacity_blocks(capacity_blocks, nodes)

                    # create, update or delete slurm reservation for the nodes according to CB details.
                    for capacity_block in capacity_blocks.values():
                        slurm_reservation_updated = self._update_slurm_reservation(
                            capacity_block=capacity_block, do_update=not self._is_initialized()
                        )
                        if not slurm_reservation_updated:
                            self._slurm_reservation_update_errors += 1

                        # If CB is in not yet active or expired add nodes to list of reserved nodes,
                        # only if slurm reservation has been correctly created/updated
                        if slurm_reservation_updated and not capacity_block.is_active():
                            reserved_nodenames.extend(capacity_block.nodenames())

                # If all Slurm reservation actions failed do not update object attributes
                if (
                    self._slurm_reservation_update_errors != len(capacity_blocks)
                    or self._slurm_reservation_update_errors == 0
                ):
                    # Once all the steps have been successful, update object attributes
                    self._capacity_blocks = capacity_blocks
                    self._capacity_blocks_update_time = now
                    self._reserved_nodenames = reserved_nodenames

                # delete slurm reservations created by CapacityBlockManager not associated to existing capacity blocks
                self._cleanup_leftover_slurm_reservations()

        except (SlurmCommandError, CapacityBlockManagerError) as e:
            logger.error(
                "Unable to retrieve list of reserved nodes, maintaining old list: %s. %s",
                self._reserved_nodenames,
                e,
            )
        except Exception as e:
            logger.error(
                "Unexpected error. Unable to retrieve list of reserved nodes, maintaining old list: %s. %s",
                self._reserved_nodenames,
                e,
            )

        return self._reserved_nodenames

    def _is_time_to_update(self, current_time: datetime):
        """
        Return true if it's the time to update capacity blocks info, from ec2 and config, and manage slurm reservations.

        This is true when the CapacityBlockManager is not yet initialized (self._capacity_blocks_update_time == None),
        when there were errors updating slurm reservations in the previous loop and every 10 minutes.
        """
        return (
            not self._is_initialized()
            or self._slurm_reservation_update_errors
            or seconds_to_minutes((current_time - self._capacity_blocks_update_time).total_seconds())
            > CAPACITY_BLOCK_RESERVATION_UPDATE_PERIOD
        )

    @staticmethod
    def _associate_nodenames_to_capacity_blocks(capacity_blocks: Dict[str, CapacityBlock], nodes: List[SlurmNode]):
        """
        Update capacity_block info adding nodenames list.

        Check configured CBs and associate nodes to them according to queue and compute resource info.
        """
        for node in nodes:
            for capacity_block in capacity_blocks.values():
                if capacity_block.does_node_belong_to(node):
                    capacity_block.add_nodename(node.name)
                    break

    def _cleanup_leftover_slurm_reservations(self):
        """Find list of slurm reservations created by ParallelCluster but not part of the configured CBs."""
        try:
            for slurm_reservation in get_slurm_reservations_info():
                if CapacityBlock.is_capacity_block_slurm_reservation(slurm_reservation.name):
                    capacity_block_id = CapacityBlock.capacity_block_id_from_slurm_reservation_name(
                        slurm_reservation.name
                    )
                    if capacity_block_id not in self._capacity_blocks.keys():
                        logger.info(
                            (
                                "Found leftover slurm reservation %s for nodes %s. "
                                "Related Capacity Block %s is no longer in the cluster configuration. "
                                "Deleting the slurm reservation."
                            ),
                            slurm_reservation.name,
                            slurm_reservation.nodes,
                            capacity_block_id,
                        )
                        try:
                            delete_slurm_reservation(name=slurm_reservation.name)
                        except SlurmCommandError as e:
                            logger.error("Unable to delete slurm reservation %s. %s", slurm_reservation.name, e)
                    else:
                        logger.debug(
                            (
                                "Slurm reservation %s is managed by ParallelCluster "
                                "and related to an existing Capacity Block. Skipping it."
                            ),
                            slurm_reservation.name,
                        )
                else:
                    logger.debug(
                        "Slurm reservation %s is not managed by ParallelCluster. Skipping it.", slurm_reservation.name
                    )
        except SlurmCommandError as e:
            logger.error("Unable to retrieve list of existing Slurm reservations. %s", e)

    @staticmethod
    def _update_slurm_reservation(capacity_block: CapacityBlock, do_update: bool):
        """
        Update Slurm reservation associated to the given Capacity Block.

        A CB has five possible states: payment-pending, pending, active, expired and payment-failed,
        we need to create/delete Slurm reservation accordingly.

        Pass do_update to True only if you want to update already created reservations
        (e.g. to update the node list or when the CapacityBlockManager is not yet initialized).

        Returns True if slurm action is completed correctly, False otherwise.
        """

        def _log_cb_info(action_info):
            logger.info(
                "Capacity Block reservation %s is in state %s. %s Slurm reservation %s for nodes %s.",
                capacity_block.capacity_block_id,
                capacity_block.state(),
                action_info,
                slurm_reservation_name,
                capacity_block_nodenames,
            )

        # retrieve list of nodes associated to a given slurm reservation/capacity block
        slurm_reservation_name = capacity_block.slurm_reservation_name()
        capacity_block_nodenames = ",".join(capacity_block.nodenames())

        try:
            reservation_exists = is_slurm_reservation(name=slurm_reservation_name)
            # if CB is active we need to remove Slurm reservation and start nodes
            if capacity_block.is_active():
                # if Slurm reservation exists, delete it.
                if reservation_exists:
                    _log_cb_info("Deleting")
                    delete_slurm_reservation(name=slurm_reservation_name)
                else:
                    _log_cb_info("Nothing to do. No existing")

            # if CB is expired or not active we need to (re)create Slurm reservation
            # to avoid considering nodes as unhealthy
            else:
                # create or update Slurm reservation
                if reservation_exists:
                    if do_update:
                        _log_cb_info("Updating existing")
                        update_slurm_reservation(name=slurm_reservation_name, nodes=capacity_block_nodenames)
                    else:
                        _log_cb_info("Nothing to do. Already existing")
                else:
                    _log_cb_info("Creating")
                    # The reservation should start now, and will be removed when the capacity block will become active
                    create_slurm_reservation(
                        name=slurm_reservation_name,
                        start_time="now",
                        nodes=capacity_block_nodenames,
                        duration="infinite",
                    )

            action_completed = True
        except SlurmCommandError as e:
            logger.error(
                "Unable to update slurm reservation %s for Capacity Block %s. Skipping it. %s",
                slurm_reservation_name,
                capacity_block.capacity_block_id,
                e,
            )
            action_completed = False

        return action_completed

    def _update_capacity_blocks_info_from_ec2(self, capacity_blocks: Dict[str, CapacityBlock]):
        """
        Update capacity blocks in given capacity_blocks by adding capacity reservation info.

        This method is called every time the CapacityBlockManager is re-initialized,
        so when it starts/is restarted or when fleet configuration changes.
        """
        capacity_block_ids = list(capacity_blocks.keys())
        logger.info("Retrieving Capacity Blocks information from EC2 for %s", ",".join(capacity_block_ids))
        try:
            capacity_block_reservations_info: List[CapacityReservationInfo] = (
                self.ec2_client.describe_capacity_reservations(capacity_block_ids)
            )

            for capacity_block_reservation_info in capacity_block_reservations_info:
                capacity_block_id = capacity_block_reservation_info.capacity_reservation_id()
                try:
                    capacity_blocks[capacity_block_id].update_capacity_block_reservation_info(
                        capacity_block_reservation_info
                    )
                except KeyError:
                    # should never happen
                    logger.error(f"Unable to find Capacity Block {capacity_block_id} in the internal map.")
        except AWSClientError as e:
            msg = f"Unable to retrieve Capacity Blocks information from EC2. {e}"
            logger.error(msg)
            raise CapacityBlockManagerError(msg)

    def _retrieve_capacity_blocks_from_fleet_config(self):
        """
        Collect list of capacity reservation target from all queues/compute-resources in the fleet config.

        Fleet config json has the following format:
        {
            "my-queue": {
                "my-compute-resource": {
                   "Api": "create-fleet",
                    "CapacityType": "on-demand|spot|capacity-block",
                    "AllocationStrategy": "lowest-price|capacity-optimized",
                    "Instances": [
                        { "InstanceType": "p4d.24xlarge" }
                    ],
                    "MaxPrice": "",
                    "Networking": {
                        "SubnetIds": ["subnet-123456"]
                    },
                    "CapacityReservationId": "id"
                }
            }
        }
        """
        capacity_blocks: Dict[str, CapacityBlock] = {}
        logger.info("Retrieving Capacity Blocks from fleet configuration.")

        for queue_name, queue_config in self._fleet_config.items():
            for compute_resource_name, compute_resource_config in queue_config.items():
                if self._is_compute_resource_associated_to_capacity_block(compute_resource_config):
                    capacity_block_id = self._capacity_reservation_id_from_compute_resource_config(
                        compute_resource_config
                    )
                    # retrieve existing CapacityBlock if exists or create a new one.
                    capacity_block = capacity_blocks.get(
                        capacity_block_id, CapacityBlock(capacity_block_id=capacity_block_id)
                    )
                    capacity_block.add_compute_resource(
                        queue_name=queue_name, compute_resource_name=compute_resource_name
                    )
                    capacity_blocks.update({capacity_block_id: capacity_block})

        return capacity_blocks

    @staticmethod
    def _is_compute_resource_associated_to_capacity_block(compute_resource_config):
        """Return True if compute resource is associated to a Capacity Block reservation."""
        capacity_type = compute_resource_config.get("CapacityType", CapacityType.ONDEMAND)
        return capacity_type == CapacityType.CAPACITY_BLOCK.value

    @staticmethod
    def _capacity_reservation_id_from_compute_resource_config(compute_resource_config):
        """Return capacity reservation target if present, None otherwise."""
        try:
            return compute_resource_config["CapacityReservationId"]
        except KeyError as e:
            # This should never happen because this file is created by cookbook config parser
            logger.error(
                "Unable to retrieve CapacityReservationId from compute resource info: %s", compute_resource_config
            )
            raise e
