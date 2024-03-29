# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
import logging
from datetime import datetime
from types import SimpleNamespace
from typing import Dict
from unittest.mock import call

import pytest
from assertpy import assert_that
from common.utils import SlurmCommandError
from slurm_plugin.capacity_block_manager import (
    SLURM_RESERVATION_NAME_PREFIX,
    CapacityBlock,
    CapacityBlockManager,
    CapacityBlockManagerError,
)
from slurm_plugin.slurm_resources import DynamicNode, SlurmReservation, StaticNode

from aws.common import AWSClientError
from aws.ec2 import CapacityReservationInfo

FAKE_CAPACITY_BLOCK_ID = "cr-a1234567"
FAKE_CAPACITY_BLOCK_INFO = {
    "CapacityReservationId": FAKE_CAPACITY_BLOCK_ID,
    "EndDateType": "limited",
    "ReservationType": "capacity-block",
    "AvailabilityZone": "eu-east-2a",
    "InstanceMatchCriteria": "targeted",
    "EphemeralStorage": False,
    "CreateDate": "2023-07-29T14:22:45Z  ",
    "StartDate": "2023-08-15T12:00:00Z",
    "EndDate": "2023-08-19T12:00:00Z",
    "AvailableInstanceCount": 0,
    "InstancePlatform": "Linux/UNIX",
    "TotalInstanceCount": 16,
    "State": "payment-pending",
    "Tenancy": "default",
    "EbsOptimized": True,
    "InstanceType": "p5.48xlarge",
}


@pytest.fixture
def capacity_block():
    capacity_block = CapacityBlock(FAKE_CAPACITY_BLOCK_ID)
    capacity_block.add_compute_resource("queue-cb", "compute-resource-cb")
    return capacity_block


class TestCapacityBlock:
    @pytest.mark.parametrize(
        ("state", "expected_output"),
        [("active", True), ("anything-else", False)],
    )
    def test_is_active(self, capacity_block, state, expected_output):
        capacity_block_reservation_info = CapacityReservationInfo({**FAKE_CAPACITY_BLOCK_INFO, "State": state})
        capacity_block.update_capacity_block_reservation_info(capacity_block_reservation_info)
        assert_that(capacity_block.is_active()).is_equal_to(expected_output)

    @pytest.mark.parametrize(
        ("initial_map", "queue", "compute_resource", "expected_map"),
        [
            ({}, "queue1", "cr1", {"queue1": {"cr1"}}),
            ({"queue1": {"cr1"}}, "queue1", "cr2", {"queue1": {"cr1", "cr2"}}),
            ({"queue1": {"cr1"}}, "queue2", "cr2", {"queue1": {"cr1"}, "queue2": {"cr2"}}),
            ({"queue1": {"cr1"}}, "queue2", "cr1", {"queue1": {"cr1"}, "queue2": {"cr1"}}),
        ],
    )
    def test_add_compute_resource(self, initial_map, queue, compute_resource, expected_map):
        capacity_block = CapacityBlock("id")
        capacity_block.compute_resources_map = initial_map
        capacity_block.add_compute_resource(queue, compute_resource)
        assert_that(capacity_block.compute_resources_map).is_equal_to(expected_map)

    @pytest.mark.parametrize(
        "nodes_to_add",
        [(["node1"]), (["node1", "node2"])],
    )
    def test_add_nodename(self, capacity_block, nodes_to_add):
        for nodename in nodes_to_add:
            capacity_block.add_nodename(nodename)
        assert_that(capacity_block.nodenames()).is_equal_to(nodes_to_add)

    @pytest.mark.parametrize(
        ("node", "expected_output"),
        [
            (StaticNode("queue-cb-st-compute-resource-cb-4", "ip-1", "hostname-1", "some_state", "queue-cb"), True),
            (StaticNode("queue-cb-st-compute-resource-other-2", "ip-1", "hostname-1", "some_state", "queue-cb"), False),
            (StaticNode("queue1-st-c5xlarge-4", "ip-1", "hostname-1", "some_state", "queue1"), False),
            (StaticNode("queue2-st-compute-resource1-4", "ip-1", "hostname-1", "some_state", "queue2"), False),
            (StaticNode("queue2-st-compute-resource-cb-2", "ip-1", "hostname-1", "some_state", "queue2"), False),
        ],
    )
    def test_does_node_belong_to(self, capacity_block, node, expected_output):
        assert_that(capacity_block.does_node_belong_to(node)).is_equal_to(expected_output)

    @pytest.mark.parametrize(
        ("reservation_name", "expected_output"),
        [("test", ""), (f"{SLURM_RESERVATION_NAME_PREFIX}anything-else", "anything-else")],
    )
    def test_capacity_block_id_from_slurm_reservation_name(self, reservation_name, expected_output):
        assert_that(CapacityBlock.capacity_block_id_from_slurm_reservation_name(reservation_name)).is_equal_to(
            expected_output
        )

    @pytest.mark.parametrize(
        ("reservation_name", "expected_output"),
        [("test", False), (f"{SLURM_RESERVATION_NAME_PREFIX}anything-else", True)],
    )
    def test_is_capacity_block_slurm_reservation(self, reservation_name, expected_output):
        assert_that(CapacityBlock.is_capacity_block_slurm_reservation(reservation_name)).is_equal_to(expected_output)


class TestCapacityBlockManager:
    @pytest.fixture
    def capacity_block_manager(self):
        return CapacityBlockManager("eu-west-2", {}, "fake_boto3_config")

    @pytest.mark.parametrize(
        ("update_time", "expected_output"),
        [(None, False), ("any-value", True), (datetime(2020, 1, 1, 0, 0, 0), True)],
    )
    def test_is_initialized(self, capacity_block_manager, update_time, expected_output):
        capacity_block_manager._capacity_blocks_update_time = update_time
        assert_that(capacity_block_manager._is_initialized()).is_equal_to(expected_output)

    def test_ec2_client(self, capacity_block_manager, mocker):
        ec2_mock = mocker.patch("slurm_plugin.capacity_block_manager.Ec2Client", return_value=mocker.MagicMock())
        capacity_block_manager.ec2_client()
        ec2_mock.assert_called_with(config="fake_boto3_config", region="eu-west-2")
        capacity_block_manager.ec2_client()
        ec2_mock.assert_called_once()

    @pytest.mark.parametrize(
        (
            "is_time_to_update",
            "previous_reserved_nodenames",
            "capacity_blocks_from_config",
            "capacity_blocks_info_from_ec2",
            "nodes",
            "slurm_reservation_creation_succeeded",
            "expected_reserved_nodenames",
        ),
        [
            (
                # no time to update, preserve old nodenames
                False,
                ["node1"],
                {},
                [],
                [StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb")],
                [True],
                ["node1"],
            ),
            (
                # capacity block from config is empty, remove old nodenames
                True,
                ["node1"],
                {},
                [],
                [StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb")],
                [True],
                [],  # empty because capacity block from config is empty
            ),
            (
                # return first nodename because there is an internal error
                # when associating info from EC2 (2 CBs) and the capacity block in the config (1 CB)
                True,
                ["node1"],
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    )
                },
                [
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "pending", "CapacityReservationId": "cr-123456"}
                    ),
                    # add another id not in the capacity block to trigger an internal error
                    # for _update_capacity_blocks_info_from_ec2, that does not stop the loop
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "active", "CapacityReservationId": "cr-234567"}
                    ),
                ],
                [StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb")],
                [True],
                ["queue-cb-st-compute-resource-cb-1"],
            ),
            (
                # return first nodename because there is an internal error
                # in the second capacity block when creating Slurm reservation
                True,
                ["node1"],
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    ),
                    "cr-234567": SimpleNamespace(
                        capacity_block_id="cr-234567", compute_resources_map={"queue-cb2": {"compute-resource-cb2"}}
                    ),
                },
                [
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "pending", "CapacityReservationId": "cr-123456"}
                    ),
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "active", "CapacityReservationId": "cr-234567"}
                    ),
                ],
                [StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb")],
                [True, False],  # reservation creation failed for the second CB
                ["queue-cb-st-compute-resource-cb-1"],
            ),
            (
                # preserve original list of nodenames because there are internal errors
                # while creating all the slurm reservations
                True,
                ["node1"],
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    ),
                    "cr-234567": SimpleNamespace(
                        capacity_block_id="cr-234567", compute_resources_map={"queue-cb2": {"compute-resource-cb2"}}
                    ),
                },
                [
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "pending", "CapacityReservationId": "cr-123456"}
                    ),
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "active", "CapacityReservationId": "cr-234567"}
                    ),
                ],
                [StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb")],
                [False, False],  # reservation creation failed for all the CBs
                ["node1"],
            ),
            (
                # preserve old nodenames because there is a managed exception when contacting EC2
                True,
                ["node1"],
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    )
                },
                AWSClientError("describe_capacity_reservations", "Boto3Error"),
                [StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb")],
                [False],
                ["node1"],
            ),
            (
                # preserve old nodenames because there is an unexpected exception when contacting EC2
                True,
                ["node1"],
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    )
                },
                Exception("generic-error"),
                [StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb")],
                [False],
                ["node1"],
            ),
            (
                # return nodenames accordingly to the state of the CB (only not active CBs)
                True,
                ["node1"],
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    ),
                    "cr-234567": SimpleNamespace(
                        capacity_block_id="cr-234567", compute_resources_map={"queue-cb2": {"compute-resource-cb2"}}
                    ),
                    "cr-345678": SimpleNamespace(
                        capacity_block_id="cr-345678", compute_resources_map={"queue-cb3": {"compute-resource-cb3"}}
                    ),
                },
                [
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "pending", "CapacityReservationId": "cr-123456"}
                    ),
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "active", "CapacityReservationId": "cr-234567"}
                    ),
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "pending-payment", "CapacityReservationId": "cr-345678"}
                    ),
                ],
                [
                    StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb"),
                    DynamicNode("queue-cb2-dy-compute-resource-cb2-1", "ip-1", "hostname-1", "some_state", "queue-cb2"),
                    DynamicNode("queue-cb3-dy-compute-resource-cb3-1", "ip-1", "hostname-1", "some_state", "queue-cb3"),
                ],
                [True, True, True],
                ["queue-cb-st-compute-resource-cb-1", "queue-cb3-dy-compute-resource-cb3-1"],
            ),
        ],
    )
    def test_get_reserved_nodenames(
        self,
        mocker,
        capacity_block_manager,
        is_time_to_update,
        previous_reserved_nodenames,
        capacity_blocks_from_config,
        capacity_blocks_info_from_ec2,
        nodes,
        slurm_reservation_creation_succeeded,
        expected_reserved_nodenames,
        caplog,
    ):
        mocker.patch.object(capacity_block_manager, "_is_time_to_update", return_value=is_time_to_update)
        mocked_capacity_blocks_from_config = self._build_capacity_blocks(capacity_blocks_from_config)
        mocker.patch.object(
            capacity_block_manager,
            "_retrieve_capacity_blocks_from_fleet_config",
            return_value=mocked_capacity_blocks_from_config,
        )
        mocked_client = mocker.MagicMock()
        expected_ec2_exception = isinstance(capacity_blocks_info_from_ec2, Exception)
        mocked_client.describe_capacity_reservations.side_effect = [capacity_blocks_info_from_ec2]
        capacity_block_manager._ec2_client = mocked_client
        update_res_mock = mocker.patch.object(
            capacity_block_manager, "_update_slurm_reservation", side_effect=slurm_reservation_creation_succeeded
        )
        cleanup_mock = mocker.patch.object(capacity_block_manager, "_cleanup_leftover_slurm_reservations")
        capacity_block_manager._reserved_nodenames = previous_reserved_nodenames
        previous_update_time = datetime(2020, 1, 1, 0, 0, 0)
        capacity_block_manager._capacity_blocks_update_time = previous_update_time

        reserved_nodenames = capacity_block_manager.get_reserved_nodenames(nodes)
        assert_that(capacity_block_manager._reserved_nodenames).is_equal_to(expected_reserved_nodenames)

        if expected_ec2_exception:
            assert_that(caplog.text).contains("Unable to retrieve list of reserved nodes, maintaining old list")
        else:
            expected_internal_error = len(capacity_blocks_from_config) != len(capacity_blocks_info_from_ec2)
            if expected_internal_error:
                assert_that(caplog.text).contains("Unable to find Capacity Block")
                assert_that(capacity_block_manager._reserved_nodenames).is_equal_to(expected_reserved_nodenames)
            else:
                assert_that(reserved_nodenames).is_equal_to(expected_reserved_nodenames)

        if is_time_to_update and not expected_ec2_exception:
            if all(not action_succeeded for action_succeeded in slurm_reservation_creation_succeeded):
                # if all slurm reservations actions failed, do not update the values
                assert_that(capacity_block_manager._capacity_blocks_update_time).is_equal_to(previous_update_time)
                assert_that(capacity_block_manager._capacity_blocks).is_not_equal_to(mocked_capacity_blocks_from_config)
            else:
                assert_that(capacity_block_manager._capacity_blocks_update_time).is_not_equal_to(previous_update_time)
                assert_that(capacity_block_manager._capacity_blocks).is_equal_to(mocked_capacity_blocks_from_config)

            assert_that(capacity_block_manager._slurm_reservation_update_errors).is_equal_to(
                slurm_reservation_creation_succeeded.count(False)
            )
            update_res_mock.assert_has_calls(
                [
                    call(capacity_block=capacity_block, do_update=False)
                    for capacity_block in capacity_block_manager._capacity_blocks.values()
                ],
                any_order=True,
            )
            cleanup_mock.assert_called_once()
        else:
            assert_that(capacity_block_manager._capacity_blocks_update_time).is_equal_to(previous_update_time)
            update_res_mock.assert_not_called()
            cleanup_mock.assert_not_called()

    @pytest.mark.parametrize(
        ("is_initialized", "slurm_reservations_errors", "now", "expected_updated_time"),
        [
            # manager not initialized
            (False, 0, datetime(2020, 1, 1, 1, 00, 0), True),
            # delta < CAPACITY_BLOCK_RESERVATION_UPDATE_PERIOD
            (True, 0, datetime(2020, 1, 1, 1, 00, 10), False),
            (True, 0, datetime(2020, 1, 1, 1, 9, 0), False),
            # delta < CAPACITY_BLOCK_RESERVATION_UPDATE_PERIOD but errors in previous update
            (True, 1, datetime(2020, 1, 1, 1, 9, 0), True),
            # delta >= CAPACITY_BLOCK_RESERVATION_UPDATE_PERIOD
            (True, 0, datetime(2020, 1, 1, 1, 10, 0), True),
            (True, 0, datetime(2020, 1, 1, 1, 21, 0), True),
            (True, 0, datetime(2020, 1, 1, 2, 00, 0), True),
            (True, 0, datetime(2020, 1, 2, 1, 00, 0), True),
            (True, 1, datetime(2020, 1, 2, 1, 00, 0), True),
        ],
    )
    def test_is_time_to_update(
        self, capacity_block_manager, is_initialized, slurm_reservations_errors, now, expected_updated_time
    ):
        capacity_block_manager._capacity_blocks_update_time = datetime(2020, 1, 1, 1, 00, 0) if is_initialized else None
        capacity_block_manager._slurm_reservation_update_errors = slurm_reservations_errors
        assert_that(capacity_block_manager._is_time_to_update(now))

    @pytest.mark.parametrize(
        ("capacity_blocks", "nodes", "expected_nodenames_in_capacity_block"),
        [
            # empty initial list
            (
                {},
                [
                    StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb"),
                    StaticNode("queue1-st-compute-resource1-2", "ip-1", "hostname-1", "some_state", "queue1"),
                    StaticNode("queue-cb-st-compute-resource-cb-3", "ip-1", "hostname-1", "some_state", "queue-cb"),
                    StaticNode("queue1-st-compute-resource1-4", "ip-1", "hostname-1", "some_state", "queue1"),
                    StaticNode("queue-cb2-st-compute-resource-cb2-5", "ip-1", "hostname-1", "some_state", "queue-cb2"),
                    StaticNode("queue-cb-st-othercr-6", "ip-1", "hostname-1", "some_state", "queue1"),
                    StaticNode("otherqueue-st-compute-resource-cb-7", "ip-1", "hostname-1", "some_state", "otherqueue"),
                ],
                {},
            ),
            # multiple CBs
            (
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    ),
                    "cr-234567": SimpleNamespace(
                        capacity_block_id="cr-234567", compute_resources_map={"queue-cb2": {"compute-resource-cb2"}}
                    ),
                },
                [
                    StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb"),
                    StaticNode("queue1-st-compute-resource1-2", "ip-1", "hostname-1", "some_state", "queue1"),
                    StaticNode("queue-cb-st-compute-resource-cb-3", "ip-1", "hostname-1", "some_state", "queue-cb"),
                    StaticNode("queue1-st-compute-resource1-4", "ip-1", "hostname-1", "some_state", "queue1"),
                    StaticNode("queue-cb2-st-compute-resource-cb2-5", "ip-1", "hostname-1", "some_state", "queue-cb2"),
                    StaticNode("queue-cb-st-othercr-6", "ip-1", "hostname-1", "some_state", "queue1"),
                    StaticNode("otherqueue-st-compute-resource-cb-7", "ip-1", "hostname-1", "some_state", "otherqueue"),
                ],
                {
                    "cr-123456": ["queue-cb-st-compute-resource-cb-1", "queue-cb-st-compute-resource-cb-3"],
                    "cr-234567": ["queue-cb2-st-compute-resource-cb2-5"],
                },
            ),
            # same CB used by multiple queues/compute resources
            (
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    ),
                },
                [
                    StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb"),
                    StaticNode("queue1-st-compute-resource1-2", "ip-1", "hostname-1", "some_state", "queue1"),
                    StaticNode("queue-cb-st-compute-resource-cb-3", "ip-1", "hostname-1", "some_state", "queue-cb"),
                    StaticNode("queue1-st-compute-resource1-4", "ip-1", "hostname-1", "some_state", "queue1"),
                    StaticNode("queue-cb2-st-compute-resource-cb2-5", "ip-1", "hostname-1", "some_state", "queue-cb2"),
                    StaticNode("queue-cb-st-othercr-6", "ip-1", "hostname-1", "some_state", "queue1"),
                    StaticNode("otherqueue-st-compute-resource-cb-7", "ip-1", "hostname-1", "some_state", "otherqueue"),
                ],
                {
                    "cr-123456": ["queue-cb-st-compute-resource-cb-1", "queue-cb-st-compute-resource-cb-3"],
                },
            ),
        ],
    )
    def test_associate_nodenames_to_capacity_blocks(
        self,
        capacity_block_manager,
        capacity_blocks,
        nodes,
        expected_nodenames_in_capacity_block,
    ):
        mocked_capacity_blocks = self._build_capacity_blocks(capacity_blocks)
        capacity_block_manager._capacity_blocks = mocked_capacity_blocks
        capacity_block_manager._associate_nodenames_to_capacity_blocks(mocked_capacity_blocks, nodes)

        for capacity_block_id in capacity_block_manager._capacity_blocks.keys():
            # assert in the nodenames list there are only nodes associated to the right queue and compute resource
            assert_that(capacity_block_manager._capacity_blocks.get(capacity_block_id).nodenames()).is_equal_to(
                expected_nodenames_in_capacity_block.get(capacity_block_id)
            )

    @pytest.mark.parametrize(
        (
            "slurm_reservations",
            "delete_reservation_behaviour",
            "expected_delete_calls",
            "expected_leftover_slurm_reservations",
            "expected_error_message",
        ),
        [
            (
                [
                    # reservation from the customer -> skipped
                    SlurmReservation(name="other_reservation", state="active", users="anyone", nodes="node1"),
                    # reservation associated with existing CB -> skipped
                    SlurmReservation(
                        name=f"{SLURM_RESERVATION_NAME_PREFIX}cr-123456", state="active", users="anyone", nodes="node1"
                    ),
                    # node associated with another old CB, no longer in the config, this is a leftover reservation
                    SlurmReservation(
                        name=f"{SLURM_RESERVATION_NAME_PREFIX}cr-987654", state="active", users="anyone", nodes="node1"
                    ),
                    # leftover reservation triggering an exception
                    SlurmReservation(
                        name=f"{SLURM_RESERVATION_NAME_PREFIX}cr-876543", state="active", users="anyone", nodes="node1"
                    ),
                ],
                [None, SlurmCommandError("delete error")],
                [f"{SLURM_RESERVATION_NAME_PREFIX}cr-987654", f"{SLURM_RESERVATION_NAME_PREFIX}cr-876543"],
                [f"{SLURM_RESERVATION_NAME_PREFIX}cr-987654"],
                f"Unable to delete slurm reservation {SLURM_RESERVATION_NAME_PREFIX}cr-876543. delete error",
            ),
            (
                # no reservation in the output because of the error retrieving res info
                SlurmCommandError("list error"),
                [],
                [],
                [],
                "Unable to retrieve list of existing Slurm reservations. list error",
            ),
        ],
    )
    def test_cleanup_leftover_slurm_reservations(
        self,
        mocker,
        capacity_block_manager,
        capacity_block,
        slurm_reservations,
        delete_reservation_behaviour,
        expected_delete_calls,
        expected_leftover_slurm_reservations,
        expected_error_message,
        caplog,
    ):
        # only cr-123456, queue-cb, compute-resource-cb is in the list of capacity blocks from config
        capacity_block_manager._capacity_blocks = {"cr-123456": capacity_block}
        mocker.patch(
            "slurm_plugin.capacity_block_manager.get_slurm_reservations_info", side_effect=[slurm_reservations]
        )
        delete_res_mock = mocker.patch(
            "slurm_plugin.capacity_block_manager.delete_slurm_reservation", side_effect=delete_reservation_behaviour
        )
        capacity_block_manager._cleanup_leftover_slurm_reservations()

        # verify that only the slurm reservation associated with a CB, no longer in the config,
        # are considered as leftover
        expected_calls = []
        for slurm_reservation in expected_delete_calls:
            expected_calls.append(call(name=slurm_reservation))
        delete_res_mock.assert_has_calls(expected_calls)

        assert_that(delete_res_mock.call_count).is_equal_to(len(delete_reservation_behaviour))
        assert_that(caplog.text).contains(expected_error_message)

    @pytest.mark.parametrize(
        (
            "state",
            "reservation_exists",
            "do_update",
            "expected_create_res_call",
            "expected_update_res_call",
            "expected_delete_res_call",
            "expected_output",
        ),
        [
            # Not existing reservation, do_update is useless
            ("pending", False, None, True, False, False, True),
            # Existing reservation, update_res is called accordingly to do_update value
            ("pending", True, True, False, True, False, True),
            ("pending", True, False, False, False, False, True),
            # Not existing reservation and CB in active state, do_update is useless
            ("active", False, None, False, False, False, True),
            # Existing reservation and CB in active state, do_update is useless
            ("active", True, None, False, False, True, True),
            # Error, do_update is useless
            ("active", SlurmCommandError("error checking res"), None, False, False, False, False),
        ],
    )
    def test_update_slurm_reservation(
        self,
        mocker,
        capacity_block_manager,
        capacity_block,
        state,
        reservation_exists,
        do_update,
        expected_create_res_call,
        expected_update_res_call,
        expected_delete_res_call,
        expected_output,
        caplog,
    ):
        caplog.set_level(logging.INFO)
        capacity_block_reservation_info = CapacityReservationInfo({**FAKE_CAPACITY_BLOCK_INFO, "State": state})
        capacity_block.update_capacity_block_reservation_info(capacity_block_reservation_info)
        capacity_block.add_nodename("node1")
        capacity_block.add_nodename("node2")
        nodenames = ",".join(capacity_block.nodenames())
        slurm_reservation_name = f"{SLURM_RESERVATION_NAME_PREFIX}{FAKE_CAPACITY_BLOCK_ID}"
        check_res_mock = mocker.patch(
            "slurm_plugin.capacity_block_manager.is_slurm_reservation", side_effect=[reservation_exists]
        )
        create_res_mock = mocker.patch("slurm_plugin.capacity_block_manager.create_slurm_reservation")
        update_res_mock = mocker.patch("slurm_plugin.capacity_block_manager.update_slurm_reservation")
        delete_res_mock = mocker.patch("slurm_plugin.capacity_block_manager.delete_slurm_reservation")
        expected_start_time = datetime(2020, 1, 1, 0, 0, 0)
        mocker.patch("slurm_plugin.capacity_block_manager.datetime").now.return_value = expected_start_time

        output_value = capacity_block_manager._update_slurm_reservation(capacity_block, do_update)
        assert_that(expected_output).is_equal_to(output_value)

        # check the right commands to create/delete/update reservations are called accordingly to the state
        check_res_mock.assert_called_with(name=slurm_reservation_name)
        msg_prefix = f"Capacity Block reservation {FAKE_CAPACITY_BLOCK_ID} is in state {state}. "
        msg_suffix = f" Slurm reservation {slurm_reservation_name} for nodes {nodenames}."

        # when state is != active
        if expected_create_res_call:
            create_res_mock.assert_called_with(
                name=slurm_reservation_name, start_time="now", nodes=nodenames, duration="infinite"
            )
            assert_that(caplog.text).contains(msg_prefix + "Creating" + msg_suffix)
        else:
            create_res_mock.assert_not_called()
        if expected_update_res_call:
            update_res_mock.assert_called_with(name=slurm_reservation_name, nodes=nodenames)
            assert_that(caplog.text).contains(msg_prefix + "Updating existing" + msg_suffix)
        else:
            update_res_mock.assert_not_called()

        # when state is active
        if expected_delete_res_call:
            delete_res_mock.assert_called_with(name=slurm_reservation_name)
            assert_that(caplog.text).contains(msg_prefix + "Deleting" + msg_suffix)
        else:
            delete_res_mock.assert_not_called()

        if state == "active" and not reservation_exists:
            assert_that(caplog.text).contains(msg_prefix + "Nothing to do. No existing" + msg_suffix)

        if isinstance(reservation_exists, SlurmCommandError):
            assert_that(caplog.text).contains(
                f"Unable to update slurm reservation pcluster-{FAKE_CAPACITY_BLOCK_ID} for "
                f"Capacity Block {FAKE_CAPACITY_BLOCK_ID}. Skipping it. error checking res"
            )

    @pytest.mark.parametrize(
        ("init_capacity_blocks", "capacity_blocks_info_from_ec2", "expected_error"),
        [
            # nothing in the config
            ({}, [], None),
            # exception, keep previous values
            (
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    )
                },
                [
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "active", "CapacityReservationId": "cr-123456"}
                    ),
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "pending", "CapacityReservationId": "cr-234567"}
                    ),
                ],
                AWSClientError("describe_capacity_reservations", "Boto3Error"),
            ),
            # internal error, because trying to update a capacity block not in the list, keep previous values
            (
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    ),
                },
                [
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "active", "CapacityReservationId": "cr-123456"}
                    ),
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "pending", "CapacityReservationId": "cr-234567"}
                    ),
                ],
                "Unable to find Capacity Block cr-234567",
            ),
            # update old values with new values
            (
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    ),
                    "cr-234567": SimpleNamespace(
                        capacity_block_id="cr-234567", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    ),
                },
                [
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "active", "CapacityReservationId": "cr-123456"}
                    ),
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "pending", "CapacityReservationId": "cr-234567"}
                    ),
                ],
                None,
            ),
        ],
    )
    def test_update_capacity_blocks_info_from_ec2(
        self,
        mocker,
        capacity_block_manager,
        init_capacity_blocks,
        capacity_blocks_info_from_ec2,
        expected_error,
        caplog,
    ):
        caplog.set_level(logging.INFO)
        mocked_capacity_blocks = self._build_capacity_blocks(init_capacity_blocks)
        capacity_block_manager._capacity_blocks = mocked_capacity_blocks
        expected_exception = isinstance(expected_error, AWSClientError)
        mocked_client = mocker.MagicMock()
        mocked_client.describe_capacity_reservations.side_effect = [
            expected_error if expected_exception else capacity_blocks_info_from_ec2
        ]
        capacity_block_manager._ec2_client = mocked_client

        if expected_exception:
            with pytest.raises(
                CapacityBlockManagerError, match="Unable to retrieve Capacity Blocks information from EC2. Boto3Error"
            ):
                capacity_block_manager._update_capacity_blocks_info_from_ec2(mocked_capacity_blocks)

        elif expected_error:
            capacity_block_manager._update_capacity_blocks_info_from_ec2(mocked_capacity_blocks)
            assert_that(caplog.text).contains(expected_error)

            # assert that only existing item has been updated
            assert_that(
                capacity_block_manager._capacity_blocks.get("cr-123456")._capacity_block_reservation_info
            ).is_equal_to(
                CapacityReservationInfo(
                    {**FAKE_CAPACITY_BLOCK_INFO, "State": "active", "CapacityReservationId": "cr-123456"}
                )
            )
            assert_that(capacity_block_manager._capacity_blocks.get("cr-234567")).is_none()
        else:
            capacity_block_manager._update_capacity_blocks_info_from_ec2(mocked_capacity_blocks)
            if init_capacity_blocks:
                # verify that all the blocks have the updated info from ec2
                assert_that(
                    capacity_block_manager._capacity_blocks.get("cr-123456")._capacity_block_reservation_info
                ).is_equal_to(
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "active", "CapacityReservationId": "cr-123456"}
                    )
                )
                assert_that(
                    capacity_block_manager._capacity_blocks.get("cr-234567")._capacity_block_reservation_info
                ).is_equal_to(
                    CapacityReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "pending", "CapacityReservationId": "cr-234567"}
                    )
                )

    @pytest.mark.parametrize(
        ("fleet_config", "expected_capacity_blocks", "expected_exception"),
        [
            ({}, {}, False),
            (
                {
                    "queue-cb": {
                        "compute-resource-cb": {"CapacityType": "capacity-block", "CapacityReservationId": "cr-123456"}
                    },
                    "queue1": {
                        "compute-resource1": {"CapacityType": "on-demand", "CapacityReservationId": "cr-123456"}
                    },
                    "queue-cb2": {
                        "compute-resource-cb2": {"CapacityType": "capacity-block", "CapacityReservationId": "cr-234567"}
                    },
                },
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456", compute_resources_map={"queue-cb": {"compute-resource-cb"}}
                    ),
                    "cr-234567": SimpleNamespace(
                        capacity_block_id="cr-234567", compute_resources_map={"queue-cb2": {"compute-resource-cb2"}}
                    ),
                },
                False,
            ),
            # Same CB in multiple queues
            (
                {
                    "queue-cb": {
                        "compute-resource-cb": {"CapacityType": "capacity-block", "CapacityReservationId": "cr-123456"}
                    },
                    "queue1": {
                        "compute-resource1": {"CapacityType": "capacity-block", "CapacityReservationId": "cr-123456"}
                    },
                },
                {
                    "cr-123456": SimpleNamespace(
                        capacity_block_id="cr-123456",
                        compute_resources_map={"queue-cb": {"compute-resource-cb"}, "queue1": {"compute-resource1"}},
                    ),
                },
                False,
            ),
            (
                {"broken-queue-without-id": {"compute-resource-cb": {"CapacityType": "capacity-block"}}},
                {},
                True,
            ),
            (
                {"queue-with-cr-id-but-no-cb": {"compute-resource-cb": {"CapacityReservationId": "cr-123456"}}},
                {},
                False,
            ),
        ],
    )
    def test_retrieve_capacity_blocks_from_fleet_config(
        self, capacity_block_manager, fleet_config, expected_capacity_blocks, expected_exception
    ):
        capacity_block_manager._fleet_config = fleet_config

        if expected_exception:
            with pytest.raises(KeyError):
                capacity_block_manager._retrieve_capacity_blocks_from_fleet_config()
        else:
            assert_that(self._build_capacity_blocks(expected_capacity_blocks)).is_equal_to(
                capacity_block_manager._retrieve_capacity_blocks_from_fleet_config()
            )

    @staticmethod
    def _build_capacity_blocks(expected_capacity_blocks_structure: Dict[str, SimpleNamespace]):
        """Convert dict with list of SimpleNamespaces to list of CapacityBlocks."""
        expected_capacity_blocks = {}
        for capacity_block_id, capacity_block_structure in expected_capacity_blocks_structure.items():
            expected_capacity_block = CapacityBlock(capacity_block_id)
            for queue, compute_resources in capacity_block_structure.compute_resources_map.items():
                for compute_resource in compute_resources:
                    expected_capacity_block.add_compute_resource(queue, compute_resource)
                    expected_capacity_blocks.update({capacity_block_id: expected_capacity_block})
        return expected_capacity_blocks

    @pytest.mark.parametrize(
        ("compute_resource_config", "expected_result"),
        [
            ({}, False),
            ({"CapacityType": "spot"}, False),
            ({"CapacityType": "on-demand"}, False),
            ({"CapacityType": "capacity-block"}, True),
        ],
    )
    def test__is_compute_resource_associated_to_capacity_block(
        self, capacity_block_manager, compute_resource_config, expected_result
    ):
        assert_that(
            capacity_block_manager._is_compute_resource_associated_to_capacity_block(compute_resource_config)
        ).is_equal_to(expected_result)

    @pytest.mark.parametrize(
        ("compute_resource_config", "expected_result", "expected_exception"),
        [
            ({}, False, True),
            ({"CapacityType": "spot"}, False, True),
            ({"CapacityType": "spot", "CapacityReservationId": "cr-123456"}, "cr-123456", False),
            ({"CapacityType": "on-demand", "CapacityReservationId": "cr-123456"}, "cr-123456", False),
            ({"CapacityType": "capacity-block"}, True, True),
            ({"CapacityType": "capacity-block", "CapacityReservationId": "cr-123456"}, "cr-123456", False),
        ],
    )
    def test_capacity_reservation_id_from_compute_resource_config(
        self, capacity_block_manager, compute_resource_config, expected_result, expected_exception
    ):
        if expected_exception:
            with pytest.raises(KeyError):
                capacity_block_manager._capacity_reservation_id_from_compute_resource_config(compute_resource_config)
        else:
            assert_that(
                capacity_block_manager._capacity_reservation_id_from_compute_resource_config(compute_resource_config)
            ).is_equal_to(expected_result)
