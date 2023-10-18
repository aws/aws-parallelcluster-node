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
from unittest.mock import call

import pytest
from assertpy import assert_that
from slurm_plugin.capacity_block_manager import SLURM_RESERVATION_NAME_PREFIX, CapacityBlock, CapacityBlockManager
from slurm_plugin.slurm_resources import StaticNode

from aws.ec2 import CapacityBlockReservationInfo

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
    return CapacityBlock(FAKE_CAPACITY_BLOCK_ID, "queue-cb", "compute-resource-cb")


class TestCapacityBlock:
    @pytest.mark.parametrize(
        ("state", "expected_output"),
        [("active", True), ("anything-else", False)],
    )
    def test_is_active(self, capacity_block, state, expected_output):
        capacity_block_reservation_info = CapacityBlockReservationInfo({**FAKE_CAPACITY_BLOCK_INFO, "State": state})
        capacity_block.update_ec2_info(capacity_block_reservation_info)
        assert_that(capacity_block.is_active()).is_equal_to(expected_output)

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
            (StaticNode("queue1-st-c5xlarge-4", "ip-1", "hostname-1", "some_state", "queue1"), False),
            (StaticNode("queue2-st-compute-resource1-4", "ip-1", "hostname-1", "some_state", "queue2"), False),
        ],
    )
    def test_does_node_belong_to(self, capacity_block, node, expected_output):
        assert_that(capacity_block.does_node_belong_to(node)).is_equal_to(expected_output)

    @pytest.mark.parametrize(
        ("reservation_name", "expected_output"),
        [("test", ""), (f"{SLURM_RESERVATION_NAME_PREFIX}anything-else", "anything-else")],
    )
    def test_slurm_reservation_name_to_id(self, reservation_name, expected_output):
        assert_that(CapacityBlock.slurm_reservation_name_to_id(reservation_name)).is_equal_to(expected_output)

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

    def test_ec2_client(self, capacity_block_manager, mocker):
        ec2_mock = mocker.patch("slurm_plugin.capacity_block_manager.Ec2Client", return_value=mocker.MagicMock())
        capacity_block_manager.ec2_client()
        ec2_mock.assert_called_with(config="fake_boto3_config")
        capacity_block_manager.ec2_client()
        ec2_mock.assert_called_once()

    @pytest.mark.parametrize(
        ("capacity_blocks", "nodes", "expected_nodenames_in_capacity_block"),
        [
            (
                {
                    "cr-123456": CapacityBlock("id", "queue-cb", "compute-resource-cb"),
                    "cr-234567": CapacityBlock("id2", "queue-cb2", "compute-resource-cb2"),
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
            )
        ],
    )
    def test_associate_nodenames_to_capacity_blocks(
        self,
        capacity_block_manager,
        capacity_blocks,
        nodes,
        expected_nodenames_in_capacity_block,
    ):
        capacity_block_manager._capacity_blocks = capacity_blocks
        capacity_block_manager._associate_nodenames_to_capacity_blocks(nodes)

        for capacity_block_id in capacity_block_manager._capacity_blocks.keys():
            # assert in the nodenames list there are only nodes associated to the right queue and compute resource
            assert_that(capacity_block_manager._capacity_blocks.get(capacity_block_id).nodenames()).is_equal_to(
                expected_nodenames_in_capacity_block.get(capacity_block_id)
            )

    @pytest.mark.parametrize(
        ("nodes", "expected_leftover_slurm_reservations"),
        [
            (
                [
                    # node without a slurm reservation -> skipped
                    StaticNode("queue1-st-compute-resource1-1", "ip-1", "hostname-1", "some_state", "queue1"),
                    # node with a reservation from the customer -> skipped
                    StaticNode(
                        "queue4-st-compute-resource1-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue4",
                        reservation_name="other-reservation",
                    ),
                    # node associated with CB, not yet part of slurm reservation -> skipped
                    StaticNode("queue-cb-st-compute-resource-cb-1", "ip-1", "hostname-1", "some_state", "queue-cb"),
                    # node with a reservation associated with CB -> skipped
                    StaticNode(
                        "queue-cb-st-compute-resource-cb-2",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue-cb",
                        reservation_name=f"{SLURM_RESERVATION_NAME_PREFIX}cr-123456",
                    ),
                    # node with a reservation associated with an old CB but in the queue/cr associated to a new CB,
                    # this is a leftover reservation
                    StaticNode(
                        "queue-cb-st-compute-resource-cb-3",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue-cb",
                        reservation_name=f"{SLURM_RESERVATION_NAME_PREFIX}cr-876543",
                    ),
                    # node with a reservation associated with existing CB,
                    # but from an older queue/cr no longer in config -> skipped
                    StaticNode(
                        "queue2-st-compute-resource1-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue2",
                        reservation_name=f"{SLURM_RESERVATION_NAME_PREFIX}cr-123456",
                    ),
                    # node associated with another old CB, no longer in the config, this is a leftover reservation
                    StaticNode(
                        "queue3-st-compute-resource1-1",
                        "ip-1",
                        "hostname-1",
                        "some_state",
                        "queue3",
                        reservation_name=f"{SLURM_RESERVATION_NAME_PREFIX}cr-987654",
                    ),
                ],
                [f"{SLURM_RESERVATION_NAME_PREFIX}cr-876543", f"{SLURM_RESERVATION_NAME_PREFIX}cr-987654"],
            )
        ],
    )
    def test_cleanup_leftover_slurm_reservations(
        self,
        mocker,
        capacity_block_manager,
        capacity_block,
        nodes,
        expected_leftover_slurm_reservations,
    ):
        # only cr-123456, queue-cb, compute-resource-cb is in the list of capacity blocks from config
        capacity_block_manager._capacity_blocks = {"cr-123456": capacity_block}
        delete_res_mock = mocker.patch("slurm_plugin.capacity_block_manager.delete_slurm_reservation")
        capacity_block_manager._cleanup_leftover_slurm_reservations(nodes)

        # verify that only the slurm reservation associated with a CB, no longer in the config,
        # are considered as leftover
        expected_calls = []
        for slurm_reservation in expected_leftover_slurm_reservations:
            expected_calls.append(call(name=slurm_reservation))
        delete_res_mock.assert_has_calls(expected_calls)

    @pytest.mark.parametrize(
        (
            "state",
            "reservation_exists",
            "expected_create_res_call",
            "expected_update_res_call",
            "expected_delete_res_call",
        ),
        [
            ("pending", False, True, False, False),
            ("pending", True, False, True, False),
            ("active", False, False, False, False),
            ("active", True, False, False, True),
        ],
    )
    def test_update_slurm_reservation(
        self,
        mocker,
        capacity_block_manager,
        capacity_block,
        state,
        reservation_exists,
        expected_create_res_call,
        expected_update_res_call,
        expected_delete_res_call,
        caplog,
    ):
        caplog.set_level(logging.INFO)
        capacity_block_reservation_info = CapacityBlockReservationInfo({**FAKE_CAPACITY_BLOCK_INFO, "State": state})
        capacity_block.update_ec2_info(capacity_block_reservation_info)
        capacity_block.add_nodename("node1")
        capacity_block.add_nodename("node2")
        nodenames = ",".join(capacity_block.nodenames())
        slurm_reservation_name = f"{SLURM_RESERVATION_NAME_PREFIX}{FAKE_CAPACITY_BLOCK_ID}"
        check_res_mock = mocker.patch(
            "slurm_plugin.capacity_block_manager.does_slurm_reservation_exist", return_value=reservation_exists
        )
        create_res_mock = mocker.patch("slurm_plugin.capacity_block_manager.create_slurm_reservation")
        update_res_mock = mocker.patch("slurm_plugin.capacity_block_manager.update_slurm_reservation")
        delete_res_mock = mocker.patch("slurm_plugin.capacity_block_manager.delete_slurm_reservation")
        expected_start_time = datetime(2020, 1, 1, 0, 0, 0)
        mocker.patch("slurm_plugin.capacity_block_manager.datetime").now.return_value = expected_start_time

        capacity_block_manager._update_slurm_reservation(capacity_block)

        # check the right commands to create/delete/update reservations are called accordingly to the state
        check_res_mock.assert_called_with(name=slurm_reservation_name)
        msg_prefix = f"Capacity Block reservation {FAKE_CAPACITY_BLOCK_ID} is in state {state}. "
        msg_suffix = f" Slurm reservation {slurm_reservation_name} for nodes {nodenames}."

        # when state is != active
        if expected_create_res_call:
            create_res_mock.assert_called_with(
                name=slurm_reservation_name, start_time=expected_start_time, nodes=nodenames
            )
            assert_that(caplog.text).contains(msg_prefix + "Creating related" + msg_suffix)
        if expected_update_res_call:
            update_res_mock.assert_called_with(name=slurm_reservation_name, nodes=nodenames)
            assert_that(caplog.text).contains(msg_prefix + "Updating existing related" + msg_suffix)

        # when state is active
        if expected_delete_res_call:
            delete_res_mock.assert_called_with(name=slurm_reservation_name)
            assert_that(caplog.text).contains(msg_prefix + "Deleting related" + msg_suffix)

        if state == "active" and not reservation_exists:
            assert_that(caplog.text).contains(msg_prefix + "Nothing to do. No existing" + msg_suffix)

    @pytest.mark.parametrize(
        (
            "init_capacity_blocks",
            "capacity_blocks_from_config",
            "capacity_blocks_info_from_ec2",
            "expected_new_capacity_blocks",
            "expected_new_update_time",
        ),
        [
            # nothing in the config, just change update time
            ({}, {}, [], {}, True),
            # new config without info, remove old block from the map
            (
                {
                    "cr-987654": CapacityBlock("id", "queue-cb", "compute-resource-cb"),
                },
                {},
                [],
                {},
                True,
            ),
            # update old values with new values
            (
                {
                    "cr-987654": CapacityBlock("id", "queue-cb", "compute-resource-cb"),
                },
                {
                    "cr-123456": CapacityBlock("id", "queue-cb", "compute-resource-cb"),
                    "cr-234567": CapacityBlock("id2", "queue-cb2", "compute-resource-cb2"),
                },
                [
                    CapacityBlockReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "active", "CapacityReservationId": "cr-123456"}
                    ),
                    CapacityBlockReservationInfo(
                        {**FAKE_CAPACITY_BLOCK_INFO, "State": "pending", "CapacityReservationId": "cr-234567"}
                    ),
                ],
                {
                    "cr-123456": CapacityBlock("id", "queue-cb", "compute-resource-cb"),
                    "cr-234567": CapacityBlock("id2", "queue-cb2", "compute-resource-cb2"),
                },
                True,
            ),
        ],
    )
    def test_update_capacity_blocks_info_from_ec2(
        self,
        mocker,
        capacity_block_manager,
        capacity_blocks_from_config,
        init_capacity_blocks,
        expected_new_capacity_blocks,
        capacity_blocks_info_from_ec2,
        expected_new_update_time,
    ):
        mocker.patch.object(
            capacity_block_manager, "_capacity_blocks_from_config", return_value=capacity_blocks_from_config
        )
        mocked_now = datetime(2020, 1, 1, 0, 0, 0)
        mocker.patch("slurm_plugin.capacity_block_manager.datetime").now.return_value = mocked_now
        capacity_block_manager._capacity_blocks = init_capacity_blocks

        mocked_client = mocker.MagicMock()
        mocked_client.return_value.describe_capacity_reservations.return_value = capacity_blocks_info_from_ec2
        capacity_block_manager._ec2_client = mocked_client

        capacity_block_manager._update_capacity_blocks_info_from_ec2()

        assert_that(expected_new_capacity_blocks).is_equal_to(capacity_block_manager._capacity_blocks)
        assert_that(capacity_block_manager._capacity_blocks_update_time).is_equal_to(mocked_now)
        if expected_new_capacity_blocks:
            # verify that all the blocks have the updated info from ec2
            assert_that(
                capacity_block_manager._capacity_blocks.get("cr-123456")._capacity_block_reservation_info
            ).is_equal_to(
                CapacityBlockReservationInfo(
                    {**FAKE_CAPACITY_BLOCK_INFO, "State": "active", "CapacityReservationId": "cr-123456"}
                )
            )
            assert_that(
                capacity_block_manager._capacity_blocks.get("cr-234567")._capacity_block_reservation_info
            ).is_equal_to(
                CapacityBlockReservationInfo(
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
                        "compute-resource-cb": {
                            "CapacityType": "capacity-block",
                            "CapacityReservationId": "cr-123456",
                        }
                    },
                    "queue1": {
                        "compute-resource1": {"CapacityType": "on-demand", "CapacityReservationId": "cr-123456"}
                    },
                    "queue-cb2": {
                        "compute-resource-cb2": {
                            "CapacityType": "capacity-block",
                            "CapacityReservationId": "cr-234567",
                        }
                    },
                },
                {
                    "cr-123456": CapacityBlock("cr-123456", "queue-cb", "compute-resource-cb"),
                    "cr-234567": CapacityBlock("cr-234567", "queue-cb2", "compute-resource-cb2"),
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
    def test_capacity_blocks_from_config(
        self, capacity_block_manager, fleet_config, expected_capacity_blocks, expected_exception
    ):
        capacity_block_manager._fleet_config = fleet_config

        if expected_exception:
            with pytest.raises(KeyError):
                capacity_block_manager._capacity_blocks_from_config()
        else:
            assert_that(expected_capacity_blocks).is_equal_to(capacity_block_manager._capacity_blocks_from_config())

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
