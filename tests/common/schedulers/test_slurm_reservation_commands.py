# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
from datetime import datetime

import pytest
from assertpy import assert_that
from common.schedulers.slurm_commands import DEFAULT_SCONTROL_COMMAND_TIMEOUT, SCONTROL
from common.schedulers.slurm_reservation_commands import (
    _add_param,
    _create_or_update_reservation,
    create_slurm_reservation,
    delete_slurm_reservation,
    update_slurm_reservation,
)


@pytest.mark.parametrize(
    (
        "base_cmd, name, nodes, partition, user, start_time, duration, "
        "number_of_nodes, flags, raise_on_error, timeout, expected_cmd"
    ),
    [
        (
            f"{SCONTROL} create reservation",
            "root_1",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            f"{SCONTROL} create reservation ReservationName=root_1",
        ),
        (
            f"{SCONTROL} create reservation",
            "root_1",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            f"{SCONTROL} create reservation ReservationName=root_1",
        ),
        (
            f"{SCONTROL} update",
            "root_2",
            "nodes-1,nodes[2-6]",
            "queue1",
            "user1",
            datetime(2023, 1, 23, 17, 57, 7),
            180,
            10,
            "testflag",
            True,
            10,
            (
                f"{SCONTROL} update ReservationName=root_2 nodes=nodes-1,nodes[2-6] partition=queue1"
                " user=user1 starttime=2023-01-23T17:57:07 duration=180 nodecnt=10 flags=testflag"
            ),
        ),
    ],
)
def test_create_or_update_reservation(
    base_cmd,
    name,
    nodes,
    partition,
    user,
    start_time,
    duration,
    number_of_nodes,
    flags,
    expected_cmd,
    raise_on_error,
    timeout,
    mocker,
):
    run_cmd_mock = mocker.patch("common.schedulers.slurm_reservation_commands.run_command")
    kwargs = {"name": name}

    if nodes:
        kwargs.update({"nodes": nodes})
    if partition:
        kwargs.update({"partition": partition})
    if user:
        kwargs.update({"user": user})
    if start_time:
        kwargs.update({"start_time": start_time})
    if duration:
        kwargs.update({"duration": duration})
    if number_of_nodes:
        kwargs.update({"number_of_nodes": number_of_nodes})
    if flags:
        kwargs.update({"flags": flags})

    raise_on_error = raise_on_error is True
    kwargs.update({"raise_on_error": raise_on_error})
    command_timeout = timeout if timeout else DEFAULT_SCONTROL_COMMAND_TIMEOUT
    kwargs.update({"command_timeout": command_timeout})

    _create_or_update_reservation(base_cmd, **kwargs)
    run_cmd_mock.assert_called_with(expected_cmd, raise_on_error=raise_on_error, timeout=command_timeout, shell=True)


@pytest.mark.parametrize(
    "name, nodes, partition, user, start_time, duration, number_of_nodes, flags",
    [
        (
            "root_1",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "root_2",
            "nodes-1,nodes[2-6]",
            "queue1",
            "user1",
            datetime(2023, 1, 23, 17, 57, 7),
            180,
            10,
            "testflag",
        ),
    ],
)
def test_create_slurm_reservation(
    name,
    nodes,
    partition,
    user,
    start_time,
    duration,
    number_of_nodes,
    flags,
    mocker,
):
    run_cmd_mock = mocker.patch("common.schedulers.slurm_reservation_commands._create_or_update_reservation")

    # Compose command to avoid passing None values
    kwargs = {"name": name}
    if nodes:
        kwargs.update({"nodes": nodes})
    if partition:
        kwargs.update({"partition": partition})
    if user:
        kwargs.update({"user": user})
    if start_time:
        kwargs.update({"start_time": start_time})
    if duration:
        kwargs.update({"duration": duration})
    if number_of_nodes:
        kwargs.update({"number_of_nodes": number_of_nodes})
    if flags:
        kwargs.update({"flags": flags})

    create_slurm_reservation(**kwargs)

    # check expected internal call
    nodes = nodes if nodes else "ALL"
    user = user if user else "slurm"
    flags = flags if flags else "maint"
    run_cmd_mock.assert_called_with(
        f"{SCONTROL} create reservation",
        name,
        nodes,
        partition,
        user,
        start_time,
        duration,
        number_of_nodes,
        flags,
        DEFAULT_SCONTROL_COMMAND_TIMEOUT,
        True,
    )


@pytest.mark.parametrize(
    "name, nodes, partition, user, start_time, duration, number_of_nodes, flags",
    [
        (
            "root_1",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "root_2",
            "nodes-1,nodes[2-6]",
            "queue1",
            "user1",
            datetime(2023, 1, 23, 17, 57, 7),
            180,
            10,
            "testflag",
        ),
    ],
)
def test_update_slurm_reservation(
    name,
    nodes,
    partition,
    user,
    start_time,
    duration,
    number_of_nodes,
    flags,
    mocker,
):
    run_cmd_mock = mocker.patch("common.schedulers.slurm_reservation_commands._create_or_update_reservation")

    # Compose command to avoid passing None values
    kwargs = {"name": name}
    if nodes:
        kwargs.update({"nodes": nodes})
    if partition:
        kwargs.update({"partition": partition})
    if user:
        kwargs.update({"user": user})
    if start_time:
        kwargs.update({"start_time": start_time})
    if duration:
        kwargs.update({"duration": duration})
    if number_of_nodes:
        kwargs.update({"number_of_nodes": number_of_nodes})
    if flags:
        kwargs.update({"flags": flags})

    update_slurm_reservation(**kwargs)

    # check expected internal call
    run_cmd_mock.assert_called_with(
        f"{SCONTROL} update",
        name,
        nodes,
        partition,
        user,
        start_time,
        duration,
        number_of_nodes,
        flags,
        DEFAULT_SCONTROL_COMMAND_TIMEOUT,
        True,
    )


@pytest.mark.parametrize(
    "name, cmd_call_kwargs",
    [
        ("root_1", {"name": "root_1"}),
    ],
)
def test_delete_reservation(name, cmd_call_kwargs, mocker):
    run_cmd_mock = mocker.patch("common.schedulers.slurm_reservation_commands.run_command")
    delete_slurm_reservation(name)

    cmd = f"{SCONTROL} delete reservation ReservationName={name}"
    run_cmd_mock.assert_called_with(cmd, raise_on_error=True, timeout=DEFAULT_SCONTROL_COMMAND_TIMEOUT, shell=True)


@pytest.mark.parametrize(
    "cmd, param_name, value, expected_cmd",
    [
        ("cmd", "", "", "cmd"),
        ("cmd", "param", None, "cmd"),
        ("cmd", "param", "value", "cmd param=value"),
    ],
)
def test_add_param(cmd, param_name, value, expected_cmd):
    assert_that(_add_param(cmd, param_name, value)).is_equal_to(expected_cmd)
