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
import logging
import subprocess
from datetime import datetime

import pytest
from assertpy import assert_that
from common.schedulers.slurm_commands import DEFAULT_SCONTROL_COMMAND_TIMEOUT, SCONTROL
from common.schedulers.slurm_reservation_commands import (
    SCONTROL_SHOW_RESERVATION_OUTPUT_AWK_PARSER,
    _add_param,
    _create_or_update_reservation,
    _parse_reservations_info,
    create_slurm_reservation,
    delete_slurm_reservation,
    get_slurm_reservations_info,
    is_slurm_reservation,
    update_slurm_reservation,
)
from common.utils import SlurmCommandError
from slurm_plugin.slurm_resources import SlurmReservation


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
    (
        "name, nodes, partition, user, start_time, duration, number_of_nodes, flags, "
        "run_command_output, expected_exception"
    ),
    [
        ("root_1", None, None, None, None, None, None, None, None, False),
        (
            # 1 failure, retry and then proceed
            "root_2",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            [
                subprocess.CalledProcessError(1, "error-msg"),
                None,
            ],
            False,
        ),
        (
            # 2 failures, exception will be raised by the command
            "root_3",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            [
                subprocess.CalledProcessError(1, "error-msg"),
                subprocess.CalledProcessError(1, "error-msg", output="some-output", stderr="some-error"),
            ],
            True,
        ),
        (
            "root_4",
            "nodes-1,nodes[2-6]",
            "queue1",
            "user1",
            datetime(2023, 1, 23, 17, 57, 7),
            180,
            10,
            "testflag",
            None,
            False,
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
    run_command_output,
    expected_exception,
):
    mocker.patch("time.sleep")
    run_cmd_mock = mocker.patch(
        "common.schedulers.slurm_reservation_commands._create_or_update_reservation",
        side_effect=run_command_output,
    )

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

    if expected_exception:
        with pytest.raises(
            SlurmCommandError, match="Failed to execute slurm command. Error is: some-error. Output is: some-output"
        ):
            create_slurm_reservation(**kwargs)
    else:
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
    (
        "name, nodes, partition, user, start_time, duration, number_of_nodes, flags,"
        "run_command_output, expected_exception"
    ),
    [
        ("root_1", None, None, None, None, None, None, None, None, False),
        (
            # 1 failure, retry and then proceed
            "root_2",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            [
                subprocess.CalledProcessError(1, "error-msg"),
                None,
            ],
            False,
        ),
        (
            # 2 failures, exception will be raised by the command
            "root_3",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            [
                subprocess.CalledProcessError(1, "error-msg"),
                subprocess.CalledProcessError(1, "error-msg", output="some-output", stderr="some-error"),
            ],
            True,
        ),
        (
            "root_4",
            "nodes-1,nodes[2-6]",
            "queue1",
            "user1",
            datetime(2023, 1, 23, 17, 57, 7),
            180,
            10,
            "testflag",
            None,
            False,
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
    run_command_output,
    expected_exception,
):
    mocker.patch("time.sleep")
    run_cmd_mock = mocker.patch(
        "common.schedulers.slurm_reservation_commands._create_or_update_reservation",
        side_effect=run_command_output,
    )

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

    if expected_exception:
        with pytest.raises(
            SlurmCommandError, match="Failed to execute slurm command. Error is: some-error. Output is: some-output"
        ):
            update_slurm_reservation(**kwargs)
    else:
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
    "name, cmd_call_kwargs, run_command_output, expected_exception",
    [
        ("root_1", {"name": "root_1"}, None, False),
        (
            "root_1",
            {"name": "root_1"},
            [
                subprocess.CalledProcessError(1, "error-msg"),
                None,
            ],
            False,
        ),
        (
            "root_1",
            {"name": "root_1"},
            [
                subprocess.CalledProcessError(1, "error-msg"),
                subprocess.CalledProcessError(1, "error-msg", output="some-output", stderr="some-error"),
            ],
            True,
        ),
    ],
)
def test_delete_reservation(name, cmd_call_kwargs, mocker, run_command_output, expected_exception):
    mocker.patch("time.sleep")
    run_cmd_mock = mocker.patch(
        "common.schedulers.slurm_reservation_commands.run_command", side_effect=run_command_output
    )

    if expected_exception:
        with pytest.raises(
            SlurmCommandError, match="Failed to execute slurm command. Error is: some-error. Output is: some-output"
        ):
            delete_slurm_reservation(name)
    else:
        delete_slurm_reservation(name)

    cmd = f"{SCONTROL} delete ReservationName={name}"
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


@pytest.mark.parametrize(
    ["name", "mocked_output", "expected_output", "expected_exception", "expected_message"],
    [
        ("root_1", ["ReservationName=root_1"], True, False, None),
        ("root_2", ["unexpected-output"], False, False, None),  # this should not happen, scontrol should exit
        # if available retrieve reservation info from stdout, even if there is an exception
        (
            "root_3",
            [subprocess.CalledProcessError(1, "", output="Reservation root_3 not found")],
            False,
            False,
            "root_3 not found",
        ),
        # if available retrieve reservation info from stderr, even if there is an exception
        (
            "root_4",
            [subprocess.CalledProcessError(1, "", stderr="Reservation root_4 not found")],
            False,
            False,
            "root_4 not found",
        ),
        (
            # 1 retry
            "root_5",
            [subprocess.CalledProcessError(1, ""), "ReservationName=root_5"],
            True,
            False,
            None,
        ),
        (
            # 2 retries, it's a failure
            "root_6",
            [
                subprocess.CalledProcessError(1, "", "Generic error"),
                subprocess.CalledProcessError(1, "", "Generic error"),
            ],
            False,
            True,
            "Failed when retrieving",
        ),
    ],
)
def test_is_slurm_reservation(
    mocker, name, mocked_output, expected_output, expected_message, expected_exception, caplog
):
    mocker.patch("time.sleep")
    caplog.set_level(logging.DEBUG)
    run_cmd_mock = mocker.patch(
        "common.schedulers.slurm_reservation_commands.check_command_output", side_effect=mocked_output
    )

    if expected_exception:
        with pytest.raises(SlurmCommandError, match=expected_message):
            is_slurm_reservation(name)
    else:
        assert_that(is_slurm_reservation(name)).is_equal_to(expected_output)
        if expected_message:
            assert_that(caplog.text).contains(expected_message)

    cmd = f"{SCONTROL} show ReservationName={name}"
    run_cmd_mock.assert_called_with(
        cmd, raise_on_error=True, timeout=DEFAULT_SCONTROL_COMMAND_TIMEOUT, shell=True, log_error=False
    )


def test_get_slurm_reservations_info(mocker):
    # Mock check_command_output call performed in get_slurm_reservations_info()
    check_command_output_mocked = mocker.patch(
        "common.schedulers.slurm_reservation_commands.check_command_output", autospec=True
    )
    get_slurm_reservations_info()
    expected_cmd = f"{SCONTROL} show reservations | {SCONTROL_SHOW_RESERVATION_OUTPUT_AWK_PARSER}"
    check_command_output_mocked.assert_called_with(
        expected_cmd, raise_on_error=True, timeout=DEFAULT_SCONTROL_COMMAND_TIMEOUT, shell=True
    )


@pytest.mark.parametrize(
    "reservations_info, expected_parsed_reservations_output",
    [
        ("######\n", []),
        (
            "ReservationName=root_8\nNodes=queuep4d-dy-crp4d-[1-5]\nUsers=root\nState=ACTIVE\n######\n",
            [SlurmReservation("root_8", "ACTIVE", "queuep4d-dy-crp4d-[1-5]", "root")],
        ),
        (
            (
                "ReservationName=root_8\n"
                "Nodes=queuep4d-dy-crp4d-[1-5]\n"
                "Users=root\n"
                "State=ACTIVE\n"
                "######\n"
                "ReservationName=root_9\n"
                "Nodes=queue1-st-crt2micro-1\n"
                "Users=root\n"
                "State=ACTIVE\n"
                "######\n"
            ),
            [
                SlurmReservation("root_8", "ACTIVE", "queuep4d-dy-crp4d-[1-5]", "root"),
                SlurmReservation("root_9", "ACTIVE", "queue1-st-crt2micro-1", "root"),
            ],
        ),
    ],
)
def test_parse_reservations_info(reservations_info, expected_parsed_reservations_output):
    parsed_info = _parse_reservations_info(reservations_info)
    assert_that(parsed_info).is_equal_to(expected_parsed_reservations_output)
