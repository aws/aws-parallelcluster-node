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

# A nosec comment is appended to the following line in order to disable the B404 check.
# In this file the input of the module subprocess is trusted.
import subprocess  # nosec B404
from datetime import datetime

from common.schedulers.slurm_commands import DEFAULT_SCONTROL_COMMAND_TIMEOUT, SCONTROL
from common.utils import run_command, validate_subprocess_argument

logger = logging.getLogger(__name__)


def _create_or_update_reservation(
    base_command: str,
    name: str,
    nodes: str = None,
    partition: str = None,
    user: str = None,
    start_time: datetime = None,
    duration: int = None,
    number_of_nodes: int = None,
    flags: str = None,
    command_timeout=DEFAULT_SCONTROL_COMMAND_TIMEOUT,
    raise_on_error=True,
):
    """
    Create or update slurm reservation, adding all the parameters.

    Official documentation is https://slurm.schedmd.com/reservations.html
    """
    cmd = _add_param(base_command, "ReservationName", name)
    cmd = _add_param(cmd, "nodes", nodes)
    cmd = _add_param(cmd, "partition", partition)
    cmd = _add_param(cmd, "user", user)
    if isinstance(start_time, datetime):
        # Convert start time to format accepted by slurm command
        cmd = _add_param(cmd, "starttime", start_time.strftime("%Y-%m-%dT%H:%M:%S"))
    cmd = _add_param(cmd, "duration", duration)
    cmd = _add_param(cmd, "nodecnt", number_of_nodes)
    cmd = _add_param(cmd, "flags", flags)

    run_command(cmd, raise_on_error=raise_on_error, timeout=command_timeout, shell=True)  # nosec B604


def create_slurm_reservation(
    name: str,
    nodes: str = "ALL",
    partition: str = None,
    user: str = "slurm",
    start_time: datetime = None,
    duration: int = None,
    number_of_nodes: int = None,
    flags: str = "maint",
    command_timeout: int = DEFAULT_SCONTROL_COMMAND_TIMEOUT,
    raise_on_error: bool = True,
):
    """
    Create slurm reservation with scontrol call.

    The command to create a reservation is something like the following:
    scontrol create reservation starttime=2009-02-06T16:00:00 duration=120 user=slurm flags=maint nodes=ALL
    scontrol create reservation user=root partition=queue1 starttime=noon duration=60 nodecnt=10

    We're using slurm as default user because it is the default Slurm administrator user in ParallelCluster,
    this is the user running slurmctld daemon.
    "maint" flag permits to overlap existing reservations.
    Official documentation is https://slurm.schedmd.com/reservations.html
    """
    cmd = f"{SCONTROL} create reservation"

    logger.info("Creating Slurm reservation with command: %s", cmd)
    _create_or_update_reservation(
        cmd, name, nodes, partition, user, start_time, duration, number_of_nodes, flags, command_timeout, raise_on_error
    )


def update_slurm_reservation(
    name: str,
    nodes: str = None,
    partition: str = None,
    user: str = None,
    start_time: datetime = None,
    duration: int = None,
    number_of_nodes: int = None,
    flags: str = None,
    command_timeout: int = DEFAULT_SCONTROL_COMMAND_TIMEOUT,
    raise_on_error: bool = True,
):
    """
    Update slurm reservation with scontrol call.

    The command to update a reservation is something like the following:
    scontrol update ReservationName=root_3 duration=150 users=admin

    Official documentation is https://slurm.schedmd.com/reservations.html
    """
    cmd = f"{SCONTROL} update"

    logger.info("Updating Slurm reservation with command: %s", cmd)
    _create_or_update_reservation(
        cmd, name, nodes, partition, user, start_time, duration, number_of_nodes, flags, command_timeout, raise_on_error
    )


def delete_slurm_reservation(
    name: str,
    command_timeout: int = DEFAULT_SCONTROL_COMMAND_TIMEOUT,
    raise_on_error: bool = True,
):
    """
    Delete slurm reservation with scontrol call.

    The command to delete a reservation is something like the following:
    scontrol delete ReservationName=root_6

    Official documentation is https://slurm.schedmd.com/reservations.html
    """
    cmd = f"{SCONTROL} delete reservation"
    cmd = _add_param(cmd, "ReservationName", name)

    logger.info("Deleting Slurm reservation with command: %s", cmd)
    run_command(cmd, raise_on_error=raise_on_error, timeout=command_timeout, shell=True)  # nosec B604


def _add_param(cmd, param_name, value):
    """If the given value is not None, validate it and concatenate ' param_name=value' to cmd."""
    if value:
        validate_subprocess_argument(value)
        cmd += f" {param_name}={value}"
    return cmd


def does_slurm_reservation_exist(
    name: str,
    command_timeout: int = DEFAULT_SCONTROL_COMMAND_TIMEOUT,
    raise_on_error: bool = True,
):
    """
    Check if slurm reservation exists, by retrieving information with scontrol call.

    Return True if reservation exists, False otherwise.
    Raise a CalledProcessError if the command fails for other reasons.

    $ scontrol show ReservationName=root_5
    Reservation root_5 not found
    $ echo $?
    1

    $ scontrol show ReservationName=root_6
    ReservationName=root_6 StartTime=2023-10-13T15:57:03 EndTime=2024-10-12T15:57:03 Duration=365-00:00:00
    Nodes=q1-st-cr2-1,q2-dy-cr4-[1-5] NodeCnt=6 CoreCnt=481 Features=(null) PartitionName=(null) Flags=MAINT,SPEC_NODES
    TRES=cpu=481
    Users=root Groups=(null) Accounts=(null) Licenses=(null) State=ACTIVE BurstBuffer=(null) Watts=n/a
    MaxStartDelay=(null)
    $ echo $?
    0

    Official documentation is https://slurm.schedmd.com/reservations.html
    """
    cmd = f"{SCONTROL} show"
    cmd = _add_param(cmd, "ReservationName", name)

    try:
        logger.debug("Retrieving Slurm reservation with command: %s", cmd)
        run_command(cmd, raise_on_error=raise_on_error, timeout=command_timeout, shell=True)  # nosec B604
        reservation_exists = True

    except subprocess.CalledProcessError as e:
        output = e.stdout.rstrip()
        if output == f"Reservation {name} not found":
            logger.info(f"Slurm reservation {name} not found.")
            reservation_exists = False
        else:
            logger.error("Failed when retrieving Slurm reservation info with command %s. Error: %s", cmd, output)
            raise e

    return reservation_exists
