# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.


import functools
import logging
from concurrent.futures import Future
from datetime import datetime
from typing import Callable, Optional, Protocol, TypedDict

from common.utils import check_command_output, time_is_up, validate_absolute_path

logger = logging.getLogger(__name__)

# timestamp used by clustermgtd and computemgtd should be in default ISO format
# YYYY-MM-DDTHH:MM:SS.ffffff+HH:MM[:SS[.ffffff]]
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f%z"
DEFAULT_COMMAND_TIMEOUT = 30

ComputeInstanceDescriptor = TypedDict(
    "ComputeInstanceDescriptor",
    {
        "Name": str,
        "InstanceId": str,
    },
)


class TaskController(Protocol):
    class TaskShutdownError(RuntimeError):
        """Exception raised if shutdown has been requested."""

        pass

    def queue_task(self, task: Callable[[], None]) -> Optional[Future]:
        """Queue a task and returns a Future for the task or None if the task could not be queued."""

    def is_shutdown(self) -> bool:
        """Is shutdown has been requested."""

    def raise_if_shutdown(self) -> None:
        """Raise an error if a shutdown has been requested."""

    def wait_unless_shutdown(self, seconds_to_wait: float) -> None:
        """Wait for seconds_to_wait or will raise an error if a shutdown has been requested."""

    def shutdown(self, wait: bool, cancel_futures: bool) -> None:
        """Request that all tasks be shutdown."""


def log_exception(
    logger,
    action_desc,
    log_level=logging.ERROR,
    catch_exception=Exception,
    raise_on_error=True,
    exception_to_raise=None,
):
    def _log_exception(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except catch_exception as e:
                logger.log(log_level, "Failed when %s with exception %s, message: %s", action_desc, type(e).__name__, e)
                if raise_on_error:
                    if exception_to_raise:
                        # preserve exception message if exception to raise is same of actual exception
                        raise e if isinstance(e, exception_to_raise) else exception_to_raise
                    else:
                        raise

        return wrapper

    return _log_exception


def print_with_count(resource_list):
    """Print resource list with the len of the list."""
    if isinstance(resource_list, str):
        return resource_list
    resource_list = [str(elem) for elem in resource_list]
    return f"(x{len(resource_list)}) {str(resource_list)}"


def get_clustermgtd_heartbeat(clustermgtd_heartbeat_file_path):
    """Get clustermgtd's last heartbeat."""
    # Use subprocess based method to read shared file to prevent hanging when NFS is down
    # Do not copy to local. Different users need to access the file, but file should be writable by root only
    # Only use last line of output to avoid taking unexpected output in stdout

    # Validation to sanitize the input argument and make it safe to use the function affected by B604
    validate_absolute_path(clustermgtd_heartbeat_file_path)

    heartbeat = (
        check_command_output(
            f"cat {clustermgtd_heartbeat_file_path}",
            timeout=DEFAULT_COMMAND_TIMEOUT,
            shell=True,  # nosec B604
        )
        .splitlines()[-1]
        .strip()
    )
    # Note: heartbeat must be written with datetime.strftime to convert localized datetime into str
    # datetime.strptime will not work with str(datetime)
    # Example timestamp written to heartbeat file: 2020-07-30 19:34:02.613338+00:00
    return datetime.strptime(heartbeat, TIMESTAMP_FORMAT)


def expired_clustermgtd_heartbeat(last_heartbeat, current_time, clustermgtd_timeout):
    """Test if clustermgtd heartbeat is expired."""
    if time_is_up(last_heartbeat, current_time, clustermgtd_timeout):
        logger.error(
            "Clustermgtd has been offline since %s. Current time is %s. Timeout of %s seconds has expired!",
            last_heartbeat,
            current_time,
            clustermgtd_timeout,
        )
        return True
    return False


def is_clustermgtd_heartbeat_valid(current_time, clustermgtd_timeout, clustermgtd_heartbeat_file_path):
    try:
        last_heartbeat = get_clustermgtd_heartbeat(clustermgtd_heartbeat_file_path)
        logger.info("Latest heartbeat from clustermgtd: %s", last_heartbeat)
        return not expired_clustermgtd_heartbeat(last_heartbeat, current_time, clustermgtd_timeout)
    except Exception as e:
        logger.error("Unable to retrieve clustermgtd heartbeat with exception: %s", e)
        return False
