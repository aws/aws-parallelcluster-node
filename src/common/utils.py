# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied.
# See the License for the specific language governing permissions and limitations under the License.

import collections
import contextlib
import itertools
import json
import logging
import os
import pwd
import shlex

# A nosec comment is appended to the following line in order to disable the B404 check.
# In this file the input of the module subprocess is trusted.
import subprocess  # nosec B404
import sys
import time
from datetime import datetime, timezone
from enum import Enum

log = logging.getLogger(__name__)


class CriticalError(Exception):
    """Critical error for the daemon."""

    pass


class EventType(Enum):
    ADD = "ADD"
    REMOVE = "REMOVE"


Host = collections.namedtuple("Host", ["instance_id", "hostname", "slots", "gpus"])
UpdateEvent = collections.namedtuple("UpdateEvent", ["action", "message", "host"])


def load_module(module):
    """
    Load python module.

    :param module: module path, relative to the caller one.
    :return: the loaded scheduler module
    """
    # import module
    __import__(module)
    # get module from the loaded maps
    scheduler_module = sys.modules[module]
    return scheduler_module


def check_command_output(
    command, env=None, raise_on_error=True, execute_as_user=None, log_error=True, timeout=60, shell=False
):
    """
    Execute shell command and retrieve command output.

    Usage of this function will result in a B604 bandit violation. When building the command string argument, if using
    an external argument, please validate it using validate_subprocess_argument and/or validate_absolute_path functions
    based on the argument type.

    :param command: command to execute
    :param env: a dictionary containing environment variables
    :param raise_on_error: True to raise subprocess.CalledProcessError on errors
    :param execute_as_user: the user executing the command
    :param log_error: control whether to log or not an error
    :return: the command output
    :raise: subprocess.CalledProcessError if the command fails
    """
    if isinstance(command, str) and not shell:
        command = shlex.split(command)
    # A nosec B602 comment is appended to the following line in order to disable the B602 check.
    # This check is disabled for the following reasons:
    # - Some callers (e.g., common slurm commands) require the use of `shell=True`.
    # - All values passed as the command arg are constructed from known inputs and are properly validated.
    result = _run_command(
        lambda _command, _env, _preexec_fn: subprocess.run(
            _command,
            env=_env,
            preexec_fn=_preexec_fn,
            timeout=timeout,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            shell=shell,  # nosec B602
        ),
        command,
        env,
        raise_on_error,
        execute_as_user,
        log_error,
    )

    return result.stdout if hasattr(result, "stdout") else ""


def run_command(command, env=None, raise_on_error=True, execute_as_user=None, log_error=True, timeout=60, shell=False):
    """
    Execute shell command.

    Usage of this function will result in a B604 bandit violation. When building the command string argument, if using
    an external argument, please validate it using validate_subprocess_argument and/or validate_absolute_path functions
    based on the argument type.

    :param command: command to execute
    :param env: a dictionary containing environment variables
    :param raise_on_error: True to raise subprocess.CalledProcessError on errors
    :param log_error: control whether to log or not an error
    :raise: subprocess.CalledProcessError if the command fails
    """
    if isinstance(command, str) and not shell:
        command = shlex.split(command)
    # A nosec B602 comment is appended to the following line in order to disable the B602 check.
    # This check is disabled for the following reasons:
    # - Some callers (e.g., common slurm commands) require the use of `shell=True`.
    # - All values passed as the command arg are constructed from known inputs and are properly validated.
    _run_command(
        lambda _command, _env, _preexec_fn: subprocess.run(
            _command,
            env=_env,
            preexec_fn=_preexec_fn,
            timeout=timeout,
            check=True,
            encoding="utf-8",
            shell=shell,  # nosec B602
        ),
        command,
        env,
        raise_on_error,
        execute_as_user,
        log_error,
    )


def _demote(user_uid, user_gid):
    def set_ids():
        os.setgid(user_gid)
        os.setuid(user_uid)

    return set_ids


def _run_command(command_function, command, env=None, raise_on_error=True, execute_as_user=None, log_error=True):
    try:
        if env is None:
            env = {}

        env.update(os.environ.copy())
        if execute_as_user:
            log.debug("Executing command as user '%s': %s", execute_as_user, command)
            pw_record = pwd.getpwnam(execute_as_user)
            user_uid = pw_record.pw_uid
            user_gid = pw_record.pw_gid
            preexec_fn = _demote(user_uid, user_gid)
            return command_function(command, env, preexec_fn)
        else:
            log.debug("Executing command: %s", command)
            return command_function(command, env, None)
    except subprocess.CalledProcessError as e:
        # CalledProcessError.__str__ already produces a significant error message
        if raise_on_error:
            if log_error:
                log.error(e)
            raise
        else:
            if log_error:
                log.warning(e)
            return e
    except OSError as e:
        log.error("Unable to execute the command %s. Failed with exception: %s", command, e)
        raise


def sleep_remaining_loop_time(total_loop_time, loop_start_time=None):
    wait_remaining_time(time.sleep, total_wait_time=total_loop_time, wait_start_time=loop_start_time)


def wait_remaining_time(wait_function, total_wait_time, wait_start_time=None):
    end_time = datetime.now(tz=timezone.utc)
    if not wait_start_time:
        wait_start_time = end_time
    # Always convert the received loop_start_time to utc timezone. This is so that we never rely on the system local
    # time and risk to compare naive datatime instances with localized ones
    wait_start_time = wait_start_time.astimezone(tz=timezone.utc)
    time_delta = (end_time - wait_start_time).total_seconds()
    if 0 <= time_delta < total_wait_time:
        wait_function(total_wait_time - time_delta)


def grouper(iterable, n):
    """Slice iterable into chunks of size n."""
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def load_additional_instance_types_data(config, section):
    """Load instance types data from configuration, if set; an empty dict is returned otherwise."""
    instance_types_data = {}
    if config.has_option(section, "instance_types_data"):
        instance_types_data_str = config.get(section, "instance_types_data")
        if instance_types_data_str:
            try:
                instance_types_data_str = str(instance_types_data_str).strip()

                # Load json value if not empty
                if instance_types_data_str:
                    instance_types_data = json.loads(instance_types_data_str)

                # Fallback to empty dict if value is None
                if not instance_types_data:
                    instance_types_data = {}

                log.info(
                    "Additional instance types data loaded for instance types '%s': %s",
                    instance_types_data.keys(),
                    instance_types_data,
                )
            except Exception as e:
                raise CriticalError("Error loading instance types data from configuration: {0}".format(e))
    return instance_types_data


def convert_range_to_list(node_range):
    """
    Convert a number range to a list.

    Example input: Input can be like one of the format: "1-3", "1-2,6", "2, 8"
    Example output: [1, 2, 3]
    """
    return sum(
        (
            (list(range(*[int(j) + k for k, j in enumerate(i.split("-"))])) if "-" in i else [int(i)])
            for i in node_range.split(",")
        ),
        [],
    )


def time_is_up(initial_time: datetime, current_time: datetime, grace_time: float):
    """Check if timeout is exceeded."""
    # Localize datetime objects to UTC if not previously localized
    # All timestamps used in this function should be already localized
    # Assume timestamp was taken from system local time if there is no localization info
    if initial_time is None:
        return True
    if not initial_time.tzinfo:
        logging.warning(
            "Timestamp %s is not localized. Please double check that this is expected, localizing to UTC.", initial_time
        )
        initial_time = initial_time.astimezone(tz=timezone.utc)
    if not current_time.tzinfo:
        logging.warning(
            "Timestamp %s is not localized. Please double check that this is expected, localizing to UTC", current_time
        )
        current_time = current_time.astimezone(tz=timezone.utc)
    time_diff = (current_time - initial_time).total_seconds()
    return time_diff >= grace_time


def read_json(file_path, default=None):
    """Read json file into a dict."""
    try:
        with open(file_path) as mapping_file:
            return json.load(mapping_file)
    except Exception as e:
        if default is None:
            log.error("Unable to read file from '%s'. Failed with exception: %s", file_path, e)
            raise
        else:
            if not isinstance(e, FileNotFoundError):
                log.info("Unable to read file '%s' due to an exception: %s. Using default: %s", file_path, e, default)
            return default


def validate_subprocess_argument(argument):
    """
    Validate an argument used to build a subprocess command.

    The validation is done forcing the encoding to be the standard
    Python Unicode / UTF-8 and searching for forbidden patterns.

    :param argument: an argument string to validate
    :raise: Exception if the argument contains a forbidden pattern
    :return: True if the argument does not contain forbidden patterns
    """
    forbidden_patterns = ["&", "|", ";", "$", ">", "<", "`", "\\", "!", "#", "\n"]

    # Forcing the encoding to be the standard Python Unicode / UTF-8
    # https://docs.python.org/3/howto/unicode.html
    # https://docs.python.org/3/library/codecs.html#standard-encodings
    _argument = (str(argument).encode("utf-8", "ignore")).decode()

    if any(pattern in _argument for pattern in forbidden_patterns):
        raise ValueError("Value of provided argument contains at least a forbidden pattern")
    return True


def validate_absolute_path(path):
    """
    Validate if a path string represents is a valid absolute path.

    :param path: path to validate
    :raise: Exception if the path is not a valid absolute path
    :return: True if the path is a valid absolute path
    """
    if not os.path.isabs(path):
        raise ValueError(f"The path {path} is not a valid absolute path")
    return True


@contextlib.contextmanager
def setup_logging_filter(logger: logging.Logger, custom_field: str):
    """Set up a custom logging filter and remove it at the end of the context."""

    class CustomFilter(logging.Filter):
        def __init__(self, custom_field: str):
            super().__init__()
            self.field = custom_field
            self.value = None

        def set_custom_value(self, custom_value: str):
            self.value = custom_value

        def filter(self, record: logging.LogRecord) -> bool:
            if self.value:
                record.msg = f"{self.field} {self.value} - {record.msg}"
            return True

    custom_filter = CustomFilter(custom_field)
    logger.addFilter(custom_filter)
    try:
        yield custom_filter
    finally:
        # Remove the custom log filter
        logger.removeFilter(custom_filter)
