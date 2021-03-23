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
import logging
import os
import time
from math import ceil
from multiprocessing import Pool

from paramiko import AutoAddPolicy, SSHClient
from retrying import retry

RemoteCommandResult = collections.namedtuple("RemoteCommandResult", ["return_code", "stdout", "stderr"])


class RemoteCommandExecutionError(Exception):
    """Signal a failure in remote command execution."""

    pass


class RemoteCommandExecutor:
    """Execute remote commands."""

    DEFAULT_TIMEOUT = 10

    def __init__(self, hostname, user, ssh_key_file=None):
        try:
            if not ssh_key_file:
                ssh_key_file = os.path.expanduser("~" + user) + "/.ssh/id_rsa"
            self.__ssh_client = SSHClient()
            self.__ssh_client.load_system_host_keys()
            self.__ssh_client.set_missing_host_key_policy(AutoAddPolicy())
            self.__ssh_client.connect(hostname=hostname, username=user, key_filename=ssh_key_file)
            self.__user_at_hostname = "{0}@{1}".format(user, hostname)
        except Exception as e:
            logging.error("Failed when connecting to host %s with error: %s", hostname, e)
            raise

    def __del__(self):
        try:
            self.__ssh_client.close()
        except Exception as e:
            # Catch all exceptions if we fail to close the clients
            logging.warning("Exception raised when closing remote clients: {0}".format(e))

    def run_remote_command(self, command, timeout=DEFAULT_TIMEOUT, log_error=True, fail_on_error=True):
        """
        Execute remote command on the configured host.

        :param command: command to execute.
        :param timeout: timeout for command execution in sec
        :param log_error: log errors.
        :param fail_on_error: raise Exception on command execution failures
        :return: result of the execution.
        """
        if isinstance(command, list):
            command = " ".join(command)
        logging.info("Executing remote command on {0}: {1}".format(self.__user_at_hostname, command))
        result = None
        try:
            stdin, stdout, stderr = self.__ssh_client.exec_command(command, get_pty=True)  # nosec
            self._wait_for_command_execution(timeout, stdout)
            result = RemoteCommandResult(
                return_code=stdout.channel.recv_exit_status(),
                stdout="\n".join(stdout.read().decode().splitlines()),
                stderr="\n".join(stderr.read().decode().splitlines()),
            )
            if result.return_code != 0 and fail_on_error:
                raise RemoteCommandExecutionError(result)
            return result
        except Exception:
            if log_error and result:
                logging.error(
                    "Command {0} failed with error:\n{1}\nand output:\n{2}".format(
                        command, result.stderr, result.stdout
                    )
                )
            raise

    @staticmethod
    def _wait_for_command_execution(timeout, stdout):
        # Using the non-blocking exit_status_ready to avoid being stuck forever on recv_exit_status
        # especially when a compute node is terminated during this operation
        while timeout > 0 and not stdout.channel.exit_status_ready():
            timeout = timeout - 1
            time.sleep(1)
        if not stdout.channel.exit_status_ready():
            raise RemoteCommandExecutionError("Timeout occurred when executing remote command")

    @staticmethod
    def run_remote_command_on_multiple_hosts(
        command, hostnames, user, ssh_key_file=None, parallelism=10, timeout=10, fail_on_error=True
    ):
        if not hostnames:
            return {}

        pool = Pool(parallelism)
        try:
            r = pool.map_async(
                _pickable_run_remote_command,
                [(hostname, command, user, ssh_key_file, timeout, fail_on_error) for hostname in hostnames],
            )
            # The pool timeout is computed by adding 2 times the command timeout for each batch of hosts that is
            # processed in sequence. Where the size of a batch is given by the degree of parallelism.
            results = r.get(timeout=int(ceil(len(hostnames) / float(parallelism)) * (2 * timeout)))
        finally:
            pool.terminate()

        return dict(results)


@retry(stop_max_attempt_number=2)
def _pickable_run_remote_command(args):
    """Pickable version of the run_command method that can be used by a pool."""
    (hostname, command, user, ssh_key_file, timeout, fail_on_error) = args
    try:
        remote_command_executor = RemoteCommandExecutor(hostname, user, ssh_key_file)
        remote_command_executor.run_remote_command(command, timeout, fail_on_error=fail_on_error)
        return hostname, True
    except Exception as e:
        logging.error("Failed when executing remote command on node %s with error %s", hostname, e)
        return hostname, False
