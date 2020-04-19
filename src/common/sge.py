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

import platform

from common.utils import check_command_output, run_command

SGE_ROOT = "/opt/sge"
BIN_PATH_SUFFIX = "amd64" if platform.machine() == "x86_64" else "arm64"
SGE_BIN_PATH = SGE_ROOT + "/bin/lx-" + BIN_PATH_SUFFIX
SGE_BIN_DIR = SGE_BIN_PATH + "/"
SGE_ENV = {"SGE_ROOT": SGE_ROOT, "PATH": "{0}/bin:{1}:/bin:/usr/bin".format(SGE_ROOT, SGE_BIN_PATH)}


def check_sge_command_output(command, raise_on_error=True):
    """
    Execute SGE shell command, by exporting the appropriate environment.

    :param command: command to execute
    :param raise_on_error: if True the method raises subprocess.CalledProcessError on errors
    :raise subprocess.CalledProcessError if the command fails
    :return the stdout and stderr of the executed command.
    """
    command = _prepend_sge_bin_dir(command)
    return check_command_output(command, SGE_ENV, raise_on_error=raise_on_error)


def run_sge_command(command):
    """
    Execute SGE shell command, by exporting the appropriate environment.

    :param command: command to execute
    :raise: subprocess.CalledProcessError if the command fails
    """
    command = _prepend_sge_bin_dir(command)
    run_command(command, SGE_ENV)


def _prepend_sge_bin_dir(command):
    if isinstance(command, str):
        command = SGE_BIN_DIR + command
    else:
        command[0] = SGE_BIN_DIR + command[0]

    return command
