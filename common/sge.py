#!/usr/bin/env python2.6

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

from common.utils import check_command_output, run_command

SGE_ROOT = "/opt/sge"
SGE_BIN_PATH = SGE_ROOT + "/bin/lx-amd64"
SGE_BIN_DIR = SGE_BIN_PATH + "/"
SGE_ENV = {"SGE_ROOT": SGE_ROOT, "PATH": "{0}/bin:{1}:/bin:/usr/bin".format(SGE_ROOT, SGE_BIN_PATH)}


def check_sge_command_output(command, log):
    """
    Execute SGE shell command, by exporting the appropriate environment.

    :param command: command to execute
    :param log: logger
    :raise: subprocess.CalledProcessError if the command fails
    """
    command = _prepend_sge_bin_dir(command)
    return check_command_output(command, log, SGE_ENV)


def run_sge_command(command, log):
    """
    Execute SGE shell command, by exporting the appropriate environment.

    :param command: command to execute
    :param log: logger
    :raise: subprocess.CalledProcessError if the command fails
    """
    command = _prepend_sge_bin_dir(command)
    run_command(command, log, SGE_ENV)


def _prepend_sge_bin_dir(command):
    if isinstance(command, str) or isinstance(command, unicode):
        command = SGE_BIN_DIR + command
    else:
        command[0] = SGE_BIN_DIR + command[0]

    return command
