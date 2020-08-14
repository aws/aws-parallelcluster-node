# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import os

import pytest
from assertpy import assert_that

import slurm_plugin
from slurm_plugin.suspend import SlurmSuspendConfig


@pytest.mark.parametrize(
    ("config_file", "expected_attributes"),
    [
        (
            "default.conf",
            {
                "logging_config": os.path.join(
                    os.path.dirname(slurm_plugin.__file__), "logging", "parallelcluster_suspend_logging.conf"
                ),
            },
        ),
        ("all_options.conf", {"logging_config": "/path/to/suspend_logging/config"}),
    ],
)
def test_suspend_config(config_file, expected_attributes, test_datadir):
    suspend_config = SlurmSuspendConfig(test_datadir / config_file)
    for key in expected_attributes:
        assert_that(suspend_config.__dict__.get(key)).is_equal_to(expected_attributes.get(key))
