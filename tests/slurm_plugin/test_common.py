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


from datetime import datetime, timedelta, timezone

import pytest
from assertpy import assert_that
from common.utils import time_is_up
from slurm_plugin.common import TIMESTAMP_FORMAT, get_clustermgtd_heartbeat


@pytest.mark.parametrize(
    "initial_time, current_time, grace_time, expected_result",
    [
        (datetime(2020, 1, 1, 0, 0, 0), datetime(2020, 1, 1, 0, 0, 29), 30, False),
        (datetime(2020, 1, 1, 0, 0, 0), datetime(2020, 1, 1, 0, 0, 30), 30, True),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours ahead of UTC, so this time stamp is actually 30 mins before initial_time
            datetime(2020, 1, 1, 0, 30, 0, tzinfo=timezone(timedelta(hours=1))),
            30 * 60,
            False,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours ahead of UTC, so this time stamp is actually 30 mins after initial_time
            datetime(2020, 1, 1, 1, 30, 0, tzinfo=timezone(timedelta(hours=1))),
            30 * 60,
            True,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours behind of UTC, so this time stamp is actually 1.5 hrs after initial_time
            datetime(2020, 1, 1, 0, 30, 0, tzinfo=timezone(-timedelta(hours=1))),
            90 * 60,
            True,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours behind of UTC, so this time stamp is actually 1 hrs after initial_time
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone(-timedelta(hours=1))),
            90 * 60,
            False,
        ),
    ],
)
def test_time_is_up(initial_time, current_time, grace_time, expected_result):
    assert_that(time_is_up(initial_time, current_time, grace_time)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "time, expected_parsed_time",
    [
        (
            datetime(2020, 7, 30, 19, 34, 2, 613338, tzinfo=timezone.utc),
            datetime(2020, 7, 30, 19, 34, 2, 613338, tzinfo=timezone.utc),
        ),
        (
            datetime(2020, 7, 30, 10, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            datetime(2020, 7, 30, 10, 1, 1, tzinfo=timezone(timedelta(hours=1))),
        ),
    ],
)
def test_get_clustermgtd_heartbeat(time, expected_parsed_time, mocker):
    mocker.patch(
        "slurm_plugin.common.check_command_output",
        return_value=f"some_random_stdout\n{time.strftime(TIMESTAMP_FORMAT)}",
    )
    assert_that(get_clustermgtd_heartbeat("some file path")).is_equal_to(expected_parsed_time)
