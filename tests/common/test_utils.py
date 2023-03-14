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
from datetime import datetime, timedelta, timezone

import common.utils as utils
import pytest
from assertpy import assert_that


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
    return "common.utils.boto3"


@pytest.mark.parametrize(
    "source_object, chunk_size, expected_grouped_output",
    [
        ([1, 2, 3, 4, 5], 2, [(1, 2), (3, 4), (5,)]),
        ([1, 2, 3, 4, 5, 6], 3, [(1, 2, 3), (4, 5, 6)]),
        ({"A": 1, "B": 2, "C": 3}, 2, [("A", "B"), ("C",)]),
        ((1, 2, 3, 4, 5), 2, [(1, 2), (3, 4), (5,)]),
        ((1, 2, 3), 1, [(1,), (2,), (3,)]),
    ],
)
def test_grouper(source_object, chunk_size, expected_grouped_output):
    assert_that(list(utils.grouper(source_object, chunk_size))).is_equal_to(expected_grouped_output)


@pytest.mark.parametrize(
    "loop_start_time, loop_end_time, loop_total_time, expected_sleep_time",
    [
        (
            datetime(2020, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
            datetime(2020, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
            60,
            60,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
            datetime(2020, 1, 1, 0, 1, 00, tzinfo=timezone.utc),
            60,
            30,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
            datetime(2020, 1, 1, 0, 1, 30, tzinfo=timezone.utc),
            60,
            0,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            60,
            0,
        ),
        (
            datetime(2020, 1, 1, 1, 0, 0, tzinfo=timezone(timedelta(hours=1))),
            datetime(2020, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
            60,
            30,
        ),
        (
            datetime(2020, 1, 1, 1, 0, 0),
            datetime(2020, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
            60,
            None,  # can't assert this with naive timezone since the value depends on the system timezone
        ),
    ],
)
def test_sleep_remaining_loop_time(mocker, loop_start_time, loop_end_time, loop_total_time, expected_sleep_time):
    sleep_mock = mocker.patch("time.sleep")
    datetime_now_mock = mocker.MagicMock()
    datetime_now_mock.now = mocker.MagicMock(return_value=loop_end_time, spec=datetime.now)
    mocker.patch("common.utils.datetime", datetime_now_mock)

    utils.sleep_remaining_loop_time(loop_total_time, loop_start_time)

    if expected_sleep_time:
        sleep_mock.assert_called_with(expected_sleep_time)
    elif expected_sleep_time == 0:
        sleep_mock.assert_not_called()
    datetime_now_mock.now.assert_called_with(tz=timezone.utc)


@pytest.mark.parametrize(
    "argument,raises_exception",
    [
        ("standard parameter name", False),
        ("my/parameter", False),
        ("execute this & then this", True),
        ("redirect | my output", True),
        ("execute\nmultiline", True),
    ],
)
def test_validate_subprocess_argument(argument, raises_exception):
    if raises_exception:
        with pytest.raises(ValueError):
            utils.validate_subprocess_argument(argument)
    else:
        assert_that(utils.validate_subprocess_argument(argument)).is_true()


@pytest.mark.parametrize(
    "argument,raises_exception",
    [
        ("/usr/my_path", False),
        ("./my_path", True),
        ("my_path", True),
        (".my_path", True),
    ],
)
def test_validate_absolute_path(argument, raises_exception):
    if raises_exception:
        with pytest.raises(ValueError):
            utils.validate_absolute_path(argument)
    else:
        assert_that(utils.validate_absolute_path(argument)).is_true()
