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
import pytest
from assertpy import assert_that
from common.time_utils import seconds_to_minutes


@pytest.mark.parametrize("value_in_seconds, expected_output", [(0, 0), (12, 0), (60, 1), (66, 1), (1202, 20)])
def test_seconds_to_minutes(value_in_seconds, expected_output):
    assert_that(seconds_to_minutes(value_in_seconds)).is_equal_to(expected_output)
