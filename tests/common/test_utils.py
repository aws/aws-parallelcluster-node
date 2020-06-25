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

import pytest

from assertpy import assert_that
from common.utils import grouper


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
    assert_that(list(grouper(source_object, chunk_size))).is_equal_to(expected_grouped_output)
