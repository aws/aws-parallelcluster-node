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
import os
from collections import namedtuple

import pytest
from assertpy import assert_that

from aws.common import AWSClientError
from aws.ec2 import CapacityReservationInfo, Ec2Client

MockedBoto3Request = namedtuple(
    "MockedBoto3Request", ["method", "response", "expected_params", "generate_error", "error_code"]
)
# Set defaults for attributes of the namedtuple. Since fields with a default value must come after any fields without
# a default, the defaults are applied to the rightmost parameters. In this case generate_error = False and
# error_code = None
MockedBoto3Request.__new__.__defaults__ = (False, None)


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    return "aws.common.boto3"


FAKE_CAPACITY_BLOCK_ID = "cr-a1234567"
FAKE_CAPACITY_BLOCK_INFO = {
    "CapacityReservationId": FAKE_CAPACITY_BLOCK_ID,
    "EndDateType": "limited",
    # "ReservationType": "capacity-block",
    "AvailabilityZone": "eu-east-2a",
    "InstanceMatchCriteria": "targeted",
    "EphemeralStorage": False,
    "CreateDate": "2023-07-29T14:22:45Z  ",
    "StartDate": "2023-08-15T12:00:00Z",
    "EndDate": "2023-08-19T12:00:00Z",
    "AvailableInstanceCount": 0,
    "InstancePlatform": "Linux/UNIX",
    "TotalInstanceCount": 16,
    "State": "payment-pending",
    "Tenancy": "default",
    "EbsOptimized": True,
    "InstanceType": "p5.48xlarge",
}


@pytest.mark.parametrize("generate_error", [True, False])
def test_describe_capacity_reservations(boto3_stubber, generate_error):
    """Verify that describe_capacity_reservations behaves as expected."""
    dummy_message = "dummy error message"
    mocked_requests = [
        MockedBoto3Request(
            method="describe_capacity_reservations",
            expected_params={"CapacityReservationIds": [FAKE_CAPACITY_BLOCK_ID]},
            response=dummy_message if generate_error else {"CapacityReservations": [FAKE_CAPACITY_BLOCK_INFO]},
            generate_error=generate_error,
            error_code=None,
        )
    ]
    boto3_stubber("ec2", mocked_requests)
    if generate_error:
        with pytest.raises(AWSClientError, match=dummy_message):
            Ec2Client().describe_capacity_reservations(capacity_reservation_ids=[FAKE_CAPACITY_BLOCK_ID])
    else:
        return_value = Ec2Client().describe_capacity_reservations(capacity_reservation_ids=[FAKE_CAPACITY_BLOCK_ID])
        assert_that(return_value).is_equal_to([CapacityReservationInfo(FAKE_CAPACITY_BLOCK_INFO)])
