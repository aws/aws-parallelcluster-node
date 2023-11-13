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
from typing import List

from common.utils import ApiMocker

from aws.common import AWSExceptionHandler, Boto3Client


class CapacityReservationInfo:
    """
    Data object wrapping the result of a describe-capacity-reservations call.

    {
        "CapacityReservationId": "cr-123456",
        "OwnerId": "123",
        "CapacityReservationArn": "arn:aws:ec2:us-east-2:123:capacity-reservation/cr-123456",
        "AvailabilityZoneId": "use2-az1",
        "InstanceType": "t3.large",
        "InstancePlatform": "Linux/UNIX",
        "AvailabilityZone": "eu-west-1a",
        "Tenancy": "default",
        "TotalInstanceCount": 1,
        "AvailableInstanceCount": 1,
        "EbsOptimized": false,
        "EphemeralStorage": false,
        "State": "active",
        "StartDate": "2023-11-15T11:30:00+00:00",
        "EndDate": "2023-11-16T11:30:00+00:00",  # capacity-block only
        "EndDateType": "limited",
        "InstanceMatchCriteria": "targeted",
        "CreateDate": "2023-10-25T20:40:13+00:00",
        "Tags": [
            {
                "Key": "aws:ec2capacityreservation:incrementalRequestedQuantity",
                "Value": "1"
            },
            {
                "Key": "aws:ec2capacityreservation:capacityReservationType",
                "Value": "capacity-block"
            }
        ],
        "CapacityAllocations": [],
        "ReservationType": "capacity-block"  # capacity-block only
    }
    """

    def __init__(self, capacity_reservation_data):
        self.capacity_reservation_data = capacity_reservation_data

    def capacity_reservation_id(self):
        """Return the id of the Capacity Reservation."""
        return self.capacity_reservation_data.get("CapacityReservationId")

    def state(self):
        """Return the state of the Capacity Reservation."""
        return self.capacity_reservation_data.get("State")

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class Ec2Client(Boto3Client):
    """Implement EC2 Boto3 client."""

    def __init__(self, config=None, region=None):
        super().__init__("ec2", region=region, config=config)

    @AWSExceptionHandler.handle_client_exception
    @ApiMocker.mockable
    def describe_capacity_reservations(self, capacity_reservation_ids: List[str]) -> List[CapacityReservationInfo]:
        """Accept a space separated list of reservation ids. Return a list of CapacityReservationInfo."""
        result = []
        response = list(
            self._paginate_results(
                self._client.describe_capacity_reservations,
                CapacityReservationIds=capacity_reservation_ids,
                # ReservationType=reservation_type,  # not yet available
            )
        )
        for capacity_reservation in response:
            result.append(CapacityReservationInfo(capacity_reservation))

        return result
