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

from aws.common import AWSExceptionHandler, Boto3Client


class CapacityReservationInfo:
    """
    Data object wrapping the result of a describe-capacity-reservations call.

    {
        "CapacityReservationId": "cr-abcdEXAMPLE9876ef ",
        "EndDateType": "unlimited",
        "AvailabilityZone": "eu-west-1a",
        "InstanceMatchCriteria": "open",
        "Tags": [],
        "EphemeralStorage": false,
        "CreateDate": "2019-08-07T11:34:19.000Z",
        "AvailableInstanceCount": 3,
        "InstancePlatform": "Linux/UNIX",
        "TotalInstanceCount": 3,
        "State": "cancelled",
        "Tenancy": "default",
        "EbsOptimized": true,
        "InstanceType": "m5.large"
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


class CapacityBlockReservationInfo(CapacityReservationInfo):
    """
    Data object wrapping the result of a describe-capacity-reservations --capacity-type capacity-block call.

    {   "CapacityReservationId": "cr-a1234567",
        "EndDateType": "limited",
        "ReservationType": "capacity-block",
        "AvailabilityZone": "eu-east-2a",
        "InstanceMatchCriteria":  "targeted",
        "EphemeralStorage": false,
        "CreateDate": "2023-07-29T14:22:45Z  ",
        “StartDate": "2023-08-15T12:00:00Z",
        “EndDate": "2023-08-19T12:00:00Z",
        "AvailableInstanceCount": 0,
        "InstancePlatform":  "Linux/UNIX",
        "TotalInstanceCount": 16,
        “State": "payment-pending",
        "Tenancy":  "default",
        "EbsOptimized": true,
        "InstanceType": "p5.48xlarge“
    }
    """

    def __init__(self, capacity_reservation_data):
        super().__init__(capacity_reservation_data)

    def start_date(self):
        """Return the start date of the CB."""
        return self.capacity_reservation_data.get("StartDate")

    def end_date(self):
        """Return the start date of the CB."""
        return self.capacity_reservation_data.get("EndDate")

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class Ec2Client(Boto3Client):
    """Implement EC2 Boto3 client."""

    def __init__(self, config=None):
        super().__init__("ec2", config=config)

    @AWSExceptionHandler.handle_client_exception
    def describe_capacity_reservations(
        self, capacity_reservation_ids: List[str], reservation_type=None
    ) -> List[CapacityBlockReservationInfo]:
        """Accept a space separated list of ids. Return a list of CapacityReservationInfo."""
        result = []
        response = list(
            self._paginate_results(
                self._client.describe_capacity_reservations,
                CapacityReservationIds=capacity_reservation_ids,
                ReservationType=reservation_type,
            )
        )
        for capacity_reservation in response:
            result.append(CapacityBlockReservationInfo(capacity_reservation))

        return result
