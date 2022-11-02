# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
from collections import namedtuple

from botocore.exceptions import ClientError

MockedBoto3Request = namedtuple(
    "MockedBoto3Request", ["method", "response", "expected_params", "generate_error", "error_code"]
)
# Set defaults for attributes of the namedtuple. Since fields with a default value must come after any fields without
# a default, the defaults are applied to the rightmost parameters. In this case generate_error = False and
# error_code = None
MockedBoto3Request.__new__.__defaults__ = (False, None)


def read_text(path):
    """Read the content of a file."""
    with path.open() as f:
        return f.read()


def client_error(error_code):
    return ClientError({"Error": {"Code": error_code}}, "failed_operation")


SINGLE_SUBNET = {"SubnetIds": ["1234567"]}
MULTIPLE_SUBNETS = {"SubnetIds": ["1234567", "7654321"]}

FLEET_CONFIG = {
    "queue": {"c5xlarge": {"Api": "run-instances", "Instances": [{"InstanceType": "c5.xlarge"}]}},
    "queue1": {
        "c5xlarge": {"Api": "run-instances", "Instances": [{"InstanceType": "c5.xlarge"}]},
        "c52xlarge": {"Api": "run-instances", "Instances": [{"InstanceType": "c5.2xlarge"}]},
        "p4d24xlarge": {"Api": "run-instances", "Instances": [{"InstanceType": "p4d.24xlarge"}]},
        "fleet-spot": {
            "Api": "create-fleet",
            "Instances": [{"InstanceType": "t2.medium"}, {"InstanceType": "t2.large"}],
            "MaxPrice": 10,
            "AllocationStrategy": "capacity-optimized",
            "CapacityType": "spot",
            "Networking": SINGLE_SUBNET,
        },
    },
    "queue2": {
        "c5xlarge": {"Api": "run-instances", "Instances": [{"InstanceType": "c5.xlarge"}]},
        "fleet-ondemand": {
            "Api": "create-fleet",
            "Instances": [{"InstanceType": "t2.medium"}, {"InstanceType": "t2.large"}],
            "AllocationStrategy": "lowest-price",
            "CapacityType": "on-demand",
            "Networking": MULTIPLE_SUBNETS,
        },
    },
    "queue3": {
        "c5xlarge": {"Api": "run-instances", "Instances": [{"InstanceType": "c5.xlarge"}]},
        "c52xlarge": {"Api": "run-instances", "Instances": [{"InstanceType": "c5.2xlarge"}]},
        "p4d24xlarge": {"Api": "run-instances", "Instances": [{"InstanceType": "p4d.24xlarge"}]},
    },
}

LAUNCH_OVERRIDES = {}
