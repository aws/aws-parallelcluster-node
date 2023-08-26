# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import pytest
from assertpy import assert_that
from common.ec2_utils import get_private_ip_address_and_dns_name


@pytest.mark.parametrize(
    "instance_info, expected_private_ip, expected_private_dns_name",
    [
        (
            {
                "InstanceId": "i-12345",
                "InstanceType": "c5.xlarge",
                "PrivateIpAddress": "ip.1.0.0.1",
                "PrivateDnsName": "ip-1-0-0-1",
                "NetworkInterfaces": [
                    {
                        "Attachment": {
                            "DeviceIndex": 0,
                            "NetworkCardIndex": 0,
                        },
                        "PrivateIpAddress": "ip.1.0.0.1",
                        "PrivateDnsName": "ip-1-0-0-1",
                    },
                ],
            },
            "ip.1.0.0.1",
            "ip-1-0-0-1",
        ),
        (
            {
                "InstanceId": "i-12345",
                "InstanceType": "c5.xlarge",
                "PrivateIpAddress": "ip.1.0.0.1",
                "PrivateDnsName": "ip-1-0-0-1",
                "NetworkInterfaces": [
                    {
                        "Attachment": {
                            "DeviceIndex": 0,
                            "NetworkCardIndex": 0,
                        },
                    },
                ],
            },
            "ip.1.0.0.1",
            "ip-1-0-0-1",
        ),
        (
            {
                "InstanceId": "i-12345",
                "InstanceType": "c5.xlarge",
                "PrivateIpAddress": "ip.1.0.0.1",
                "PrivateDnsName": "ip-1-0-0-1",
                "NetworkInterfaces": [
                    {
                        "Attachment": {},
                    },
                ],
            },
            "ip.1.0.0.1",
            "ip-1-0-0-1",
        ),
        (
            {
                "InstanceId": "i-12345",
                "InstanceType": "c5.xlarge",
                "PrivateIpAddress": "ip.1.0.0.1",
                "PrivateDnsName": "ip-1-0-0-1",
                "NetworkInterfaces": [
                    {
                        "Attachment": {
                            "DeviceIndex": 0,
                            "NetworkCardIndex": 1,
                        },
                        "PrivateIpAddress": "ip.1.0.0.1",
                        "PrivateDnsName": "ip-1-0-0-1",
                    },
                    {
                        "Attachment": {
                            "DeviceIndex": 0,
                            "NetworkCardIndex": 0,
                        },
                        "PrivateIpAddress": "ip.1.0.0.2",
                        "PrivateDnsName": "ip-1-0-0-2",
                    },
                ],
            },
            "ip.1.0.0.2",
            "ip-1-0-0-2",
        ),
        (
            {
                "InstanceId": "i-12345",
                "InstanceType": "c5.xlarge",
                "PrivateIpAddress": "ip.1.0.0.1",
                "PrivateDnsName": "ip-1-0-0-1",
                "NetworkInterfaces": [
                    {
                        "Attachment": {
                            "DeviceIndex": 0,
                            "NetworkCardIndex": 0,
                        },
                        "PrivateIpAddress": "ip.1.0.0.1",
                        "PrivateDnsName": "ip-1-0-0-1",
                    },
                    {
                        "Attachment": {
                            "DeviceIndex": 0,
                            "NetworkCardIndex": 1,
                        },
                        "PrivateIpAddress": "ip.1.0.0.2",
                        "PrivateDnsName": "ip-1-0-0-2",
                    },
                ],
            },
            "ip.1.0.0.1",
            "ip-1-0-0-1",
        ),
    ],
)
def test_get_private_ip_address_and_dns_name(mocker, instance_info, expected_private_ip, expected_private_dns_name):
    actual_private_ip, actual_private_dns_name = get_private_ip_address_and_dns_name(instance_info)
    assert_that(actual_private_ip).is_equal_to(expected_private_ip)
    assert_that(actual_private_dns_name).is_equal_to(expected_private_dns_name)
