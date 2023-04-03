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


def get_private_ip_address(instance_info):
    """
    Return the PrivateIpAddress of the EC2 instance.

    The PrivateIpAddress is considered to be the one for the
    network interface with DeviceIndex = NetworkCardIndex = 0.
    :param instance_info: the dictionary returned by a EC2:DescribeInstances call.
    :return: the PrivateIpAddress of the instance.
    """
    private_ip = instance_info["PrivateIpAddress"]
    for network_interface in instance_info["NetworkInterfaces"]:
        attachment = network_interface["Attachment"]
        if attachment["DeviceIndex"] == 0 and attachment["NetworkCardIndex"] == 0:
            private_ip = network_interface["PrivateIpAddress"]
            break
    return private_ip
