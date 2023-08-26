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


def get_private_ip_address_and_dns_name(instance_info):
    """
    Return the PrivateIpAddress and PrivateDnsName of the EC2 instance.

    The PrivateIpAddress and PrivateDnsName are considered to be the ones for the
    network interface with DeviceIndex = NetworkCardIndex = 0.
    :param instance_info: the dictionary returned by a EC2:DescribeInstances call.
    :return: the PrivateIpAddress and PrivateDnsName of the instance.
    """
    private_ip = instance_info["PrivateIpAddress"]
    private_dns_name = instance_info["PrivateDnsName"]
    for network_interface in instance_info["NetworkInterfaces"]:
        attachment = network_interface["Attachment"]
        if attachment.get("DeviceIndex", -1) == 0 and attachment.get("NetworkCardIndex", -1) == 0:
            private_ip = network_interface.get("PrivateIpAddress", private_ip)
            private_dns_name = network_interface.get("PrivateDnsName", private_dns_name)
            break
    return private_ip, private_dns_name
