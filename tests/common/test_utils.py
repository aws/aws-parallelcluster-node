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
import logging
import os

import pytest
from assertpy import assert_that
from botocore.exceptions import ClientError

import common.utils as utils
from tests.common import MockedBoto3Request


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


def test_get_instance_info_from_pricing_file(caplog, mocker):
    """Verify function that returns instance vCPU and GPU info is calling the expected functions."""
    caplog.set_level(logging.DEBUG)
    dummy_instance_type = "dummy-instance-type"
    dummy_args = ("dummy-region", "dummy-proxy-config", dummy_instance_type)
    dummy_instance_info = {"InstanceType": dummy_instance_type}
    dummy_vcpus, dummy_gpus, dummy_memory = 3, 1, 2
    fetch_instance_info_patch = mocker.patch("common.utils._fetch_instance_info", return_value=dummy_instance_info)
    get_vcpus_patch = mocker.patch("common.utils._get_vcpus_from_instance_info", return_value=dummy_vcpus)
    get_gpus_patch = mocker.patch("common.utils._get_gpus_from_instance_info", return_value=dummy_gpus)
    get_memory_patch = mocker.patch("common.utils._get_memory_from_instance_info", return_value=dummy_memory)
    returned_vcpus, returned_gpus, returned_memory = utils._get_instance_info_from_pricing_file(*dummy_args)
    fetch_instance_info_patch.assert_called_with(*dummy_args)
    for instance_info_func_patch in (get_vcpus_patch, get_gpus_patch, get_memory_patch):
        instance_info_func_patch.assert_called_with(dummy_instance_info)
    assert_that(returned_vcpus).is_equal_to(dummy_vcpus)
    assert_that(returned_gpus).is_equal_to(dummy_gpus)
    assert_that(returned_memory).is_equal_to(dummy_memory)
    for log_message in [
        "Fetching info for instance_type {0}".format(dummy_instance_type),
        "Received the following information for instance type {0}: {1}".format(
            dummy_instance_type, dummy_instance_info
        ),
    ]:
        assert_that(caplog.text).contains(log_message)


@pytest.mark.parametrize(
    "instance_info, expected_value, error_expected",
    [
        ({}, 0, False),
        ({"GpuInfo": {}}, 0, False),
        ({"GpuInfo": {"Gpus": []}}, 0, False),
        ({"GpuInfo": {"Gpus": [{}]}}, 0, False),
        ({"GpuInfo": {"Gpus": [{"Count": 1}]}}, 1, False),
        ({"GpuInfo": {"Gpus": [{"Count": 1}, {"Count": 5}]}}, 6, False),
        (None, None, True),
        ({"GpuInfo": [{"Count": 1}, {"Count": 5}]}, None, True),
        ({"GpuInfo": {"Gpus": {"Count": 1}}}, None, True),
    ],
)
def test_get_gpus_from_instance_info(caplog, instance_info, expected_value, error_expected):
    """Verify function used to extract number of GPUs from dict returned by DescribeInstanceTypes works as expected."""
    if error_expected:
        error_message = "Unable to get gpus for the instance type"
        with pytest.raises(utils.CriticalError, match=error_message):
            utils._get_gpus_from_instance_info(instance_info)
        assert_that(caplog.text).contains(error_message)
    else:
        assert_that(utils._get_gpus_from_instance_info(instance_info)).is_equal_to(expected_value)


@pytest.mark.parametrize(
    "instance_info, expected_value, error_expected",
    [
        ({"VCpuInfo": {"DefaultVCpus": 10}}, 10, False),
        ({"VCpuInfo": {"DefaultVCpus": []}}, None, True),
        ({}, None, True),
        (None, None, True),
    ],
)
def test_get_vcpus_from_instance_info(caplog, instance_info, expected_value, error_expected):
    """Verify function used to extract number of vCPUs from dict returned by DescribeInstanceTypes works as expected."""
    if error_expected:
        error_message = "Unable to get vcpus for the instance type"
        with pytest.raises(utils.CriticalError, match=error_message):
            utils._get_vcpus_from_instance_info(instance_info)
        assert_that(caplog.text).contains(error_message)
    else:
        assert_that(utils._get_vcpus_from_instance_info(instance_info)).is_equal_to(expected_value)


@pytest.mark.parametrize(
    "generate_boto3_error, response, error_expected",
    [
        (True, None, True),
        (False, {"InstanceTypes": [{"InstanceType": "dummy-instance-type"}]}, False),
        (False, {"InstanceTypes": []}, True),
    ],
)
def test_fetch_instance_info(mocker, boto3_stubber, generate_boto3_error, response, error_expected):
    """Verify function that calls DescribeInstanceTypes behaves as expected."""
    dummy_region = "us-east-2"
    dummy_proxy_config = None
    dummy_instance_type = "dummy-instance-type"
    log_patch = mocker.patch.object(utils.log, "critical")
    mocked_requests = [
        MockedBoto3Request(
            method="describe_instance_types",
            response=response,
            expected_params={
                "InstanceTypes": [dummy_instance_type],
            },
            generate_error=generate_boto3_error,
        ),
    ]
    boto3_stubber("ec2", mocked_requests)
    if error_expected:
        expected_log_msg = "Error when calling DescribeInstanceTypes for instance type {0}".format(dummy_instance_type)
        if generate_boto3_error:
            expected_exception_type = ClientError
            expected_exception_msg = r"An error occurred \(.*\) when calling the DescribeInstanceTypes operation"
        else:
            expected_exception_type = utils.CriticalError
            expected_exception_msg = expected_log_msg
        with pytest.raises(expected_exception_type, match=expected_exception_msg):
            utils._fetch_instance_info(dummy_region, dummy_proxy_config, dummy_instance_type)
        assert_that(log_patch.call_count).is_equal_to(1)
        assert_that(log_patch.call_args[0][0]).matches(expected_log_msg)
    else:
        assert_that(utils._fetch_instance_info(dummy_region, dummy_proxy_config, dummy_instance_type)).is_equal_to(
            response.get("InstanceTypes")[0]
        )
