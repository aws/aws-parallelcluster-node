# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
import os
from datetime import datetime, timezone

import pytest
from assertpy import assert_that
from botocore.exceptions import ClientError
from slurm_plugin.fleet_manager import (
    INSTANCE_INFO_RETRIEVAL_MAX_BACKOFF,
    INSTANCE_INFO_RETRIEVAL_TIMEOUT_DEFAULT,
    Ec2CreateFleetManager,
    EC2Instance,
    Ec2RunInstancesManager,
    FleetManagerFactory,
    LaunchInstancesError,
)

from tests.common import FLEET_CONFIG, MockedBoto3Request


UNFULFILLED_OVERRIDE = {
    "ErrorCode": "UnfulfillableCapacity",
    "ErrorMessage": "Failed to fulfill capacity. Please review errors in the response.",
}
UNSUPPORTED_ERROR = {"ErrorCode": "Unsupported", "ErrorMessage": "Not supported in this AZ."}
MIN_TARGET_CAPACITY_ERROR = {
    "ErrorCode": "UnfulfillableCapacity",
    "ErrorMessage": "Unable to fulfill request due to MinTargetCapacity constraints. Please adjust your request.",
}


def _raises_launch_error(err_list):
    """Mirror when _launch_instances turns a CreateFleet response with no instances into an exception."""
    real = [err for err in err_list if err != UNFULFILLED_OVERRIDE] or err_list
    return any(err.get("ErrorCode") == "RequestLimitExceeded" for err in real) or len(real) == 1


def _expected_describe_attempts(timeout):
    """Compute DescribeInstances attempts for a never-converging instance, mirroring _get_instances_info."""
    attempts = 0
    elapsed_backoff = 0
    while True:
        attempts += 1
        base_backoff = min(0.3 * 2**attempts, INSTANCE_INFO_RETRIEVAL_MAX_BACKOFF)
        if elapsed_backoff + base_backoff > timeout:
            break
        elapsed_backoff += base_backoff
    return attempts


@pytest.fixture()
def boto3_stubber_path():
    # we need to set the region in the environment because the Boto3ClientFactory requires it.
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
    return "slurm_plugin.fleet_manager.boto3"


class TestFleetManagerFactory:
    @pytest.mark.parametrize(
        ("fleet_config", "expected_failure", "expected_manager"),
        [
            ({}, "Unable to find queue .* or compute resource .* in the fleet config: {}", None),
            ({"bad_queue": {}}, "Unable to find queue .* or compute resource .* in the .*", None),
            ({"q1": {"bad_cr": {}}}, "Unable to find queue .* or compute resource .* in the .*", None),
            ({"q1": {"cr1": {}}}, "Unable to find 'Api' key in the compute resource 'cr1', .* fleet config: .*", None),
            ({"q1": {"cr1": {"Api": "wrong"}}}, "Unsupported Api 'wrong' specified in queue .*", None),
            ({"q1": {"cr1": {"Api": "run-instances"}, "other": {"Api": "create-fleet"}}}, None, Ec2RunInstancesManager),
            ({"q1": {"cr1": {"Api": "create-fleet"}, "other": {"Api": "run-instances"}}}, None, Ec2CreateFleetManager),
        ],
        ids=[
            "empty_config",
            "missing_queue_in_config",
            "missing_cr_in_config",
            "missing_api_in_config",
            "unsupported_api_in_config",
            "right_config_run_instances",
            "right_config_create_fleet",
        ],
    )
    def test_get_manager(self, fleet_config, expected_failure, expected_manager):
        if expected_failure:
            with pytest.raises(Exception, match=expected_failure):
                FleetManagerFactory.get_manager(
                    "cluster_name", "region", "boto3_config", fleet_config, "q1", "cr1", False, {}, {}
                )
        else:
            manager = FleetManagerFactory.get_manager(
                "cluster_name", "region", "boto3_config", fleet_config, "q1", "cr1", False, {}, {}
            )
            assert_that(manager).is_instance_of(expected_manager)


# -------- Ec2RunInstancesManager ------


class TestEc2RunInstancesManager:
    @pytest.mark.parametrize(
        (
            "batch_size",
            "compute_resource",
            "all_or_nothing",
            "launch_overrides",
            "expected_params",
        ),
        [
            (
                5,
                "p4d24xlarge",
                False,
                {},
                {
                    "MinCount": 1,
                    "MaxCount": 5,
                    "LaunchTemplate": {
                        "LaunchTemplateName": "hit-queue1-p4d24xlarge",
                        "Version": "$Latest",
                    },
                },
            ),
            (
                5,
                "c5xlarge",
                True,
                {},
                {
                    "MinCount": 5,
                    "MaxCount": 5,
                    "LaunchTemplate": {
                        "LaunchTemplateName": "hit-queue1-c5xlarge",
                        "Version": "$Latest",
                    },
                },
            ),
            (
                5,
                "p4d24xlarge",
                False,
                {
                    "queue1": {
                        "p4d24xlarge": {
                            "CapacityReservationSpecification": {
                                "CapacityReservationTarget": {"CapacityReservationId": "cr-12345"}
                            }
                        }
                    }
                },
                {
                    "MinCount": 1,
                    "MaxCount": 5,
                    "LaunchTemplate": {
                        "LaunchTemplateName": "hit-queue1-p4d24xlarge",
                        "Version": "$Latest",
                    },
                    "CapacityReservationSpecification": {
                        "CapacityReservationTarget": {"CapacityReservationId": "cr-12345"}
                    },
                },
            ),
        ],
        ids=["normal", "all_or_nothing_batch", "launch_overrides"],
    )
    def test_evaluate_launch_params(
        self,
        batch_size,
        compute_resource,
        all_or_nothing,
        launch_overrides,
        expected_params,
        caplog,
    ):
        caplog.set_level(logging.INFO)
        # run test
        fleet_manager = FleetManagerFactory.get_manager(
            "hit",
            "region",
            "boto3_config",
            FLEET_CONFIG,
            "queue1",
            compute_resource,
            all_or_nothing,
            launch_overrides,
            {},
        )
        launch_params = fleet_manager._evaluate_launch_params(batch_size)
        if launch_overrides:
            assert_that(caplog.text).contains("Found RunInstances parameters override")
        assert_that(launch_params).is_equal_to(expected_params)

    @pytest.mark.parametrize(
        ("launch_params", "mocked_boto3_request", "expected_assigned_nodes"),
        [
            (
                {
                    "MinCount": 1,
                    "MaxCount": 5,
                    "LaunchTemplate": {
                        "LaunchTemplateName": "hit-queue1-p4d24xlarge",
                        "Version": "$Latest",
                    },
                },
                [
                    MockedBoto3Request(
                        method="run_instances",
                        response={
                            "Instances": [
                                {
                                    "InstanceId": "i-12345",
                                    "PrivateIpAddress": "ip-2",
                                    "PrivateDnsName": "hostname",
                                    "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                }
                            ]
                        },
                        expected_params={
                            "MinCount": 1,
                            "MaxCount": 5,
                            "LaunchTemplate": {
                                "LaunchTemplateName": "hit-queue1-p4d24xlarge",
                                "Version": "$Latest",
                            },
                        },
                    ),
                ],
                [
                    {
                        "InstanceId": "i-12345",
                        "PrivateIpAddress": "ip-2",
                        "PrivateDnsName": "hostname",
                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                    }
                ],
            ),
            (
                {
                    "MinCount": 1,
                    "MaxCount": 5,
                    "LaunchTemplate": {
                        "LaunchTemplateName": "hit-queue1-p4d24xlarge",
                        "Version": "$Latest",
                    },
                },
                [
                    MockedBoto3Request(
                        method="run_instances",
                        response={},
                        expected_params={
                            "MinCount": 1,
                            "MaxCount": 5,
                            "LaunchTemplate": {
                                "LaunchTemplateName": "hit-queue1-p4d24xlarge",
                                "Version": "$Latest",
                            },
                        },
                        generate_error=True,
                        error_code="RequestLimitExceeded",
                    ),
                ],
                [],
            ),
        ],
        ids=["normal", "throttling"],
    )
    def test_launch_instances(self, boto3_stubber, launch_params, mocked_boto3_request, expected_assigned_nodes):
        # patch boto3 call
        boto3_stubber("ec2", mocked_boto3_request)
        # run test
        fleet_manager = FleetManagerFactory.get_manager(
            "hit", "region", "boto3_config", FLEET_CONFIG, "queue1", "p4d24xlarge", False, {}, {}
        )
        if mocked_boto3_request[0].generate_error:
            with pytest.raises(Exception) as e:
                fleet_manager._launch_instances(launch_params)
                assert isinstance(e, ClientError)
        else:
            assigned_nodes = fleet_manager._launch_instances(launch_params)
            assert_that(assigned_nodes.get("Instances", [])).is_equal_to(expected_assigned_nodes)


# -------- Ec2CreateFleetManager ------


class TestEc2CreateFleetManager:
    test_fleet_exception_params = {
        "LaunchTemplateConfigs": [
            {
                "LaunchTemplateSpecification": {"LaunchTemplateName": "hit-queue1-fleet-spot", "Version": "$Latest"},
                "Overrides": [
                    {
                        "InstanceRequirements": {
                            "VCpuCount": {"Min": 2},
                            "MemoryMiB": {"Min": 2048},
                            "AllowedInstanceTypes": ["inf*"],
                            "AcceleratorManufacturers": ["nvidia"],
                        }
                    }
                ],
            }
        ],
        "SpotOptions": {
            "AllocationStrategy": "capacity-optimized",
            "SingleInstanceType": False,
            "SingleAvailabilityZone": True,
            "MinTargetCapacity": 1,
        },
        "TargetCapacitySpecification": {"TotalTargetCapacity": 5, "DefaultTargetCapacityType": "spot"},
        "Type": "instant",
    }

    test_fleet_spot_params = {
        "LaunchTemplateConfigs": [
            {
                "LaunchTemplateSpecification": {"LaunchTemplateName": "hit-queue1-fleet-spot", "Version": "$Latest"},
                "Overrides": [
                    {"MaxPrice": "10", "InstanceType": "t2.medium", "SubnetId": "1234567"},
                    {"MaxPrice": "10", "InstanceType": "t2.large", "SubnetId": "1234567"},
                ],
            }
        ],
        "SpotOptions": {
            "AllocationStrategy": "capacity-optimized",
            "SingleInstanceType": False,
            "SingleAvailabilityZone": True,
            "MinTargetCapacity": 1,
        },
        "TargetCapacitySpecification": {"TotalTargetCapacity": 5, "DefaultTargetCapacityType": "spot"},
        "Type": "instant",
    }

    test_on_demand_params = {
        "LaunchTemplateConfigs": [
            {
                "LaunchTemplateSpecification": {
                    "LaunchTemplateName": "hit-queue2-fleet-ondemand",
                    "Version": "$Latest",
                },
                "Overrides": [
                    {"InstanceType": "t2.medium", "SubnetId": "1234567"},
                    {"InstanceType": "t2.large", "SubnetId": "1234567"},
                ],
            }
        ],
        "OnDemandOptions": {
            "AllocationStrategy": "lowest-price",
            "SingleInstanceType": False,
            "SingleAvailabilityZone": True,
            "MinTargetCapacity": 1,
            "CapacityReservationOptions": {"UsageStrategy": "use-capacity-reservations-first"},
        },
        "TargetCapacitySpecification": {"TotalTargetCapacity": 5, "DefaultTargetCapacityType": "on-demand"},
        "Type": "instant",
    }

    test_capacity_block_params = {
        "LaunchTemplateConfigs": [
            {
                "LaunchTemplateSpecification": {
                    "LaunchTemplateName": "queue-cb-fleet-capacity-block",
                    "Version": "$Latest",
                },
            }
        ],
        "OnDemandOptions": {
            "SingleInstanceType": False,
            "SingleAvailabilityZone": True,
            "MinTargetCapacity": 1,
            "CapacityReservationOptions": {"UsageStrategy": "use-capacity-reservations-first"},
        },
        "TargetCapacitySpecification": {"TotalTargetCapacity": 5, "DefaultTargetCapacityType": "capacity-block"},
        "Type": "instant",
    }

    @pytest.mark.parametrize(
        ("batch_size", "queue", "compute_resource", "all_or_nothing", "launch_overrides", "log_assertions"),
        [
            # normal - spot
            (5, "queue1", "fleet-spot", False, {}, None),
            # normal - on-demand
            (5, "queue2", "fleet-ondemand", False, {}, None),
            # normal - capacity-block
            (5, "queue-cb", "fleet-capacity-block", False, {}, None),
            # all or nothing
            (5, "queue1", "fleet-spot", True, {}, None),
            # launch_overrides
            (
                5,
                "queue2",
                "fleet-ondemand",
                False,
                {
                    "queue2": {
                        "fleet-ondemand": {
                            "TagSpecifications": [
                                {"ResourceType": "capacity-reservation", "Tags": [{"Key": "string", "Value": "string"}]}
                            ]
                        }
                    }
                },
                None,
            ),
            # Fleet with (Single-Subnet, Multi-InstanceType) AND all_or_nothing is True --> MinTargetCapacity is set
            (5, "queue4", "fleet1", True, {}, None),
            # Fleet with (Multi-Subnet, Single-InstanceType) AND all_or_nothing is True --> MinTargetCapacity is set
            (5, "queue5", "fleet1", True, {}, None),
            # Fleet with (Multi-Subnet, Multi-InstanceType) AND all_or_nothing is False --> NOT set MinTargetCapacity
            (5, "queue6", "fleet1", False, {}, None),
            # Fleet with (Multi-Subnet, Multi-InstanceType) AND all_or_nothing is True --> Log a warning
            (
                5,
                "queue6",
                "fleet1",
                True,
                {},
                "All-or-Nothing is only available with single instance type compute resources or single subnet queues",
            ),
            # Use "prioritized" Allocation Strategy AND Launch Override with Priority
            (5, "queue-prioritized", "fleet1", False, {}, None),
            # Use "capacity-optimized-prioritized" Allocation Strategy AND Launch Override with Priority
            (5, "queue-capacity-optimized-prioritized", "fleet1", False, {}, None),
            # Use "prioritized" Allocation Strategy AND Launch Override with Priority AND all_or_nothing is True
            (5, "queue-prioritized-all-or-nothing", "fleet1", True, {}, None),
            # Use "capacity-optimized-prioritized" Allocation Strategy
            # AND Launch Override with Priority AND all_or_nothing is True
            (5, "queue-capacity-optimized-prioritized-all-or-nothing", "fleet1", True, {}, None),
        ],
        ids=[
            "fleet_spot",
            "fleet_ondemand",
            "fleet_capacity_block",
            "all_or_nothing",
            "launch_overrides",
            "fleet-single-az-multi-it-all_or_nothing",
            "fleet-multi-az-single-it-all_or_nothing",
            "fleet-multi-az-multi-it",
            "fleet-multi-az-multi-it-all_or_nothing",
            "prioritized",
            "capacity_optimized_prioritized",
            "prioritized_all_or_nothing",
            "capacity_optimized_prioritized_all_or_nothing",
        ],
    )
    def test_evaluate_launch_params(
        self,
        batch_size,
        queue,
        compute_resource,
        all_or_nothing,
        launch_overrides,
        log_assertions,
        caplog,
        test_datadir,
        request,
    ):
        caplog.set_level(logging.INFO)
        # run tests
        fleet_manager = FleetManagerFactory.get_manager(
            "hit", "region", "boto3_config", FLEET_CONFIG, queue, compute_resource, all_or_nothing, {}, launch_overrides
        )
        launch_params = fleet_manager._evaluate_launch_params(batch_size)

        params_path = test_datadir / request.node.callspec.id / "expected_launch_params.json"
        assert_that(launch_params).is_equal_to(json.loads(params_path.read_text()))
        if launch_overrides:
            assert_that(caplog.text).contains("Found CreateFleet parameters override")

        if log_assertions:
            assert_that(caplog.text).contains(log_assertions)

    @pytest.mark.parametrize(
        ("launch_params", "mocked_boto3_request", "expected_assigned_nodes"),
        [
            # normal - spot
            (
                test_fleet_spot_params,
                [
                    MockedBoto3Request(
                        method="create_fleet",
                        response={
                            "Instances": [{"InstanceIds": ["i-12345", "i-23456"]}],
                            "Errors": [
                                {"ErrorCode": "InsufficientInstanceCapacity", "ErrorMessage": "Insufficient capacity."}
                            ],
                            "ResponseMetadata": {"RequestId": "1234-abcde"},
                        },
                        expected_params=test_fleet_spot_params,
                    ),
                    MockedBoto3Request(
                        method="describe_instances",
                        response={
                            "Reservations": [
                                {
                                    "Instances": [
                                        {
                                            "InstanceId": "i-12345",
                                            "PrivateIpAddress": "ip-2",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                    "PrivateIpAddress": "ip-2",
                                                },
                                            ],
                                        },
                                        {
                                            "InstanceId": "i-23456",
                                            "PrivateIpAddress": "ip-3",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                    "PrivateIpAddress": "ip-3",
                                                },
                                            ],
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345", "i-23456"]},
                        generate_error=False,
                    ),
                ],
                [
                    {
                        "InstanceId": "i-12345",
                        "PrivateIpAddress": "ip-2",
                        "PrivateDnsName": "hostname",
                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                        "NetworkInterfaces": [
                            {
                                "Attachment": {
                                    "DeviceIndex": 0,
                                    "NetworkCardIndex": 0,
                                },
                                "PrivateIpAddress": "ip-2",
                            },
                        ],
                    },
                    {
                        "InstanceId": "i-23456",
                        "PrivateIpAddress": "ip-3",
                        "PrivateDnsName": "hostname",
                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                        "NetworkInterfaces": [
                            {
                                "Attachment": {
                                    "DeviceIndex": 0,
                                    "NetworkCardIndex": 0,
                                },
                                "PrivateIpAddress": "ip-3",
                            },
                        ],
                    },
                ],
            ),
            # create-fleet - exception
            (
                test_fleet_exception_params,
                [
                    MockedBoto3Request(
                        method="create_fleet",
                        response={
                            "Instances": [],
                            "Errors": [
                                {"ErrorCode": "InvalidParameterValue", "ErrorMessage": "Insufficient capacity."}
                            ],
                            "ResponseMetadata": {"RequestId": "1234-abcde"},
                        },
                        expected_params=test_fleet_exception_params,
                        generate_error=True,
                        error_code="InvalidParameterValue",
                    ),
                ],
                [],
            ),
            # normal - on-demand
            (
                test_on_demand_params,
                [
                    MockedBoto3Request(
                        method="create_fleet",
                        response={
                            "Instances": [{"InstanceIds": ["i-12345"]}],
                            "Errors": [
                                {"ErrorCode": "InsufficientInstanceCapacity", "ErrorMessage": "Insufficient capacity."}
                            ],
                            "ResponseMetadata": {"RequestId": "1234-abcde"},
                        },
                        expected_params=test_on_demand_params,
                    ),
                    MockedBoto3Request(
                        method="describe_instances",
                        response={
                            "Reservations": [
                                {
                                    "Instances": [
                                        {
                                            "InstanceId": "i-12345",
                                            "PrivateIpAddress": "ip-2",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                    "PrivateIpAddress": "ip-2",
                                                },
                                            ],
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=False,
                    ),
                ],
                [
                    {
                        "InstanceId": "i-12345",
                        "PrivateIpAddress": "ip-2",
                        "PrivateDnsName": "hostname",
                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                        "NetworkInterfaces": [
                            {
                                "Attachment": {
                                    "DeviceIndex": 0,
                                    "NetworkCardIndex": 0,
                                },
                                "PrivateIpAddress": "ip-2",
                            },
                        ],
                    }
                ],
            ),
            # normal - capacity-block
            (
                test_capacity_block_params,
                [
                    MockedBoto3Request(
                        method="create_fleet",
                        response={
                            "Instances": [{"InstanceIds": ["i-12345"]}],
                            "Errors": [
                                {"ErrorCode": "InsufficientInstanceCapacity", "ErrorMessage": "Insufficient capacity."}
                            ],
                            "ResponseMetadata": {"RequestId": "1234-abcde"},
                        },
                        expected_params=test_capacity_block_params,
                    ),
                    MockedBoto3Request(
                        method="describe_instances",
                        response={
                            "Reservations": [
                                {
                                    "Instances": [
                                        {
                                            "InstanceId": "i-12345",
                                            "PrivateIpAddress": "ip-2",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                    "PrivateIpAddress": "ip-2",
                                                },
                                            ],
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=False,
                    ),
                ],
                [
                    {
                        "InstanceId": "i-12345",
                        "PrivateIpAddress": "ip-2",
                        "PrivateDnsName": "hostname",
                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                        "NetworkInterfaces": [
                            {
                                "Attachment": {
                                    "DeviceIndex": 0,
                                    "NetworkCardIndex": 0,
                                },
                                "PrivateIpAddress": "ip-2",
                            },
                        ],
                    }
                ],
            ),
            # create-fleet - throttling
            (
                test_on_demand_params,
                [
                    MockedBoto3Request(
                        method="create_fleet",
                        response={
                            "Instances": [],
                            "Errors": [
                                {"ErrorCode": "RequestLimitExceeded", "ErrorMessage": "Request limit exceeded."}
                            ],
                            "ResponseMetadata": {"RequestId": "37633199-bcc6-4a88-89e3-89d859d76096"},
                        },
                        expected_params=test_on_demand_params,
                    ),
                ],
                [],
            ),
            # create-fleet - multiple errors
            (
                test_on_demand_params,
                [
                    MockedBoto3Request(
                        method="create_fleet",
                        response={
                            "Instances": [],
                            "Errors": [
                                {"ErrorCode": "RequestLimitExceeded", "ErrorMessage": "Request limit exceeded."},
                                {"ErrorCode": "AnotherError", "ErrorMessage": "Something went wrong"},
                            ],
                            "ResponseMetadata": {"RequestId": "37633199-bcc6-4a88-89e3-89d859d76096"},
                        },
                        expected_params=test_on_demand_params,
                    ),
                ],
                [],
            ),
            # create-fleet - throttling reported together with one generic error per override
            (
                test_on_demand_params,
                [
                    MockedBoto3Request(
                        method="create_fleet",
                        response={
                            "Instances": [],
                            "Errors": [
                                {"ErrorCode": "RequestLimitExceeded", "ErrorMessage": "Request limit exceeded."},
                            ]
                            + [
                                {
                                    "ErrorCode": "UnfulfillableCapacity",
                                    "ErrorMessage": "Failed to fulfill capacity. Please review errors in the response.",
                                }
                            ]
                            * 35,
                            "ResponseMetadata": {"RequestId": "37633199-bcc6-4a88-89e3-89d859d76096"},
                        },
                        expected_params=test_on_demand_params,
                    ),
                ],
                [],
            ),
        ],
        ids=[
            "fleet_spot",
            "fleet_exception",
            "fleet_ondemand",
            "fleet_capacity_block",
            "fleet_throttling",
            "fleet_multiple_errors",
            "fleet_throttling_with_generic_errors",
        ],
    )
    def test_launch_instances(
        self,
        boto3_stubber,
        launch_params,
        mocked_boto3_request,
        expected_assigned_nodes,
        mocker,
    ):
        mocker.patch("time.sleep")
        # patch boto3 call
        boto3_stubber("ec2", mocked_boto3_request)
        # run test
        fleet_manager = FleetManagerFactory.get_manager(
            "hit", "region", "boto3_config", FLEET_CONFIG, "queue2", "fleet-ondemand", False, {}, {}
        )

        if mocked_boto3_request[0].generate_error:
            with pytest.raises(Exception) as e:
                fleet_manager._launch_instances(launch_params)
                assert isinstance(e, ClientError)
        elif not expected_assigned_nodes and _raises_launch_error(mocked_boto3_request[0].response.get("Errors", [])):
            with pytest.raises(LaunchInstancesError) as e:
                fleet_manager._launch_instances(launch_params)
            assert_that(e.value.code).is_equal_to(mocked_boto3_request[0].response.get("Errors")[0].get("ErrorCode"))
        else:
            assigned_nodes = fleet_manager._launch_instances(launch_params)
            assert_that(assigned_nodes.get("Instances", [])).is_equal_to(expected_assigned_nodes)

    @pytest.mark.parametrize(
        ("instance_ids", "mocked_boto3_request", "expected_result"),
        [
            # normal - on-demand
            (
                ["i-12345"],
                [
                    MockedBoto3Request(
                        method="describe_instances",
                        response={
                            "Reservations": [
                                {
                                    "Instances": [
                                        {
                                            "InstanceId": "i-12345",
                                            "PrivateIpAddress": "ip-2",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                    "PrivateIpAddress": "ip-2",
                                                },
                                            ],
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=False,
                    ),
                ],
                (
                    [
                        {
                            "InstanceId": "i-12345",
                            "PrivateIpAddress": "ip-2",
                            "PrivateDnsName": "hostname",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip-2",
                                },
                            ],
                        },
                    ],
                    [],
                ),
            ),
            # incomplete instance info
            (
                ["i-12345", "i-23456"],
                [
                    MockedBoto3Request(
                        method="describe_instances",
                        response={
                            "Reservations": [
                                {
                                    "Instances": [
                                        {
                                            # no private dns and address info
                                            "InstanceId": "i-12345",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                },
                                            ],
                                        },
                                        {
                                            "InstanceId": "i-23456",
                                            "PrivateIpAddress": "ip-3",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                    "PrivateIpAddress": "ip-3",
                                                },
                                            ],
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345", "i-23456"]},
                        generate_error=False,
                    ),
                    MockedBoto3Request(
                        method="describe_instances",
                        response={
                            "Reservations": [
                                {
                                    "Instances": [
                                        {
                                            "InstanceId": "i-12345",
                                            "PrivateIpAddress": "ip-2",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                    "PrivateIpAddress": "ip-2",
                                                },
                                            ],
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=False,
                    ),
                ],
                (
                    [
                        {
                            "InstanceId": "i-23456",
                            "PrivateIpAddress": "ip-3",
                            "PrivateDnsName": "hostname",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip-3",
                                },
                            ],
                        },
                        {
                            "InstanceId": "i-12345",
                            "PrivateIpAddress": "ip-2",
                            "PrivateDnsName": "hostname",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip-2",
                                },
                            ],
                        },
                    ],
                    [],
                ),
            ),
            # too many incomplete instance info
            (
                ["i-12345", "i-23456"],
                [
                    MockedBoto3Request(
                        method="describe_instances",
                        response={
                            "Reservations": [
                                {
                                    "Instances": [
                                        {
                                            # no private dns and address info
                                            "InstanceId": "i-12345",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                },
                                            ],
                                        },
                                        {
                                            "InstanceId": "i-23456",
                                            "PrivateIpAddress": "ip-3",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                    "PrivateIpAddress": "ip-3",
                                                },
                                            ],
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345", "i-23456"]},
                        generate_error=False,
                    ),
                ]
                + 4
                * [
                    MockedBoto3Request(
                        method="describe_instances",
                        response={
                            "Reservations": [
                                {
                                    "Instances": [
                                        {
                                            # no private dns and address info
                                            "InstanceId": "i-12345",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                },
                                            ],
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=False,
                    ),
                ],
                (
                    [
                        {
                            "InstanceId": "i-23456",
                            "PrivateIpAddress": "ip-3",
                            "PrivateDnsName": "hostname",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip-3",
                                },
                            ],
                        },
                    ],
                    ["i-12345"],
                ),
            ),
            # client error
            (
                ["i-12345"],
                5
                * [
                    MockedBoto3Request(
                        method="describe_instances",
                        response={},
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=True,
                    ),
                ],
                ([], ["i-12345"]),
            ),
            # transitory client error
            (
                ["i-12345"],
                [
                    MockedBoto3Request(
                        method="describe_instances",
                        response={},
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=True,
                    ),
                    MockedBoto3Request(
                        method="describe_instances",
                        response={},
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=True,
                    ),
                    MockedBoto3Request(
                        method="describe_instances",
                        response={},
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=True,
                    ),
                    MockedBoto3Request(
                        method="describe_instances",
                        response={
                            "Reservations": [
                                {
                                    "Instances": [
                                        {
                                            "InstanceId": "i-12345",
                                            "PrivateIpAddress": "ip-2",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                    "PrivateIpAddress": "ip-2",
                                                },
                                            ],
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=False,
                    ),
                ],
                (
                    [
                        {
                            "InstanceId": "i-12345",
                            "PrivateIpAddress": "ip-2",
                            "PrivateDnsName": "hostname",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                            "NetworkInterfaces": [
                                {
                                    "Attachment": {
                                        "DeviceIndex": 0,
                                        "NetworkCardIndex": 0,
                                    },
                                    "PrivateIpAddress": "ip-2",
                                },
                            ],
                        },
                    ],
                    [],
                ),
            ),
        ],
        ids=[
            "fleet_ondemand",
            "incomplete_instance_info",
            "too_many_incomplete_instance_info",
            "client_error",
            "transitory_client_error",
        ],
    )
    def test_get_instances_info(  # Note: some tests cases are covered by test_launch_instances too.
        self,
        boto3_stubber,
        mocker,
        instance_ids,
        mocked_boto3_request,
        expected_result,
        caplog,
    ):
        # patch boto3 call
        mocker.patch("time.sleep")
        boto3_stubber("ec2", mocked_boto3_request)
        # run test
        # A 10s retrieval timeout bounds the never-converging cases to exactly 5 DescribeInstances attempts,
        # matching the number of mocked responses, while leaving room for the converging cases to succeed.
        fleet_manager = FleetManagerFactory.get_manager(
            "hit",
            "region",
            "boto3_config",
            FLEET_CONFIG,
            "queue2",
            "fleet-ondemand",
            True,
            {},
            {},
            instance_info_retrieval_timeout=10,
        )

        complete_instances, partial_instance_ids = fleet_manager._get_instances_info(instance_ids)
        assert_that(expected_result).is_equal_to((complete_instances, partial_instance_ids))

    def test_instance_info_retrieval_timeout_default(self):
        # Default timeout is wired through the factory into the CreateFleet manager
        fleet_manager = FleetManagerFactory.get_manager(
            "hit", "region", "boto3_config", FLEET_CONFIG, "queue2", "fleet-ondemand", True, {}, {}
        )
        assert_that(fleet_manager._instance_info_retrieval_timeout).is_equal_to(INSTANCE_INFO_RETRIEVAL_TIMEOUT_DEFAULT)

    def test_instance_info_retrieval_timeout_override(self):
        # A custom timeout is propagated through the factory into the CreateFleet manager
        fleet_manager = FleetManagerFactory.get_manager(
            "hit",
            "region",
            "boto3_config",
            FLEET_CONFIG,
            "queue2",
            "fleet-ondemand",
            True,
            {},
            {},
            instance_info_retrieval_timeout=240,
        )
        assert_that(fleet_manager._instance_info_retrieval_timeout).is_equal_to(240)

    @pytest.mark.parametrize(
        ("instance_info_retrieval_timeout", "expected_describe_calls"),
        [
            # never-converging instance -> attempts bounded by the timeout budget (capped per-attempt backoff)
            (10, _expected_describe_attempts(10)),
            (1, _expected_describe_attempts(1)),
            (
                INSTANCE_INFO_RETRIEVAL_TIMEOUT_DEFAULT,
                _expected_describe_attempts(INSTANCE_INFO_RETRIEVAL_TIMEOUT_DEFAULT),
            ),
        ],
        ids=["timeout_10s", "timeout_1s", "timeout_default"],
    )
    def test_get_instances_info_retry_count_scales_with_timeout(
        self, mocker, instance_info_retrieval_timeout, expected_describe_calls
    ):
        # Patch sleep so the test runs instantly and stub the EC2 describe to always return incomplete info.
        mocker.patch("time.sleep")
        fleet_manager = FleetManagerFactory.get_manager(
            "hit",
            "region",
            "boto3_config",
            FLEET_CONFIG,
            "queue2",
            "fleet-ondemand",
            True,
            {},
            {},
            instance_info_retrieval_timeout=instance_info_retrieval_timeout,
        )
        # Always-incomplete response keeps the instance in partial_instance_ids, forcing retries until timeout.
        retrieve_mock = mocker.patch.object(
            fleet_manager, "_retrieve_instances_info_from_ec2", return_value=([], ["i-12345"])
        )

        instances, partial_instance_ids = fleet_manager._get_instances_info(["i-12345"])

        assert_that(instances).is_empty()
        assert_that(partial_instance_ids).is_equal_to(["i-12345"])
        assert_that(retrieve_mock.call_count).is_equal_to(expected_describe_calls)

    @pytest.mark.parametrize(
        ("instance_ids", "mocked_boto3_request", "expected_result"),
        [
            (
                ["i-12345"],
                [
                    MockedBoto3Request(
                        method="describe_instances",
                        response={
                            "Reservations": [
                                {
                                    "Instances": [
                                        {
                                            "InstanceId": "i-12345",
                                            "PrivateIpAddress": "ip-2",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                            "NetworkInterfaces": [
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 1,
                                                    },
                                                    "PrivateIpAddress": "ip-1",
                                                },
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 1,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                    "PrivateIpAddress": "ip-2",
                                                },
                                                {
                                                    "Attachment": {
                                                        "DeviceIndex": 0,
                                                        "NetworkCardIndex": 0,
                                                    },
                                                    "PrivateIpAddress": "ip-3",
                                                },
                                            ],
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=False,
                    ),
                ],
                "ip-3",
            )
        ],
    )
    def test_from_describe_instance_data(
        self,
        boto3_stubber,
        mocker,
        instance_ids,
        mocked_boto3_request,
        expected_result,
    ):
        # patch boto3 call
        mocker.patch("time.sleep")
        ec2_client = boto3_stubber("ec2", mocked_boto3_request)
        instance_info = ec2_client.describe_instances(InstanceIds=instance_ids)["Reservations"][0]["Instances"][0]
        instance_description = EC2Instance.from_describe_instance_data(instance_info)
        assert_that(expected_result).is_equal_to(instance_description.private_ip)


class TestFleetManager:
    @pytest.mark.parametrize(
        "count, job_id",
        [
            (0, None),
            (1, None),
            (1, "1"),
        ],
    )
    def test_launch_ec2_instances(self, mocker, count, job_id):
        fleet_manager = FleetManagerFactory.get_manager(
            "hit", "region", "boto3_config", FLEET_CONFIG, "queue2", "fleet-ondemand", True, {}, {}
        )

        # patch internal functions
        setup_logging_filter = mocker.patch(
            "slurm_plugin.fleet_manager.setup_logging_filter", return_value=mocker.MagicMock()
        )
        fleet_manager._evaluate_launch_params = mocker.MagicMock()
        fleet_manager._launch_instances = mocker.MagicMock()

        fleet_manager.launch_ec2_instances(count, job_id)

        if not job_id:
            setup_logging_filter.assert_not_called()
        else:
            setup_logging_filter.assert_called_once()
            setup_logging_filter.return_value.__enter__.return_value.set_custom_value.assert_any_call(job_id)

        fleet_manager._evaluate_launch_params.assert_called_once_with(count)
        fleet_manager._launch_instances.assert_called_once()

    def test_launch_ec2_instances_retries_on_throttling(self, mocker):
        """Verify CreateFleet throttling is retried, also when the response carries one error per override."""
        mocker.patch("time.sleep")
        fleet_manager = FleetManagerFactory.get_manager(
            "hit", "region", "boto3_config", FLEET_CONFIG, "queue2", "fleet-ondemand", True, {}, {}
        )
        mocker.patch.object(fleet_manager, "_evaluate_launch_params", return_value={})
        launched_instance_info = {
            "InstanceId": "i-12345",
            "PrivateIpAddress": "ip-1",
            "PrivateDnsName": "hostname",
            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "NetworkInterfaces": [
                {"Attachment": {"DeviceIndex": 0, "NetworkCardIndex": 0}, "PrivateIpAddress": "ip-1"},
            ],
        }
        mocker.patch.object(
            fleet_manager,
            "_get_instances_info",
            side_effect=lambda instance_ids: ([launched_instance_info] if instance_ids else [], []),
        )
        throttled_response = {
            "Instances": [],
            "Errors": [{"ErrorCode": "RequestLimitExceeded", "ErrorMessage": "Request limit exceeded."}]
            + [
                {
                    "ErrorCode": "UnfulfillableCapacity",
                    "ErrorMessage": "Failed to fulfill capacity. Please review errors in the response.",
                }
            ]
            * 35,
            "ResponseMetadata": {"RequestId": "1234-abcde"},
        }
        create_fleet = mocker.patch(
            "slurm_plugin.fleet_manager.create_fleet",
            side_effect=[throttled_response, {"Instances": [{"InstanceIds": ["i-12345"]}]}],
        )

        launched = fleet_manager.launch_ec2_instances(1)

        assert_that(create_fleet.call_count).is_equal_to(2)
        assert_that(launched).is_length(1)

    @pytest.mark.parametrize(
        ("err_list", "expected_error_code"),
        [
            # The real cause is reported even though every other override adds an entry pointing back at it.
            ([UNSUPPORTED_ERROR] + [UNFULFILLED_OVERRIDE] * 35, "Unsupported"),
            # The entries pointing at the real cause carry no order guarantee.
            ([UNFULFILLED_OVERRIDE] * 35 + [UNSUPPORTED_ERROR], "Unsupported"),
            # UnfulfillableCapacity messages other than that one do describe a cause and are kept.
            ([MIN_TARGET_CAPACITY_ERROR] + [UNFULFILLED_OVERRIDE] * 35, "UnfulfillableCapacity"),
            # A single override is unaffected, whatever the entry says.
            ([UNFULFILLED_OVERRIDE], "UnfulfillableCapacity"),
            ([MIN_TARGET_CAPACITY_ERROR], "UnfulfillableCapacity"),
            # Nothing to prefer: reporting stays as it was, so no cause is claimed.
            ([UNFULFILLED_OVERRIDE] * 36, None),
            ([UNSUPPORTED_ERROR, {"ErrorCode": "VcpuLimitExceeded", "ErrorMessage": "vCPU limit"}], None),
        ],
        ids=[
            "real_cause_first",
            "real_cause_last",
            "min_target_capacity_kept",
            "single_override_unfulfilled",
            "single_override_min_target_capacity",
            "only_unfulfilled_overrides",
            "two_real_causes",
        ],
    )
    def test_launch_instances_reports_the_cause_among_unfulfilled_overrides(
        self, mocker, err_list, expected_error_code
    ):
        """An entry is added per override that was not fulfilled; only a real cause is worth reporting."""
        fleet_manager = FleetManagerFactory.get_manager(
            "hit", "region", "boto3_config", FLEET_CONFIG, "queue2", "fleet-ondemand", False, {}, {}
        )
        mocker.patch.object(fleet_manager, "_evaluate_launch_params", return_value={})
        mocker.patch.object(fleet_manager, "_get_instances_info", return_value=([], []))
        mocker.patch(
            "slurm_plugin.fleet_manager.create_fleet",
            return_value={"Instances": [], "Errors": err_list, "ResponseMetadata": {"RequestId": "1234-abcde"}},
        )

        if expected_error_code:
            with pytest.raises(LaunchInstancesError) as e:
                fleet_manager._launch_instances({})
            assert_that(e.value.code).is_equal_to(expected_error_code)
        else:
            assert_that(fleet_manager._launch_instances({})).is_equal_to({"Instances": []})
