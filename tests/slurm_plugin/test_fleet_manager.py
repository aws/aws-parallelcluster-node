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
import logging
import os
from datetime import datetime, timezone

import pytest
from assertpy import assert_that
from botocore.exceptions import ClientError
from slurm_plugin.fleet_manager import Ec2CreateFleetManager, Ec2RunInstancesManager, FleetManagerFactory

from tests.common import FLEET_CONFIG, MockedBoto3Request


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
            ({"q1": {"cr1": {}}}, "InstanceTypeList or InstanceType field not found .*", None),
            ({"q1": {"cr1": {"InstanceType": "x"}, "other": {"InstanceTypeList": "x"}}}, None, Ec2RunInstancesManager),
            ({"q1": {"cr1": {"InstanceTypeList": "x"}, "other": {"InstanceType": "x"}}}, None, Ec2CreateFleetManager),
        ],
        ids=[
            "not_existing_config_file",
            "missing_queue_in_config_file",
            "missing_cr_in_config_file",
            "missing_instance_type_in_config_file",
            "right_config_file_run_instances",
            "right_config_file_create_fleet",
        ],
    )
    def test_get_manager(self, fleet_config, expected_failure, expected_manager):
        if expected_failure:
            with pytest.raises(Exception, match=expected_failure):
                FleetManagerFactory.get_manager(
                    "cluster_name", "region", "boto3_config", fleet_config, "q1", "cr1", all_or_nothing=False
                )
        else:
            manager = FleetManagerFactory.get_manager(
                "cluster_name", "region", "boto3_config", fleet_config, "q1", "cr1", all_or_nothing=False
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
                    "CapacityReservationSpecification": {
                        "CapacityReservationTarget": {"CapacityReservationId": "cr-12345"}
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
            "hit", "region", "boto3_config", FLEET_CONFIG, "queue1", compute_resource, all_or_nothing
        )
        launch_params = fleet_manager._evaluate_launch_params(batch_size, launch_overrides=launch_overrides)
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
        ],
        ids=["normal"],
    )
    def test_launch_instances(self, boto3_stubber, launch_params, mocked_boto3_request, expected_assigned_nodes):
        # patch boto3 call
        boto3_stubber("ec2", mocked_boto3_request)
        # run test
        fleet_manager = FleetManagerFactory.get_manager(
            "hit", "region", "boto3_config", FLEET_CONFIG, "queue1", "p4d24xlarge", all_or_nothing=False
        )
        assigned_nodes = fleet_manager._launch_instances(launch_params)
        assert_that(assigned_nodes.get("Instances", [])).is_equal_to(expected_assigned_nodes)


# -------- Ec2CreateFleetManager ------


def _mocked_create_fleet_params(
    queue, compute_resource, min_capacity, allocation_strategy, capacity_type, overrides=None
):
    template_overrides = []
    for instance_type in ["t2.medium", "t2.large"]:
        override = {"InstanceType": instance_type}
        if capacity_type == "spot":
            override["MaxPrice"] = str(10)
            launch_options = {
                "SpotOptions": {
                    "AllocationStrategy": allocation_strategy,
                    "SingleInstanceType": False,
                    "SingleAvailabilityZone": True,
                    "MinTargetCapacity": min_capacity,
                }
            }
        else:
            launch_options = {
                "OnDemandOptions": {
                    "AllocationStrategy": allocation_strategy,
                    "CapacityReservationOptions": {"UsageStrategy": "use-capacity-reservations-first"},
                    "SingleInstanceType": False,
                    "SingleAvailabilityZone": True,
                    "MinTargetCapacity": min_capacity,
                },
            }

        template_overrides.append(override)

    params = {
        "LaunchTemplateConfigs": [
            {
                "LaunchTemplateSpecification": {
                    "LaunchTemplateName": f"hit-{queue}-{compute_resource}",
                    "Version": "$Latest",
                },
                "Overrides": template_overrides,
            }
        ],
        "TargetCapacitySpecification": {
            "TotalTargetCapacity": 5,
            "DefaultTargetCapacityType": capacity_type,
        },
        "Type": "instant",
        **launch_options,
    }
    if overrides:
        params.update(overrides)
    return params


class TestCreateFleetManager:
    @pytest.mark.parametrize(
        (
            "batch_size",
            "queue",
            "compute_resource",
            "all_or_nothing",
            "launch_overrides",
            "expected_params",
        ),
        [
            # normal - spot
            (
                5,
                "queue1",
                "fleet-spot",
                False,
                {},
                _mocked_create_fleet_params("queue1", "fleet-spot", 1, "capacity-optimized", "spot"),
            ),
            # normal - on-demand
            (
                5,
                "queue2",
                "fleet-ondemand",
                False,
                {},
                _mocked_create_fleet_params("queue2", "fleet-ondemand", 1, "lowest-price", "on-demand"),
            ),
            # all or nothing
            (
                5,
                "queue1",
                "fleet-spot",
                True,
                {},
                _mocked_create_fleet_params("queue1", "fleet-spot", 5, "capacity-optimized", "spot"),
            ),
            # launch_overrides
            (
                5,
                "queue2",
                "fleet-ondemand",
                False,
                {
                    "TagSpecifications": [
                        {"ResourceType": "capacity-reservation", "Tags": [{"Key": "string", "Value": "string"}]}
                    ]
                },
                _mocked_create_fleet_params(
                    "queue2",
                    "fleet-ondemand",
                    1,
                    "lowest-price",
                    "on-demand",
                    {
                        "TagSpecifications": [
                            {"ResourceType": "capacity-reservation", "Tags": [{"Key": "string", "Value": "string"}]}
                        ]
                    },
                ),
            ),
        ],
        ids=["fleet_spot", "fleet_ondemand", "all_or_nothing", "launch_overrides"],
    )
    def test_evaluate_launch_params(
        self,
        batch_size,
        queue,
        compute_resource,
        all_or_nothing,
        launch_overrides,
        expected_params,
        caplog,
    ):
        caplog.set_level(logging.INFO)
        # run tests
        fleet_manager = FleetManagerFactory.get_manager(
            "hit", "region", "boto3_config", FLEET_CONFIG, queue, compute_resource, all_or_nothing
        )
        launch_params = fleet_manager._evaluate_launch_params(batch_size, launch_overrides)
        assert_that(launch_params).is_equal_to(expected_params)
        if launch_overrides:
            assert_that(caplog.text).contains("Found CreateFleet parameters override")

    @pytest.mark.parametrize(
        ("launch_params", "mocked_boto3_request", "expected_assigned_nodes"),
        [
            # normal - spot
            (
                _mocked_create_fleet_params("queue1", "fleet-spot", 1, "capacity-optimized", "spot"),
                [
                    MockedBoto3Request(
                        method="create_fleet",
                        response={"Instances": [{"InstanceIds": ["i-12345", "i-23456"]}]},
                        expected_params=_mocked_create_fleet_params(
                            "queue1", "fleet-spot", 1, "capacity-optimized", "spot"
                        ),
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
                                        },
                                        {
                                            "InstanceId": "i-23456",
                                            "PrivateIpAddress": "ip-3",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
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
                    },
                    {
                        "InstanceId": "i-23456",
                        "PrivateIpAddress": "ip-3",
                        "PrivateDnsName": "hostname",
                        "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                    },
                ],
            ),
            # normal - on-demand
            (
                _mocked_create_fleet_params("queue2", "fleet-ondemand", 1, "lowest-price", "on-demand"),
                [
                    MockedBoto3Request(
                        method="create_fleet",
                        response={"Instances": [{"InstanceIds": ["i-12345"]}]},
                        expected_params=_mocked_create_fleet_params(
                            "queue2", "fleet-ondemand", 1, "lowest-price", "on-demand"
                        ),
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
                    }
                ],
            ),
        ],
        ids=["fleet_spot", "fleet_ondemand"],
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
            "hit", "region", "boto3_config", FLEET_CONFIG, "queue2", "fleet-ondemand", all_or_nothing=False
        )

        assigned_nodes = fleet_manager._launch_instances(launch_params)
        assert_that(assigned_nodes.get("Instances", [])).is_equal_to(expected_assigned_nodes)

    @pytest.mark.parametrize(
        ("instance_ids", "mocked_boto3_request", "expected_exception", "expected_error", "expected_result"),
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
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=False,
                    ),
                ],
                False,
                None,
                (
                    [
                        {
                            "InstanceId": "i-12345",
                            "PrivateIpAddress": "ip-2",
                            "PrivateDnsName": "hostname",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
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
                                        },
                                        {
                                            "InstanceId": "i-23456",
                                            "PrivateIpAddress": "ip-3",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
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
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=False,
                    ),
                ],
                False,
                None,
                (
                    [
                        {
                            "InstanceId": "i-23456",
                            "PrivateIpAddress": "ip-3",
                            "PrivateDnsName": "hostname",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                        },
                        {
                            "InstanceId": "i-12345",
                            "PrivateIpAddress": "ip-2",
                            "PrivateDnsName": "hostname",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
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
                                        },
                                        {
                                            "InstanceId": "i-23456",
                                            "PrivateIpAddress": "ip-3",
                                            "PrivateDnsName": "hostname",
                                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345", "i-23456"]},
                        generate_error=False,
                    ),
                ]
                + 2
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
                                        },
                                    ]
                                }
                            ]
                        },
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=False,
                    ),
                ],
                False,
                "Unable to retrieve instance info for instances: ['i-12345']",
                (
                    [
                        {
                            "InstanceId": "i-23456",
                            "PrivateIpAddress": "ip-3",
                            "PrivateDnsName": "hostname",
                            "LaunchTime": datetime(2020, 1, 1, tzinfo=timezone.utc),
                        },
                    ],
                    ["i-12345"],
                ),
            ),
            # client error
            (
                ["i-12345"],
                [
                    MockedBoto3Request(
                        method="describe_instances",
                        response={},
                        expected_params={"InstanceIds": ["i-12345"]},
                        generate_error=True,
                    ),
                ],
                True,
                "An error occurred .* when calling the DescribeInstances operation",
                ([], []),
            ),
        ],
        ids=["fleet_ondemand", "incomplete_instance_info", "too_many_incomplete_instance_info", "client_error"],
    )
    def test_get_instances_info(  # Note: some tests cases are covered by test_launch_instances too.
        self,
        boto3_stubber,
        mocker,
        instance_ids,
        mocked_boto3_request,
        expected_exception,
        expected_error,
        expected_result,
        caplog,
    ):
        # patch boto3 call
        mocker.patch("time.sleep")
        boto3_stubber("ec2", mocked_boto3_request)
        # run test
        fleet_manager = FleetManagerFactory.get_manager(
            "hit", "region", "boto3_config", FLEET_CONFIG, "queue2", "fleet-ondemand", True
        )

        if expected_exception:
            with pytest.raises(ClientError, match=expected_error):
                fleet_manager._get_instances_info(instance_ids)
        else:
            complete_instances, partial_instance_ids = fleet_manager._get_instances_info(instance_ids)
            assert_that(expected_result).is_equal_to((complete_instances, partial_instance_ids))
