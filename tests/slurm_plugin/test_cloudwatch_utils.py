# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from assertpy import assert_that
from botocore.config import Config
from botocore.exceptions import ClientError
from slurm_plugin.cloudwatch_utils import METRICS_NAMESPACE, CloudWatchMetricsPublisher


class TestCloudWatchMetricsPublisher:
    """Tests for CloudWatchMetricsPublisher class."""

    @pytest.fixture
    def boto3_config(self):
        return Config(retries={"max_attempts": 1, "mode": "standard"})

    @pytest.fixture
    def metrics_publisher(self, boto3_config):
        return CloudWatchMetricsPublisher(
            region="us-east-1",
            cluster_name="test-cluster",
            instance_id="i-1234567890abcdef0",
            boto3_config=boto3_config,
        )

    def test_init(self, metrics_publisher, boto3_config):
        """Test CloudWatchMetricsPublisher initialization."""
        assert_that(metrics_publisher._region).is_equal_to("us-east-1")
        assert_that(metrics_publisher._cluster_name).is_equal_to("test-cluster")
        assert_that(metrics_publisher._boto3_config).is_equal_to(boto3_config)
        assert_that(metrics_publisher._instance_id).is_equal_to("i-1234567890abcdef0")
        assert_that(metrics_publisher._cloudwatch_client).is_none()

    def test_cloudwatch_client_lazy_initialization(self, metrics_publisher, mocker):
        """Test that CloudWatch client is lazily initialized."""
        mock_client = MagicMock()
        mock_boto3 = mocker.patch("slurm_plugin.cloudwatch_utils.boto3")
        mock_boto3.client.return_value = mock_client

        # First access should create the client
        client = metrics_publisher.cloudwatch_client
        assert_that(client).is_equal_to(mock_client)
        mock_boto3.client.assert_called_once_with(
            "cloudwatch",
            region_name="us-east-1",
            config=metrics_publisher._boto3_config,
        )

        # Second access should return the cached client
        mock_boto3.client.reset_mock()
        client2 = metrics_publisher.cloudwatch_client
        assert_that(client2).is_equal_to(mock_client)
        mock_boto3.client.assert_not_called()

    @pytest.mark.parametrize(
        "metric_name, value, unit, additional_dimensions, expected_dimensions",
        [
            pytest.param(
                "TestMetric",
                42,
                "Count",
                None,
                [
                    {"Name": "ClusterName", "Value": "test-cluster"},
                    {"Name": "InstanceId", "Value": "i-1234567890abcdef0"},
                ],
                id="basic",
            ),
            pytest.param(
                "HeadNodeDaemonHeartbeat",
                1,
                "Count",
                [{"Name": "DaemonName", "Value": "clustermgtd"}],
                [
                    {"Name": "ClusterName", "Value": "test-cluster"},
                    {"Name": "InstanceId", "Value": "i-1234567890abcdef0"},
                    {"Name": "DaemonName", "Value": "clustermgtd"},
                ],
                id="with_additional_dimension",
            ),
            pytest.param(
                "LatencyMetric",
                150.5,
                "Milliseconds",
                None,
                [
                    {"Name": "ClusterName", "Value": "test-cluster"},
                    {"Name": "InstanceId", "Value": "i-1234567890abcdef0"},
                ],
                id="with_custom_unit",
            ),
            pytest.param(
                "CustomMetric",
                100,
                "Count",
                [
                    {"Name": "DaemonName", "Value": "clustermgtd"},
                    {"Name": "NodeType", "Value": "HeadNode"},
                ],
                [
                    {"Name": "ClusterName", "Value": "test-cluster"},
                    {"Name": "InstanceId", "Value": "i-1234567890abcdef0"},
                    {"Name": "DaemonName", "Value": "clustermgtd"},
                    {"Name": "NodeType", "Value": "HeadNode"},
                ],
                id="with_multiple_additional_dimensions",
            ),
        ],
    )
    def test_put_metric(
        self,
        metrics_publisher,
        mocker,
        metric_name: str,
        value: float,
        unit: str,
        additional_dimensions: list,
        expected_dimensions: list,
    ):
        """Test put_metric with various parameter combinations."""
        mock_client = MagicMock()
        metrics_publisher._cloudwatch_client = mock_client
        mock_datetime = mocker.patch("slurm_plugin.cloudwatch_utils.datetime")
        fixed_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_time

        metrics_publisher.put_metric(
            metric_name=metric_name,
            value=value,
            unit=unit,
            additional_dimensions=additional_dimensions,
        )

        mock_client.put_metric_data.assert_called_once_with(
            Namespace=METRICS_NAMESPACE,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Dimensions": expected_dimensions,
                    "Timestamp": fixed_time,
                    "Value": value,
                    "Unit": unit,
                }
            ],
        )

    def test_put_metric_handles_exception(self, metrics_publisher, caplog):
        """Test that put_metric handles exceptions gracefully."""
        mock_client = MagicMock()
        mock_client.put_metric_data.side_effect = ClientError(
            {"Error": {"Code": "WHATEVER_CODE", "Message": "WHATEVER_MESSAGE"}},
            "PutMetricData",
        )
        metrics_publisher._cloudwatch_client = mock_client

        with caplog.at_level(logging.WARNING):
            # Should not raise exception
            metrics_publisher.put_metric(metric_name="WHATEVER_METRIC_NAME", value=1)

        assert_that(caplog.text).matches(
            r"Failed to publish metric WHATEVER_METRIC_NAME:.*WHATEVER_CODE.*WHATEVER_MESSAGE"
        )
