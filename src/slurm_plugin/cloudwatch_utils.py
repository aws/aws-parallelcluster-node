# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)

METRICS_NAMESPACE = "ParallelCluster"
METRICS_DIMENSION_CLUSTER_NAME = "ClusterName"
METRICS_DIMENSION_INSTANCE_ID = "InstanceId"


class CloudWatchMetricsPublisher:
    """Class for publishing metrics to CloudWatch."""

    def __init__(self, region: str, cluster_name: str, instance_id: str, boto3_config: Config):
        """
        Initialize CloudWatchMetricsPublisher.

        Args:
            region: AWS region
            cluster_name: Name of the ParallelCluster cluster
            instance_id: EC2 instance ID to include in metric dimensions
            boto3_config: Boto3 configuration for retries and proxies
        """
        self._region = region
        self._cluster_name = cluster_name
        self._instance_id = instance_id
        self._boto3_config = boto3_config
        self._cloudwatch_client = None

    @property
    def cloudwatch_client(self):
        """Lazy initialization of CloudWatch client."""
        if self._cloudwatch_client is None:
            self._cloudwatch_client = boto3.client("cloudwatch", region_name=self._region, config=self._boto3_config)
        return self._cloudwatch_client

    def put_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        additional_dimensions: Optional[List[Dict[str, str]]] = None,
    ):
        """
        Publish a metric to CloudWatch.

        Automatically sets timestamp and includes ClusterName as a dimension.

        Args:
            metric_name: Name of the metric to publish
            value: Metric value
            unit: CloudWatch unit (default: "Count")
            additional_dimensions: Optional list of additional dimensions [{"Name": "...", "Value": "..."}]
        """
        dimensions = [
            {"Name": METRICS_DIMENSION_CLUSTER_NAME, "Value": self._cluster_name},
            {"Name": METRICS_DIMENSION_INSTANCE_ID, "Value": self._instance_id},
        ]
        if additional_dimensions:
            dimensions.extend(additional_dimensions)

        try:
            self.cloudwatch_client.put_metric_data(
                Namespace=METRICS_NAMESPACE,
                MetricData=[
                    {
                        "MetricName": metric_name,
                        "Dimensions": dimensions,
                        "Timestamp": datetime.now(tz=timezone.utc),
                        "Value": value,
                        "Unit": unit,
                    }
                ],
            )
            logger.debug("Published metric %s with value %s", metric_name, value)
        except Exception as e:
            logger.error("Failed to publish metric %s: %s", metric_name, e)
