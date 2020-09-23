# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.


import abc
import sys
from datetime import datetime, timezone

if sys.version_info >= (3, 4):  # FIXME: Is it really needed? Which Python version is installed on Master instance?
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta("ABC", (), {})

MISSED_METRICS_UPDATE_FACTOR = 3


class MetricsProvider(ABC):
    @abc.abstractmethod
    def update_metric_values(self, metrics):
        pass


class ActiveNodesMetricsProvider(MetricsProvider):
    def __init__(self, slurm_active_nodes):
        self._slurm_active_nodes = slurm_active_nodes

    def update_metric_values(self, metrics):
        num_power_up_nodes = 0
        num_power_down_nodes = 0

        for node in self._slurm_active_nodes:
            if node.is_nodeaddr_set():
                num_power_up_nodes += 1
            else:
                num_power_down_nodes += 1
        metrics["slurm_number_power_up_nodes"] = num_power_up_nodes
        metrics["slurm_number_power_down_nodes"] = num_power_down_nodes


class MetricsCollector:
    def __init__(self):
        self.timestamp = datetime(2020, 7, 6, tzinfo=timezone.utc)
        self.metrics = {}
        self.metrics_providers = {}
        self.config = None

    def add_metric(self, name, value):
        self.metrics[name] = value

    def add_metrics_provider(self, name, metrics_provider):
        self.metrics_providers[name] = metrics_provider

    def get_values(self):
        time_since_last_metric = int((datetime.now(tz=timezone.utc) - self.timestamp).total_seconds())
        time_between_metrics_update = self.config.loop_time if self.config else -1
        # If the metrics have not been updated for a long time (MISSED_METRICS_UPDATE_FACTOR updates missed)
        # do not return anything
        if time_since_last_metric < MISSED_METRICS_UPDATE_FACTOR * time_between_metrics_update:
            # Getting metrics from metrics providers
            for metrics_provider in self.metrics_providers.values():
                metrics_provider.update_metric_values(self.metrics)
            return self.metrics
        return {}
