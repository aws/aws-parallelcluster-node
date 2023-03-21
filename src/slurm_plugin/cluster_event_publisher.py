# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import json
import logging
import sys
import traceback
from collections import ChainMap
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List

from slurm_plugin.common import log_exception
from slurm_plugin.slurm_resources import DynamicNode, SlurmNode, StaticNode

logger = logging.getLogger(__name__)

_LAUNCH_FAILURE_GROUPING = {
    **{failure: "ice-failures" for failure in [*SlurmNode.EC2_ICE_ERROR_CODES, "LimitedInstanceCapacity"]},
    **{failure: "vcpu-limit-failures" for failure in ["VcpuLimitExceeded"]},
    **{failure: "volume-limit-failures" for failure in ["VolumeLimitExceeded", "InsufficientVolumeCapacity"]},
    **{failure: "iam-policy-errors" for failure in ["UnauthorizedOperation"]},
}


class ClusterEventPublisher:
    """Class for generating structured log events for cluster."""

    def __init__(self, event_publisher: Callable = lambda *args, **kwargs: None, max_list_size=100):
        self._publish_event = event_publisher
        self._max_list_size = max_list_size

    @property
    def publish_event(self):
        return self._publish_event

    @staticmethod
    def timestamp():
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    @staticmethod
    def create_with_default_publisher(
        event_logger, cluster_name, node_role, component, instance_id, max_list_size=100, **global_args
    ):
        """Create an instance of ClusterEventPublisher with the standard event publisher."""
        publiser = ClusterEventPublisher._get_event_publisher(
            event_logger, cluster_name, node_role, component, instance_id, **global_args
        )
        return ClusterEventPublisher(publiser, max_list_size)

    @log_exception(logger, "publish_unhealthy_static_node_events", catch_exception=Exception, raise_on_error=False)
    def publish_unhealthy_static_node_events(
        self,
        unhealthy_static_nodes: List[SlurmNode],
        nodes_in_replacement: Iterable[str],
        launched_nodes: Iterable[str],
        failed_nodes: Dict[str, List[str]],
    ):
        """Publish events for unhealthy static nodes."""
        timestamp = ClusterEventPublisher.timestamp()

        for count, error_detail in self._generate_launch_failure_details(failed_nodes):
            self.publish_event(
                logging.WARNING if count else logging.DEBUG,
                "Number of static nodes that failed to launch a backing instance after node maintenance",
                event_type="node-launch-failure-count",
                timestamp=timestamp,
                detail=error_detail,
            )

        self.publish_event(
            logging.DEBUG,
            "After node maintenance, node failed to launch a backing instance",
            event_type="node-launch-failure",
            timestamp=timestamp,
            event_supplier=self._failed_node_supplier(unhealthy_static_nodes, failed_nodes),
        )

        self.publish_event(
            logging.DEBUG,
            "Number of static nodes failing scheduler health check",
            event_type="static-node-health-check-failure-count",
            timestamp=timestamp,
            event_supplier=self._node_list_and_count_supplier(unhealthy_static_nodes),
        )

        self.publish_event(
            logging.DEBUG,
            "Static node failing scheduler health check",
            event_type="static-node-health-check-failure",
            timestamp=timestamp,
            event_supplier=self._unhealthy_node_supplier(unhealthy_static_nodes),
        )

        self.publish_event(
            logging.DEBUG,
            "Number of instances terminated due to backing unhealthy static nodes",
            event_type="static-node-instance-terminate-count",
            timestamp=timestamp,
            event_supplier=self._terminated_instances_supplier(
                (node for node in unhealthy_static_nodes if node.instance)
            ),
        )

        self.publish_event(
            logging.DEBUG,
            "After node maintenance, nodes currently in replacement",
            event_type="static-nodes-in-replacement-count",
            timestamp=timestamp,
            event_supplier=self._node_list_and_count_supplier(nodes_in_replacement),
        )

        self.publish_event(
            logging.DEBUG,
            "Number of static nodes that successfully launched after node mainenance",
            event_type="static-node-launched-count",
            timestamp=timestamp,
            event_supplier=self._node_list_and_count_supplier(launched_nodes),
        )

    @log_exception(logger, "publish_nodes_failing_health_check_events", catch_exception=Exception, raise_on_error=False)
    def publish_nodes_failing_health_check_events(
        self, health_check_type: any, node_names_failing_health_check: Iterable[str]
    ):
        """Publish events for nodes failing `health_check_type`."""

        def detail_supplier(node_names):
            yield {
                "detail": {
                    "failure-type": str(health_check_type),
                    "count": len(node_names),
                    "nodes": self._generate_node_name_list(node_names),
                }
            }

        timestamp = ClusterEventPublisher.timestamp()

        node_names_failing_health_check = list(node_names_failing_health_check)
        self.publish_event(
            logging.WARNING if node_names_failing_health_check else logging.DEBUG,
            f"Number of nodes failing health check {health_check_type}",
            "nodes-failing-health-check-count",
            timestamp=timestamp,
            event_supplier=detail_supplier(node_names_failing_health_check),
        )

    @log_exception(logger, "publish_unhealthy_node_events", catch_exception=Exception, raise_on_error=False)
    def publish_unhealthy_node_events(self, unhealthy_nodes: List[SlurmNode]):
        """Publish events for unhealthy nodes without a backing instance and for nodes that are not responding."""
        timestamp = ClusterEventPublisher.timestamp()

        nodes_with_invalid_backing_instance = [
            node for node in unhealthy_nodes if not node.is_backing_instance_valid(log_warn_if_unhealthy=False)
        ]
        self.publish_event(
            logging.WARNING if nodes_with_invalid_backing_instance else logging.DEBUG,
            "Number of nodes without a valid backing instance",
            "invalid-backing-instance-count",
            timestamp=timestamp,
            event_supplier=self._node_list_and_count_supplier(nodes_with_invalid_backing_instance),
        )

        nodes_not_responding = [node for node in unhealthy_nodes if node.is_down_not_responding()]
        self.publish_event(
            logging.WARNING if nodes_not_responding else logging.DEBUG,
            "Number of nodes set to DOWN because they are not responding",
            "node-not-responding-down-count",
            timestamp=timestamp,
            event_supplier=self._node_list_and_count_supplier(nodes_not_responding),
        )

    @log_exception(logger, "publish_bootstrap_failure_events", catch_exception=Exception, raise_on_error=False)
    def publish_bootstrap_failure_events(self, bootstrap_failure_nodes: List[SlurmNode]):
        """Publish events for nodes failing to bootstrap."""
        timestamp = ClusterEventPublisher.timestamp()

        for count, detail in self._protected_mode_error_count_supplier(bootstrap_failure_nodes):
            self.publish_event(
                logging.WARNING if count else logging.DEBUG,
                "Number of nodes that failed to bootstrap",
                "protected-mode-error-count",
                timestamp=timestamp,
                detail=detail,
            )

    # Slurm Resume Events
    @log_exception(logger, "publish_node_launch_events", catch_exception=Exception, raise_on_error=False)
    def publish_node_launch_events(self, failed_nodes: Dict[str, List[str]]):
        """Publish events for nodes that failed to launch from slurm_resume."""
        timestamp = ClusterEventPublisher.timestamp()

        for count, error_detail in self._generate_launch_failure_details(failed_nodes):
            self.publish_event(
                logging.WARNING if count else logging.DEBUG,
                "Number of nodes that failed to launch",
                event_type="node-launch-failure-count",
                timestamp=timestamp,
                detail=error_detail,
            )

        self.publish_event(
            logging.DEBUG,
            "Setting failed node to DOWN state",
            event_type="node-launch-failure",
            timestamp=timestamp,
            event_supplier=self._flatten_failed_launch_nodes(failed_nodes),
        )

    def _generate_launch_failure_details(self, failed_nodes: Dict[str, List[str]]):
        """
        Build a dictionary based on failure category (e.g. ice-failure).

        The elements contain the the number of nodes and the of node names in that category.
        """
        detail_map = {"other-failures": {"count": 0, "error-details": {}}}
        for failure_type in _LAUNCH_FAILURE_GROUPING.values():
            detail_map.setdefault(failure_type, {"count": 0, "error-details": {}})

        for error_code, nodes in failed_nodes.items():
            failure_type = ClusterEventPublisher._get_failure_type_from_error_code(error_code)
            error_entry = detail_map.get(failure_type)
            error_entry.update(
                {
                    "count": error_entry.get("count") + len(nodes),
                }
            )
            error_details = error_entry.get("error-details")
            error_details.update(
                {
                    error_code: {
                        "count": len(nodes),
                        "nodes": self._generate_node_name_list(list(nodes)),
                    }
                }
            )

        for failure_type, detail in detail_map.items():
            count = detail.get("count", 0)
            yield count, {
                "failure-type": failure_type,
                "count": count,
                "error-details": detail.get("error-details", None),
            }

    def _limit_list(self, source_list: List) -> List:
        """Limit lists of nodes to _max_list_size."""
        value = source_list[: self._max_list_size] if self._max_list_size else source_list
        return value

    def _generate_node_name_list(self, node_list):
        return [{"name": node.name if isinstance(node, SlurmNode) else node} for node in self._limit_list(node_list)]

    def _terminated_instances_supplier(self, terminated_instances):
        terminated_instances = list(terminated_instances)
        yield {
            "detail": {
                "count": len(terminated_instances),
                "nodes": [
                    {
                        "name": node.name,
                        "id": node.instance.id,
                        "ip": node.instance.private_ip,
                        "error-code": node.error_code,
                        "reason": node.reason,
                    }
                    for node in self._limit_list(terminated_instances)
                ],
            }
        }

    def _node_list_and_count_supplier(self, node_list):
        node_list = list(node_list)
        yield {
            "detail": {
                "count": len(node_list),
                "nodes": self._generate_node_name_list(node_list),
            }
        }

    def _unhealthy_node_supplier(self, unhealthy_static_nodes):
        for node in self._limit_list(unhealthy_static_nodes):
            yield {"detail": {"node": ClusterEventPublisher._describe_node(node)}}

    def _failed_node_supplier(self, unhealthy_static_nodes, failed_nodes):
        for error_code, failed_node_list in failed_nodes.items():
            for node in unhealthy_static_nodes:
                if node.name in failed_node_list:
                    yield {
                        "detail": {
                            "node": ClusterEventPublisher._describe_node(node),
                            "error-code": error_code,
                            "failure-type": ClusterEventPublisher._get_failure_type_from_error_code(error_code),
                        }
                    }

    def _protected_mode_error_count_supplier(self, bootstrap_failure_nodes):
        dynamic_nodes = []
        static_nodes = []
        other_nodes = []
        for node in bootstrap_failure_nodes:
            if node.is_bootstrap_timeout():
                (dynamic_nodes if isinstance(node, DynamicNode) else static_nodes).append(node)
            else:
                other_nodes.append(node)
        for error_type, nodes in [
            ("static-replacement-timeout-error", static_nodes),
            ("dynamic-resume-timeout-error", dynamic_nodes),
            ("other-bootstrap-error", other_nodes),
        ]:
            count = len(nodes)
            yield count, {
                "failure-type": error_type,
                "count": count,
                "nodes": self._generate_node_name_list(nodes),
            }

    @staticmethod
    def _flatten_failed_launch_nodes(failed_nodes: Dict[str, List[str]]):
        for error_code, nodes in failed_nodes.items():
            for node_name in nodes:
                yield {
                    "detail": {
                        "error-code": error_code,
                        "failure-type": ClusterEventPublisher._get_failure_type_from_error_code(error_code),
                        "node": {"name": node_name},
                    }
                }

    @staticmethod
    def _get_failure_type_from_error_code(error_code: str) -> str:
        return _LAUNCH_FAILURE_GROUPING.get(error_code, "other-failures")

    @staticmethod
    def _get_event_publisher(event_logger, cluster_name, node_role, component, instance_id, **global_args):
        def emit_event(event_level, message, event_type, timestamp=None, event_supplier=None, **kwargs):
            if event_logger.isEnabledFor(event_level):
                now = timestamp if timestamp else datetime.now(timezone.utc).isoformat(timespec="milliseconds")
                if not event_supplier:
                    event_supplier = [kwargs]
                for details in event_supplier:
                    try:
                        event = ChainMap(
                            details,
                            kwargs,
                            {
                                "datetime": now,
                                "version": 0,
                                "scheduler": "slurm",
                                "cluster-name": cluster_name,
                                "node-role": node_role,
                                "component": component,
                                "level": logging.getLevelName(event_level),
                                "instance-id": instance_id,
                                "event-type": event_type,
                                "message": message,
                                "detail": {},
                            },
                            global_args,
                        )

                        event_logger.log(event_level, "%s", json.dumps(dict(event)))
                    except Exception as e:
                        logger.error("Failed to publish event: %s\n%s", e, traceback.format_exception(*sys.exc_info()))

        return emit_event

    @staticmethod
    def _describe_node(node: SlurmNode):
        node_states = list(node.state_string.split("+"))
        return (
            {
                "name": node.name,
                "type": "static" if isinstance(node, StaticNode) else "dynamic",
                "address": node.nodeaddr,
                "hostname": node.nodehostname,
                "state-string": node.state_string,
                "state": node_states[0] if node.states else None,
                "state-flags": node_states[1:],
                "instance": ClusterEventPublisher._describe_instance(node.instance) if node.instance else None,
                "partitions": list(node.partitions),
                "queue-name": node.queue_name,
                "compute-resource": node.compute_resource_name,
            }
            if node
            else None
        )

    @staticmethod
    def _describe_instance(instance):
        return (
            {
                "id": instance.id,
                "private-ip": instance.private_ip,
                "hostname": instance.hostname,
                "launch-time": instance.launch_time,
            }
            if instance
            else None
        )
