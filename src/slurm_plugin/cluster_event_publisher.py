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
    **{failure: "custom-ami-errors" for failure in ["InvalidBlockDeviceMapping"]},
    **{failure: "iam-policy-errors" for failure in ["UnauthorizedOperation", "AccessDeniedException"]},
}

_EVENT_TO_LOG_LEVEL_MAPPING = {
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
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
    def create(event_logger, cluster_name, node_role, component, instance_id, max_list_size=100, **global_args):
        publiser = ClusterEventPublisher._get_event_publisher(
            event_logger, cluster_name, node_role, component, instance_id, **global_args
        )
        return ClusterEventPublisher(publiser, max_list_size)

    @log_exception(logger, "publish_unhealthy_static_node_events", catch_exception=Exception, raise_on_error=False)
    def publish_unhealthy_static_node_events(
        self,
        unhealthy_static_nodes: List[SlurmNode],
        nodes_in_replacement: Iterable[str],
        failed_nodes: Dict[str, List[str]],
    ):
        timestamp = ClusterEventPublisher.timestamp()

        nodes_in_replacement = list(nodes_in_replacement)

        self.publish_event(
            logging.WARNING if failed_nodes else logging.DEBUG,
            "Number of static nodes that failed replacement after node maintenance",
            event_type="node-launch-failure-count",
            timestamp=timestamp,
            event_supplier=self._get_launch_failure_details(failed_nodes),
        )

        self.publish_event(
            logging.DEBUG,
            "After node maintenance, node failed replacement",
            event_type="node-launch-failure",
            timestamp=timestamp,
            event_supplier=self._failed_node_supplier(unhealthy_static_nodes, failed_nodes),
        )

        self.publish_event(
            logging.DEBUG,
            "Number of static nodes failing scheduler health check",
            event_type="static-node-health-check-failure-count",
            timestamp=timestamp,
            event_supplier=self._unhealthy_node_count_supplier(unhealthy_static_nodes),
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
            event_supplier=self._node_count_in_replacement_supplier(nodes_in_replacement),
        )

        self.publish_event(
            logging.DEBUG,
            "After node maintenance, node currently in replacement",
            event_type="static-node-in-replacement",
            timestamp=timestamp,
            event_supplier=self._node_in_replacement_supplier(
                (node for node in unhealthy_static_nodes if node.name in nodes_in_replacement)
            ),
        )

    @log_exception(logger, "publish_nodes_failing_health_check_events", catch_exception=Exception, raise_on_error=False)
    def publish_nodes_failing_health_check_events(
        self, health_check_type: any, node_names_failing_health_check: Iterable[str]
    ):
        def detail_supplier():
            node_names = list(node_names_failing_health_check)
            if node_names_failing_health_check:
                yield {
                    "detail": {
                        "health-check-type": str(health_check_type),
                        "count": len(node_names_failing_health_check),
                        "nodes": [{"name": node_name} for node_name in self._limit_list(node_names)],
                    }
                }

        timestamp = ClusterEventPublisher.timestamp()

        self.publish_event(
            logging.WARNING,
            f"Number of nodes failing health check {health_check_type}",
            "nodes-failing-health-check-count",
            timestamp=timestamp,
            event_supplier=detail_supplier(),
        )

    @log_exception(logger, "publish_unhealthy_node_events", catch_exception=Exception, raise_on_error=False)
    def publish_unhealthy_node_events(self, unhealthy_nodes: List[SlurmNode]):
        def detail_supplier():
            invalid_backing_instance_nodes = [
                node for node in unhealthy_nodes if not node.is_backing_instance_valid(log_warn_if_unhealthy=False)
            ]
            if invalid_backing_instance_nodes:
                yield {
                    "detail": {
                        "count": len(invalid_backing_instance_nodes),
                        "nodes": self._generate_node_name_list(invalid_backing_instance_nodes),
                    }
                }

        timestamp = ClusterEventPublisher.timestamp()

        self.publish_event(
            logging.WARNING if unhealthy_nodes else logging.DEBUG,
            "Number of nodes without a valid backing instance",
            "invalid-backing-instance-count",
            timestamp=timestamp,
            event_supplier=detail_supplier(),
        )

    @log_exception(logger, "publish_bootstrap_failure_events", catch_exception=Exception, raise_on_error=False)
    def publish_bootstrap_failure_events(self, bootstrap_failure_nodes: List[SlurmNode]):
        timestamp = ClusterEventPublisher.timestamp()

        self.publish_event(
            logging.WARNING if bootstrap_failure_nodes else logging.DEBUG,
            "Number of nodes that failed to bootstrap",
            "protected-mode-error-count",
            timestamp=timestamp,
            event_supplier=self._protected_mode_error_count_supplier(bootstrap_failure_nodes),
        )

    # Slurm Resume Events
    @log_exception(logger, "publish_node_launch_events", catch_exception=Exception, raise_on_error=False)
    def publish_node_launch_events(self, failed_nodes: Dict[str, List[str]]):
        timestamp = ClusterEventPublisher.timestamp()

        self.publish_event(
            logging.WARNING if failed_nodes else logging.DEBUG,
            "Number of nodes that failed to launch",
            event_type="node-launch-failure-count",
            timestamp=timestamp,
            event_supplier=self._get_launch_failure_details(failed_nodes),
        )

        self.publish_event(
            logging.DEBUG,
            "Setting failed node to DOWN state",
            event_type="node-launch-failure",
            timestamp=timestamp,
            event_supplier=self._flatten_failed_launch_nodes(failed_nodes),
        )

    def _get_launch_failure_details(self, failed_nodes: Dict[str, List[str]]) -> Dict:
        detail_map = {"other-failures": {"count": 0}}
        for failure_type in _LAUNCH_FAILURE_GROUPING.values():
            detail_map.setdefault(failure_type, {"count": 0})

        total_failures = 0
        for error_code, nodes in failed_nodes.items():
            total_failures += len(nodes)
            failure_type = ClusterEventPublisher._get_failure_type_from_error_code(error_code)
            error_entry = detail_map.get(failure_type)
            error_entry.update(
                {
                    "count": error_entry.get("count") + len(nodes),
                    error_code: self._limit_list(list(nodes)),
                }
            )

        detail_map.update({"total": total_failures})

        yield {
            "detail": detail_map,
        }

    def _limit_list(self, source_list: List) -> List:
        return source_list[: self._max_list_size] if self._max_list_size else source_list

    def _generate_node_name_list(self, node_list):
        return [{"name": node.name} for node in self._limit_list(node_list)]

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

    def _node_count_in_replacement_supplier(self, nodes_in_replacement):
        yield {
            "detail": {
                "count": len(nodes_in_replacement),
                "nodes": [{"name": node_name} for node_name in self._limit_list(nodes_in_replacement)],
            }
        }

    def _node_in_replacement_supplier(self, nodes_in_replacement):
        for node in nodes_in_replacement:
            yield {
                "detail": {
                    "node": ClusterEventPublisher._describe_node(node),
                }
            }

    def _unhealthy_node_count_supplier(self, unhealthy_static_nodes):
        yield {
            "detail": {
                "count": len(unhealthy_static_nodes),
                "nodes": self._generate_node_name_list(unhealthy_static_nodes),
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
        yield {
            "detail": {
                "count": len(dynamic_nodes) + len(static_nodes) + len(other_nodes),
                "static-replacement-timeout-errors": {
                    "count": len(static_nodes),
                    "nodes": self._generate_node_name_list(static_nodes),
                },
                "dynamic-resume-timeout-errors": {
                    "count": len(static_nodes),
                    "nodes": self._generate_node_name_list(dynamic_nodes),
                },
                "other-bootstrap-errors": {
                    "count": len(other_nodes),
                    "nodes": self._generate_node_name_list(other_nodes),
                },
            }
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
        def map_event_level(event_level):
            return (
                _EVENT_TO_LOG_LEVEL_MAPPING.get(event_level, logging.NOTSET)
                if isinstance(event_level, str)
                else event_level
            )

        def emit_event(event_level, message, event_type, timestamp=None, event_supplier=None, **kwargs):
            log_level = map_event_level(event_level)
            if event_logger.isEnabledFor(log_level):
                event_level = logging.getLevelName(log_level)
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
                                "level": event_level,
                                "instance-id": instance_id,
                                "event-type": event_type,
                                "message": message,
                                "detail": {},
                            },
                            global_args,
                        )

                        event_logger.log(log_level, "%s", json.dumps(dict(event)))
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
