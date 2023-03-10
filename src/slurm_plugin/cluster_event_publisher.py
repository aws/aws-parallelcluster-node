import logging
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List

from slurm_plugin.common import log_exception
from slurm_plugin.slurm_resources import DynamicNode, SlurmNode, StaticNode

logger = logging.getLogger(__name__)

LAUNCH_FAILURE_GROUPING = {}
for failure in [*SlurmNode.EC2_ICE_ERROR_CODES, "LimitedInstanceCapacity"]:
    LAUNCH_FAILURE_GROUPING.update({failure: "ice-failures"})

for failure in ["VcpuLimitExceeded"]:
    LAUNCH_FAILURE_GROUPING.update({failure: "vcpu-limit-failures"})

for failure in ["VolumeLimitExceeded", "InsufficientVolumeCapacity"]:
    LAUNCH_FAILURE_GROUPING.update({failure: "volume-limit-failures"})

for failure in ["InvalidBlockDeviceMapping"]:
    LAUNCH_FAILURE_GROUPING.update({failure: "custom-ami-errors"})

for failure in ["UnauthorizedOperation", "AccessDeniedException"]:
    LAUNCH_FAILURE_GROUPING.update({failure: "iam-policy-errors"})


class ClusterEventPublisher:
    def __init__(self, event_publisher: Callable, max_list_size=100):
        self._publish_event = event_publisher
        self._max_list_size = max_list_size

    @property
    def publish_event(self):
        return self._publish_event

    @staticmethod
    def timestamp():
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

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
            "WARNING" if failed_nodes else "DEBUG",
            "Number of static nodes that failed replacement after node maintenance",
            event_type="node-launch-failure-count",
            timestamp=timestamp,
            event_supplier=self._get_launch_failure_details(failed_nodes),
        )

        self.publish_event(
            "DEBUG",
            "After node maintenance, node failed replacement",
            event_type="node-launch-failure",
            timestamp=timestamp,
            event_supplier=self._failed_node_supplier(unhealthy_static_nodes, failed_nodes),
        )

        self.publish_event(
            "DEGUG",
            "Number of static nodes failing scheduler health check",
            event_type="static-node-health-check-failure-count",
            timestamp=timestamp,
            event_supplier=self._unhealthy_node_count_supplier(unhealthy_static_nodes),
        )

        self.publish_event(
            "DEBUG",
            "Static node failing scheduler health check",
            event_type="static-node-health-check-failure",
            timestamp=timestamp,
            event_supplier=self._unhealthy_node_supplier(unhealthy_static_nodes),
        )

        self.publish_event(
            "DEBUG",
            "Number of instances terminated due to backing unhealthy static nodes",
            event_type="static-node-instance-terminate-count",
            timestamp=timestamp,
            event_supplier=self._terminated_instances_supplier(
                (node for node in unhealthy_static_nodes if node.instance)
            ),
        )

        self.publish_event(
            "DEBUG",
            "After node maintenance, nodes currently in replacement",
            event_type="static-nodes-in-replacement-count",
            timestamp=timestamp,
            event_supplier=self._node_count_in_replacement_supplier(nodes_in_replacement),
        )

        self.publish_event(
            "DEBUG",
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
            "WARNING",
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
            "WARNING",
            "Number of nodes without a valid backing instance",
            "invalid-backing-instance-count",
            timestamp=timestamp,
            event_supplier=detail_supplier(),
        )

    @log_exception(logger, "publish_bootstrap_failure_events", catch_exception=Exception, raise_on_error=False)
    def publish_bootstrap_failure_events(self, bootstrap_failure_nodes: List[SlurmNode]):
        def dynamic_node_filter(node: SlurmNode) -> bool:
            return isinstance(node, DynamicNode) and node.is_resume_failed() and node.is_nodeaddr_set()

        def static_node_filter(node: SlurmNode) -> bool:
            return isinstance(node, StaticNode) and node._is_replacement_timeout

        def protected_node_list(filter: Callable[[SlurmNode], bool]):
            return [node for node in bootstrap_failure_nodes if filter(node)]

        def protected_mode_error_count_supplier():
            dynamic_nodes = protected_node_list(dynamic_node_filter)
            static_nodes = protected_node_list(static_node_filter)
            if dynamic_nodes or static_nodes:
                yield {
                    "detail": {
                        "count": len(dynamic_nodes) + len(static_nodes),
                        "static-replacement-timeout-errors": {
                            "count": len(static_nodes),
                            "nodes": self._generate_node_name_list(static_nodes),
                        },
                        "dynamic-resume-timeout-errors": {
                            "count": len(static_nodes),
                            "nodes": self._generate_node_name_list(dynamic_nodes),
                        },
                    }
                }

        def bootstrap_failure_supplier():
            if bootstrap_failure_nodes:
                yield {
                    "detail": {
                        "count": len(bootstrap_failure_nodes),
                        "nodes": self._generate_node_name_list(bootstrap_failure_nodes),
                    }
                }

        timestamp = ClusterEventPublisher.timestamp()

        self.publish_event(
            "WARNING",
            "Number of nodes that failed to bootstrap",
            "protected-mode-error-count",
            timestamp=timestamp,
            event_supplier=protected_mode_error_count_supplier(),
        )

        self.publish_event(
            "DEBUG",
            "Number of nodes that failed to bootstrap",
            "bootstrap-failure-count",
            timestamp=timestamp,
            event_supplier=bootstrap_failure_supplier(),
        )

    # Slurm Resume Events
    @log_exception(logger, "publish_node_launch_events", catch_exception=Exception, raise_on_error=False)
    def publish_node_launch_events(self, failed_nodes: Dict[str, List[str]]):
        timestamp = ClusterEventPublisher.timestamp()
        self.publish_event(
            "WARNING" if failed_nodes else "DEBUG",
            "Number of nodes that failed to launch",
            event_type="node-launch-failure-count",
            timestamp=timestamp,
            event_supplier=self._get_launch_failure_details(failed_nodes),
        )

        self.publish_event(
            "DEBUG",
            "Setting failed node to DOWN state",
            event_type="node-launch-failure",
            timestamp=timestamp,
            event_supplier=self._flatten_failed_launch_nodes(failed_nodes),
        )

    def _get_launch_failure_details(self, failed_nodes: Dict[str, List[str]]) -> Dict:
        detail_map = {"other-failures": {"count": 0}}
        for failure_type in LAUNCH_FAILURE_GROUPING.values():
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
                    "node": _describe_node(node),
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
            yield {"detail": {"node": _describe_node(node)}}

    def _failed_node_supplier(self, unhealthy_static_nodes, failed_nodes):
        for error_code, failed_node_list in failed_nodes.items():
            for node in unhealthy_static_nodes:
                if node.name in failed_node_list:
                    yield {
                        "detail": {
                            "node": _describe_node(node),
                            "error-code": error_code,
                            "failure-type": ClusterEventPublisher._get_failure_type_from_error_code(error_code),
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
        return LAUNCH_FAILURE_GROUPING.get(error_code, "other-failures")


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
            "instance": _describe_instance(node.instance) if node.instance else None,
            "partitions": list(node.partitions),
            "queue-name": node.queue_name,
            "compute-resource": node.compute_resource_name,
        }
        if node
        else None
    )


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
