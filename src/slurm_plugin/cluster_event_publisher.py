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
    **{
        failure: "iam-policy-errors"
        for failure in [
            "UnauthorizedOperation",
        ]
    },
}

NODE_LAUNCH_FAILURE_COUNT = {
    "message": "Number of static nodes that failed to launch a backing instance after node maintenance",
    "event_type": "node-launch-failure-count",
}
NODE_LAUNCH_FAILURE = {
    "message": "After node maintenance, node failed to launch a backing instance",
    "event_type": "node-launch-failure",
}
STATIC_NODE_HEALTH_CHECK_FAILURE_COUNT = {
    "message": "Number of static nodes failing scheduler health check",
    "event_type": "static-node-health-check-failure-count",
}
STATIC_NODE_HEALTH_CHECK_FAILURE = {
    "message": "Static node failing scheduler health check",
    "event_type": "static-node-health-check-failure",
}
STATIC_NODE_INSTANCE_TERMINATE_COUNT = {
    "message": "Number of instances terminated due to backing unhealthy static nodes",
    "event_type": "static-node-instance-terminate-count",
}
STATIC_NODES_IN_REPLACEMENT_COUNT = {
    "message": "After node maintenance, nodes currently in replacement",
    "event_type": "static-nodes-in-replacement-count",
}
STATIC_NODE_LAUNCHED_COUNT = {
    "message": "Number of static nodes that successfully launched after node maintenance",
    "event_type": "static-node-launched-count",
}
NODES_FAILING_HEALTH_CHECK_COUNT = {
    "message": "Number of nodes failing health check",
    "event_type": "nodes-failing-health-check-count",
}
INVALID_BACKING_INSTANCE_COUNT = {
    "message": "Number of nodes without a valid backing instance",
    "event_type": "invalid-backing-instance-count",
}
NODE_NOT_RESPONDING_DOWN_COUNT = {
    "message": "Number of nodes set to DOWN because they are not responding",
    "event_type": "node-not-responding-down-count",
}
PROTECTED_MODE_ERROR_COUNT = {
    "message": "Number of nodes that failed to bootstrap",
    "event_type": "protected-mode-error-count",
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
        publisher = ClusterEventPublisher._get_event_publisher(
            event_logger, cluster_name, node_role, component, instance_id, **global_args
        )
        return ClusterEventPublisher(publisher, max_list_size)

    # Example event generated from this function:
    # {
    #     "datetime": "2023-03-09T23:52:11.975+00:00",
    #     "version": 0,
    #     "scheduler": "slurm",
    #     "cluster-name": "integ-tests-dw50g5diz12uvx3p",
    #     "node-role": "HeadNode",
    #     "component": "clustermgtd",
    #     "level": "WARNING",
    #     "instance-id": "i-019e782246a24d07e",
    #     "event-type": "node-launch-failure-count",
    #     "message": "Number of nodes that failed to launch",
    #     "detail": {
    #         "failure-type": "ice-failures",
    #         "count": 17,
    #         "error-details": {
    #             "InsufficientInstanceCapacity": {
    #                 "count": 3,
    #                 "nodes": [{"name": "ice-a-1"}, {"name": "ice-a-2"}, {"name": "ice-a-3"}],
    #             },
    #             "InsufficientHostCapacity": {
    #                 "count": 2,
    #                 "nodes": [{"name": "ice-b-1"}, {"name": "ice-b-2"}],
    #             },
    #             "LimitedInstanceCapacity": {
    #                 "count": 2,
    #                 "nodes": [{"name": "ice-g-1"}, {"name": "ice-g-2"}],
    #             },
    #             "InsufficientReservedInstanceCapacity": {
    #                 "count": 3,
    #                 "nodes": [{"name": "ice-c-1"}, {"name": "ice-c-2"}, {"name": "ice-c-3"}],
    #             },
    #             "MaxSpotInstanceCountExceeded": {
    #                 "count": 2,
    #                 "nodes": [{"name": "ice-d-1"}, {"name": "ice-d-2"}],
    #             },
    #             "Unsupported": {
    #                 "count": 3,
    #                 "nodes": [{"name": "ice-e-1"}, {"name": "ice-e-2"}, {"name": "ice-e-3"}],
    #             },
    #             "SpotMaxPriceTooLow": {
    #                 "count": 2,
    #                 "nodes": [{"name": "ice-f-1"}, {"name": "ice-f-2"}]
    #             },
    #         },
    #     }
    # }
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
                **NODE_LAUNCH_FAILURE_COUNT,
                timestamp=timestamp,
                detail=error_detail,
            )

        self.publish_event(
            logging.DEBUG,
            **NODE_LAUNCH_FAILURE,
            timestamp=timestamp,
            event_supplier=self._failed_node_supplier(unhealthy_static_nodes, failed_nodes),
        )

        self.publish_event(
            logging.DEBUG,
            **STATIC_NODE_HEALTH_CHECK_FAILURE_COUNT,
            timestamp=timestamp,
            event_supplier=self._node_list_and_count_supplier(unhealthy_static_nodes),
        )

        self.publish_event(
            logging.DEBUG,
            **STATIC_NODE_HEALTH_CHECK_FAILURE,
            timestamp=timestamp,
            event_supplier=self._unhealthy_node_supplier(unhealthy_static_nodes),
        )

        self.publish_event(
            logging.DEBUG,
            **STATIC_NODE_INSTANCE_TERMINATE_COUNT,
            timestamp=timestamp,
            event_supplier=self._terminated_instances_supplier(
                (node for node in unhealthy_static_nodes if node.instance)
            ),
        )

        self.publish_event(
            logging.DEBUG,
            **STATIC_NODES_IN_REPLACEMENT_COUNT,
            timestamp=timestamp,
            event_supplier=self._node_list_and_count_supplier(nodes_in_replacement),
        )

        self.publish_event(
            logging.DEBUG,
            **STATIC_NODE_LAUNCHED_COUNT,
            timestamp=timestamp,
            event_supplier=self._node_list_and_count_supplier(launched_nodes),
        )

    # Example event generated by this function:
    # {
    #     "datetime": "2023-03-09T23:52:11.975+00:00",
    #     "version": 0,
    #     "scheduler": "slurm",
    #     "cluster-name": "integ-tests-dw50g5diz12uvx3p",
    #     "node-role": "HeadNode",
    #     "component": "slurm-resume",
    #     "level": "WARNING",
    #     "instance-id": "i-019e782246a24d07e",
    #     "event-type": "nodes-failing-health-check-count",
    #     "message": "Number of nodes that failed to launch",
    #     "detail": {
    #         "health-check-type": "ec2_health_check",
    #         "count": 2,
    #         "nodes": [
    #             {
    #                 "name": "node-a-1"
    #             },
    #             {
    #                 "name": "node-a-2"
    #             }
    #         ],
    #     }
    # }
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
            **NODES_FAILING_HEALTH_CHECK_COUNT,
            timestamp=timestamp,
            event_supplier=detail_supplier(node_names_failing_health_check),
        )

    # Example event generated by this function:
    # {
    #     "datetime": "2023-03-15T23:54:57.464+00:00",
    #     "version": 0,
    #     "scheduler": "slurm",
    #     "cluster-name": "integ-tests-w8qf3l9pemazzlmw",
    #     "node-role": "HeadNode",
    #     "component": "clustermgtd",
    #     "level": "WARNING",
    #     "instance-id": "i-037686887b7dbc7be",
    #     "event-type": "node-not-responding-down-count",
    #     "message": "Number of nodes set to DOWN because they are not responding",
    #     "detail": {
    #         "count": 1,
    #         "nodes": [
    #             {
    #                 "name": "queue-2-st-compute-b-1"
    #             }
    #         ]
    #     }
    # }
    @log_exception(logger, "publish_unhealthy_node_events", catch_exception=Exception, raise_on_error=False)
    def publish_unhealthy_node_events(self, unhealthy_nodes: List[SlurmNode]):
        """Publish events for unhealthy nodes without a backing instance and for nodes that are not responding."""
        timestamp = ClusterEventPublisher.timestamp()

        nodes_with_invalid_backing_instance = [
            node for node in unhealthy_nodes if not node.is_backing_instance_valid(log_warn_if_unhealthy=False)
        ]
        self.publish_event(
            logging.WARNING if nodes_with_invalid_backing_instance else logging.DEBUG,
            **INVALID_BACKING_INSTANCE_COUNT,
            timestamp=timestamp,
            event_supplier=self._node_list_and_count_supplier(nodes_with_invalid_backing_instance),
        )

        nodes_not_responding = [node for node in unhealthy_nodes if node.is_down_not_responding()]
        self.publish_event(
            logging.WARNING if nodes_not_responding else logging.DEBUG,
            **NODE_NOT_RESPONDING_DOWN_COUNT,
            timestamp=timestamp,
            event_supplier=self._node_list_and_count_supplier(nodes_not_responding),
        )

    # Example event generated by this function:
    # {
    #     "datetime": "2023-03-09T23:21:54.541+00:00",
    #     "version": 0,
    #     "scheduler": "slurm",
    #     "cluster-name": "integ-tests-dw50g5diz12uvx3p",
    #     "node-role": "HeadNode",
    #     "component": "clustermgtd",
    #     "level": "WARNING",
    #     "instance-id": "i-019e782246a24d07e",
    #     "event-type": "protected-mode-error-count",
    #     "message": "Number of nodes that failed to bootstrap",
    #     "detail": {
    #         "failure-type": "static-replacement-timeout-error",
    #         "count": 1,
    #         "nodes": [{"name": "queue2-st-c5large-2"}],
    #     }
    # }
    @log_exception(logger, "publish_bootstrap_failure_events", catch_exception=Exception, raise_on_error=False)
    def publish_bootstrap_failure_events(self, bootstrap_failure_nodes: List[SlurmNode]):
        """Publish events for nodes failing to bootstrap."""
        timestamp = ClusterEventPublisher.timestamp()

        for count, detail in self._protected_mode_error_count_supplier(bootstrap_failure_nodes):
            self.publish_event(
                logging.WARNING if count else logging.DEBUG,
                **PROTECTED_MODE_ERROR_COUNT,
                timestamp=timestamp,
                detail=detail,
            )

    # Slurm Resume Events
    # Note: This uses the same schema as `publish_unhealthy_static_node_events` from clustermgtd
    # Example event generated by this function:
    # {
    #     "datetime": "2023-03-09T23:52:11.975+00:00",
    #     "version": 0,
    #     "scheduler": "slurm",
    #     "cluster-name": "integ-tests-dw50g5diz12uvx3p",
    #     "node-role": "HeadNode",
    #     "component": "slurm-resume",
    #     "level": "WARNING",
    #     "instance-id": "i-019e782246a24d07e",
    #     "event-type": "node-launch-failure-count",
    #     "message": "Number of nodes that failed to launch",
    #     "detail": {
    #         "failure-type": "vcpu-limit-failures",
    #         "count": 1,
    #         "error-details": {
    #             "VcpuLimitExceeded": {
    #                 "count": 1,
    #                 "nodes": [{"name": "vcpu-g-1"}]
    #             }
    #         },
    #     }
    # }
    @log_exception(logger, "publish_node_launch_events", catch_exception=Exception, raise_on_error=False)
    def publish_node_launch_events(self, failed_nodes: Dict[str, List[str]]):
        """Publish events for nodes that failed to launch from slurm_resume."""
        timestamp = ClusterEventPublisher.timestamp()

        for count, error_detail in self._generate_launch_failure_details(failed_nodes):
            self.publish_event(
                logging.WARNING if count else logging.DEBUG,
                **NODE_LAUNCH_FAILURE_COUNT,
                timestamp=timestamp,
                detail=error_detail,
            )

        self.publish_event(
            logging.DEBUG,
            **NODE_LAUNCH_FAILURE,
            timestamp=timestamp,
            event_supplier=self._flatten_failed_launch_nodes(failed_nodes),
        )

    def _generate_launch_failure_details(self, failed_nodes: Dict[str, List[str]]):
        """
        Build a dictionary based on failure category (e.g. ice-failure).

        The elements contain the number of nodes and the node names in that category.
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

    @staticmethod
    def _failed_node_supplier(unhealthy_static_nodes, failed_nodes):
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
    def _get_event_publisher(
        event_logger: any,
        cluster_name: str,
        node_role: str,
        component: str,
        instance_id: str,
        **global_args: Dict[str, any]
    ) -> Callable:
        """
        Return a function that will publish a structured event to `event_logger`.

        The passed in parameters, aside from `event_logger`, contribute to the properties of every generated event but
        can be overridden by the parameters passed to the inner function.

        The returned function supports conditional event generation through the `event_supplier` parameter. The idea is
        for the case where you need to iterate over a large list of elements (like a list of cluster nodes) or perform
        an expensive operation, then this method will only iterate over event_supplier if the `event_level` is loggable.
        By using a generator for `event_supplier', large lists or other expensive code can be deferred or skipped over
        altogether when no event will be logged for the given log level.

        This function also supports layering of event properties through the use of a ChainMap. This way, more specific
        property values can override the more general property values. The priority order from least to most is:
        global_args, named parameters from the outer function (`_get_event_publisher`), named parameters from the inner
        function (`callable_event_publisher`), `kwargs` from the inner function (`callable_event_publisher`), and
        finally, the specific per element values from `event_supplier`, if provided.
        """

        def callable_event_publisher(
            event_level: int,
            message: str,
            event_type: str,
            timestamp: str = None,
            event_supplier: Iterable = None,
            **kwargs
        ) -> None:
            if event_logger.isEnabledFor(event_level):
                now = timestamp if timestamp else datetime.now(timezone.utc).isoformat(timespec="milliseconds")
                default_properties = {
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
                }
                if not event_supplier:
                    event_supplier = [kwargs]
                for event_properties in event_supplier:
                    try:
                        event = ChainMap(
                            event_properties,
                            kwargs,
                            default_properties,
                            global_args,
                        )

                        event_logger.log(event_level, "%s", json.dumps(dict(event)))
                    except Exception as e:
                        logger.error("Failed to publish event: %s\n%s", e, traceback.format_exception(*sys.exc_info()))

        return callable_event_publisher

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
