from typing import Dict, List

import pytest
from assertpy import assert_that
from slurm_plugin.cluster_event_publisher import ClusterEventPublisher
from slurm_plugin.clustermgtd import ClusterManager
from slurm_plugin.fleet_manager import EC2Instance
from slurm_plugin.slurm_resources import StaticNode


def event_handler(received_events: List[Dict], level_filter: List[str] = None):
    def _handler(level, message, event_type, *args, detail=None, **kwargs):
        if not level_filter or level in level_filter:
            if detail:
                received_events.append({event_type: detail})
            event_supplier = kwargs.get("event_supplier", [])
            for event in event_supplier:
                received_events.append({event_type: event.get("detail", None)})

    return _handler


@pytest.mark.parametrize(
    "test_nodes, expected_details, level_filter, max_list_size",
    [
        (
            [
                StaticNode("queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"),
                StaticNode("queue-dy-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),
                StaticNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),
                StaticNode("queue1-dy-c4xlarge-1", "ip-1", "hostname", "DOWN", "queue1"),
                StaticNode(
                    "queue1-dy-c5xlarge-3",
                    "nodeip",
                    "nodehostname",
                    "COMPLETING+DRAIN",
                    "queue1",
                    "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-3",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:UnauthorizedOperation)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-4",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InvalidBlockDeviceMapping)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-5",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:AccessDeniedException)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-6",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:VcpuLimitExceeded)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-8",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:VolumeLimitExceeded)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-9",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientVolumeCapacity)Error",
                ),
            ],
            [
                {
                    "node-launch-failure-count": {
                        "other-failures": {"count": 0},
                        "ice-failures": {
                            "count": 3,
                            "InsufficientReservedInstanceCapacity": ["queue1-dy-c5xlarge-3"],
                            "InsufficientHostCapacity": ["queue2-dy-c5large-1", "queue2-dy-c5large-2"],
                        },
                        "vcpu-limit-failures": {"count": 1, "VcpuLimitExceeded": ["queue2-dy-c5large-6"]},
                        "volume-limit-failures": {
                            "count": 2,
                            "VolumeLimitExceeded": ["queue2-dy-c5large-8"],
                            "InsufficientVolumeCapacity": ["queue2-dy-c5large-9"],
                        },
                        "custom-ami-errors": {"count": 1, "InvalidBlockDeviceMapping": ["queue2-dy-c5large-4"]},
                        "iam-policy-errors": {
                            "count": 2,
                            "UnauthorizedOperation": ["queue2-dy-c5large-3"],
                            "AccessDeniedException": ["queue2-dy-c5large-5"],
                        },
                        "total": 9,
                    }
                }
            ],
            ["ERROR", "WARNING", "INFO"],
            None,
        ),
        (
            [
                StaticNode("queue1-dy-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"),
                StaticNode("queue-dy-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),
                StaticNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),
                StaticNode("queue1-dy-c4xlarge-1", "ip-1", "hostname", "DOWN", "queue1"),
                StaticNode(
                    "queue1-dy-c5xlarge-3",
                    "nodeip",
                    "nodehostname",
                    "COMPLETING+DRAIN",
                    "queue1",
                    "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-10",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-dy-c5large-11",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-dy-c5large-12",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-dy-c5large-3",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:UnauthorizedOperation)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-4",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InvalidBlockDeviceMapping)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-5",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:AccessDeniedException)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-6",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:VcpuLimitExceeded)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-8",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:VolumeLimitExceeded)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-9",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientVolumeCapacity)Error",
                ),
            ],
            [
                {
                    "node-launch-failure-count": {
                        "other-failures": {"count": 0},
                        "ice-failures": {
                            "count": 6,
                            "InsufficientReservedInstanceCapacity": ["queue1-dy-c5xlarge-3"],
                            "InsufficientHostCapacity": ["queue2-dy-c5large-1", "queue2-dy-c5large-2"],
                        },
                        "vcpu-limit-failures": {"count": 1, "VcpuLimitExceeded": ["queue2-dy-c5large-6"]},
                        "volume-limit-failures": {
                            "count": 2,
                            "VolumeLimitExceeded": ["queue2-dy-c5large-8"],
                            "InsufficientVolumeCapacity": ["queue2-dy-c5large-9"],
                        },
                        "custom-ami-errors": {"count": 1, "InvalidBlockDeviceMapping": ["queue2-dy-c5large-4"]},
                        "iam-policy-errors": {
                            "count": 2,
                            "UnauthorizedOperation": ["queue2-dy-c5large-3"],
                            "AccessDeniedException": ["queue2-dy-c5large-5"],
                        },
                        "total": 12,
                    }
                }
            ],
            ["ERROR", "WARNING", "INFO"],
            2,
        ),
        (
            [
                StaticNode(
                    "queue2-dy-c5large-9",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientVolumeCapacity)Error",
                ),
            ],
            [
                {
                    "node-launch-failure-count": {
                        "other-failures": {"count": 0},
                        "ice-failures": {"count": 0},
                        "vcpu-limit-failures": {"count": 0},
                        "volume-limit-failures": {"count": 1, "InsufficientVolumeCapacity": ["queue2-dy-c5large-9"]},
                        "custom-ami-errors": {"count": 0},
                        "iam-policy-errors": {"count": 0},
                        "total": 1,
                    }
                },
                {
                    "node-launch-failure": {
                        "node": {
                            "name": "queue2-dy-c5large-9",
                            "type": "static",
                            "address": "nodeip",
                            "hostname": "nodehostname",
                            "state-string": "DOWN+CLOUD",
                            "state": "DOWN",
                            "state-flags": ["CLOUD"],
                            "instance": {
                                "id": "i-id-0",
                                "private-ip": "1.2.3.0",
                                "hostname": "host-0",
                                "launch-time": "sometime",
                            },
                            "partitions": ["queue2"],
                            "queue-name": "queue2",
                            "compute-resource": "c5large",
                        },
                        "error-code": "InsufficientVolumeCapacity",
                        "failure-type": "volume-limit-failures",
                    }
                },
                {"static-node-health-check-failure-count": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-9"}]}},
                {
                    "static-node-health-check-failure": {
                        "node": {
                            "name": "queue2-dy-c5large-9",
                            "type": "static",
                            "address": "nodeip",
                            "hostname": "nodehostname",
                            "state-string": "DOWN+CLOUD",
                            "state": "DOWN",
                            "state-flags": ["CLOUD"],
                            "instance": {
                                "id": "i-id-0",
                                "private-ip": "1.2.3.0",
                                "hostname": "host-0",
                                "launch-time": "sometime",
                            },
                            "partitions": ["queue2"],
                            "queue-name": "queue2",
                            "compute-resource": "c5large",
                        }
                    }
                },
                {
                    "static-node-instance-terminate-count": {
                        "count": 1,
                        "nodes": [
                            {
                                "name": "queue2-dy-c5large-9",
                                "id": "i-id-0",
                                "ip": "1.2.3.0",
                                "error-code": "InsufficientVolumeCapacity",
                                "reason": "(Code:InsufficientVolumeCapacity)Error",
                            }
                        ],
                    }
                },
                {"static-nodes-in-replacement-count": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-9"}]}},
                {
                    "static-node-in-replacement": {
                        "node": {
                            "name": "queue2-dy-c5large-9",
                            "type": "static",
                            "address": "nodeip",
                            "hostname": "nodehostname",
                            "state-string": "DOWN+CLOUD",
                            "state": "DOWN",
                            "state-flags": ["CLOUD"],
                            "instance": {
                                "id": "i-id-0",
                                "private-ip": "1.2.3.0",
                                "hostname": "host-0",
                                "launch-time": "sometime",
                            },
                            "partitions": ["queue2"],
                            "queue-name": "queue2",
                            "compute-resource": "c5large",
                        }
                    }
                },
            ],
            [],
            2,
        ),
    ],
    ids=["default list limit", "list limit of 2", "debug output"],
)
def test_publish_unhealthy_static_node_events(test_nodes, expected_details, level_filter, max_list_size):
    received_events = []
    if max_list_size:
        event_publisher = ClusterEventPublisher(
            event_handler(received_events, level_filter=level_filter), max_list_size=max_list_size
        )
    else:
        event_publisher = ClusterEventPublisher(event_handler(received_events, level_filter=level_filter))

    instances = [
        EC2Instance(f"i-id-{instance_id}", f"1.2.3.{instance_id}", f"host-{instance_id}", "sometime")
        for instance_id in range(len(test_nodes))
    ]

    nodes_and_instances = zip(test_nodes, instances)

    for node, instance in nodes_and_instances:
        node.instance = instance

    # Make sure non-lists work
    nodes_in_replacement = (node.name for node in test_nodes)
    failed_nodes = {}
    for node in test_nodes:
        if node.error_code:
            failed_nodes.setdefault(node.error_code, []).append(node.name)

    # Run test
    event_publisher.publish_unhealthy_static_node_events(
        test_nodes,
        nodes_in_replacement,
        failed_nodes,
    )

    # Assert calls
    assert_that(received_events).is_length(len(expected_details))
    for received_event, expected_detail in zip(received_events, expected_details):
        assert_that(received_event).is_equal_to(expected_detail)


@pytest.mark.parametrize(
    "health_check_type, failed_nodes, expected_details, level_filter",
    [
        (
            ClusterManager.HealthCheckTypes.ec2_health,
            [
                "node-a-1",
                "node-a-2",
            ],
            [
                {
                    "nodes-failing-health-check-count": {
                        "health-check-type": "ec2_health_check",
                        "count": 2,
                        "nodes": [{"name": "node-a-1"}, {"name": "node-a-2"}],
                    }
                }
            ],
            ["ERROR", "WARNING", "INFO"],
        ),
    ],
)
def test_publish_nodes_failing_health_check_events(health_check_type, failed_nodes, expected_details, level_filter):
    received_events = []
    event_publisher = ClusterEventPublisher(event_handler(received_events, level_filter=level_filter))

    # Run test
    event_publisher.publish_nodes_failing_health_check_events(health_check_type, failed_nodes)

    # Assert calls
    assert_that(received_events).is_length(len(expected_details))
    for received_event, expected_detail in zip(received_events, expected_details):
        assert_that(received_event).is_equal_to(expected_detail)


@pytest.mark.parametrize(
    "failed_nodes, expected_details, level_filter",
    [
        (
            [
                (
                    StaticNode(
                        "queue2-dy-c5large-1",
                        "nodeip",
                        "nodehostname",
                        "DOWN+CLOUD",
                        "queue2",
                        "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                    ),
                    False,
                ),
                (
                    StaticNode(
                        "queue2-dy-c5large-2",
                        "nodeip",
                        "nodehostname",
                        "DOWN+CLOUD",
                        "queue2",
                        "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                    ),
                    True,
                ),
                (
                    StaticNode(
                        "queue2-dy-c5large-3",
                        "nodeip",
                        "nodehostname",
                        "DOWN+CLOUD",
                        "queue2",
                        "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                    ),
                    False,
                ),
            ],
            [
                {
                    "invalid-backing-instance-count": {
                        "count": 2,
                        "nodes": [{"name": "queue2-dy-c5large-1"}, {"name": "queue2-dy-c5large-3"}],
                    }
                }
            ],
            ["ERROR", "WARNING", "INFO"],
        ),
    ],
)
def test_publish_unhealthy_node_events(failed_nodes, expected_details, level_filter):
    received_events = []
    event_publisher = ClusterEventPublisher(event_handler(received_events, level_filter=level_filter))

    bad_nodes = []
    for node, bootstrap_failure in failed_nodes:
        node.is_static_nodes_in_replacement = bootstrap_failure
        bad_nodes.append(node)

    # Run test
    event_publisher.publish_unhealthy_node_events(bad_nodes)

    # Assert calls
    assert_that(received_events).is_length(len(expected_details))
    for received_event, expected_detail in zip(received_events, expected_details):
        assert_that(received_event).is_equal_to(expected_detail)


@pytest.mark.parametrize(
    "failed_nodes, expected_details, level_filter",
    [
        (
            [
                StaticNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-dy-c5large-3",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
            ],
            [
                {
                    "bootstrap-failure-count": {
                        "count": 3,
                        "nodes": [
                            {"name": "queue2-dy-c5large-1"},
                            {"name": "queue2-dy-c5large-2"},
                            {"name": "queue2-dy-c5large-3"},
                        ],
                    }
                }
            ],
            ["ERROR", "WARNING", "INFO"],
        ),
    ],
)
def test_publish_bootstrap_failure_events(failed_nodes, expected_details, level_filter):
    received_events = []
    event_publisher = ClusterEventPublisher(event_handler(received_events, level_filter=level_filter))

    # Run test
    event_publisher.publish_bootstrap_failure_events(failed_nodes)

    # Assert calls
    assert_that(received_events).is_length(len(expected_details))
    for received_event, expected_detail in zip(received_events, expected_details):
        assert_that(received_event).is_equal_to(expected_detail)


@pytest.mark.parametrize(
    "failed_nodes, expected_details, level_filter",
    [
        (
            {
                "Error1": [
                    "node-a-1",
                    "node-a-2",
                    "node-a-3",
                ],
                "Error2": [
                    "node-b-1",
                    "node-b-2",
                ],
                "InsufficientInstanceCapacity": [
                    "ice-a-1",
                    "ice-a-2",
                    "ice-a-3",
                ],
                "InsufficientHostCapacity": [
                    "ice-b-1",
                    "ice-b-2",
                ],
                "LimitedInstanceCapacity": [
                    "ice-g-1",
                    "ice-g-2",
                ],
                "InsufficientReservedInstanceCapacity": [
                    "ice-c-1",
                    "ice-c-2",
                    "ice-c-3",
                ],
                "MaxSpotInstanceCountExceeded": [
                    "ice-d-1",
                    "ice-d-2",
                ],
                "Unsupported": [
                    "ice-e-1",
                    "ice-e-2",
                    "ice-e-3",
                ],
                "SpotMaxPriceTooLow": [
                    "ice-f-1",
                    "ice-f-2",
                ],
                "VcpuLimitExceeded": [
                    "vcpu-g-1",
                ],
                "VolumeLimitExceeded": [
                    "vle-h-1",
                    "vle-h-2",
                ],
                "InsufficientVolumeCapacity": [
                    "ivc-i-1",
                    "ivc-i-2",
                    "ivc-i-3",
                ],
                "InvalidBlockDeviceMapping": [
                    "ibdm-j-1",
                    "ibdm-j-2",
                    "ibdm-j-3",
                ],
                "UnauthorizedOperation": [
                    "iam-k-1",
                    "iam-k-2",
                ],
                "AccessDeniedException": [
                    "iam-l-1",
                ],
            },
            [
                {
                    "node-launch-failure-count": {
                        "other-failures": {
                            "count": 5,
                            "Error1": ["node-a-1", "node-a-2", "node-a-3"],
                            "Error2": ["node-b-1", "node-b-2"],
                        },
                        "ice-failures": {
                            "count": 17,
                            "InsufficientInstanceCapacity": ["ice-a-1", "ice-a-2", "ice-a-3"],
                            "InsufficientHostCapacity": ["ice-b-1", "ice-b-2"],
                            "LimitedInstanceCapacity": ["ice-g-1", "ice-g-2"],
                            "InsufficientReservedInstanceCapacity": ["ice-c-1", "ice-c-2", "ice-c-3"],
                            "MaxSpotInstanceCountExceeded": ["ice-d-1", "ice-d-2"],
                            "Unsupported": ["ice-e-1", "ice-e-2", "ice-e-3"],
                            "SpotMaxPriceTooLow": ["ice-f-1", "ice-f-2"],
                        },
                        "vcpu-limit-failures": {"count": 1, "VcpuLimitExceeded": ["vcpu-g-1"]},
                        "volume-limit-failures": {
                            "count": 5,
                            "VolumeLimitExceeded": ["vle-h-1", "vle-h-2"],
                            "InsufficientVolumeCapacity": ["ivc-i-1", "ivc-i-2", "ivc-i-3"],
                        },
                        "custom-ami-errors": {
                            "count": 3,
                            "InvalidBlockDeviceMapping": ["ibdm-j-1", "ibdm-j-2", "ibdm-j-3"],
                        },
                        "iam-policy-errors": {
                            "count": 3,
                            "UnauthorizedOperation": ["iam-k-1", "iam-k-2"],
                            "AccessDeniedException": ["iam-l-1"],
                        },
                        "total": 34,
                    }
                }
            ],
            ["ERROR", "WARNING", "INFO"],
        ),
        (
            {},
            [],
            ["ERROR", "WARNING", "INFO"],
        ),
        (
            {
                "LimitedInstanceCapacity": [
                    "ice-g-1",
                ],
            },
            [
                {
                    "node-launch-failure-count": {
                        "other-failures": {"count": 0},
                        "ice-failures": {"count": 1, "LimitedInstanceCapacity": ["ice-g-1"]},
                        "vcpu-limit-failures": {"count": 0},
                        "volume-limit-failures": {"count": 0},
                        "custom-ami-errors": {"count": 0},
                        "iam-policy-errors": {"count": 0},
                        "total": 1,
                    }
                },
                {
                    "node-launch-failure": {
                        "error-code": "LimitedInstanceCapacity",
                        "failure-type": "ice-failures",
                        "node": {"name": "ice-g-1"},
                    }
                },
            ],
            [],
        ),
    ],
)
def test_publish_node_launch_events(failed_nodes, expected_details, level_filter):
    received_events = []
    event_publisher = ClusterEventPublisher(event_handler(received_events, level_filter=level_filter))

    # Run test
    event_publisher.publish_node_launch_events(failed_nodes)

    # Assert calls
    assert_that(received_events).is_length(len(expected_details))
    for received_event, expected_detail in zip(received_events, expected_details):
        assert_that(received_event).is_equal_to(expected_detail)
