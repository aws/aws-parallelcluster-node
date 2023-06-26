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
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Dict, List

import pytest
from assertpy import assert_that
from slurm_plugin.cluster_event_publisher import ClusterEventPublisher
from slurm_plugin.clustermgtd import ClusterManager
from slurm_plugin.fleet_manager import EC2Instance
from slurm_plugin.slurm_resources import DynamicNode, StaticNode


def event_handler(received_events: List[Dict], level_filter: List[str] = None):
    def _handler(level, message, event_type, *args, detail=None, **kwargs):
        level = level if isinstance(level, str) else logging.getLevelName(level)
        if not level_filter or level in level_filter:
            if detail:
                received_events.append({event_type: detail})
            event_supplier = kwargs.get("event_supplier", [])
            for event in event_supplier:
                received_events.append({event_type: event.get("detail", None)})

    return _handler


@pytest.mark.parametrize(
    "log_level, base_args, events, expected_events",
    [
        (
            logging.INFO,
            {"a-setting": "value-a"},
            [
                (
                    logging.INFO,
                    "info-message",
                    "info-event",
                    {
                        "datetime": "a-date-and-time",
                        "a-setting": "value-b",
                        "detail": {
                            "a-detail": "detail-a",
                        },
                    },
                )
            ],
            [
                {
                    "a-setting": "value-b",
                    "datetime": "a-date-and-time",
                    "version": 0,
                    "scheduler": "slurm",
                    "cluster-name": "cluster",
                    "node-role": "HeadNode",
                    "component": "component",
                    "level": "INFO",
                    "instance-id": "instance_id",
                    "event-type": "info-event",
                    "message": "info-message",
                    "detail": {"a-detail": "detail-a"},
                },
            ],
        ),
        (
            logging.INFO,
            {"a-setting": "value-a"},
            [
                (
                    logging.INFO,
                    "info-message",
                    "info-event",
                    {
                        "datetime": "a-date-and-time",
                        "a-setting": "value-b",
                        "detail": {
                            "a-detail": "detail-a",
                        },
                    },
                ),
                (
                    logging.DEBUG,
                    "debug-message",
                    "debug-event",
                    {
                        "datetime": "a-date-and-time",
                        "a-setting": "value-b",
                        "detail": {
                            "a-detail": "detail-a",
                        },
                    },
                ),
            ],
            [
                {
                    "a-setting": "value-b",
                    "datetime": "a-date-and-time",
                    "version": 0,
                    "scheduler": "slurm",
                    "cluster-name": "cluster",
                    "node-role": "HeadNode",
                    "component": "component",
                    "level": "INFO",
                    "instance-id": "instance_id",
                    "event-type": "info-event",
                    "message": "info-message",
                    "detail": {"a-detail": "detail-a"},
                },
            ],
        ),
        (
            logging.INFO,
            {"a-setting": "value-a", "b-setting": "global-b-setting"},
            [
                (
                    logging.INFO,
                    "info-message",
                    "info-event",
                    {
                        "datetime": "a-date-and-time",
                        "a-setting": "value-b",
                        "detail": {
                            "a-detail": "detail-a",
                        },
                    },
                )
            ],
            [
                {
                    "a-setting": "value-b",
                    "b-setting": "global-b-setting",
                    "datetime": "a-date-and-time",
                    "version": 0,
                    "scheduler": "slurm",
                    "cluster-name": "cluster",
                    "node-role": "HeadNode",
                    "component": "component",
                    "level": "INFO",
                    "instance-id": "instance_id",
                    "event-type": "info-event",
                    "message": "info-message",
                    "detail": {"a-detail": "detail-a"},
                },
            ],
        ),
        (
            logging.INFO,
            {"a-setting": "value-a"},
            [
                (
                    logging.INFO,
                    "info-message",
                    "info-event",
                    {
                        "datetime": "a-date-and-time",
                        "a-setting": "value-b",
                        "detail": {
                            "a-detail": "detail-a",
                        },
                    },
                ),
                (
                    logging.DEBUG,
                    "debug-message",
                    "debug-event",
                    {
                        "datetime": "a-date-and-time",
                        "a-setting": "value-b",
                        "detail": {
                            "a-detail": "detail-a",
                        },
                    },
                ),
                (
                    logging.WARNING,
                    "warning-message",
                    "warning-event",
                    {
                        "datetime": "a-date-and-time",
                        "detail": {
                            "a-detail": "detail-a",
                        },
                    },
                ),
            ],
            [
                {
                    "a-setting": "value-b",
                    "datetime": "a-date-and-time",
                    "version": 0,
                    "scheduler": "slurm",
                    "cluster-name": "cluster",
                    "node-role": "HeadNode",
                    "component": "component",
                    "level": "INFO",
                    "instance-id": "instance_id",
                    "event-type": "info-event",
                    "message": "info-message",
                    "detail": {"a-detail": "detail-a"},
                },
                {
                    "a-setting": "value-a",
                    "datetime": "a-date-and-time",
                    "version": 0,
                    "scheduler": "slurm",
                    "cluster-name": "cluster",
                    "node-role": "HeadNode",
                    "component": "component",
                    "level": "WARNING",
                    "instance-id": "instance_id",
                    "event-type": "warning-event",
                    "message": "warning-message",
                    "detail": {"a-detail": "detail-a"},
                },
            ],
        ),
    ],
    ids=[
        "event overrides global",
        "info level filters debug events",
        "global value is set when not overridden",
        "info level does not filter warning events",
    ],
)
def test_event_publisher(log_level, base_args, events, expected_events):
    received_events = []

    def log_handler(level, format_string, value):
        received_events.append([level, value])

    metric_logger = SimpleNamespace()
    metric_logger.isEnabledFor = lambda level: level >= log_level
    metric_logger.log = log_handler

    publisher = ClusterEventPublisher.create_with_default_publisher(
        metric_logger, "cluster", "HeadNode", "component", "instance_id", **base_args
    )

    # Run test
    for event in events:
        publisher.publish_event(event[0], event[1], event[2], **event[3])

    # Assert calls
    assert_that(received_events).is_length(len(expected_events))
    for actual, expected in zip(received_events, expected_events):
        assert_that(actual[0]).is_greater_than_or_equal_to(log_level)
        actual_json = json.loads(actual[1])
        assert_that(actual_json).is_equal_to(expected)


@pytest.mark.parametrize(
    "log_level, events, expected_events, expected_supplied_count",
    [
        (
            logging.INFO,
            [
                (
                    logging.INFO,
                    "info-message",
                    "info-event",
                    [
                        {
                            "datetime": "a-date-and-time",
                            "a_setting": "value-b",
                            "detail": {
                                "a-detail": "detail-a",
                            },
                        },
                    ],
                ),
            ],
            [
                {
                    "datetime": "a-date-and-time",
                    "version": 0,
                    "scheduler": "slurm",
                    "cluster-name": "cluster",
                    "node-role": "HeadNode",
                    "component": "component",
                    "level": "INFO",
                    "instance-id": "instance_id",
                    "event-type": "info-event",
                    "message": "info-message",
                    "detail": {"a-detail": "detail-a"},
                    "a_setting": "value-b",
                    "b_setting": "b-kwargs-setting",
                },
            ],
            1,
        ),
        (
            logging.INFO,
            [
                (
                    logging.INFO,
                    "info-message",
                    "info-event",
                    [
                        {
                            "datetime": "a-date-and-time",
                            "a_setting": "value-b",
                            "detail": {
                                "a-detail": "detail-a",
                            },
                        },
                        {
                            "datetime": "a-date-and-time",
                            "a_setting": "value-b",
                            "detail": {
                                "a-detail": "detail-a",
                            },
                        },
                    ],
                ),
            ],
            [
                {
                    "datetime": "a-date-and-time",
                    "version": 0,
                    "scheduler": "slurm",
                    "cluster-name": "cluster",
                    "node-role": "HeadNode",
                    "component": "component",
                    "level": "INFO",
                    "instance-id": "instance_id",
                    "event-type": "info-event",
                    "message": "info-message",
                    "detail": {"a-detail": "detail-a"},
                    "a_setting": "value-b",
                    "b_setting": "b-kwargs-setting",
                },
                {
                    "datetime": "a-date-and-time",
                    "version": 0,
                    "scheduler": "slurm",
                    "cluster-name": "cluster",
                    "node-role": "HeadNode",
                    "component": "component",
                    "level": "INFO",
                    "instance-id": "instance_id",
                    "event-type": "info-event",
                    "message": "info-message",
                    "detail": {"a-detail": "detail-a"},
                    "a_setting": "value-b",
                    "b_setting": "b-kwargs-setting",
                },
            ],
            2,
        ),
        (
            logging.INFO,
            [
                (
                    logging.INFO,
                    "info-message",
                    "info-event",
                    [
                        {
                            "datetime": "a-date-and-time",
                            "a_setting": "value-b",
                            "detail": {
                                "a-detail": "detail-a",
                            },
                        },
                        {
                            "datetime": "a-date-and-time",
                            "a_setting": "value-b",
                            "detail": {
                                "a-detail": "detail-a",
                            },
                        },
                    ],
                ),
                (
                    logging.DEBUG,
                    "debug-message",
                    "debug-event",
                    [
                        {
                            "datetime": "a-date-and-time",
                            "a_setting": "debug-value-b",
                            "detail": {
                                "a-detail": "detail-a",
                            },
                        },
                        {
                            "datetime": "a-date-and-time",
                            "a_setting": "debug-value-b",
                            "detail": {
                                "a-detail": "detail-a",
                            },
                        },
                    ],
                ),
            ],
            [
                {
                    "datetime": "a-date-and-time",
                    "version": 0,
                    "scheduler": "slurm",
                    "cluster-name": "cluster",
                    "node-role": "HeadNode",
                    "component": "component",
                    "level": "INFO",
                    "instance-id": "instance_id",
                    "event-type": "info-event",
                    "message": "info-message",
                    "detail": {"a-detail": "detail-a"},
                    "a_setting": "value-b",
                    "b_setting": "b-kwargs-setting",
                },
                {
                    "datetime": "a-date-and-time",
                    "version": 0,
                    "scheduler": "slurm",
                    "cluster-name": "cluster",
                    "node-role": "HeadNode",
                    "component": "component",
                    "level": "INFO",
                    "instance-id": "instance_id",
                    "event-type": "info-event",
                    "message": "info-message",
                    "detail": {"a-detail": "detail-a"},
                    "a_setting": "value-b",
                    "b_setting": "b-kwargs-setting",
                },
            ],
            2,
        ),
        (
            logging.WARNING,
            [
                (
                    logging.INFO,
                    "info-message",
                    "info-event",
                    [
                        {
                            "datetime": "a-date-and-time",
                            "a_setting": "value-b",
                            "detail": {
                                "a-detail": "detail-a",
                            },
                        },
                        {
                            "datetime": "a-date-and-time",
                            "a_setting": "value-b",
                            "detail": {
                                "a-detail": "detail-a",
                            },
                        },
                    ],
                ),
                (
                    logging.DEBUG,
                    "debug-message",
                    "debug-event",
                    [
                        {
                            "datetime": "a-date-and-time",
                            "a_setting": "debug-value-b",
                            "detail": {
                                "a-detail": "detail-a",
                            },
                        },
                        {
                            "datetime": "a-date-and-time",
                            "a_setting": "debug-value-b",
                            "detail": {
                                "a-detail": "detail-a",
                            },
                        },
                    ],
                ),
            ],
            [],
            0,
        ),
    ],
    ids=[
        "1 supplied event generates 1 log message",
        "2 supplied events generates 2 log messages",
        "info level does not call debug level suppliers",
        "warning level does not call info/debug supplier",
    ],
)
def test_event_publisher_with_event_supplier(log_level, events, expected_events, expected_supplied_count):
    received_events = []
    events_supplied = 0

    def log_handler(level, format_string, value):
        received_events.append(value)

    def event_supplier(events_to_supply):
        nonlocal events_supplied
        for supplied_event in events_to_supply:
            yield supplied_event
            events_supplied += 1

    metric_logger = SimpleNamespace()
    metric_logger.isEnabledFor = lambda level: level >= log_level
    metric_logger.log = log_handler

    publisher = ClusterEventPublisher.create_with_default_publisher(
        metric_logger, "cluster", "HeadNode", "component", "instance_id"
    )

    for event in events:
        publisher.publish_event(
            event[0],
            event[1],
            event[2],
            event_supplier=event_supplier(event[3]),
            a_setting="a-kwargs-setting",
            b_setting="b-kwargs-setting",
        )

    assert_that(events_supplied).is_equal_to(expected_supplied_count)

    assert_that(received_events).is_length(len(expected_events))
    for actual, expected in zip(received_events, expected_events):
        actual_json = json.loads(actual)
        assert_that(actual_json).is_equal_to(expected)


def test_event_publisher_swallows_exceptions(caplog):
    handler_called = False

    def log_handler(level, format_string, value):
        nonlocal handler_called
        handler_called = True
        raise Exception("hello")

    metric_logger = SimpleNamespace()
    metric_logger.isEnabledFor = lambda level: True
    metric_logger.log = log_handler

    publisher = ClusterEventPublisher.create_with_default_publisher(
        metric_logger, "cluster", "HeadNode", "component", "instance_id"
    )

    publisher.publish_event(logging.INFO, "hello", "event-type", detail={"hello": "goodbye"})

    assert_that(handler_called).is_true()

    assert_that(caplog.records).is_length(1)


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
                        "failure-type": "other-failures",
                        "count": 2,
                        "error-details": {
                            "InvalidBlockDeviceMapping": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-4"}]},
                            "AccessDeniedException": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-5"}]},
                        },
                    }
                },
                {
                    "node-launch-failure-count": {
                        "failure-type": "ice-failures",
                        "count": 3,
                        "error-details": {
                            "InsufficientReservedInstanceCapacity": {
                                "count": 1,
                                "nodes": [{"name": "queue1-dy-c5xlarge-3"}],
                            },
                            "InsufficientHostCapacity": {
                                "count": 2,
                                "nodes": [{"name": "queue2-dy-c5large-1"}, {"name": "queue2-dy-c5large-2"}],
                            },
                        },
                    }
                },
                {
                    "node-launch-failure-count": {
                        "failure-type": "vcpu-limit-failures",
                        "count": 1,
                        "error-details": {
                            "VcpuLimitExceeded": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-6"}]}
                        },
                    }
                },
                {
                    "node-launch-failure-count": {
                        "failure-type": "volume-limit-failures",
                        "count": 2,
                        "error-details": {
                            "VolumeLimitExceeded": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-8"}]},
                            "InsufficientVolumeCapacity": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-9"}]},
                        },
                    }
                },
                {
                    "node-launch-failure-count": {
                        "failure-type": "iam-policy-errors",
                        "count": 1,
                        "error-details": {
                            "UnauthorizedOperation": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-3"}]}
                        },
                    }
                },
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
            ],
            [
                {
                    "node-launch-failure-count": {
                        "failure-type": "ice-failures",
                        "count": 6,
                        "error-details": {
                            "InsufficientReservedInstanceCapacity": {
                                "count": 1,
                                "nodes": [{"name": "queue1-dy-c5xlarge-3"}],
                            },
                            "InsufficientHostCapacity": {
                                "count": 5,
                                "nodes": [{"name": "queue2-dy-c5large-1"}, {"name": "queue2-dy-c5large-2"}],
                            },
                        },
                    }
                }
            ],
            ["ERROR", "WARNING", "INFO"],
            2,
        ),
        (
            [
                StaticNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientVolumeCapacity)Error",
                ),
                StaticNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                ),
            ],
            [
                {"node-launch-failure-count": {"failure-type": "other-failures", "count": 0, "error-details": {}}},
                {"node-launch-failure-count": {"failure-type": "ice-failures", "count": 0, "error-details": {}}},
                {"node-launch-failure-count": {"failure-type": "vcpu-limit-failures", "count": 0, "error-details": {}}},
                {
                    "node-launch-failure-count": {
                        "failure-type": "volume-limit-failures",
                        "count": 1,
                        "error-details": {
                            "InsufficientVolumeCapacity": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-1"}]}
                        },
                    }
                },
                {"node-launch-failure-count": {"failure-type": "iam-policy-errors", "count": 0, "error-details": {}}},
                {
                    "node-launch-failure": {
                        "node": {
                            "name": "queue2-dy-c5large-1",
                            "type": "static",
                            "address": "nodeip",
                            "hostname": "nodehostname",
                            "state-string": "DOWN+CLOUD",
                            "state-reason": "(Code:InsufficientVolumeCapacity)Error",
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
                            "last-busy-time": None,
                            "slurm-started-time": None,
                        },
                        "error-code": "InsufficientVolumeCapacity",
                        "failure-type": "volume-limit-failures",
                    }
                },
                {
                    "static-node-health-check-failure-count": {
                        "count": 2,
                        "nodes": [{"name": "queue2-dy-c5large-1"}, {"name": "queue2-dy-c5large-2"}],
                    }
                },
                {
                    "static-node-health-check-failure": {
                        "node": {
                            "name": "queue2-dy-c5large-1",
                            "type": "static",
                            "address": "nodeip",
                            "hostname": "nodehostname",
                            "state-string": "DOWN+CLOUD",
                            "state-reason": "(Code:InsufficientVolumeCapacity)Error",
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
                            "last-busy-time": None,
                            "slurm-started-time": None,
                        }
                    }
                },
                {
                    "static-node-health-check-failure": {
                        "node": {
                            "name": "queue2-dy-c5large-2",
                            "type": "static",
                            "address": "nodeip",
                            "hostname": "nodehostname",
                            "state-string": "DOWN+CLOUD",
                            "state-reason": None,
                            "state": "DOWN",
                            "state-flags": ["CLOUD"],
                            "instance": {
                                "id": "i-id-1",
                                "private-ip": "1.2.3.1",
                                "hostname": "host-1",
                                "launch-time": "sometime",
                            },
                            "partitions": ["queue2"],
                            "queue-name": "queue2",
                            "compute-resource": "c5large",
                            "last-busy-time": None,
                            "slurm-started-time": None,
                        }
                    }
                },
                {
                    "static-node-instance-terminate-count": {
                        "count": 2,
                        "nodes": [
                            {
                                "name": "queue2-dy-c5large-1",
                                "id": "i-id-0",
                                "ip": "1.2.3.0",
                                "error-code": "InsufficientVolumeCapacity",
                                "reason": "(Code:InsufficientVolumeCapacity)Error",
                            },
                            {
                                "name": "queue2-dy-c5large-2",
                                "id": "i-id-1",
                                "ip": "1.2.3.1",
                                "error-code": None,
                                "reason": None,
                            },
                        ],
                    }
                },
                {
                    "static-nodes-in-replacement-count": {
                        "count": 2,
                        "nodes": [{"name": "queue2-dy-c5large-1"}, {"name": "queue2-dy-c5large-2"}],
                    }
                },
                {"static-node-launched-count": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-2"}]}},
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
    launched_nodes = []
    for node in test_nodes:
        if node.error_code:
            failed_nodes.setdefault(node.error_code, []).append(node.name)
        else:
            launched_nodes.append(node.name)

    # Run test
    event_publisher.publish_unhealthy_static_node_events(
        test_nodes,
        nodes_in_replacement,
        launched_nodes,
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
                        "failure-type": "ec2_health_check",
                        "count": 2,
                        "nodes": [{"name": "node-a-1"}, {"name": "node-a-2"}],
                    }
                }
            ],
            ["ERROR", "WARNING", "INFO"],
        ),
        (
            ClusterManager.HealthCheckTypes.ec2_health,
            [],
            [],
            ["ERROR", "WARNING", "INFO"],
        ),
        (
            ClusterManager.HealthCheckTypes.ec2_health,
            [],
            [{"nodes-failing-health-check-count": {"failure-type": "ec2_health_check", "count": 0, "nodes": []}}],
            [],
        ),
    ],
    ids=[
        "health-check-failures",
        "no-failures",
        "no-failures-debug",
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
                (
                    StaticNode(
                        "queue2-dy-c5large-4",
                        "nodeip",
                        "nodehostname",
                        "DOWN+CLOUD+NOT_RESPONDING",
                        "queue2",
                        "Not responding [slurm@2023-03-15T22:18:00]",
                    ),
                    False,
                ),
                (
                    StaticNode(
                        "queue2-dy-c5large-5",
                        "nodeip",
                        "nodehostname",
                        "DOWN+CLOUD+NOT_RESPONDING",
                        "queue2",
                        "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                    ),
                    False,
                ),
                (
                    StaticNode(
                        "queue2-dy-c5large-6",
                        "nodeip",
                        "nodehostname",
                        "DOWN+CLOUD",
                        "queue2",
                        "Not responding [slurm@2023-03-15T22:18:00]",
                    ),
                    False,
                ),
            ],
            [
                {"invalid-backing-instance-count": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-2"}]}},
                {"node-not-responding-down-count": {"count": 1, "nodes": [{"name": "queue2-dy-c5large-4"}]}},
            ],
            ["ERROR", "WARNING", "INFO"],
        ),
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
                    False,
                ),
                (
                    StaticNode(
                        "queue2-dy-c5large-3",
                        "",
                        "nodehostname",
                        "DOWN+CLOUD",
                        "queue2",
                        "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                    ),
                    False,
                ),
            ],
            [],
            ["ERROR", "WARNING", "INFO"],
        ),
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
                    False,
                ),
                (
                    StaticNode(
                        "queue2-dy-c5large-3",
                        "",
                        "nodehostname",
                        "DOWN+CLOUD",
                        "queue2",
                        "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                    ),
                    False,
                ),
            ],
            [
                {"invalid-backing-instance-count": {"count": 0, "nodes": []}},
                {"node-not-responding-down-count": {"count": 0, "nodes": []}},
                {
                    "unhealthy-node": {
                        "node": {
                            "name": "queue2-dy-c5large-1",
                            "type": "static",
                            "address": "queue2-dy-c5large-1",
                            "hostname": "nodehostname",
                            "state-string": "DOWN+CLOUD",
                            "state-reason": "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                            "state": "DOWN",
                            "state-flags": ["CLOUD"],
                            "instance": None,
                            "partitions": ["queue2"],
                            "queue-name": "queue2",
                            "compute-resource": "c5large",
                            "last-busy-time": None,
                            "slurm-started-time": None,
                        }
                    }
                },
                {
                    "unhealthy-node": {
                        "node": {
                            "name": "queue2-dy-c5large-2",
                            "type": "static",
                            "address": "queue2-dy-c5large-2",
                            "hostname": "nodehostname",
                            "state-string": "DOWN+CLOUD",
                            "state-reason": "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                            "state": "DOWN",
                            "state-flags": ["CLOUD"],
                            "instance": None,
                            "partitions": ["queue2"],
                            "queue-name": "queue2",
                            "compute-resource": "c5large",
                            "last-busy-time": None,
                            "slurm-started-time": None,
                        }
                    }
                },
                {
                    "unhealthy-node": {
                        "node": {
                            "name": "queue2-dy-c5large-3",
                            "type": "static",
                            "address": "queue2-dy-c5large-3",
                            "hostname": "nodehostname",
                            "state-string": "DOWN+CLOUD",
                            "state-reason": "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                            "state": "DOWN",
                            "state-flags": ["CLOUD"],
                            "instance": None,
                            "partitions": ["queue2"],
                            "queue-name": "queue2",
                            "compute-resource": "c5large",
                            "last-busy-time": None,
                            "slurm-started-time": None,
                        }
                    }
                },
            ],
            [],
        ),
    ],
    ids=["has invalid backing instances", "no invalid backing instances", "debug output"],
)
def test_publish_unhealthy_node_events(failed_nodes, expected_details, level_filter):
    received_events = []
    event_publisher = ClusterEventPublisher(event_handler(received_events, level_filter=level_filter))

    bad_nodes = []
    for node, invalid_backing_instance in failed_nodes:
        if not invalid_backing_instance:
            node.nodeaddr = node.name
        bad_nodes.append(node)

    # Run test
    event_publisher.publish_unhealthy_node_events(bad_nodes)

    # Assert calls
    assert_that(received_events).is_length(len(expected_details))
    for received_event, expected_detail in zip(received_events, expected_details):
        assert_that(received_event).is_equal_to(expected_detail)


@pytest.mark.parametrize(
    "failed_nodes, replacement_timeouts, expected_details, level_filter",
    [
        (
            [
                DynamicNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD+POWERED_DOWN+NOT_RESPONDING",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-st-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-st-c5large-3",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                DynamicNode(
                    "queue2-dy-c5large-4",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
            ],
            [True, True, False, False],
            [
                {
                    "protected-mode-error-count": {
                        "failure-type": "static-replacement-timeout-error",
                        "count": 1,
                        "nodes": [{"name": "queue2-st-c5large-2"}],
                    }
                },
                {
                    "protected-mode-error-count": {
                        "failure-type": "dynamic-resume-timeout-error",
                        "count": 1,
                        "nodes": [{"name": "queue2-dy-c5large-1"}],
                    }
                },
                {
                    "protected-mode-error-count": {
                        "failure-type": "other-bootstrap-error",
                        "count": 2,
                        "nodes": [{"name": "queue2-st-c5large-3"}, {"name": "queue2-dy-c5large-4"}],
                    }
                },
            ],
            ["ERROR", "WARNING", "INFO"],
        ),
        (
            [
                DynamicNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-st-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                StaticNode(
                    "queue2-st-c5large-3",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                DynamicNode(
                    "queue2-dy-c5large-4",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
            ],
            [False, False, False, False],
            [
                {
                    "protected-mode-error-count": {
                        "failure-type": "other-bootstrap-error",
                        "count": 4,
                        "nodes": [
                            {"name": "queue2-dy-c5large-1"},
                            {"name": "queue2-st-c5large-2"},
                            {"name": "queue2-st-c5large-3"},
                            {"name": "queue2-dy-c5large-4"},
                        ],
                    }
                }
            ],
            ["ERROR", "WARNING", "INFO"],
        ),
        (
            [],
            [],
            [],
            ["ERROR", "WARNING", "INFO"],
        ),
        (
            [],
            [],
            [],
            [
                {
                    "protected-mode-error-count": {
                        "failure-type": "static-replacement-timeout-error",
                        "count": 0,
                        "nodes": [],
                    }
                },
                {
                    "protected-mode-error-count": {
                        "failure-type": "dynamic-resume-timeout-error",
                        "count": 0,
                        "nodes": [],
                    }
                },
                {"protected-mode-error-count": {"failure-type": "other-bootstrap-error", "count": 0, "nodes": []}},
            ],
        ),
    ],
    ids=["With protected mode errors", "No protected mode errors", "No Errors", "No Errors debug output"],
)
def test_publish_bootstrap_failure_events(failed_nodes, replacement_timeouts, expected_details, level_filter):
    received_events = []
    event_publisher = ClusterEventPublisher(event_handler(received_events, level_filter=level_filter))

    def define_bootstrap_timeout(is_failure):
        return lambda *args: is_failure

    for node, is_timeout in zip(failed_nodes, replacement_timeouts):
        node.is_bootstrap_timeout = define_bootstrap_timeout(is_timeout)

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
                        "failure-type": "other-failures",
                        "count": 9,
                        "error-details": {
                            "Error1": {
                                "count": 3,
                                "nodes": [{"name": "node-a-1"}, {"name": "node-a-2"}, {"name": "node-a-3"}],
                            },
                            "Error2": {"count": 2, "nodes": [{"name": "node-b-1"}, {"name": "node-b-2"}]},
                            "InvalidBlockDeviceMapping": {
                                "count": 3,
                                "nodes": [{"name": "ibdm-j-1"}, {"name": "ibdm-j-2"}, {"name": "ibdm-j-3"}],
                            },
                            "AccessDeniedException": {"count": 1, "nodes": [{"name": "iam-l-1"}]},
                        },
                    }
                },
                {
                    "node-launch-failure-count": {
                        "failure-type": "ice-failures",
                        "count": 17,
                        "error-details": {
                            "InsufficientInstanceCapacity": {
                                "count": 3,
                                "nodes": [{"name": "ice-a-1"}, {"name": "ice-a-2"}, {"name": "ice-a-3"}],
                            },
                            "InsufficientHostCapacity": {
                                "count": 2,
                                "nodes": [{"name": "ice-b-1"}, {"name": "ice-b-2"}],
                            },
                            "LimitedInstanceCapacity": {
                                "count": 2,
                                "nodes": [{"name": "ice-g-1"}, {"name": "ice-g-2"}],
                            },
                            "InsufficientReservedInstanceCapacity": {
                                "count": 3,
                                "nodes": [{"name": "ice-c-1"}, {"name": "ice-c-2"}, {"name": "ice-c-3"}],
                            },
                            "MaxSpotInstanceCountExceeded": {
                                "count": 2,
                                "nodes": [{"name": "ice-d-1"}, {"name": "ice-d-2"}],
                            },
                            "Unsupported": {
                                "count": 3,
                                "nodes": [{"name": "ice-e-1"}, {"name": "ice-e-2"}, {"name": "ice-e-3"}],
                            },
                            "SpotMaxPriceTooLow": {"count": 2, "nodes": [{"name": "ice-f-1"}, {"name": "ice-f-2"}]},
                        },
                    }
                },
                {
                    "node-launch-failure-count": {
                        "failure-type": "vcpu-limit-failures",
                        "count": 1,
                        "error-details": {"VcpuLimitExceeded": {"count": 1, "nodes": [{"name": "vcpu-g-1"}]}},
                    }
                },
                {
                    "node-launch-failure-count": {
                        "failure-type": "volume-limit-failures",
                        "count": 5,
                        "error-details": {
                            "VolumeLimitExceeded": {"count": 2, "nodes": [{"name": "vle-h-1"}, {"name": "vle-h-2"}]},
                            "InsufficientVolumeCapacity": {
                                "count": 3,
                                "nodes": [{"name": "ivc-i-1"}, {"name": "ivc-i-2"}, {"name": "ivc-i-3"}],
                            },
                        },
                    }
                },
                {
                    "node-launch-failure-count": {
                        "failure-type": "iam-policy-errors",
                        "count": 2,
                        "error-details": {
                            "UnauthorizedOperation": {"count": 2, "nodes": [{"name": "iam-k-1"}, {"name": "iam-k-2"}]}
                        },
                    }
                },
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
                {"node-launch-failure-count": {"failure-type": "other-failures", "count": 0, "error-details": {}}},
                {
                    "node-launch-failure-count": {
                        "failure-type": "ice-failures",
                        "count": 1,
                        "error-details": {"LimitedInstanceCapacity": {"count": 1, "nodes": [{"name": "ice-g-1"}]}},
                    }
                },
                {"node-launch-failure-count": {"failure-type": "vcpu-limit-failures", "count": 0, "error-details": {}}},
                {
                    "node-launch-failure-count": {
                        "failure-type": "volume-limit-failures",
                        "count": 0,
                        "error-details": {},
                    }
                },
                {"node-launch-failure-count": {"failure-type": "iam-policy-errors", "count": 0, "error-details": {}}},
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
    ids=[
        "all-errors",
        "no-errors",
        "debug-level",
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


@pytest.mark.parametrize(
    "compute_nodes, expected_details, level_filter, max_list_size",
    [
        (
            [
                StaticNode("queue1-st-c5xlarge-2", "ip-2", "hostname", "IDLE+CLOUD+POWERING_DOWN", "queue1"),
                StaticNode("queue-st-c5xlarge-1", "ip-3", "hostname", "IDLE+CLOUD", "queue"),
                DynamicNode(
                    "queue1-dy-c5xlarge-1", "ip-1", "hostname", "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "queue1"
                ),
                StaticNode("queue1-st-c4xlarge-1", "ip-1", "hostname", "DOWN", "queue1"),
                DynamicNode(
                    "queue1-dy-c5xlarge-3",
                    "nodeip",
                    "nodehostname",
                    "MIXED+CLOUD+POWERING_UP",
                    "queue1",
                    "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes",
                    instance=EC2Instance(
                        id="id-1", private_ip="ip-1", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                DynamicNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "MIXED+CLOUD+POWERING_DOWN",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=12, minute=23, second=14, tzinfo=timezone.utc
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-3",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:UnauthorizedOperation)Error",
                    instance=EC2Instance(
                        id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-4",
                    "nodeip",
                    "nodehostname",
                    "MIXED+CLOUD+POWERED_UP",
                    "queue2",
                    "(Code:InvalidBlockDeviceMapping)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=21, minute=23, second=14, tzinfo=timezone.utc
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-5",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:AccessDeniedException)Error",
                ),
                StaticNode(
                    "queue2-st-c5large-6",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERED_UP",
                    "queue2",
                    "(Code:VcpuLimitExceeded)Error",
                ),
                StaticNode(
                    "queue2-st-c5large-8",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:VolumeLimitExceeded)Error",
                ),
                DynamicNode(
                    "queue2-dy-c5large-9",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD+NOT_RESPONDING",
                    "queue2",
                    "(Code:InsufficientVolumeCapacity)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=11, hour=11, minute=23, second=14, tzinfo=timezone.utc
                    ),
                ),
            ],
            [],
            ["ERROR", "WARNING", "INFO"],
            None,
        ),
        (
            [
                StaticNode(
                    "queue1-st-c5xlarge-2",
                    "ip-2",
                    "hostname",
                    "IDLE+CLOUD+POWERING_DOWN",
                    "queue1",
                    instance=EC2Instance(
                        id="id-1", private_ip="ip-1", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue-st-c5xlarge-1",
                    "ip-3",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue",
                    instance=EC2Instance(
                        id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-1",
                    "ip-1",
                    "hostname",
                    "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP",
                    "queue1",
                    instance=EC2Instance(
                        id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue1-st-c4xlarge-1",
                    "ip-1",
                    "hostname",
                    "DOWN",
                    "queue1",
                    instance=EC2Instance(
                        id="id-3", private_ip="ip-3", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-3",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERING_UP",
                    "queue1",
                    "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=24, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-4", private_ip="ip-4", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                    instance=EC2Instance(
                        id="id-5", private_ip="ip-5", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERING_DOWN",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=12, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-6", private_ip="ip-6", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-3",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:UnauthorizedOperation)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=23, second=10, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-7", private_ip="ip-7", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-4",
                    "nodeip",
                    "nodehostname",
                    "MIXED+CLOUD+POWERED_UP",
                    "queue2",
                    "(Code:InvalidBlockDeviceMapping)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=21, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-8", private_ip="ip-8", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-5",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:AccessDeniedException)Error",
                    instance=EC2Instance(
                        id="id-9", private_ip="ip-9", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-6",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERED_UP",
                    "queue2",
                    "(Code:VcpuLimitExceeded)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=25, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-10", private_ip="ip-10", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-8",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:VolumeLimitExceeded)Error",
                    instance=EC2Instance(
                        id="id-11", private_ip="ip-11", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-9",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:InsufficientVolumeCapacity)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=11, hour=11, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-12", private_ip="ip-12", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
            ],
            [
                {
                    "compute-node-idle-time": {
                        "node-type": "dynamic",
                        "longest-idle-time": 129540.0,
                        "longest-idle-node": {
                            "name": "queue1-dy-c5xlarge-3",
                            "type": "dynamic",
                            "address": "nodeip",
                            "hostname": "nodehostname",
                            "state-string": "IDLE+CLOUD+POWERING_UP",
                            "state-reason": "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes",
                            "state": "IDLE",
                            "state-flags": ["CLOUD", "POWERING_UP"],
                            "instance": {
                                "id": "id-4",
                                "private-ip": "ip-4",
                                "hostname": "hostname",
                                "launch-time": "some_launch_time",
                            },
                            "partitions": ["queue1"],
                            "queue-name": "queue1",
                            "compute-resource": "c5xlarge",
                            "last-busy-time": "2023-03-10T11:24:14.000+00:00",
                            "slurm-started-time": None,
                        },
                        "count": 3,
                    }
                },
                {
                    "compute-node-idle-time": {
                        "node-type": "static",
                        "longest-idle-time": 129604.0,
                        "longest-idle-node": {
                            "name": "queue2-st-c5large-3",
                            "type": "static",
                            "address": "nodeip",
                            "hostname": "nodehostname",
                            "state-string": "IDLE+CLOUD",
                            "state-reason": "(Code:UnauthorizedOperation)Error",
                            "state": "IDLE",
                            "state-flags": ["CLOUD"],
                            "instance": {
                                "id": "id-7",
                                "private-ip": "ip-7",
                                "hostname": "hostname",
                                "launch-time": "some_launch_time",
                            },
                            "partitions": ["queue2"],
                            "queue-name": "queue2",
                            "compute-resource": "c5large",
                            "last-busy-time": "2023-03-10T11:23:10.000+00:00",
                            "slurm-started-time": None,
                        },
                        "count": 2,
                    }
                },
            ],
            ["ERROR", "WARNING", "INFO"],
            None,
        ),
        (
            [
                StaticNode(
                    "queue1-st-c5xlarge-2",
                    "ip-2",
                    "hostname",
                    "IDLE+CLOUD+POWERING_DOWN",
                    "queue1",
                    instance=EC2Instance(
                        id="id-1", private_ip="ip-1", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue-st-c5xlarge-1",
                    "ip-3",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue",
                    instance=EC2Instance(
                        id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-1",
                    "ip-1",
                    "hostname",
                    "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP",
                    "queue1",
                    instance=EC2Instance(
                        id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue1-st-c4xlarge-1",
                    "ip-1",
                    "hostname",
                    "DOWN",
                    "queue1",
                    instance=EC2Instance(
                        id="id-3", private_ip="ip-3", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-3",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERING_UP",
                    "queue1",
                    "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=24, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-4", private_ip="ip-4", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                    instance=EC2Instance(
                        id="id-5", private_ip="ip-5", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERING_DOWN",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=12, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-6", private_ip="ip-6", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-3",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:UnauthorizedOperation)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=23, second=10, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-7", private_ip="ip-7", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-4",
                    "nodeip",
                    "nodehostname",
                    "MIXED+CLOUD+POWERED_UP",
                    "queue2",
                    "(Code:InvalidBlockDeviceMapping)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=21, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-8", private_ip="ip-8", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-5",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:AccessDeniedException)Error",
                    instance=EC2Instance(
                        id="id-9", private_ip="ip-9", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-6",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERED_UP",
                    "queue2",
                    "(Code:VcpuLimitExceeded)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=25, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-10", private_ip="ip-10", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-8",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:VolumeLimitExceeded)Error",
                    instance=EC2Instance(
                        id="id-11", private_ip="ip-11", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-9",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:InsufficientVolumeCapacity)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=11, hour=11, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-12", private_ip="ip-12", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
            ],
            [
                {
                    "compute-node-idle-time": {
                        "node-type": "dynamic",
                        "longest-idle-time": 129540.0,
                        "longest-idle-node": {
                            "name": "queue1-dy-c5xlarge-3",
                            "type": "dynamic",
                            "address": "nodeip",
                            "hostname": "nodehostname",
                            "state-string": "IDLE+CLOUD+POWERING_UP",
                            "state-reason": "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes",
                            "state": "IDLE",
                            "state-flags": ["CLOUD", "POWERING_UP"],
                            "instance": {
                                "id": "id-4",
                                "private-ip": "ip-4",
                                "hostname": "hostname",
                                "launch-time": "some_launch_time",
                            },
                            "partitions": ["queue1"],
                            "queue-name": "queue1",
                            "compute-resource": "c5xlarge",
                            "last-busy-time": "2023-03-10T11:24:14.000+00:00",
                            "slurm-started-time": None,
                        },
                        "count": 3,
                    }
                },
                {
                    "compute-node-idle-time": {
                        "node-type": "static",
                        "longest-idle-time": 129604.0,
                        "longest-idle-node": {
                            "name": "queue2-st-c5large-3",
                            "type": "static",
                            "address": "nodeip",
                            "hostname": "nodehostname",
                            "state-string": "IDLE+CLOUD",
                            "state-reason": "(Code:UnauthorizedOperation)Error",
                            "state": "IDLE",
                            "state-flags": ["CLOUD"],
                            "instance": {
                                "id": "id-7",
                                "private-ip": "ip-7",
                                "hostname": "hostname",
                                "launch-time": "some_launch_time",
                            },
                            "partitions": ["queue2"],
                            "queue-name": "queue2",
                            "compute-resource": "c5large",
                            "last-busy-time": "2023-03-10T11:23:10.000+00:00",
                            "slurm-started-time": None,
                        },
                        "count": 2,
                    }
                },
                {"compute-node-state-count": {"node-state": "IDLE+CLOUD+POWERING_DOWN", "count": 2}},
                {"compute-node-state-count": {"node-state": "IDLE+CLOUD", "count": 4}},
                {"compute-node-state-count": {"node-state": "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "count": 1}},
                {"compute-node-state-count": {"node-state": "DOWN", "count": 1}},
                {"compute-node-state-count": {"node-state": "IDLE+CLOUD+POWERING_UP", "count": 1}},
                {"compute-node-state-count": {"node-state": "MIXED+CLOUD+POWERED_UP", "count": 1}},
                {"compute-node-state-count": {"node-state": "DOWN+CLOUD", "count": 2}},
                {"compute-node-state-count": {"node-state": "IDLE+CLOUD+POWERED_UP", "count": 1}},
                {"cluster-instance-count": {"count": 13}},
                {
                    "compute-node-state": {
                        "name": "queue1-st-c5xlarge-2",
                        "type": "static",
                        "address": "ip-2",
                        "hostname": "hostname",
                        "state-string": "IDLE+CLOUD+POWERING_DOWN",
                        "state-reason": None,
                        "state": "IDLE",
                        "state-flags": ["CLOUD", "POWERING_DOWN"],
                        "instance": {
                            "id": "id-1",
                            "private-ip": "ip-1",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue1"],
                        "queue-name": "queue1",
                        "compute-resource": "c5xlarge",
                        "last-busy-time": None,
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue-st-c5xlarge-1",
                        "type": "static",
                        "address": "ip-3",
                        "hostname": "hostname",
                        "state-string": "IDLE+CLOUD",
                        "state-reason": None,
                        "state": "IDLE",
                        "state-flags": ["CLOUD"],
                        "instance": {
                            "id": "id-2",
                            "private-ip": "ip-2",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue"],
                        "queue-name": "queue",
                        "compute-resource": "c5xlarge",
                        "last-busy-time": None,
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue1-dy-c5xlarge-1",
                        "type": "dynamic",
                        "address": "ip-1",
                        "hostname": "hostname",
                        "state-string": "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP",
                        "state-reason": None,
                        "state": "MIXED",
                        "state-flags": ["CLOUD", "NOT_RESPONDING", "POWERING_UP"],
                        "instance": {
                            "id": "id-2",
                            "private-ip": "ip-2",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue1"],
                        "queue-name": "queue1",
                        "compute-resource": "c5xlarge",
                        "last-busy-time": None,
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue1-st-c4xlarge-1",
                        "type": "static",
                        "address": "ip-1",
                        "hostname": "hostname",
                        "state-string": "DOWN",
                        "state-reason": None,
                        "state": "DOWN",
                        "state-flags": [],
                        "instance": {
                            "id": "id-3",
                            "private-ip": "ip-3",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue1"],
                        "queue-name": "queue1",
                        "compute-resource": "c4xlarge",
                        "last-busy-time": None,
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue1-dy-c5xlarge-3",
                        "type": "dynamic",
                        "address": "nodeip",
                        "hostname": "nodehostname",
                        "state-string": "IDLE+CLOUD+POWERING_UP",
                        "state-reason": "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes",
                        "state": "IDLE",
                        "state-flags": ["CLOUD", "POWERING_UP"],
                        "instance": {
                            "id": "id-4",
                            "private-ip": "ip-4",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue1"],
                        "queue-name": "queue1",
                        "compute-resource": "c5xlarge",
                        "last-busy-time": "2023-03-10T11:24:14.000+00:00",
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue2-dy-c5large-1",
                        "type": "dynamic",
                        "address": "nodeip",
                        "hostname": "nodehostname",
                        "state-string": "IDLE+CLOUD",
                        "state-reason": "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                        "state": "IDLE",
                        "state-flags": ["CLOUD"],
                        "instance": {
                            "id": "id-5",
                            "private-ip": "ip-5",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue2"],
                        "queue-name": "queue2",
                        "compute-resource": "c5large",
                        "last-busy-time": None,
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue2-dy-c5large-2",
                        "type": "dynamic",
                        "address": "nodeip",
                        "hostname": "nodehostname",
                        "state-string": "IDLE+CLOUD+POWERING_DOWN",
                        "state-reason": "(Code:InsufficientHostCapacity)Error",
                        "state": "IDLE",
                        "state-flags": ["CLOUD", "POWERING_DOWN"],
                        "instance": {
                            "id": "id-6",
                            "private-ip": "ip-6",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue2"],
                        "queue-name": "queue2",
                        "compute-resource": "c5large",
                        "last-busy-time": "2023-03-10T12:23:14.000+00:00",
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue2-st-c5large-3",
                        "type": "static",
                        "address": "nodeip",
                        "hostname": "nodehostname",
                        "state-string": "IDLE+CLOUD",
                        "state-reason": "(Code:UnauthorizedOperation)Error",
                        "state": "IDLE",
                        "state-flags": ["CLOUD"],
                        "instance": {
                            "id": "id-7",
                            "private-ip": "ip-7",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue2"],
                        "queue-name": "queue2",
                        "compute-resource": "c5large",
                        "last-busy-time": "2023-03-10T11:23:10.000+00:00",
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue2-st-c5large-4",
                        "type": "static",
                        "address": "nodeip",
                        "hostname": "nodehostname",
                        "state-string": "MIXED+CLOUD+POWERED_UP",
                        "state-reason": "(Code:InvalidBlockDeviceMapping)Error",
                        "state": "MIXED",
                        "state-flags": ["CLOUD", "POWERED_UP"],
                        "instance": {
                            "id": "id-8",
                            "private-ip": "ip-8",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue2"],
                        "queue-name": "queue2",
                        "compute-resource": "c5large",
                        "last-busy-time": "2023-03-10T21:23:14.000+00:00",
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue2-dy-c5large-5",
                        "type": "dynamic",
                        "address": "nodeip",
                        "hostname": "nodehostname",
                        "state-string": "DOWN+CLOUD",
                        "state-reason": "(Code:AccessDeniedException)Error",
                        "state": "DOWN",
                        "state-flags": ["CLOUD"],
                        "instance": {
                            "id": "id-9",
                            "private-ip": "ip-9",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue2"],
                        "queue-name": "queue2",
                        "compute-resource": "c5large",
                        "last-busy-time": None,
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue2-st-c5large-6",
                        "type": "static",
                        "address": "nodeip",
                        "hostname": "nodehostname",
                        "state-string": "IDLE+CLOUD+POWERED_UP",
                        "state-reason": "(Code:VcpuLimitExceeded)Error",
                        "state": "IDLE",
                        "state-flags": ["CLOUD", "POWERED_UP"],
                        "instance": {
                            "id": "id-10",
                            "private-ip": "ip-10",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue2"],
                        "queue-name": "queue2",
                        "compute-resource": "c5large",
                        "last-busy-time": "2023-03-10T11:25:14.000+00:00",
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue2-st-c5large-8",
                        "type": "static",
                        "address": "nodeip",
                        "hostname": "nodehostname",
                        "state-string": "DOWN+CLOUD",
                        "state-reason": "(Code:VolumeLimitExceeded)Error",
                        "state": "DOWN",
                        "state-flags": ["CLOUD"],
                        "instance": {
                            "id": "id-11",
                            "private-ip": "ip-11",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue2"],
                        "queue-name": "queue2",
                        "compute-resource": "c5large",
                        "last-busy-time": None,
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue2-dy-c5large-9",
                        "type": "dynamic",
                        "address": "nodeip",
                        "hostname": "nodehostname",
                        "state-string": "IDLE+CLOUD",
                        "state-reason": "(Code:InsufficientVolumeCapacity)Error",
                        "state": "IDLE",
                        "state-flags": ["CLOUD"],
                        "instance": {
                            "id": "id-12",
                            "private-ip": "ip-12",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue2"],
                        "queue-name": "queue2",
                        "compute-resource": "c5large",
                        "last-busy-time": "2023-03-11T11:23:14.000+00:00",
                        "slurm-started-time": None,
                    }
                },
            ],
            ["ERROR", "WARNING", "INFO", "DEBUG"],
            None,
        ),
        (
            [
                DynamicNode(
                    "queue1-dy-c5xlarge-1",
                    "ip-1",
                    "hostname",
                    "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP",
                    "queue1",
                    instance=EC2Instance(
                        id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue1-st-c4xlarge-1",
                    "ip-1",
                    "hostname",
                    "DOWN",
                    "queue1",
                    instance=EC2Instance(
                        id="id-3", private_ip="ip-3", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
            ],
            [
                {
                    "compute-node-idle-time": {
                        "node-type": "dynamic",
                        "longest-idle-time": 0,
                        "longest-idle-node": None,
                        "count": 0,
                    }
                },
                {
                    "compute-node-idle-time": {
                        "node-type": "static",
                        "longest-idle-time": 0,
                        "longest-idle-node": None,
                        "count": 0,
                    }
                },
                {"compute-node-state-count": {"node-state": "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP", "count": 1}},
                {"compute-node-state-count": {"node-state": "DOWN", "count": 1}},
                {"cluster-instance-count": {"count": 2}},
                {
                    "compute-node-state": {
                        "name": "queue1-dy-c5xlarge-1",
                        "type": "dynamic",
                        "address": "ip-1",
                        "hostname": "hostname",
                        "state-string": "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP",
                        "state-reason": None,
                        "state": "MIXED",
                        "state-flags": ["CLOUD", "NOT_RESPONDING", "POWERING_UP"],
                        "instance": {
                            "id": "id-2",
                            "private-ip": "ip-2",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue1"],
                        "queue-name": "queue1",
                        "compute-resource": "c5xlarge",
                        "last-busy-time": None,
                        "slurm-started-time": None,
                    }
                },
                {
                    "compute-node-state": {
                        "name": "queue1-st-c4xlarge-1",
                        "type": "static",
                        "address": "ip-1",
                        "hostname": "hostname",
                        "state-string": "DOWN",
                        "state-reason": None,
                        "state": "DOWN",
                        "state-flags": [],
                        "instance": {
                            "id": "id-3",
                            "private-ip": "ip-3",
                            "hostname": "hostname",
                            "launch-time": "some_launch_time",
                        },
                        "partitions": ["queue1"],
                        "queue-name": "queue1",
                        "compute-resource": "c4xlarge",
                        "last-busy-time": None,
                        "slurm-started-time": None,
                    }
                },
            ],
            [],
            None,
        ),
        (
            [
                StaticNode(
                    "queue1-st-c5xlarge-2",
                    "ip-2",
                    "hostname",
                    "IDLE+CLOUD+POWERING_DOWN",
                    "queue1",
                    instance=EC2Instance(
                        id="id-1", private_ip="ip-1", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue-st-c5xlarge-1",
                    "ip-3",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue",
                    instance=EC2Instance(
                        id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-1",
                    "ip-1",
                    "hostname",
                    "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP",
                    "queue1",
                    instance=EC2Instance(
                        id="id-2", private_ip="ip-2", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue1-st-c4xlarge-1",
                    "ip-1",
                    "hostname",
                    "DOWN",
                    "queue1",
                    instance=EC2Instance(
                        id="id-3", private_ip="ip-3", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-3",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERING_UP",
                    "queue1",
                    "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=24, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-4", private_ip="ip-4", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                    instance=EC2Instance(
                        id="id-5", private_ip="ip-5", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERING_DOWN",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=12, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-6", private_ip="ip-6", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-3",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:UnauthorizedOperation)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=23, second=10, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-7", private_ip="ip-7", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-4",
                    "nodeip",
                    "nodehostname",
                    "MIXED+CLOUD+POWERED_UP",
                    "queue2",
                    "(Code:InvalidBlockDeviceMapping)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=21, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-8", private_ip="ip-8", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-5",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:AccessDeniedException)Error",
                    instance=EC2Instance(
                        id="id-9", private_ip="ip-9", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-6",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERED_UP",
                    "queue2",
                    "(Code:VcpuLimitExceeded)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=25, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-10", private_ip="ip-10", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-8",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:VolumeLimitExceeded)Error",
                    instance=EC2Instance(
                        id="id-11", private_ip="ip-11", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-9",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:InsufficientVolumeCapacity)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=11, hour=11, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-12", private_ip="ip-12", hostname="hostname", launch_time="some_launch_time"
                    ),
                ),
            ],
            [],
            ["ERROR", "WARNING"],
            None,
        ),
    ],
    ids=[
        "no-idle-nodes",
        "some-idle-nodes",
        "idle-nodes-debug",
        "no-idle-nodes-debug",
        "nothing-at-warning",
    ],
)
def test_publish_compute_node_events(compute_nodes, expected_details, level_filter, max_list_size, mocker):
    received_events = []
    event_publisher = ClusterEventPublisher(
        event_handler(received_events, level_filter=level_filter), max_list_size=max_list_size
    )
    test_time = datetime(year=2023, month=3, day=11, hour=23, minute=23, second=14, tzinfo=timezone.utc)

    # Run test
    mock_now = mocker.patch(
        "slurm_plugin.cluster_event_publisher.ClusterEventPublisher.current_time",
    )
    mock_now.return_value = test_time
    cluster_instances = [node.instance for node in compute_nodes if node.instance]
    event_publisher.publish_compute_node_events(compute_nodes, cluster_instances)

    # Assert calls
    assert_that(received_events).is_length(len(expected_details))
    for received_event, expected_detail in zip(received_events, expected_details):
        assert_that(received_event).is_equal_to(expected_detail)


@pytest.mark.parametrize(
    "compute_nodes, expected_details, level_filter, max_list_size",
    [
        (
            [
                StaticNode(
                    "queue1-st-c5xlarge-2",
                    "ip-2",
                    "hostname",
                    "IDLE+CLOUD+POWERING_DOWN",
                    "queue1",
                    instance=EC2Instance(
                        id="id-1",
                        private_ip="ip-1",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                ),
                StaticNode(
                    "queue-st-c5xlarge-1",
                    "ip-3",
                    "hostname",
                    "IDLE+CLOUD",
                    "queue",
                    instance=EC2Instance(
                        id="id-2",
                        private_ip="ip-2",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-1",
                    "ip-1",
                    "hostname",
                    "MIXED+CLOUD+NOT_RESPONDING+POWERING_UP",
                    "queue1",
                    instance=EC2Instance(
                        id="id-2",
                        private_ip="ip-2",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                ),
                StaticNode(
                    "queue1-st-c4xlarge-1",
                    "ip-1",
                    "hostname",
                    "DOWN",
                    "queue1",
                    instance=EC2Instance(
                        id="id-3",
                        private_ip="ip-3",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                ),
                DynamicNode(
                    "queue1-dy-c5xlarge-3",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERING_UP",
                    "queue1",
                    "(Code:InsufficientReservedInstanceCapacity)Failure when resuming nodes",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=24, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-4",
                        private_ip="ip-4",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-1",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Failure when resuming nodes",
                ),
                DynamicNode(
                    "queue2-dy-c5large-2",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERING_DOWN",
                    "queue2",
                    "(Code:InsufficientHostCapacity)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=12, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-6",
                        private_ip="ip-6",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-3",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:UnauthorizedOperation)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=23, second=10, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-7",
                        private_ip="ip-7",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-4",
                    "nodeip",
                    "nodehostname",
                    "MIXED+CLOUD+POWERED_UP",
                    "queue2",
                    "(Code:InvalidBlockDeviceMapping)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=21, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-8",
                        private_ip="ip-8",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-5",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:AccessDeniedException)Error",
                    instance=EC2Instance(
                        id="id-9",
                        private_ip="ip-9",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-6",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERED_UP",
                    "queue2",
                    "(Code:VcpuLimitExceeded)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=25, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-10",
                        private_ip="ip-10",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                ),
                StaticNode(
                    "queue2-st-c5large-8",
                    "nodeip",
                    "nodehostname",
                    "DOWN+CLOUD",
                    "queue2",
                    "(Code:VolumeLimitExceeded)Error",
                    instance=EC2Instance(
                        id="id-11",
                        private_ip="ip-11",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                ),
                DynamicNode(
                    "queue2-dy-c5large-9",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD",
                    "queue2",
                    "(Code:InsufficientVolumeCapacity)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=11, hour=11, minute=23, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-12",
                        private_ip="ip-12",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="t2.micro",
                        threads_per_core=3,
                    ),
                ),
            ],
            [
                {
                    "node-instance-mapping-event": {
                        "node_list": [
                            {
                                "instance_id": "id-1",
                                "node_name": "queue1-st-c5xlarge-2",
                                "instance_type": "instance_type",
                                "threads_per_core": 2,
                            },
                            {
                                "instance_id": "id-2",
                                "node_name": "queue-st-c5xlarge-1",
                                "instance_type": "instance_type",
                                "threads_per_core": 2,
                            },
                            {
                                "instance_id": "id-2",
                                "node_name": "queue1-dy-c5xlarge-1",
                                "instance_type": "instance_type",
                                "threads_per_core": 2,
                            },
                            {
                                "instance_id": "id-3",
                                "node_name": "queue1-st-c4xlarge-1",
                                "instance_type": "instance_type",
                                "threads_per_core": 2,
                            },
                            {
                                "instance_id": "id-4",
                                "node_name": "queue1-dy-c5xlarge-3",
                                "instance_type": "instance_type",
                                "threads_per_core": 2,
                            },
                            {
                                "instance_id": "id-6",
                                "node_name": "queue2-dy-c5large-2",
                                "instance_type": "instance_type",
                                "threads_per_core": 2,
                            },
                            {
                                "instance_id": "id-7",
                                "node_name": "queue2-st-c5large-3",
                                "instance_type": "instance_type",
                                "threads_per_core": 2,
                            },
                            {
                                "instance_id": "id-8",
                                "node_name": "queue2-st-c5large-4",
                                "instance_type": "instance_type",
                                "threads_per_core": 2,
                            },
                            {
                                "instance_id": "id-9",
                                "node_name": "queue2-dy-c5large-5",
                                "instance_type": "instance_type",
                                "threads_per_core": 2,
                            },
                            {
                                "instance_id": "id-10",
                                "node_name": "queue2-st-c5large-6",
                                "instance_type": "instance_type",
                                "threads_per_core": 2,
                            },
                            {
                                "instance_id": "id-11",
                                "node_name": "queue2-st-c5large-8",
                                "instance_type": "instance_type",
                                "threads_per_core": 2,
                            },
                            {
                                "instance_id": "id-12",
                                "node_name": "queue2-dy-c5large-9",
                                "instance_type": "t2.micro",
                                "threads_per_core": 3,
                            },
                        ]
                    }
                }
            ],
            [],
            None,
        ),
        (
            [],
            [{"node-instance-mapping-event": {"node_list": []}}],
            [],
            100,
        ),
        (
            [
                StaticNode(
                    "queue2-st-c5large-6",
                    "nodeip",
                    "nodehostname",
                    "IDLE+CLOUD+POWERED_UP",
                    "queue2",
                    "(Code:VcpuLimitExceeded)Error",
                    lastbusytime=datetime(
                        year=2023, month=3, day=10, hour=11, minute=25, second=14, tzinfo=timezone.utc
                    ),
                    instance=EC2Instance(
                        id="id-10",
                        private_ip="ip-10",
                        hostname="hostname",
                        launch_time="some_launch_time",
                        instance_type="instance_type",
                        threads_per_core=2,
                    ),
                )
            ],
            [],
            ["WARNING"],
            100,
        ),
    ],
    ids=["node_map", "node_map no instance", "Node_map does not log warning"],
)
def test_publish_node_mapping_events(compute_nodes, expected_details, level_filter, max_list_size, mocker):
    received_events = []
    event_publisher = ClusterEventPublisher(
        event_handler(received_events, level_filter=level_filter), max_list_size=max_list_size
    )
    test_time = datetime(year=2023, month=3, day=11, hour=23, minute=23, second=14, tzinfo=timezone.utc)

    # Run test
    mock_now = mocker.patch(
        "slurm_plugin.cluster_event_publisher.ClusterEventPublisher.current_time",
    )
    mock_now.return_value = test_time

    event_publisher.publish_node_mapping_events(compute_nodes)

    # Assert calls

    assert_that(received_events).is_length(len(expected_details))
    for received_event, expected_detail in zip(received_events, expected_details):
        assert_that(received_event).is_equal_to(expected_detail)
