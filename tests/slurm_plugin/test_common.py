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
import json
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from assertpy import assert_that
from common.utils import read_json, time_is_up
from slurm_plugin.common import TIMESTAMP_FORMAT, event_publisher, get_clustermgtd_heartbeat


@pytest.mark.parametrize(
    "initial_time, current_time, grace_time, expected_result",
    [
        (datetime(2020, 1, 1, 0, 0, 0), datetime(2020, 1, 1, 0, 0, 29), 30, False),
        (datetime(2020, 1, 1, 0, 0, 0), datetime(2020, 1, 1, 0, 0, 30), 30, True),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours ahead of UTC, so this time stamp is actually 30 mins before initial_time
            datetime(2020, 1, 1, 0, 30, 0, tzinfo=timezone(timedelta(hours=1))),
            30 * 60,
            False,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours ahead of UTC, so this time stamp is actually 30 mins after initial_time
            datetime(2020, 1, 1, 1, 30, 0, tzinfo=timezone(timedelta(hours=1))),
            30 * 60,
            True,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours behind of UTC, so this time stamp is actually 1.5 hrs after initial_time
            datetime(2020, 1, 1, 0, 30, 0, tzinfo=timezone(-timedelta(hours=1))),
            90 * 60,
            True,
        ),
        (
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # local timezone is 1 hours behind of UTC, so this time stamp is actually 1 hrs after initial_time
            datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone(-timedelta(hours=1))),
            90 * 60,
            False,
        ),
        (
            None,
            datetime(2020, 1, 24, 23, 42, 12),
            180,
            True,
        ),
    ],
)
def test_time_is_up(initial_time, current_time, grace_time, expected_result):
    assert_that(time_is_up(initial_time, current_time, grace_time)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "time, expected_parsed_time",
    [
        (
            datetime(2020, 7, 30, 19, 34, 2, 613338, tzinfo=timezone.utc),
            datetime(2020, 7, 30, 19, 34, 2, 613338, tzinfo=timezone.utc),
        ),
        (
            datetime(2020, 7, 30, 10, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            datetime(2020, 7, 30, 10, 1, 1, tzinfo=timezone(timedelta(hours=1))),
        ),
    ],
)
def test_get_clustermgtd_heartbeat(time, expected_parsed_time, mocker):
    mocker.patch(
        "slurm_plugin.common.check_command_output",
        return_value=f"some_random_stdout\n{time.strftime(TIMESTAMP_FORMAT)}",
    )
    assert_that(get_clustermgtd_heartbeat("/some/file/path")).is_equal_to(expected_parsed_time)


@pytest.mark.parametrize(
    "json_file, default_value, raises_exception, message_in_log",
    [
        ("faulty.json", None, True, "Failed with exception"),
        ("faulty.json", {}, False, "due to an exception"),  # info message
        ("standard.json", None, False, None),
        ("non_existing.json", None, True, "Failed with exception"),
        ("non_existing.json", {}, False, None),  # info message not displayed
    ],
)
def test_read_json(test_datadir, caplog, json_file, default_value, raises_exception, message_in_log):
    caplog.set_level(logging.INFO)
    json_file_path = str(test_datadir.joinpath(json_file))
    if raises_exception:
        with pytest.raises((ValueError, FileNotFoundError)):
            read_json(json_file_path, default_value)
    else:
        read_json(json_file_path, default_value)

    if message_in_log:
        assert_that(caplog.text).matches(message_in_log)
    else:
        assert_that(caplog.text).does_not_match("exception")


@pytest.mark.parametrize(
    "log_level, base_args, events, expected_events",
    [
        (
            logging.INFO,
            {"a-setting": "value-a"},
            [
                (
                    "INFO",
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
                    "INFO",
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
                    "DEBUG",
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
                    "INFO",
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
                    "INFO",
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
                    "DEBUG",
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
                    "WARNING",
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
)
def test_event_publisher(log_level, base_args, events, expected_events):
    received_events = []

    def log_handler(level, format_string, value):
        received_events.append([level, value])

    metric_logger = SimpleNamespace()
    metric_logger.isEnabledFor = lambda level: level >= log_level
    metric_logger.log = log_handler

    publisher = event_publisher(metric_logger, "cluster", "HeadNode", "component", "instance_id", **base_args)

    # Run test
    for event in events:
        publisher(event[0], event[1], event[2], **event[3])

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
                    "INFO",
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
                    "INFO",
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
                    "INFO",
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
                    "DEBUG",
                    "info-message",
                    "info-event",
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
                    "INFO",
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
                    "DEBUG",
                    "info-message",
                    "info-event",
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

    publisher = event_publisher(metric_logger, "cluster", "HeadNode", "component", "instance_id")

    for event in events:
        publisher(
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

    publisher = event_publisher(metric_logger, "cluster", "HeadNode", "component", "instance_id")

    publisher(logging.INFO, "hello", "event-type", detail={"hello": "goodbye"})

    assert_that(handler_called).is_true()

    assert_that(caplog.records).is_length(1)
