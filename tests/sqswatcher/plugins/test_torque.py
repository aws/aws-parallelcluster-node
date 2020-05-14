import pytest

from assertpy import assert_that
from common.schedulers.torque_commands import TorqueHost
from common.utils import EventType, Host, UpdateEvent
from sqswatcher.plugins.torque import _is_node_locked, perform_health_actions


@pytest.mark.parametrize(
    "hostname, get_compute_nodes_info_output, expected_result",
    [
        (
            "ip-10-0-1-242",
            {"ip-10-0-1-242": TorqueHost(name="ip-10-0-1-242", slots=4, state=["free"], jobs=None, note="")},
            False,
        ),
        (
            "ip-10-0-1-242",
            {"ip-10-0-1-242": TorqueHost(name="ip-10-0-1-242", slots=4, state=["job-exclusive"], jobs=None, note="")},
            False,
        ),
        (
            "ip-10-0-1-242",
            {"ip-10-0-1-242": TorqueHost(name="ip-10-0-1-242", slots=4, state=["down"], jobs=None, note="")},
            False,
        ),
        (
            "ip-10-0-1-242",
            {"ip-10-0-1-242": TorqueHost(name="ip-10-0-1-242", slots=4, state=["offline"], jobs=None, note="")},
            True,
        ),
        (
            "ip-10-0-1-242",
            {
                "ip-10-0-1-242": TorqueHost(
                    name="ip-10-0-1-242",
                    slots=4,
                    state=["down,offline,free,job-exclusive"],
                    jobs="1/136.ip-10-0-0-196.eu-west-1.compute.internal",
                    note="",
                ),
            },
            False,
        ),
        ("ip-10-0-1-242", Exception, False,),
    ],
)
def test_is_node_locked(hostname, get_compute_nodes_info_output, expected_result, mocker):
    if get_compute_nodes_info_output is Exception:
        mocker.patch("sqswatcher.plugins.torque.get_compute_nodes_info", side_effect=Exception(), autospec=True)
    else:
        mocker.patch(
            "sqswatcher.plugins.torque.get_compute_nodes_info",
            return_value=get_compute_nodes_info_output,
            autospec=True,
        )

    assert_that(_is_node_locked(hostname)).is_equal_to(expected_result)


@pytest.mark.parametrize(
    "events, lock_node_side_effect, is_node_locked_output, failed_events, succeeded_events",
    [
        (
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            None,
            [False, True],
            [],
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
        ),
        (
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            None,
            [True, True],
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            [],
        ),
        (
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            Exception,
            [False, False],
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            [],
        ),
        (
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            Exception,
            [False, True],
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            [],
        ),
        (
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            None,
            [False, False],
            [UpdateEvent(EventType.HEALTH, None, Host("i-12345", "ip-10-0-000-111", None, None))],
            [],
        ),
    ],
    ids=[
        "all_good",
        "node_already_in_lock",
        "exception_when_locking_node_unlocked",
        "exception_when_locking_node_locked",
        "no_exception_failed_to_lock",
    ],
)
def test_perform_health_actions(
    events, lock_node_side_effect, is_node_locked_output, failed_events, succeeded_events, mocker
):
    if lock_node_side_effect is Exception:
        mocker.patch("sqswatcher.plugins.torque.lock_node", side_effect=Exception, autospec=True)
    else:
        mocker.patch("sqswatcher.plugins.torque.lock_node", autospec=True)
    mocker.patch(
        "sqswatcher.plugins.torque._is_node_locked", side_effect=is_node_locked_output, autospec=True,
    )
    failed, succeeded = perform_health_actions(events)
    assert_that(failed_events).is_equal_to(failed)
    assert_that(succeeded_events).is_equal_to(succeeded)
