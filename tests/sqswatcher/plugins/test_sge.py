import pytest

from assertpy import assert_that
from common.schedulers.sge_commands import SgeHost, SgeJob
from common.utils import EventType, Host, UpdateEvent
from sqswatcher.plugins.sge import _is_node_locked, perform_health_actions


@pytest.mark.parametrize(
    "hostname, get_compute_nodes_info_output, expected_result",
    [
        (
            "ip-10-0-000-111",
            {
                "ip-10-0-000-111.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-000-111.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=3,
                    slots_reserved=0,
                    state="",
                    jobs=[],
                )
            },
            False,
        ),
        (
            "ip-10-0-000-111",
            {
                "ip-10-0-000-111.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-000-111.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=3,
                    slots_reserved=0,
                    state="aux",
                    jobs=[],
                )
            },
            False,
        ),
        (
            "ip-10-0-000-111",
            {
                "ip-10-0-000-111.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-000-111.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=3,
                    slots_reserved=0,
                    state="d",
                    jobs=[],
                )
            },
            True,
        ),
        (
            "ip-10-0-000-111",
            {
                "ip-10-0-000-111.eu-west-1.compute.internal": SgeHost(
                    name="ip-10-0-000-111.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=3,
                    slots_reserved=0,
                    state="auxd",
                    jobs=[
                        SgeJob(number="89", slots=1, state="r", node_type="MASTER", array_index=None, hostname=None),
                    ],
                )
            },
            True,
        ),
        ("ip-10-0-000-111", Exception, False,),
    ],
)
def test_is_node_locked(hostname, get_compute_nodes_info_output, expected_result, mocker):
    mocker.patch(
        "sqswatcher.plugins.sge.socket.getfqdn",
        return_value="{}.eu-west-1.compute.internal".format(hostname),
        autospec=True,
    )
    if get_compute_nodes_info_output is Exception:
        mocker.patch("sqswatcher.plugins.sge.get_compute_nodes_info", side_effect=Exception(), autospec=True)
    else:
        mocker.patch(
            "sqswatcher.plugins.sge.get_compute_nodes_info", return_value=get_compute_nodes_info_output, autospec=True
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
        mocker.patch("sqswatcher.plugins.sge.lock_node", side_effect=Exception, autospec=True)
    else:
        mocker.patch("sqswatcher.plugins.sge.lock_node", autospec=True)
    mocker.patch(
        "sqswatcher.plugins.sge._is_node_locked", side_effect=is_node_locked_output, autospec=True,
    )
    failed, succeeded = perform_health_actions(events)
    assert_that(failed_events).is_equal_to(failed)
    assert_that(succeeded_events).is_equal_to(succeeded)
