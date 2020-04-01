import pytest

from assertpy import assert_that
from common.schedulers.torque_commands import TorqueHost
from sqswatcher.plugins.torque import _is_node_locked


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
    ],
)
def test_is_node_locked(hostname, get_compute_nodes_info_output, expected_result, mocker):
    mocker.patch(
        "sqswatcher.plugins.torque.get_compute_nodes_info", return_value=get_compute_nodes_info_output, autospec=True
    )

    assert_that(_is_node_locked(hostname)).is_equal_to(expected_result)
