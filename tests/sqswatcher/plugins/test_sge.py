import pytest

from assertpy import assert_that
from common.schedulers.sge_commands import SgeHost, SgeJob
from sqswatcher.plugins.sge import _is_node_locked


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
    ],
)
def test_is_node_locked(hostname, get_compute_nodes_info_output, expected_result, mocker):
    mocker.patch(
        "sqswatcher.plugins.sge.socket.getfqdn",
        return_value="{}.eu-west-1.compute.internal".format(hostname),
        autospec=True,
    )
    mocker.patch(
        "sqswatcher.plugins.sge.get_compute_nodes_info", return_value=get_compute_nodes_info_output, autospec=True
    )

    assert_that(_is_node_locked(hostname)).is_equal_to(expected_result)
