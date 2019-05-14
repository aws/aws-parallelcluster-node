import pytest

from assertpy import assert_that
from common.schedulers.sge_commands import SgeHost
from nodewatcher.plugins.sge import is_node_down


@pytest.mark.parametrize(
    "hostname, compute_nodes_output, expected_result",
    [
        (
            "ip-10-0-0-166",
            {
                "all.q@ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="all.q@ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="",
                    jobs=[],
                )
            },
            False,
        ),
        (
            "ip-10-0-0-166",
            {
                "all.q@ip-10-0-0-166": SgeHost(
                    name="all.q@ip-10-0-0-166", slots_total=4, slots_used=0, slots_reserved=0, state="", jobs=[]
                )
            },
            False,
        ),
        ("ip-10-0-0-166", {}, True),
        (
            "ip-10-0-0-166",
            {
                "all.q@ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="all.q@ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="u",
                    jobs=[],
                )
            },
            True,
        ),
        ("ip-10-0-0-166", Exception, True),
    ],
    ids=["healthy", "healthy_short", "not_attached", "error_state", "exception"],
)
def test_terminate_if_down(hostname, compute_nodes_output, expected_result, mocker):
    mocker.patch("nodewatcher.plugins.sge.check_command_output", return_value=hostname, autospec=True)
    mocker.patch(
        "nodewatcher.plugins.sge.socket.getfqdn", return_value=hostname + ".eu-west-1.compute.internal", autospec=True
    )
    if compute_nodes_output is Exception:
        mock = mocker.patch("nodewatcher.plugins.sge.get_compute_nodes_info", side_effect=Exception(), autospec=True)
    else:
        mock = mocker.patch(
            "nodewatcher.plugins.sge.get_compute_nodes_info", return_value=compute_nodes_output, autospec=True
        )

    assert_that(is_node_down()).is_equal_to(expected_result)
    mock.assert_called_with(hostname)
