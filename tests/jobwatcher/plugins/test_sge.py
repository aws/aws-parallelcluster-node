import pytest

from assertpy import assert_that
from common.schedulers.sge_commands import SgeHost, SgeJob
from jobwatcher.plugins.sge import get_busy_nodes


@pytest.mark.parametrize(
    "cluster_nodes, expected_busy_nodes",
    [
        (
            {
                "all.q@ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="all.q@ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=3,
                    slots_reserved=0,
                    state="",
                    jobs=[SgeJob(number="89", slots=1, state="r", node_type="MASTER", array_index=None, hostname=None)],
                )
            },
            1,
        ),
        (
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
            1,
        ),
        (
            {
                "all.q@ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="all.q@ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="a",
                    jobs=[],
                ),
                "all.q@ip-10-0-0-167.eu-west-1.compute.internal": SgeHost(
                    name="all.q@ip-10-0-0-167.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="",
                    jobs=[],
                ),
            },
            0,
        ),
        (
            {
                "all.q@ip-10-0-0-166.eu-west-1.compute.internal": SgeHost(
                    name="all.q@ip-10-0-0-166.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="a",
                    jobs=[],
                ),
                "all.q@ip-10-0-0-167.eu-west-1.compute.internal": SgeHost(
                    name="all.q@ip-10-0-0-167.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="",
                    jobs=[],
                ),
                "all.q@ip-10-0-0-168.eu-west-1.compute.internal": SgeHost(
                    name="all.q@ip-10-0-0-168.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=1,
                    state="",
                    jobs=[],
                ),
                "all.q@ip-10-0-0-169.eu-west-1.compute.internal": SgeHost(
                    name="all.q@ip-10-0-0-169.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=3,
                    slots_reserved=0,
                    state="",
                    jobs=[SgeJob(number="89", slots=1, state="r", node_type="MASTER", array_index=None, hostname=None)],
                ),
                "all.q@ip-10-0-0-170.eu-west-1.compute.internal": SgeHost(
                    name="all.q@ip-10-0-0-170.eu-west-1.compute.internal",
                    slots_total=4,
                    slots_used=0,
                    slots_reserved=0,
                    state="d",
                    jobs=[],
                ),
            },
            3,
        ),
    ],
    ids=["single_running_job", "unavailable_node", "available_nodes", "mixed_nodes"],
)
def test_get_busy_nodes(cluster_nodes, expected_busy_nodes, mocker):
    mocker.patch("jobwatcher.plugins.sge.get_compute_nodes_info", return_value=cluster_nodes, autospec=True)

    assert_that(get_busy_nodes()).is_equal_to(expected_busy_nodes)
