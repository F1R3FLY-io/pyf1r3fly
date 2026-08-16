import pytest

from f1r3fly.deploy import (
    DeployError, check_deploy_consumed_cost, check_deploy_succeeded,
)
from f1r3fly.pb.DeployServiceCommon_pb2 import BlockInfo, DeployInfo


def block_with_deploy(*, cost: int, errored: bool = False) -> BlockInfo:
    return BlockInfo(
        deploys=[
            DeployInfo(
                sig="deploy-id",
                cost=cost,
                errored=errored,
                systemDeployError="runtime failure" if errored else "",
            )
        ]
    )


@pytest.mark.parametrize("cost", [0, 1, 10_000])
def test_success_accepts_every_nonnegative_semantic_cost(cost: int) -> None:
    check_deploy_succeeded(block_with_deploy(cost=cost), "deploy-id")


def test_success_rejects_errored_deploy() -> None:
    with pytest.raises(DeployError, match="runtime failure"):
        check_deploy_succeeded(block_with_deploy(cost=0, errored=True), "deploy-id")


def test_consumed_cost_requires_a_comm_reduction() -> None:
    with pytest.raises(DeployError, match="expected a COMM reduction"):
        check_deploy_consumed_cost(block_with_deploy(cost=0), "deploy-id")

    check_deploy_consumed_cost(block_with_deploy(cost=1), "deploy-id")
