"""Deploy result inspection utilities.

Helpers for checking whether a deploy succeeded or failed after block
inclusion. Operates on BlockInfo protobuf messages from
``F1r3flyClient.show_block()``.

Example::

    block_info = client.show_block(block_hash)
    check_deploy_not_errored(block_info, deploy_id)  # raises DeployError if errored
    deploy = find_deploy_in_block(block_info, deploy_id)
    print(f"Deploy cost: {deploy.cost}")
"""
from __future__ import annotations

from typing import Optional


class DeployError(Exception):
    """Raised when a deploy is errored or missing from a block."""


def find_deploy_in_block(block_info, deploy_id: str):
    """Find a deploy by signature in a block's deploy list.

    Args:
        block_info: BlockInfo from ``F1r3flyClient.show_block()``.
        deploy_id: Deploy signature hex string.

    Returns:
        The matching deploy entry.

    Raises:
        DeployError: If the deploy is not found in the block.
    """
    for d in block_info.deploys:
        if d.sig == deploy_id:
            return d
    sigs = [d.sig[:16] for d in block_info.deploys]
    raise DeployError(
        f"Deploy {deploy_id[:24]} not found in block. "
        f"Block has {len(block_info.deploys)} deploys: {sigs}"
    )


def check_deploy_not_errored(block_info, deploy_id: str) -> None:
    """Verify a deploy is in the block and was not errored.

    Args:
        block_info: BlockInfo from ``F1r3flyClient.show_block()``.
        deploy_id: Deploy signature hex string.

    Raises:
        DeployError: If the deploy is errored or not found.
    """
    deploy = find_deploy_in_block(block_info, deploy_id)
    if deploy.errored:
        raise DeployError(
            f"Deploy {deploy_id[:24]} errored: {deploy.systemDeployError}"
        )


def check_deploy_succeeded(block_info, deploy_id: str) -> None:
    """Verify a deploy is in the block, not errored, and has cost > 0.

    Args:
        block_info: BlockInfo from ``F1r3flyClient.show_block()``.
        deploy_id: Deploy signature hex string.

    Raises:
        DeployError: If the deploy is errored, missing, or has zero cost.
    """
    deploy = find_deploy_in_block(block_info, deploy_id)
    if deploy.errored:
        raise DeployError(
            f"Deploy {deploy_id[:24]} errored: {deploy.systemDeployError}"
        )
    if deploy.cost <= 0:
        raise DeployError(
            f"Deploy {deploy_id[:24]} has zero cost -- "
            f"execution may have been skipped"
        )


def check_deploy_errored(
    block_info,
    deploy_id: str,
    error_contains: Optional[str] = None,
) -> None:
    """Verify a deploy is in the block and marked as errored.

    Args:
        block_info: BlockInfo from ``F1r3flyClient.show_block()``.
        deploy_id: Deploy signature hex string.
        error_contains: If set, verify the error message contains this substring.

    Raises:
        DeployError: If the deploy is not errored, or if the error message
            doesn't contain the expected substring.
    """
    deploy = find_deploy_in_block(block_info, deploy_id)
    if not deploy.errored:
        raise DeployError(
            f"Deploy {deploy_id[:24]} was NOT errored (expected errored=true)"
        )
    if error_contains and error_contains not in deploy.systemDeployError:
        raise DeployError(
            f"Deploy error '{deploy.systemDeployError}' does not contain "
            f"'{error_contains}'"
        )
