"""Polling and deploy workflow utilities.

Generic polling primitives and higher-level deploy workflows for
waiting on blockchain state changes. These are client-side utilities
that any pyf1r3fly consumer can use — not tied to any test framework.

Example::

    from f1r3fly.client import F1r3flyClient
    from f1r3fly.polling import deploy_and_read
    from f1r3fly.par import par_as_int

    with F1r3flyClient("localhost", 40401) as client:
        pars, block_hash, block_number = deploy_and_read(
            client, 'new x in { x!(42) }', my_key,
            inclusion_timeout=30, finalization_timeout=60,
        )
        value = par_as_int(pars[0])
"""
from __future__ import annotations

import logging
import time
from typing import Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


def poll_until(
    predicate: Callable[[], T],
    timeout: int,
    interval: float = 3.0,
    description: str = "",
) -> T:
    """Poll ``predicate`` every ``interval`` seconds until it returns
    a truthy value or ``timeout`` seconds elapse.

    Returns the truthy result on success. Raises ``TimeoutError`` with
    diagnostic info on timeout.

    If ``predicate`` raises an exception, it is caught and retried.
    The last exception is included in the timeout error message.
    """
    deadline = time.time() + timeout
    last_err: Optional[Exception] = None
    attempts = 0

    while time.time() < deadline:
        attempts += 1
        try:
            result = predicate()
            if result:
                return result
        except Exception as e:
            last_err = e
        time.sleep(interval)

    err_detail = f" (last error: {last_err})" if last_err else ""
    raise TimeoutError(
        f"{description or 'poll_until'}: timed out after {timeout}s "
        f"({attempts} attempts){err_detail}"
    )


def wait_for_deploy_included(client, deploy_id: str, timeout: int):
    """Poll ``find_deploy`` until the deploy is included in a block.

    Args:
        client: F1r3flyClient instance.
        deploy_id: Deploy signature hex string.
        timeout: Maximum seconds to wait.

    Returns:
        LightBlockInfo for the block containing the deploy.
    """
    def _check():
        try:
            return client.find_deploy(deploy_id)
        except Exception:
            return None

    return poll_until(
        predicate=_check,
        timeout=timeout,
        interval=3.0,
        description=f"deploy {deploy_id[:24]} inclusion",
    )


def wait_for_finalized(client, block_number: int, timeout: int) -> None:
    """Poll until the last finalized block reaches or exceeds ``block_number``.

    Args:
        client: F1r3flyClient instance.
        block_number: Target block number to wait for.
        timeout: Maximum seconds to wait.
    """
    def _check():
        lfb = client.last_finalized_block()
        if lfb.blockInfo.blockNumber >= block_number:
            return lfb
        return None

    poll_until(
        predicate=_check,
        timeout=timeout,
        interval=5.0,
        description=f"LFB >= #{block_number}",
    )


def wait_for_deploy_finalized(
    client,
    deploy_id: str,
    timeout: int,
    interval: float = 3.0,
):
    """Poll deploy_finalization_status until the deploy reaches Finalized.

    Unlike ``wait_for_finalized`` (which polls block-hash finalization),
    this polls the deploy's actual canonical-state inclusion via
    ``deploy_finalization_status``. A block can finalize while some of its
    deploy effects were dropped by merge rejection; this helper reports the
    deploy's true state.

    Args:
        client: F1r3flyClient instance.
        deploy_id: Deploy signature hex string.
        timeout: Maximum seconds to wait.
        interval: Seconds between polls.

    Returns:
        DeployFinalizationStatusInfo with state=DEPLOY_STATE_FINALIZED.

    Raises:
        DeployError: If the deploy reaches terminal DEPLOY_STATE_FAILED
            (explicit Rholang failure) or DEPLOY_STATE_EXPIRED (past
            deployLifespan without successful inclusion).
        TimeoutError: If the deploy stays in Pending past ``timeout``.
    """
    from .pb.DeployServiceCommon_pb2 import (
        DEPLOY_STATE_EXPIRED, DEPLOY_STATE_FAILED, DEPLOY_STATE_FINALIZED,
    )
    deadline = time.time() + timeout
    attempts = 0
    last_err: Optional[Exception] = None
    last_info = None

    while time.time() < deadline:
        attempts += 1
        try:
            info = client.deploy_finalization_status(deploy_id)
            last_info = info
            if info.state == DEPLOY_STATE_FINALIZED:
                return info
            if info.state == DEPLOY_STATE_FAILED:
                raise DeployError(
                    f"Deploy {deploy_id[:24]} reached terminal state Failed "
                    f"(rejection_count={info.rejectionCount})"
                )
            if info.state == DEPLOY_STATE_EXPIRED:
                raise DeployError(
                    f"Deploy {deploy_id[:24]} reached terminal state Expired "
                    f"(rejection_count={info.rejectionCount})"
                )
        except DeployError:
            raise
        except Exception as e:
            last_err = e
        time.sleep(interval)

    status_detail = (
        f" (last state: Pending, rejection_count={last_info.rejectionCount})"
        if last_info is not None else ""
    )
    err_detail = f" (last error: {last_err})" if last_err else ""
    raise TimeoutError(
        f"deploy {deploy_id[:24]} finalization: timed out after {timeout}s "
        f"({attempts} attempts){status_detail}{err_detail}"
    )


def deploy_and_read(
    client,
    term: str,
    private_key,
    inclusion_timeout: int,
    finalization_timeout: int,
    phlo_limit: int = 100_000,
    phlo_price: int = 1,
    shard_id: str = "root",
) -> tuple:
    """Deploy Rholang code, wait for canonical-state finalization, read deployId channel.

    Full workflow:
    1. Deploy with auto-filled validAfterBlockNumber
    2. Wait for deploy inclusion in a block (first observation)
    3. Wait for ``deploy_finalization_status`` to report ``DEPLOY_STATE_FINALIZED``.
       This polls the deploy's actual canonical-state inclusion, not just
       block-hash finalization — so a block that finalizes while the deploy's
       effects were dropped by merge rejection does NOT satisfy this check,
       and the helper continues to wait for re-inclusion via the
       rejected-deploy-buffer recovery path.
    4. Read data from the deployId channel at the canonical block.

    Args:
        client: F1r3flyClient instance.
        term: Rholang source code to deploy.
        private_key: PrivateKey for signing.
        inclusion_timeout: Seconds to wait for first block inclusion.
        finalization_timeout: Seconds to wait for canonical-state finalization.
        phlo_limit: Maximum phlo to spend.
        phlo_price: Phlo price per unit.
        shard_id: Target shard identifier.

    Returns:
        Tuple of ``(par_list, block_hash, block_number)`` where ``block_hash``
        and ``block_number`` refer to the canonical-state block containing
        the deploy's effects. This may differ from the first inclusion block
        if the deploy was merge-rejected and re-included in a later block.

    Raises:
        TimeoutError: If inclusion or finalization times out.
        DeployError: If the deploy reaches terminal Failed (Rholang execution
            failure) or Expired (past ``deployLifespan`` without inclusion).
    """
    deploy_id = client.deploy_with_vabn_filled(
        key=private_key,
        term=term,
        phlo_price=phlo_price,
        phlo_limit=phlo_limit,
        shard_id=shard_id,
    )
    logger.info("Deployed, deploy_id=%s", deploy_id[:24])

    info = wait_for_deploy_included(client, deploy_id, inclusion_timeout)
    logger.info(
        "Deploy included in block #%d (%s)", info.blockNumber, info.blockHash[:16]
    )

    status = wait_for_deploy_finalized(client, deploy_id, finalization_timeout)

    if status.latestBlockHash:
        canonical_block_hash = status.latestBlockHash.hex()
    else:
        canonical_block_hash = info.blockHash
    if canonical_block_hash != info.blockHash:
        logger.info(
            "Deploy %s recovered: initial block %s -> canonical block %s (rejection_count=%d)",
            deploy_id[:24], info.blockHash[:16], canonical_block_hash[:16],
            status.rejectionCount,
        )
    canonical_block_number = client.show_block(canonical_block_hash).blockInfo.blockNumber

    data = client.get_data_at_deploy_id(deploy_id, block_hash=canonical_block_hash)
    if data is None:
        raise DeployError(
            f"Deploy {deploy_id[:24]} returned None from get_data_at_deploy_id"
        )
    par_list = list(data.par)
    if not par_list:
        raise DeployError(
            f"Deploy {deploy_id[:24]} returned empty par list from deployId channel"
        )

    return par_list, canonical_block_hash, canonical_block_number


def deploy_with_fallback(
    clients,
    term: str,
    private_key,
    timeout_per_client: int,
    phlo_limit: int = 100_000,
    phlo_price: int = 1,
    valid_after_block_no: Optional[int] = None,
    shard_id: str = "root",
):
    """Submit a deploy, falling back to other clients if inclusion times out.

    Builds the deploy proto once, submits to the first client, polls for
    inclusion. If timed out, resubmits the same signed deploy to the next
    client.

    Args:
        clients: List of F1r3flyClient instances to try.
        term: Rholang code to deploy.
        private_key: PrivateKey for signing.
        timeout_per_client: Seconds to wait for inclusion on each client.
        phlo_limit: Maximum phlo to spend.
        phlo_price: Phlo price per unit.
        valid_after_block_no: If None, auto-filled from the first client.
        shard_id: Target shard identifier.

    Returns:
        Tuple of (deploy_id, block_info).

    Raises:
        TimeoutError: If no client includes the deploy.
    """
    from .util import create_deploy_data

    if valid_after_block_no is None:
        blocks = clients[0].show_blocks(1)
        valid_after_block_no = blocks[0].blockNumber if blocks else 0

    proto = create_deploy_data(
        private_key, term, phlo_price, phlo_limit,
        valid_after_block_no, shard_id=shard_id,
    )
    deploy_id = proto.sig.hex()

    for i, client in enumerate(clients):
        try:
            client.send_deploy(proto)
            logger.info(
                "Deploy %s submitted to client %d/%d",
                deploy_id[:24], i + 1, len(clients),
            )
        except Exception as e:
            logger.warning("Failed to submit deploy to client %d: %s", i + 1, e)
            continue

        try:
            block_info = wait_for_deploy_included(client, deploy_id, timeout_per_client)
            return deploy_id, block_info
        except TimeoutError:
            logger.warning(
                "Deploy %s not included on client %d within %ds, trying next",
                deploy_id[:24], i + 1, timeout_per_client,
            )

    raise TimeoutError(
        f"Deploy {deploy_id[:24]} not included on any of "
        f"{len(clients)} clients (timeout={timeout_per_client}s each)"
    )


class DeployError(Exception):
    """Raised when a deploy fails validation after inclusion."""
