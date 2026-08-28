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
            finalization_absolute_timeout=180,
        )
        value = par_as_int(pars[0])
"""
from __future__ import annotations

import logging
import math
import time
from typing import TYPE_CHECKING, Callable, List, Optional, TypeVar

if TYPE_CHECKING:
    from .client import F1r3flyClient
    from .crypto import PrivateKey
    from .pb.DeployServiceCommon_pb2 import (
        BlockInfo, DeployFinalizationStatusInfo, LightBlockInfo,
    )

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _require_positive_finite_duration(name: str, value: float) -> None:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be a numeric duration, not bool")
    try:
        numeric = float(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive finite duration") from exc
    if not math.isfinite(numeric) or numeric <= 0:
        raise ValueError(f"{name} must be a positive finite duration")


def poll_until(
    predicate: Callable[[], Optional[T]],
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


def wait_for_deploy_included(client: F1r3flyClient, deploy_id: str, timeout: int) -> LightBlockInfo:
    """Poll ``find_deploy`` until the deploy is included in a block.

    Args:
        client: F1r3flyClient instance.
        deploy_id: Deploy signature hex string.
        timeout: Maximum seconds to wait.

    Returns:
        LightBlockInfo for the block containing the deploy.
    """
    def _check() -> Optional[LightBlockInfo]:
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


def wait_for_finalized(client: F1r3flyClient, block_number: int, timeout: int) -> None:
    """Poll until the last finalized block reaches or exceeds ``block_number``.

    Args:
        client: F1r3flyClient instance.
        block_number: Target block number to wait for.
        timeout: Maximum seconds to wait.
    """
    def _check() -> Optional[BlockInfo]:
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
    client: F1r3flyClient,
    deploy_id: str,
    timeout: int,
    interval: float = 3.0,
    *,
    absolute_timeout: int | None = None,
) -> DeployFinalizationStatusInfo:
    """Poll deploy_finalization_status until the deploy reaches Finalized.

    Unlike ``wait_for_finalized`` (which polls block-hash finalization),
    this polls the deploy's actual canonical-state inclusion via
    ``deploy_finalization_status``. A block can finalize while some of its
    deploy effects were dropped by merge rejection; this helper reports the
    deploy's true state.

    Args:
        client: F1r3flyClient instance.
        deploy_id: Deploy signature hex string.
        timeout: Maximum seconds without a strict LFB-height advance when
            ``absolute_timeout`` is set; otherwise the fixed total timeout.
        interval: Seconds between polls.
        absolute_timeout: Optional non-renewable total timeout. When set, a
            strict increase in the observed LFB block number renews the stall
            budget, while same-height hash changes do not. Success still
            requires the target deploy's exact Finalized status.

    Returns:
        DeployFinalizationStatusInfo with state=DEPLOY_STATE_FINALIZED.

    Raises:
        DeployError: If the deploy reaches terminal DEPLOY_STATE_FAILED
            (explicit Rholang failure) or DEPLOY_STATE_EXPIRED (past
            deployLifespan without successful inclusion).
        FinalizedHistoryError: If the observed LFB regresses or changes hash at
            the same height.
        TimeoutError: If the observation exhausts the fixed timeout, the
            progress stall budget, or the absolute timeout.
        ValueError: If a duration is non-positive or non-finite, or if the
            absolute timeout is shorter than the stall timeout.
        TypeError: If a boolean is supplied as a duration.
    """
    from .pb.DeployServiceCommon_pb2 import (
        DEPLOY_STATE_EXPIRED, DEPLOY_STATE_FAILED, DEPLOY_STATE_FINALIZED,
    )
    _require_positive_finite_duration("timeout", timeout)
    _require_positive_finite_duration("interval", interval)
    if absolute_timeout is not None:
        _require_positive_finite_duration("absolute_timeout", absolute_timeout)
    if absolute_timeout is not None and absolute_timeout < timeout:
        raise ValueError("absolute_timeout must be greater than or equal to timeout")

    started_at = time.monotonic()
    stall_deadline = started_at + timeout
    absolute_deadline = started_at + (
        absolute_timeout if absolute_timeout is not None else timeout
    )
    attempts = 0
    last_status_err: Exception | None = None
    last_lfb_err: Exception | None = None
    last_info = None
    last_lfb_number: int | None = None
    last_lfb_hash = ""
    progress_count = 0
    timeout_reason = "fixed"
    late_observation = ""

    def _remaining_budget(now: float) -> float:
        deadlines = [absolute_deadline]
        if absolute_timeout is not None:
            deadlines.append(stall_deadline)
        return min(deadlines) - now

    def _rpc_timeout(now: float) -> float:
        remaining = max(0.0, _remaining_budget(now))
        try:
            configured = float(getattr(client, "timeout", remaining))
        except (OverflowError, TypeError, ValueError):
            configured = remaining
        if not math.isfinite(configured) or configured <= 0:
            configured = remaining
        return min(configured, remaining)

    def _state_name(info: DeployFinalizationStatusInfo | None) -> str:
        if info is None:
            return "Unknown"
        return {
            DEPLOY_STATE_FINALIZED: "Finalized",
            DEPLOY_STATE_FAILED: "Failed",
            DEPLOY_STATE_EXPIRED: "Expired",
        }.get(info.state, "Pending")

    while True:
        now = time.monotonic()
        if now >= absolute_deadline:
            timeout_reason = "absolute" if absolute_timeout is not None else "fixed"
            break
        if absolute_timeout is not None and now >= stall_deadline:
            timeout_reason = "stalled"
            break

        attempts += 1
        try:
            status_rpc_timeout = _rpc_timeout(time.monotonic())
            if status_rpc_timeout <= 0:
                continue
            info = client.deploy_finalization_status(
                deploy_id,
                timeout=status_rpc_timeout,
            )
            last_info = info
            last_status_err = None
            observed_at = time.monotonic()
            if observed_at >= absolute_deadline or (
                absolute_timeout is not None and observed_at >= stall_deadline
            ):
                timeout_reason = (
                    "absolute" if observed_at >= absolute_deadline else "stalled"
                )
                late_observation = f"status {_state_name(info)} observed after deadline"
                break
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
        except Exception as e:  # noqa: BLE001
            last_status_err = e

        if absolute_timeout is not None:
            try:
                lfb_rpc_timeout = _rpc_timeout(time.monotonic())
                if lfb_rpc_timeout <= 0:
                    continue
                lfb = client.last_finalized_block(
                    timeout=lfb_rpc_timeout
                ).blockInfo
                last_lfb_err = None
                observed_at = time.monotonic()
                lfb_number = int(lfb.blockNumber)
                lfb_hash = str(lfb.blockHash)
                if observed_at >= absolute_deadline or observed_at >= stall_deadline:
                    timeout_reason = (
                        "absolute" if observed_at >= absolute_deadline else "stalled"
                    )
                    break
                if last_lfb_number is None:
                    last_lfb_number = lfb_number
                    last_lfb_hash = lfb_hash
                elif lfb_number > last_lfb_number:
                    last_lfb_number = lfb_number
                    last_lfb_hash = lfb_hash
                    stall_deadline = observed_at + timeout
                    progress_count += 1
                elif lfb_number < last_lfb_number:
                    raise FinalizedHistoryError(
                        f"Finalized history regressed while waiting for deploy "
                        f"{deploy_id[:24]}: LFB #{last_lfb_number} "
                        f"{last_lfb_hash[:16]} -> #{lfb_number} {lfb_hash[:16]}"
                    )
                elif lfb_hash != last_lfb_hash:
                    raise FinalizedHistoryError(
                        f"Finalized history revised at equal height while waiting "
                        f"for deploy {deploy_id[:24]}: LFB #{lfb_number} "
                        f"{last_lfb_hash[:16]} -> {lfb_hash[:16]}"
                    )
            except FinalizedHistoryError:
                raise
            except Exception as e:  # noqa: BLE001
                last_lfb_err = e

        remaining = min(absolute_deadline, stall_deadline) - time.monotonic()
        if remaining <= 0:
            continue
        time.sleep(min(interval, remaining))

    status_detail = (
        f" (last state: {_state_name(last_info)}, "
        f"rejection_count={last_info.rejectionCount})"
        if last_info is not None
        else ""
    )
    error_parts = []
    if last_status_err is not None:
        error_parts.append(f"status error: {last_status_err}")
    if last_lfb_err is not None:
        error_parts.append(f"LFB error: {last_lfb_err}")
    err_detail = f" (last errors: {'; '.join(error_parts)})" if error_parts else ""
    late_detail = f" ({late_observation})" if late_observation else ""
    elapsed = time.monotonic() - started_at
    elapsed_detail = (
        f"{elapsed:.1f}s" if absolute_timeout is not None else f"{timeout}s"
    )
    progress_detail = ""
    if absolute_timeout is not None:
        progress_detail = (
            f" (reason={timeout_reason}, progress_count={progress_count}, "
            f"last_lfb_number={last_lfb_number}, last_lfb_hash={last_lfb_hash[:16]})"
        )
    raise TimeoutError(
        f"deploy {deploy_id[:24]} finalization: timed out after {elapsed_detail} "
        f"({attempts} attempts){progress_detail}{status_detail}{late_detail}{err_detail}"
    )


def deploy_and_read(
    client: F1r3flyClient,
    term: str,
    private_key: PrivateKey,
    inclusion_timeout: int,
    finalization_timeout: int,
    shard_id: str = "root",
    finalization_absolute_timeout: int | None = None,
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
        shard_id: Target shard identifier.
        finalization_absolute_timeout: Optional total finalization bound. When
            set, ``finalization_timeout`` is the no-progress bound renewed only
            by strict LFB-height advances.

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
        shard_id=shard_id,
    )
    logger.info("Deployed, deploy_id=%s", deploy_id[:24])

    info = wait_for_deploy_included(client, deploy_id, inclusion_timeout)
    logger.info(
        "Deploy included in block #%d (%s)", info.blockNumber, info.blockHash[:16]
    )

    status = wait_for_deploy_finalized(
        client,
        deploy_id,
        finalization_timeout,
        absolute_timeout=finalization_absolute_timeout,
    )

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
    clients: List[F1r3flyClient],
    term: str,
    private_key: PrivateKey,
    timeout_per_client: int,
    valid_after_block_no: Optional[int] = None,
    shard_id: str = "root",
) -> tuple:
    """Submit a deploy, falling back to other clients if inclusion times out.

    Builds the deploy proto once, submits to the first client, polls for
    inclusion. If timed out, resubmits the same signed deploy to the next
    client.

    Args:
        clients: List of F1r3flyClient instances to try.
        term: Rholang code to deploy.
        private_key: PrivateKey for signing.
        timeout_per_client: Seconds to wait for inclusion on each client.
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
        private_key, term, valid_after_block_no, shard_id=shard_id,
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


class FinalizedHistoryError(DeployError):
    """Raised when one node revises or regresses its published finalized history."""
