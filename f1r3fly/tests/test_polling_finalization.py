from types import SimpleNamespace

import pytest

from f1r3fly.client import F1r3flyClient
from f1r3fly.pb.DeployServiceCommon_pb2 import (
    DEPLOY_STATE_EXPIRED, DEPLOY_STATE_FAILED, DEPLOY_STATE_FINALIZED,
    DEPLOY_STATE_PENDING,
)
from f1r3fly.polling import (
    DeployError, FinalizedHistoryError, wait_for_deploy_finalized,
)


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, duration):
        self.now += duration


class ScheduledClient:
    def __init__(self, clock, finalized_at, lfb_schedule):
        self.clock = clock
        self.finalized_at = finalized_at
        self.lfb_schedule = lfb_schedule

    def deploy_finalization_status(self, deploy_id, timeout=None):
        state = (
            DEPLOY_STATE_FINALIZED
            if self.clock.now >= self.finalized_at
            else DEPLOY_STATE_PENDING
        )
        return SimpleNamespace(state=state, rejectionCount=0, latestBlockHash=b"")

    def last_finalized_block(self, timeout=None):
        height, block_hash = self.lfb_schedule(self.clock.now)
        return SimpleNamespace(
            blockInfo=SimpleNamespace(blockNumber=height, blockHash=block_hash)
        )


def install_clock(monkeypatch, clock):
    monkeypatch.setattr("f1r3fly.polling.time.monotonic", clock.monotonic)
    monkeypatch.setattr("f1r3fly.polling.time.sleep", clock.sleep)


def reproduced_lfb_schedule(now):
    if now >= 43:
        return 7, "floor-7"
    if now >= 22:
        return 6, "floor-6"
    return 4, "floor-4"


def test_fixed_timeout_rejects_reproduced_live_trace(monkeypatch):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = ScheduledClient(
        clock, finalized_at=49, lfb_schedule=reproduced_lfb_schedule
    )

    with pytest.raises(TimeoutError, match="timed out after 45s"):
        wait_for_deploy_finalized(client, "deploy", timeout=45, interval=1)


def test_progress_aware_wait_accepts_exact_terminal_status(monkeypatch):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = ScheduledClient(
        clock, finalized_at=49, lfb_schedule=reproduced_lfb_schedule
    )

    result = wait_for_deploy_finalized(
        client,
        "deploy",
        timeout=45,
        interval=1,
        absolute_timeout=135,
    )

    assert result.state == DEPLOY_STATE_FINALIZED
    assert clock.now == 49


def test_same_height_observations_do_not_renew_stall_budget(monkeypatch):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = ScheduledClient(
        clock,
        finalized_at=100,
        lfb_schedule=lambda now: (4, "floor-4"),
    )

    with pytest.raises(TimeoutError, match="reason=stalled, progress_count=0"):
        wait_for_deploy_finalized(
            client,
            "deploy",
            timeout=3,
            interval=1,
            absolute_timeout=8,
        )

    assert clock.now == 3


def test_absolute_bound_cannot_be_renewed_by_continuous_progress(monkeypatch):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = ScheduledClient(
        clock,
        finalized_at=100,
        lfb_schedule=lambda now: (int(now) // 2, f"floor-{int(now) // 2}"),
    )

    with pytest.raises(TimeoutError, match="reason=absolute"):
        wait_for_deploy_finalized(
            client,
            "deploy",
            timeout=3,
            interval=1,
            absolute_timeout=8,
        )

    assert clock.now == 8


def test_intermediate_lfb_progress_is_not_target_success(monkeypatch):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = ScheduledClient(
        clock,
        finalized_at=100,
        lfb_schedule=lambda now: (int(now), f"floor-{int(now)}"),
    )

    with pytest.raises(TimeoutError, match="reason=absolute"):
        wait_for_deploy_finalized(
            client,
            "deploy",
            timeout=3,
            interval=1,
            absolute_timeout=8,
        )


class DeadlineConsumingClient(ScheduledClient):
    timeout = 30

    def __init__(self, clock, terminal_state=DEPLOY_STATE_PENDING):
        super().__init__(
            clock, finalized_at=100, lfb_schedule=lambda now: (0, "floor-0")
        )
        self.terminal_state = terminal_state
        self.received_timeouts = []

    def deploy_finalization_status(self, deploy_id, timeout=None):
        self.received_timeouts.append(timeout)
        self.clock.now += timeout
        return SimpleNamespace(
            state=self.terminal_state,
            rejectionCount=0,
            latestBlockHash=b"",
        )


def test_rpc_timeout_is_capped_by_remaining_stall_budget(monkeypatch):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = DeadlineConsumingClient(clock)

    with pytest.raises(TimeoutError, match="reason=stalled"):
        wait_for_deploy_finalized(
            client,
            "deploy",
            timeout=3,
            interval=1,
            absolute_timeout=8,
        )

    assert client.received_timeouts == [3]
    assert clock.now == 3


def test_rpc_timeout_is_capped_by_remaining_absolute_budget(monkeypatch):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = DeadlineConsumingClient(clock)

    with pytest.raises(TimeoutError, match="reason=absolute"):
        wait_for_deploy_finalized(
            client,
            "deploy",
            timeout=10,
            interval=1,
            absolute_timeout=10,
        )

    assert client.received_timeouts == [10]
    assert clock.now == 10


def test_fixed_wait_caps_rpc_to_its_remaining_budget(monkeypatch):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = DeadlineConsumingClient(clock)

    with pytest.raises(TimeoutError, match="timed out after 3s"):
        wait_for_deploy_finalized(
            client,
            "deploy",
            timeout=3,
            interval=1,
        )

    assert client.received_timeouts == [3]
    assert clock.now == 3


@pytest.mark.parametrize(
    ("terminal_state", "state_name"),
    [
        (DEPLOY_STATE_FINALIZED, "Finalized"),
        (DEPLOY_STATE_FAILED, "Failed"),
        (DEPLOY_STATE_EXPIRED, "Expired"),
    ],
)
def test_late_terminal_response_cannot_bypass_deadline(
    monkeypatch, terminal_state, state_name
):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = DeadlineConsumingClient(clock, terminal_state=terminal_state)

    with pytest.raises(
        TimeoutError,
        match=rf"last state: {state_name}.*status {state_name} observed after deadline",
    ):
        wait_for_deploy_finalized(
            client,
            "deploy",
            timeout=3,
            interval=1,
            absolute_timeout=8,
        )


@pytest.mark.parametrize(
    ("terminal_state", "state_name"),
    [
        (DEPLOY_STATE_FAILED, "Failed"),
        (DEPLOY_STATE_EXPIRED, "Expired"),
    ],
)
def test_in_budget_failed_and_expired_are_terminal_errors(
    monkeypatch, terminal_state, state_name
):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = DeadlineConsumingClient(clock, terminal_state=terminal_state)
    client.timeout = 1

    with pytest.raises(DeployError, match=rf"terminal state {state_name}"):
        wait_for_deploy_finalized(
            client,
            "deploy",
            timeout=3,
            interval=1,
            absolute_timeout=8,
        )

    assert clock.now == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout": 0, "interval": 1},
        {"timeout": -1, "interval": 1},
        {"timeout": float("nan"), "interval": 1},
        {"timeout": float("inf"), "interval": 1},
        {"timeout": 3, "interval": 0},
        {"timeout": 3, "interval": float("nan")},
        {"timeout": 3, "interval": 1, "absolute_timeout": float("inf")},
    ],
)
def test_invalid_observation_durations_are_rejected(monkeypatch, kwargs):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = ScheduledClient(
        clock,
        finalized_at=100,
        lfb_schedule=lambda now: (0, "floor-0"),
    )

    with pytest.raises(ValueError, match="positive finite duration"):
        wait_for_deploy_finalized(client, "deploy", **kwargs)

    assert clock.now == 0


def test_boolean_duration_is_rejected_as_a_type_error(monkeypatch):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = ScheduledClient(
        clock,
        finalized_at=100,
        lfb_schedule=lambda now: (0, "floor-0"),
    )

    with pytest.raises(TypeError, match="numeric duration, not bool"):
        wait_for_deploy_finalized(client, "deploy", timeout=True)

    assert clock.now == 0


@pytest.mark.parametrize("configured_timeout", [0, -1, float("nan"), float("inf")])
def test_invalid_client_rpc_timeout_cannot_escape_observation_budget(
    monkeypatch, configured_timeout
):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = DeadlineConsumingClient(clock)
    client.timeout = configured_timeout

    with pytest.raises(TimeoutError, match="reason=stalled"):
        wait_for_deploy_finalized(
            client,
            "deploy",
            timeout=3,
            interval=1,
            absolute_timeout=8,
        )

    assert client.received_timeouts == [3]
    assert clock.now == 3


@pytest.mark.parametrize(
    ("lfb_schedule", "message"),
    [
        (lambda now: (6, "other") if now >= 1 else (6, "first"), "revised"),
        (lambda now: (5, "older") if now >= 1 else (6, "first"), "regressed"),
    ],
)
def test_finalized_history_anomalies_fail_loudly(monkeypatch, lfb_schedule, message):
    clock = FakeClock()
    install_clock(monkeypatch, clock)
    client = ScheduledClient(clock, finalized_at=100, lfb_schedule=lfb_schedule)

    with pytest.raises(FinalizedHistoryError, match=message):
        wait_for_deploy_finalized(
            client,
            "deploy",
            timeout=3,
            interval=1,
            absolute_timeout=8,
        )


def test_first_successful_lfb_sample_is_baseline_not_progress(monkeypatch):
    clock = FakeClock()
    install_clock(monkeypatch, clock)

    class DelayedBaselineClient(ScheduledClient):
        def last_finalized_block(self, timeout=None):
            if self.clock.now < 1:
                raise ConnectionError("baseline unavailable")
            return super().last_finalized_block(timeout=timeout)

    client = DelayedBaselineClient(
        clock,
        finalized_at=100,
        lfb_schedule=lambda now: (6, "floor-6"),
    )

    with pytest.raises(TimeoutError, match="reason=stalled, progress_count=0"):
        wait_for_deploy_finalized(
            client,
            "deploy",
            timeout=3,
            interval=1,
            absolute_timeout=8,
        )

    assert clock.now == 3


def test_client_forwards_explicit_deadlines_to_both_observer_rpcs():
    calls = []
    lfb_payload = object()
    status_payload = object()

    class Stub:
        def lastFinalizedBlock(self, query, *, timeout):
            calls.append(("lfb", timeout))
            return SimpleNamespace(blockInfo=lfb_payload)

        def deployFinalizationStatus(self, query, *, timeout):
            calls.append(("status", timeout))
            return SimpleNamespace(status=status_payload)

    client = F1r3flyClient.__new__(F1r3flyClient)
    client.timeout = 30
    client._deploy_stub = Stub()
    client._check_response = lambda response: None

    assert client.last_finalized_block(timeout=3.5) is lfb_payload
    assert client.deploy_finalization_status("00", timeout=2.5) is status_payload
    assert calls == [("lfb", 3.5), ("status", 2.5)]
