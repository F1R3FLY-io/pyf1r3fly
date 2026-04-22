"""WebSocket event streaming client utilities.

Provides connection management, event collection, and validation for
the F1R3FLY node's ``/ws/events`` WebSocket endpoint.

The node emits structured JSON events for block lifecycle, genesis
ceremony, and node startup. Startup events are buffered and replayed
to new subscribers on connect.

Example::

    from f1r3fly.websocket import connect_ws, wait_for_events, BLOCK_LIFECYCLE_EVENTS

    events, errors = [], []
    ws, thread = connect_ws("ws://localhost:40403/ws/events", events, errors)
    wait_for_events(events, BLOCK_LIFECYCLE_EVENTS, timeout=60)
    ws.close()
    thread.join()

Event types:
    Block lifecycle (continuous):
        block-created, block-added, block-finalised

    Genesis ceremony (once at startup, replayed to new subscribers):
        sent-unapproved-block, block-approval-received,
        sent-approved-block, approved-block-received,
        entered-running-state

    Node lifecycle:
        node-started
"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Dict, List, Set

import websocket as ws_lib

logger = logging.getLogger(__name__)

# ── Event type constants ────────────────────────────────────────────

BLOCK_LIFECYCLE_EVENTS = {"block-created", "block-added", "block-finalised"}

BLOCK_PAYLOAD_FIELDS = {
    "block-hash", "block-number", "timestamp",
    "parent-hashes", "justification-hashes",
    "deploys", "creator", "seq-num",
}

VALIDATOR_STARTUP_EVENTS = {
    "node-started",
    "approved-block-received",
    "entered-running-state",
}

BOOT_GENESIS_EVENTS = {
    "sent-unapproved-block",
    "sent-approved-block",
}

BOOT_STARTUP_EVENTS = BOOT_GENESIS_EVENTS | {
    "node-started",
    "entered-running-state",
}

EXPECTED_VALIDATOR_EVENTS = VALIDATOR_STARTUP_EVENTS | BLOCK_LIFECYCLE_EVENTS
EXPECTED_BOOT_EVENTS = BOOT_STARTUP_EVENTS | {"block-added", "block-finalised"}

# Known disconnect errors that are expected when the test tears down
_EXPECTED_DISCONNECT_ERRORS = (
    "Connection to remote host was lost",
    "Connection reset by peer",
    "Connection refused",
)


# ── Connection ──────────────────────────────────────────────────────

def connect_ws(
    ws_url: str,
    events: List[dict],
    errors: List[str],
    timeout: int = 30,
) -> tuple:
    """Connect a WebSocket client with retry.

    Messages are parsed as JSON and appended to ``events``.
    Errors are appended to ``errors`` (expected disconnect errors
    are filtered out).

    Args:
        ws_url: WebSocket URL (e.g. ``ws://localhost:40403/ws/events``).
        events: List to append received events to (shared with caller).
        errors: List to append error messages to (shared with caller).
        timeout: Maximum seconds to wait for connection.

    Returns:
        Tuple of (ws_app, ws_thread). Caller must close the ws_app
        and join the thread when done.

    Raises:
        AssertionError: If connection fails within timeout.
    """
    connected = threading.Event()

    def on_message(ws, message):
        try:
            event = json.loads(message)
            events.append(event)
            logger.debug("WS event: %s", event.get("event", "unknown"))
        except json.JSONDecodeError as e:
            errors.append(f"Bad JSON: {e}")

    def on_error(ws, error):
        msg = str(error)
        if any(expected in msg for expected in _EXPECTED_DISCONNECT_ERRORS):
            return
        errors.append(msg)

    def on_open(ws):
        logger.info("WebSocket connected to %s", ws_url)
        connected.set()

    deadline = time.time() + timeout
    ws_app = None
    ws_thread = None

    while time.time() < deadline:
        errors.clear()
        connected.clear()
        ws_app = ws_lib.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_open=on_open,
        )
        ws_thread = threading.Thread(target=ws_app.run_forever, daemon=True)
        ws_thread.start()

        if connected.wait(timeout=5):
            return ws_app, ws_thread

        ws_app.close()
        ws_thread.join(timeout=3)
        ws_app = None
        ws_thread = None
        time.sleep(1)

    raise AssertionError(
        f"WebSocket failed to connect to {ws_url} within {timeout}s. "
        f"Errors: {errors}"
    )


# ── Event waiting and validation ────────────────────────────────────

def wait_for_events(
    events: List[dict],
    required: Set[str],
    timeout: int = 60,
) -> None:
    """Poll until all required event types have been seen.

    Args:
        events: List of received events (populated by connect_ws).
        required: Set of event type strings to wait for.
        timeout: Maximum seconds to wait.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        seen = {e.get("event") for e in events}
        if required <= seen:
            return
        time.sleep(1)


def validate_block_event(event: dict) -> None:
    """Validate a block lifecycle event has correct structure.

    Checks schema-version, payload presence, and required payload fields.

    Raises:
        AssertionError: If any validation fails.
    """
    event_type = event["event"]
    assert event.get("schema-version") == 1, (
        f"{event_type} missing or wrong schema-version: {event}"
    )
    assert "payload" in event, f"{event_type} missing payload: {event}"
    payload = event["payload"]
    missing = BLOCK_PAYLOAD_FIELDS - set(payload.keys())
    assert not missing, (
        f"{event_type} payload missing fields: {sorted(missing)}"
    )
    assert isinstance(payload["block-hash"], str) and len(payload["block-hash"]) > 0
    assert isinstance(payload["block-number"], int) and payload["block-number"] >= 0
    assert isinstance(payload["timestamp"], int) and payload["timestamp"] > 0
    assert isinstance(payload["parent-hashes"], list)
    assert isinstance(payload["deploys"], list)
    assert isinstance(payload["seq-num"], int)


def log_event_counts(events: List[dict], label: str) -> None:
    """Log a summary of event type counts."""
    counts: Dict[str, int] = {}
    for e in events:
        t = e.get("event", "unknown")
        counts[t] = counts.get(t, 0) + 1
    logger.info(
        "%s: %d events (%s)", label, len(events),
        ", ".join(f"{t}:{counts[t]}" for t in sorted(counts)),
    )
