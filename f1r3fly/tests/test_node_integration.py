"""
Integration tests for pyf1r3fly client against a live f1r3node.

Requires a running shard. Run with:
    pytest f1r3fly/tests/test_node_integration.py -v -s

Tests the client methods needed by system-integration's test_bridge_admin.py:
deploy, find_deploy, last_finalized_block, get_data_at_deploy_id (with block hash),
and exploratory_deploy.
"""

import time

import pytest

from f1r3fly.client import F1r3flyClient
from f1r3fly.crypto import PrivateKey
from f1r3fly.util import create_deploy_data

VALIDATOR1_GRPC = 40411
READONLY_GRPC = 40451
PRIVATE_KEY = PrivateKey.from_hex(
    "357cdc4201a5650830e0bc5a03299a30038d9934ba4c7ab73ec164ad82471ff9"
)


def _is_node_available() -> bool:
    try:
        with F1r3flyClient("localhost", VALIDATOR1_GRPC) as client:
            client.last_finalized_block()
        return True
    except Exception:
        return False


skip_no_node = pytest.mark.skipif(
    not _is_node_available(),
    reason="No running node on localhost:40411",
)


def _deploy_and_finalize(code: str) -> tuple:
    """Deploy code, wait for block inclusion and finalization.

    Returns (deploy_id, block_hash, block_number).
    """
    with F1r3flyClient("localhost", VALIDATOR1_GRPC) as client:
        lfb = client.last_finalized_block()
        vabn = max(0, lfb.blockInfo.blockNumber - 1)
        deploy_data = create_deploy_data(
            PRIVATE_KEY, code, 1, 500_000_000, vabn,
            int(time.time() * 1000), "root",
        )
        deploy_id = client.send_deploy(deploy_data)

    # Wait for block inclusion (30s)
    block_hash = None
    block_number = 0
    for _ in range(10):
        time.sleep(3)
        try:
            with F1r3flyClient("localhost", VALIDATOR1_GRPC) as client:
                block = client.find_deploy(deploy_id)
                block_hash = block.blockHash
                block_number = block.blockNumber
                break
        except Exception:
            pass
    assert block_hash, f"Deploy {deploy_id[:24]} not in block within 30s"

    # Wait for finalization (60s)
    for _ in range(12):
        time.sleep(5)
        with F1r3flyClient("localhost", VALIDATOR1_GRPC) as client:
            lfb = client.last_finalized_block()
            if lfb.blockInfo.blockNumber >= block_number:
                return deploy_id, block_hash, block_number

    pytest.fail(f"LFB did not reach #{block_number} within 60s")


@skip_no_node
def test_deploy_find_and_read_data():
    """Deploy code that writes to deployId, find it, wait for finalization, read it back."""
    deploy_id, block_hash, block_number = _deploy_and_finalize(
        'new deployId(`rho:system:deployId`) in { deployId!(42) }'
    )

    with F1r3flyClient("localhost", VALIDATOR1_GRPC) as client:
        data = client.get_data_at_deploy_id(deploy_id, block_hash=block_hash)

    assert data is not None, "get_data_at_deploy_id returned None — no data on deployId channel"
    assert len(data.par) > 0, f"Empty par list: {data}"
    found_42 = any(
        expr.g_int == 42
        for par in data.par
        for expr in par.exprs
    )
    assert found_42, f"Expected g_int=42, got: {data}"


@skip_no_node
def test_exploratory_deploy():
    """Run exploratory deploy on readonly node."""
    with F1r3flyClient("localhost", READONLY_GRPC) as client:
        lfb = client.last_finalized_block()
        result = client.exploratory_deploy('new x in { x!(99) }', lfb.blockInfo.blockHash)

    assert len(result) > 0, f"Exploratory deploy returned empty: {result}"
    found_99 = any(
        expr.g_int == 99
        for par in result
        for expr in par.exprs
    )
    assert found_99, f"Expected g_int=99, got: {result}"
