"""Unit tests for Int and BigInt decoding in the Par helpers."""
from unittest.mock import Mock

import pytest

from ..par import par_as_int, par_value
from ..pb import RhoTypes_pb2 as rt
from ..vault import VaultAPI


def _int_par(n: int) -> rt.Par:
    p = rt.Par()
    p.exprs.add().g_int = n
    return p


def _big_int_par(n: int) -> rt.Par:
    length = (n + (n < 0)).bit_length() // 8 + 1
    p = rt.Par()
    p.exprs.add().g_big_int = n.to_bytes(length, "big", signed=True)
    return p


@pytest.mark.parametrize("n", [0, 1, -1, 9_000_000, 2**63 - 1, 10**30, 2**256 - 1, -(10**30)])
def test_par_as_int_decodes_big_int(n: int) -> None:
    assert par_as_int(_big_int_par(n)) == n
    assert par_value(_big_int_par(n)) == n


def test_par_as_int_decodes_empty_big_int_as_zero() -> None:
    p = rt.Par()
    p.exprs.add().g_big_int = b""
    assert par_as_int(p) == 0


def test_par_as_int_still_decodes_int() -> None:
    assert par_as_int(_int_par(-42)) == -42
    assert par_value(_int_par(42)) == 42


def test_get_balance_reads_big_int_result() -> None:
    client = Mock()
    client.exploratory_deploy.return_value = [_big_int_par(10**30)]
    assert VaultAPI(client).get_balance("1111addr") == 10**30
