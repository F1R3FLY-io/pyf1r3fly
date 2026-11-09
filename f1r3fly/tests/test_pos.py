"""Unit tests for the PoS domain API and byte-array Par decoding.

Term construction and result/map decoding are exercised against real
RhoTypes protobuf Par messages and a mocked client — no node required.
"""
from unittest.mock import Mock

from ..par import par_as_bytes, par_as_map, par_value
from ..pb import RhoTypes_pb2 as rt
from ..pos import PosAPI

# ── Par builders ─────────────────────────────────────────────────────────

def _int_par(n: int) -> rt.Par:
    p = rt.Par()
    p.exprs.add().g_int = n
    return p


def _bool_par(b: bool) -> rt.Par:
    p = rt.Par()
    p.exprs.add().g_bool = b
    return p


def _str_par(s: str) -> rt.Par:
    p = rt.Par()
    p.exprs.add().g_string = s
    return p


def _bytes_par(b: bytes) -> rt.Par:
    p = rt.Par()
    p.exprs.add().g_byte_array = b
    return p


def _nil_par() -> rt.Par:
    return rt.Par()


def _tuple_par(elems) -> rt.Par:
    p = rt.Par()
    e = p.exprs.add()
    for el in elems:
        e.e_tuple_body.ps.append(el)
    return p


def _map_par(pairs) -> rt.Par:
    p = rt.Par()
    e = p.exprs.add()
    e.e_map_body.SetInParent()
    for k, v in pairs:
        kv = e.e_map_body.kvs.add()
        kv.key.CopyFrom(k)
        kv.value.CopyFrom(v)
    return p


# ── byte-array decoding (par.py additions) ─────────────────────────────────

def test_par_as_bytes():
    raw = bytes.fromhex("04fa70d7be")
    assert par_as_bytes(_bytes_par(raw)) == raw


def test_par_value_byte_array_is_hex():
    raw = bytes.fromhex("0457febafc")
    assert par_value(_bytes_par(raw)) == "0457febafc"


def test_par_as_map_with_byte_array_keys():
    pk = bytes.fromhex("04abcd")
    m = _map_par([(_bytes_par(pk), _int_par(100))])
    assert par_as_map(m) == {"04abcd": 100}


def test_par_value_nil_is_none():
    assert par_value(_nil_par()) is None


def test_par_as_map_with_nil_values():
    m = _map_par([(_str_par("to"), _nil_par()), (_str_par("topup"), _int_par(5))])
    assert par_as_map(m) == {"to": None, "topup": 5}


# ── PoS term construction ──────────────────────────────────────────────────

def test_bond_term_substitutes_amount():
    client = Mock()
    client.deploy_with_vabn_filled.return_value = "deadbeef"
    api = PosAPI(client, shard_id="root")
    did = api.bond(Mock(), 200)
    assert did == "deadbeef"
    contract = client.deploy_with_vabn_filled.call_args.args[1]
    assert '"bond", *deployerId, 200, *retCh' in contract
    assert "deployId!(result)" in contract


def test_withdraw_term():
    client = Mock()
    client.deploy_with_vabn_filled.return_value = "cafe"
    api = PosAPI(client)
    did = api.withdraw(Mock())
    assert did == "cafe"
    contract = client.deploy_with_vabn_filled.call_args.args[1]
    assert '"withdraw", *deployerId, *retCh' in contract


def test_rejoin_from_ejection_term_substitutes_topup():
    client = Mock()
    client.deploy_with_vabn_filled.return_value = "f00d"
    did = PosAPI(client).rejoin_from_ejection(Mock(), 150)
    assert did == "f00d"
    contract = client.deploy_with_vabn_filled.call_args.args[1]
    assert '"rejoinFromEjection", *deployerId, 150, *retCh' in contract
    assert "deployId!(result)" in contract


# ── (Boolean, message) return parsing ──────────────────────────────────────

def test_read_result_success():
    client = Mock()
    client.get_data_at_deploy_id.return_value = Mock(
        par=[_tuple_par([_bool_par(True), _nil_par()])])
    res = PosAPI(client).read_result("d1")
    assert res.success is True
    assert res.reason == ""


def test_read_result_rejection():
    client = Mock()
    client.get_data_at_deploy_id.return_value = Mock(
        par=[_tuple_par([_bool_par(False), _str_par("Public key is already bonded.")])])
    res = PosAPI(client).read_result("d2")
    assert res.success is False
    assert res.reason == "Public key is already bonded."


def test_read_result_no_data():
    client = Mock()
    client.get_data_at_deploy_id.return_value = Mock(par=[])
    res = PosAPI(client).read_result("d3")
    assert res.success is False
    assert res.reason == "no data"


# ── Exploratory map reads ───────────────────────────────────────────────────

def test_get_bonds_int_map():
    pk = bytes.fromhex("04aa")
    client = Mock()
    client.exploratory_deploy.return_value = [_map_par([(_bytes_par(pk), _int_par(100))])]
    assert PosAPI(client).get_bonds() == {"04aa": 100}


def test_get_pending_withdrawer_int_map():
    pk = bytes.fromhex("04bb")
    client = Mock()
    client.exploratory_deploy.return_value = [_map_par([(_bytes_par(pk), _int_par(18))])]
    assert PosAPI(client).get_pending_withdrawer() == {"04bb": 18}


def test_get_withdrawers_tuple_map():
    pk = bytes.fromhex("04cc")
    pair = _tuple_par([_int_par(250), _int_par(18)])  # (bond+reward, quarantineUntil)
    client = Mock()
    client.exploratory_deploy.return_value = [_map_par([(_bytes_par(pk), pair)])]
    assert PosAPI(client).get_withdrawers() == {"04cc": (250, 18)}


def _set_par(elems) -> rt.Par:
    p = rt.Par()
    e = p.exprs.add()
    for el in elems:
        e.e_set_body.ps.append(el)
    return p


def test_get_active_validators_set():
    client = Mock()
    client.exploratory_deploy.return_value = [
        _set_par([_bytes_par(bytes.fromhex("04aa")), _bytes_par(bytes.fromhex("04bb"))])]
    api = PosAPI(client)
    assert api.get_active_validators() == {"04aa", "04bb"}
    assert '@PoS!("getActiveValidators", *return)' in client.exploratory_deploy.call_args.args[0]


def test_get_ejected_validators_nested_records():
    target, voter = bytes.fromhex("04dd"), bytes.fromhex("04ee")
    record = _map_par([
        (_str_par("graceEnd"), _int_par(64)),
        (_str_par("ejectFloor"), _int_par(24)),
        (_str_par("ejectFloorHash"), _bytes_par(bytes.fromhex("abcd"))),
        (_str_par("ejectionSnapshot"), _map_par([(_bytes_par(voter), _int_par(100))])),
    ])
    client = Mock()
    client.exploratory_deploy.return_value = [_map_par([(_bytes_par(target), record)])]
    assert PosAPI(client).get_ejected_validators(block_hash="ff") == {
        "04dd": {
            "graceEnd": 64,
            "ejectFloor": 24,
            "ejectFloorHash": "abcd",
            "ejectionSnapshot": {"04ee": 100},
        }
    }
    assert client.exploratory_deploy.call_args.args[1] == "ff"


def test_get_freeze_history_open_and_closed_freezes():
    pk = bytes.fromhex("04dd")
    freezes = _map_par([
        (_int_par(24), _map_par([(_str_par("to"), _int_par(31)), (_str_par("topup"), _int_par(100))])),
        (_int_par(70), _map_par([(_str_par("to"), _nil_par()), (_str_par("topup"), _nil_par())])),
    ])
    client = Mock()
    client.exploratory_deploy.return_value = [_map_par([(_bytes_par(pk), freezes)])]
    assert PosAPI(client).get_freeze_history() == {
        "04dd": {24: {"to": 31, "topup": 100}, 70: {"to": None, "topup": None}}
    }


def test_get_pending_rejoins_and_ejection_evidence_read_their_methods():
    client = Mock()
    client.exploratory_deploy.return_value = [_map_par([])]
    api = PosAPI(client)
    assert api.get_pending_rejoins() == {}
    assert '@PoS!("getPendingRejoins", *return)' in client.exploratory_deploy.call_args.args[0]
    assert api.get_ejection_evidence() == {}
    assert '@PoS!("getEjectionEvidence", *return)' in client.exploratory_deploy.call_args.args[0]
