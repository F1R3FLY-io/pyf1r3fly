import pytest
from ecdsa.keys import BadSignatureError

from f1r3fly import token_amount
from f1r3fly.crypto import PrivateKey
from f1r3fly.data_types import BondInfo, DeployInfo
from f1r3fly.pb.DeployServiceCommon_pb2 import (
    BondInfo as BondInfoProto, DeployInfo as DeployInfoProto,
)
from f1r3fly.pos import BOND_RHO_TPL, render_contract_template
from f1r3fly.util import create_deploy_data, verify_deploy_data

BOUNDARIES = [0, 1, 2**63 - 1, 2**63, 2**128, token_amount.MAX]


@pytest.mark.parametrize('value', BOUNDARIES)
def test_amounts_round_trip_through_wire_and_decimal_forms(value: int) -> None:
    wire = token_amount.encode(value)
    assert len(wire) == token_amount.WIRE_BYTES
    assert token_amount.decode(wire) == value
    assert token_amount.parse(str(value)) == value


@pytest.mark.parametrize('value', [-1, token_amount.MAX + 1])
def test_out_of_range_amounts_are_rejected(value: int) -> None:
    with pytest.raises(ValueError):
        token_amount.encode(value)
    with pytest.raises(ValueError):
        token_amount.parse(str(value))


@pytest.mark.parametrize('value', [True, 1.0, '1'])
def test_non_integer_amounts_are_rejected(value: object) -> None:
    with pytest.raises(TypeError):
        token_amount.encode(value)  # type: ignore[arg-type]


@pytest.mark.parametrize('text', ['', '00', '01', '+1', '-0', '1.0', '1e2', ' 1', '1 ', '١'])
def test_non_canonical_decimals_are_rejected(text: str) -> None:
    with pytest.raises(ValueError):
        token_amount.parse(text)


@pytest.mark.parametrize('length', [0, 1, 31, 33])
def test_wrong_width_encodings_are_rejected(length: int) -> None:
    with pytest.raises(ValueError):
        token_amount.decode(bytes(length))


@pytest.mark.parametrize('price', [1, 2**128, token_amount.MAX])
def test_deploys_sign_the_32_byte_price(price: int) -> None:
    key = PrivateKey.generate()
    data = create_deploy_data(key, 'Nil', price, 1, 0, 1000, 'root')
    assert data.phloPrice == price.to_bytes(32, 'big')
    assert verify_deploy_data(key.get_public_key(), data.sig, data)
    data.phloPrice = (price - 1).to_bytes(32, 'big')
    with pytest.raises(BadSignatureError):
        verify_deploy_data(key.get_public_key(), data.sig, data)


def test_api_amount_strings_become_ints() -> None:
    bond = BondInfo.from_pb(BondInfoProto(validator='v', stake=str(token_amount.MAX)))
    assert bond.stake == token_amount.MAX
    deploy = DeployInfo.from_pb(DeployInfoProto(phloPrice=str(2**200)))
    assert deploy.phloPrice == 2**200


def test_bond_template_sends_a_bigint_literal() -> None:
    rendered = render_contract_template(BOND_RHO_TPL, {'amount': str(2**200)})
    assert f'{2**200}n, *retCh' in rendered
